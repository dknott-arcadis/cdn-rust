use anyhow::Result;
use axum::{
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, HeaderName, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use bytes::Bytes;
use opentelemetry_sdk::{
    runtime,
    trace::{BatchConfig, RandomIdGenerator, Sampler, Tracer},
    Resource,
};
use opentelemetry_semantic_conventions::{
    resource::{DEPLOYMENT_ENVIRONMENT, SERVICE_NAME, SERVICE_VERSION},
    SCHEMA_URL,
};
use reqwest::Client;
use std::{env, time::Duration};
use std::sync::{Arc, OnceLock};
use tracing::{debug, Level, Span};

const DEFAULT_AZURE_STORAGE_ACCOUNT: &str = "sacitdevdev01";

/// Creates a `DefaultAzureCredential` by default with default options.
/// If `AZURE_CREDENTIAL_KIND` environment variable is set, it creates a `SpecificAzureCredential` with default options
fn get_azure_credentials() -> &'static Arc<dyn azure_core::auth::TokenCredential> {
    static AZURE_CREDENTIALS: OnceLock<Arc<dyn azure_core::auth::TokenCredential>> =
        OnceLock::new();
    AZURE_CREDENTIALS.get_or_init(|| {
        azure_identity::create_credential().expect("Unable to create Azure credentials!")
    })
}

#[tracing::instrument]
async fn blob_request(client: Client, container: &str, blob: &str) -> Result<reqwest::Response> {
    let account = env::var("AZURE_STORAGE_ACCOUNT").unwrap_or_else(|_| {
        debug!(
            "AZURE_STORAGE_ACCOUNT not set, using default value of {}",
            DEFAULT_AZURE_STORAGE_ACCOUNT
        );
        DEFAULT_AZURE_STORAGE_ACCOUNT.to_owned()
    });

    let creds = get_azure_credentials().clone();
    let token = creds
        .get_token(&["https://storage.azure.com/.default"])
        .await?;
    debug!("Token: {:?}", token.token.secret());
    let response = client
        .get(format!(
            "https://{account}.blob.core.windows.net/{container}/{blob}"
        ))
        .bearer_auth(token.token.secret())
        .header("x-ms-version", "2024-05-04")
        .send()
        .await?;
    Ok(response)
}

#[tracing::instrument]
async fn proxy_request(Path((container, blob)): Path<(String, String)>, State(client): State<Client>) -> Response {
    debug!("Container: {:?}", container);
    debug!("Blob: {:?}", blob);

    let reqwest_response = match blob_request(client, &container, &blob).await {
        Ok(res) => res,
        Err(err) => {
            tracing::error!(%err, "request failed");
            return (StatusCode::BAD_REQUEST, Body::empty()).into_response();
        }
    };

    let response_builder =
        axum::response::Response::builder().status(reqwest_response.status().as_u16());

    // Here the mapping of headers is required due to reqwest and axum differ on the http crate versions
    let mut headers = HeaderMap::with_capacity(reqwest_response.headers().len());
    headers.extend(reqwest_response.headers().into_iter().map(|(name, value)| {
        let name = HeaderName::from_bytes(name.as_ref()).unwrap();
        let value = HeaderValue::from_bytes(value.as_ref()).unwrap();
        (name, value)
    }));

    response_builder
        .body(Body::from_stream(reqwest_response.bytes_stream()))
        // This unwrap is fine because the body is empty here
        .unwrap()
}

fn resource() -> Resource {
    use opentelemetry::KeyValue;

    Resource::from_schema_url(
        [
            KeyValue::new(SERVICE_NAME, env!("CARGO_PKG_NAME")),
            KeyValue::new(SERVICE_VERSION, env!("CARGO_PKG_VERSION")),
            KeyValue::new(DEPLOYMENT_ENVIRONMENT, "develop"),
        ],
        SCHEMA_URL,
    )
}

fn init_tracer() -> Tracer {
    opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_trace_config(
            opentelemetry_sdk::trace::Config::default()
                // Customize sampling strategy
                .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
                    1.0,
                ))))
                // If export trace to AWS X-Ray, you can use XrayIdGenerator
                .with_id_generator(RandomIdGenerator::default())
                .with_resource(resource()),
        )
        .with_batch_config(BatchConfig::default())
        .with_exporter(opentelemetry_otlp::new_exporter().tonic())
        .install_batch(runtime::Tokio)
        .unwrap()
}

async fn start_server() -> Result<()> {
    use tower_http::trace::TraceLayer;

    let client = Client::new();
    let app = Router::new()
        .route("/healthz", get(|| async { "OK" }))
        .route("/:container/*blob", get(proxy_request))
        .layer(TraceLayer::new_for_http().on_body_chunk(
            |chunk: &Bytes, _latency: Duration, _span: &Span| {
                tracing::debug!("streaming {} bytes", chunk.len());
            },
        ))
        .with_state(client);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
    debug!("listening on {}", listener.local_addr()?);
    axum::serve(listener, app).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    use tracing_opentelemetry::OpenTelemetryLayer;
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
    tracing_subscriber::registry()
        .with(tracing_subscriber::filter::LevelFilter::from_level(
            Level::DEBUG,
        ))
        .with(tracing_subscriber::fmt::layer())
        .with(OpenTelemetryLayer::new(init_tracer()))
        .init();

    match start_server().await {
        Ok(_) => Ok(()),
        Err(e) => {
            tracing::error!("Error: {:?}", e);
            Err(e)
        }
    }
}

// Make our own error that wraps `anyhow::Error`.
struct AppError(anyhow::Error);

// Tell axum how to convert `AppError` into a response.
impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Something went wrong: {}", self.0),
        )
            .into_response()
    }
}

// This enables using `?` on functions that return `Result<_, anyhow::Error>` to turn them into
// `Result<_, AppError>`. That way you don't need to do that manually.
impl<E> From<E> for AppError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self(err.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_env() {
        env::var("AZURE_CREDENTIAL_KIND")
            .expect("Unable to get environment variable AZURE_CREDENTIAL_KIND");
    }

    #[tokio::test]
    async fn get_azure_credentials_test() -> Result<()> {
        let creds = get_azure_credentials();
        let scopes = &["https://management.azure.com/.default"];
        let _token = creds.get_token(scopes).await.expect("Error getting token");
        Ok(())
    }
}
