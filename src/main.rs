use anyhow::Result;
use azure_storage::prelude::StorageCredentials;
use azure_storage_blobs::blob::BlobProperties;
use azure_storage_blobs::prelude::{BlobClient, ClientBuilder};
use bytes::Bytes;
use futures::channel::mpsc;
use futures::stream::StreamExt;
use futures::SinkExt;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, StreamBody};
use hyper::service::service_fn;
use hyper::{body::Frame, Request};
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto::Builder,
};
use opentelemetry_sdk::{
    metrics::{
        reader::{DefaultAggregationSelector, DefaultTemporalitySelector},
        Aggregation, MeterProviderBuilder, PeriodicReader, SdkMeterProvider, Stream,
    },
    runtime,
    trace::{BatchConfig, RandomIdGenerator, Sampler, Tracer},
    Resource,
};
use opentelemetry_semantic_conventions::{
    resource::{DEPLOYMENT_ENVIRONMENT, SERVICE_NAME, SERVICE_VERSION},
    SCHEMA_URL,
};
use std::env;
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tracing::{debug, info, span, Instrument, Level};

const DEFAULT_AZURE_STORAGE_ACCOUNT: &str = "sacitdevdev01";

type ChannelPayload = azure_core::Result<Frame<Bytes>>;

/// Creates a `DefaultAzureCredential` by default with default options.
/// If `AZURE_CREDENTIAL_KIND` environment variable is set, it creates a `SpecificAzureCredential` with default options
fn get_azure_credentials() -> &'static Arc<dyn azure_core::auth::TokenCredential> {
    static AZURE_CREDENTIALS: OnceLock<Arc<dyn azure_core::auth::TokenCredential>> =
        OnceLock::new();
    AZURE_CREDENTIALS.get_or_init(|| {
        azure_identity::create_credential().expect("Unable to create Azure credentials!")
    })
}

fn create_blob_client(container_name: &str, blob_name: &str) -> BlobClient {
    let account = env::var("AZURE_STORAGE_ACCOUNT").unwrap_or_else(|_| {
        debug!(
            "AZURE_STORAGE_ACCOUNT not set, using default value of {}",
            DEFAULT_AZURE_STORAGE_ACCOUNT
        );
        DEFAULT_AZURE_STORAGE_ACCOUNT.to_owned()
    });

    let storage_credentials = StorageCredentials::token_credential(get_azure_credentials().clone());
    ClientBuilder::new(account, storage_credentials).blob_client(container_name, blob_name)
}

#[tracing::instrument]
async fn get_blob_properties(container_name: &str, blob_name: &str) -> Result<BlobProperties> {
    let blob_client = create_blob_client(container_name, blob_name);
    tracing::trace!("Getting blob properties...");
    let blob_properties = blob_client.get_properties().await?;
    Ok(blob_properties.blob.properties)
}

#[tracing::instrument]
async fn get_blob(mut tx: mpsc::Sender<ChannelPayload>) -> Result<BlobProperties> {
    let container = "test";
    let blob_name = "README.md";

    let blob_client = create_blob_client(container, blob_name);
    let blob_properties = blob_client.get_properties();

    let mut pageable = blob_client.get().into_stream();
    while let Some(value) = pageable.next().await {
        let mut body = value?.data;
        while let Some(value) = body.next().await {
            let value = value;
            tx.send(value.map(|d| Frame::data(d))).await?;
        }
    }

    Ok(blob_properties.await?.blob.properties)
}

#[tracing::instrument]
async fn try_get_blob(
    req: Request<hyper::body::Incoming>,
) -> Result<hyper::Response<BoxBody<bytes::Bytes, azure_core::Error>>> {
    let mut path_parts = req.uri().path().split('/');
    let container_name = path_parts.next().unwrap_or("");
    if container_name.is_empty() {
        return Ok(not_found());
    }

    let blob_name = path_parts.collect::<Vec<&str>>().join("/");
    if blob_name.is_empty() {
        return Ok(not_found());
    }

    if req.headers().contains_key(hyper::header::IF_NONE_MATCH) {
        let blob_properties = get_blob_properties(container_name, &blob_name).await?;
        tracing::trace!("Blob properties: {:?}", blob_properties);
        let etag = req
            .headers()
            .get(hyper::header::IF_NONE_MATCH)
            .unwrap()
            .to_str()
            .unwrap();
        if etag == blob_properties.etag.to_string() {
            debug!("ETag match, returning 304 Not Modified");
            return Ok(not_modified());
        }
    }

    let (tx, rx) = mpsc::channel::<ChannelPayload>(32);

    let handle: tokio::task::JoinHandle<Result<BlobProperties>> = tokio::spawn(
        async move {
            let blob_properties = get_blob(tx.clone()).await?;
            Ok(blob_properties)
        }
        .in_current_span(),
    );

    let blob_stream = StreamBody::new(rx);
    let mut response = hyper::Response::builder()
        .status(hyper::StatusCode::OK)
        .body(BodyExt::boxed(blob_stream))?;

    let blob_properties = handle.await??;
    let response_headers = response.headers_mut();
    response_headers.insert(
        hyper::header::CONTENT_TYPE,
        blob_properties.content_type.parse()?,
    );
    response_headers.insert(
        hyper::header::ETAG,
        blob_properties.etag.to_string().parse()?,
    );
    // response_headers.insert(hyper::header::LAST_MODIFIED, blob_properties.last_modified.to_string().parse()?);

    if let Some(cache_control) = blob_properties.cache_control {
        response_headers.insert(hyper::header::CACHE_CONTROL, cache_control.parse()?);
    }

    response_headers.insert(
        hyper::header::CONTENT_LENGTH,
        blob_properties.content_length.to_string().parse()?,
    );

    Ok(response)
}

fn not_found() -> hyper::Response<BoxBody<Bytes, azure_core::Error>> {
    let mut res = hyper::Response::builder()
        .status(hyper::StatusCode::NOT_FOUND)
        .body(empty())
        .unwrap();

    res.headers_mut()
        .insert(hyper::header::CONTENT_LENGTH, "0".parse().unwrap());

    res
}

fn not_modified() -> hyper::Response<BoxBody<Bytes, azure_core::Error>> {
    let mut res = hyper::Response::builder()
        .status(hyper::StatusCode::NOT_MODIFIED)
        .body(empty())
        .unwrap();

    res.headers_mut()
        .insert(hyper::header::CONTENT_LENGTH, "0".parse().unwrap());

    res
}

fn healthy_response() -> hyper::Response<BoxBody<Bytes, azure_core::Error>> {
    let mut res = hyper::Response::builder()
        .status(hyper::StatusCode::OK)
        .body(empty())
        .unwrap();

    let headers = res.headers_mut();
    headers.insert(hyper::header::CONTENT_LENGTH, "0".parse().unwrap());
    headers.insert(hyper::header::CONTENT_TYPE, "text/plain".parse().unwrap());
    headers.insert(hyper::header::CACHE_CONTROL, "no-cache".parse().unwrap());

    res
}

fn empty() -> BoxBody<Bytes, azure_core::Error> {
    http_body_util::Empty::<Bytes>::new()
        .map_err(|never| match never {})
        .boxed()
}

fn bad_request() -> hyper::Response<BoxBody<Bytes, azure_core::Error>> {
    hyper::Response::builder()
        .status(hyper::StatusCode::BAD_REQUEST)
        .body(empty())
        .unwrap()
}

#[tracing::instrument]
async fn proxy_request(
    req: Request<hyper::body::Incoming>,
) -> Result<hyper::Response<BoxBody<Bytes, azure_core::Error>>> {
    let start = Instant::now();

    let res = match try_get_blob(req).await {
        Ok(res) => res,
        Err(e) => {
            tracing::error!("Error: {:?}", e);
            bad_request()
        }
    };

    let duration = start.elapsed();
    debug!("Request took: {:?}", duration);
    Ok(res)
}

#[tracing::instrument]
async fn request_router(
    req: Request<hyper::body::Incoming>,
) -> Result<hyper::Response<BoxBody<Bytes, azure_core::Error>>> {
    match (req.method(), req.uri().path()) {
        (&hyper::Method::GET, "/healthz") => Ok(healthy_response()),
        _ => proxy_request(req).await,
    }
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
    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    let server_span = span!(Level::TRACE, "server", %addr);
    info!("Listening on http://{addr}");
    let tcp_listener = TcpListener::bind(addr).await?;

    let mut join_set = tokio::task::JoinSet::new();
    loop {
        let (stream, addr) = match tcp_listener.accept().await {
            Ok(x) => x,
            Err(e) => {
                tracing::error!("failed to accept connection: {e}");
                continue;
            }
        };

        let serve_connection = async move {
            tracing::debug!("handling a request from {addr}");

            let result = Builder::new(TokioExecutor::new())
                .serve_connection(TokioIo::new(stream), service_fn(request_router))
                .await;

            if let Err(e) = result {
                tracing::error!("error serving {addr}: {e}");
            }

            tracing::trace!("handled a request from {addr}");
        };

        join_set.spawn(serve_connection.instrument(server_span.clone()));
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    use tracing_opentelemetry::OpenTelemetryLayer;
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
    tracing_subscriber::registry()
        .with(tracing_subscriber::filter::LevelFilter::from_level(
            Level::TRACE,
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
