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
use std::env;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{debug, info, span, Instrument, Level};

const DEFAULT_AZURE_STORAGE_ACCOUNT: &str = "sacitdevdev01";

type ChannelPayload = azure_core::Result<Frame<Bytes>>;

fn create_blob_client(container_name: &str, blob_name: &str) -> BlobClient {
    let account = env::var("AZURE_STORAGE_ACCOUNT").unwrap_or_else(|_| {
        debug!(
            "AZURE_STORAGE_ACCOUNT not set, using default value of {}",
            DEFAULT_AZURE_STORAGE_ACCOUNT
        );
        DEFAULT_AZURE_STORAGE_ACCOUNT.to_owned()
    });

    let storage_credentials = StorageCredentials::token_credential(get_azure_credentials());
    ClientBuilder::new(account, storage_credentials).blob_client(container_name, blob_name)
}

async fn get_blob_properties(container_name: &str, blob_name: &str) -> Result<BlobProperties> {
    let blob_client = create_blob_client(container_name, blob_name);
    tracing::trace!("Getting blob properties...");
    let blob_properties = blob_client.get_properties().await?;
    Ok(blob_properties.blob.properties)
}

async fn get_blob(mut tx: mpsc::Sender<ChannelPayload>) -> Result<BlobProperties> {
    let container = "test";
    let blob_name = "README.md";

    let blob_client = create_blob_client(container, blob_name);

    blob_client.exists().await?;
    let blob_properties = blob_client.get_properties().await?;

    let mut pageable = blob_client.get().into_stream();
    while let Some(value) = pageable.next().await {
        let mut body = value?.data;
        while let Some(value) = body.next().await {
            let value = value;
            tx.send(value.map(|d| Frame::data(d))).await?;
        }
    }

    Ok(blob_properties.blob.properties)
}

/// Creates a `DefaultAzureCredential` by default with default options.
/// If `AZURE_CREDENTIAL_KIND` environment variable is set, it creates a `SpecificAzureCredential` with default options
fn get_azure_credentials() -> Arc<dyn azure_core::auth::TokenCredential> {
    azure_identity::create_credential().expect("Unable to create Azure credentials!")
}

async fn try_get_blob(
    req: Request<hyper::body::Incoming>,
) -> Result<hyper::Response<BoxBody<bytes::Bytes, azure_core::Error>>> {
    if req.headers().contains_key(hyper::header::IF_NONE_MATCH) {
        let blob_properties = get_blob_properties("test", "README.md").await?;
        tracing::trace!("Blob properties: {:?}", blob_properties);
        let etag = req
            .headers()
            .get(hyper::header::IF_NONE_MATCH)
            .unwrap()
            .to_str()
            .unwrap();
        if etag == blob_properties.etag.to_string() {
            return Ok(hyper::Response::builder()
                .status(hyper::StatusCode::NOT_MODIFIED)
                .body(empty())
                .unwrap());
        }
    }

    let (tx, rx) = mpsc::channel::<ChannelPayload>(32);

    let handle: tokio::task::JoinHandle<Result<BlobProperties>> = tokio::spawn(async move {
        let blob_properties = get_blob(tx.clone()).await?;
        Ok(blob_properties)
    });

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

async fn proxy_request(
    req: Request<hyper::body::Incoming>,
) -> Result<hyper::Response<BoxBody<Bytes, azure_core::Error>>> {
    let blob_stream = try_get_blob(req).await;
    let res = if blob_stream.is_ok() {
        blob_stream.unwrap()
    } else {
        let err = blob_stream.unwrap_err();
        tracing::error!("Error: {:?}", err);
        bad_request()
    };
    Ok(res)
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(Level::TRACE)
        .init();

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    let server_span = span!(Level::TRACE, "server", %addr);
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
            tracing::trace!("handling a request from {addr}");

            let result = Builder::new(TokioExecutor::new())
                .serve_connection(TokioIo::new(stream), service_fn(proxy_request))
                .await;

            if let Err(e) = result {
                tracing::error!("error serving {addr}: {e}");
            }

            tracing::trace!("handled a request from {addr}");
        };

        join_set.spawn(serve_connection.instrument(server_span.clone()));
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
