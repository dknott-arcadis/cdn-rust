use anyhow::Result;
use azure_storage::prelude::StorageCredentials;
use azure_storage_blobs::prelude::ClientBuilder;
use bytes::Bytes;
use futures::channel::mpsc;
use futures::stream::StreamExt;
use futures::SinkExt;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, StreamBody};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{body::Frame, Request};
use hyper_util::rt::TokioIo;
use std::env;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;

const DEFAULT_AZURE_STORAGE_ACCOUNT: &str = "sacitdevdev01";

type ChannelPayload = azure_core::Result<Frame<Bytes>>;

async fn get_blob(mut tx: mpsc::Sender<ChannelPayload>) -> Result<()> {
    let account = env::var("AZURE_STORAGE_ACCOUNT").unwrap_or_else(|_| {
        println!(
            "AZURE_STORAGE_ACCOUNT not set, using default value of {}",
            DEFAULT_AZURE_STORAGE_ACCOUNT
        );
        DEFAULT_AZURE_STORAGE_ACCOUNT.to_owned()
    });

    let container = "test";
    let blob_name = "README.md";

    let storage_credentials = StorageCredentials::token_credential(get_azure_credentials());
    let blob_client =
        ClientBuilder::new(account, storage_credentials).blob_client(container, blob_name);

    blob_client.exists().await?;

    let mut pageable = blob_client.get().into_stream();
    while let Some(value) = pageable.next().await {
        let mut body = value?.data;
        while let Some(value) = body.next().await {
            let value = value;
            tx.send(value.map(|d| Frame::data(d))).await?;
        }
    }

    Ok(())
}

/// Creates a `DefaultAzureCredential` by default with default options.
/// If `AZURE_CREDENTIAL_KIND` environment variable is set, it creates a `SpecificAzureCredential` with default options
fn get_azure_credentials() -> Arc<dyn azure_core::auth::TokenCredential> {
    azure_identity::create_credential().expect("Unable to create Azure credentials!")
}

async fn try_get_blob(
    req: Request<hyper::body::Incoming>,
) -> Result<hyper::Response<BoxBody<bytes::Bytes, azure_core::Error>>> {
    let (tx, rx) = mpsc::channel::<ChannelPayload>(32);

    let handle: tokio::task::JoinHandle<Result<()>> = tokio::spawn(async move {
        get_blob(tx.clone()).await?;
        Ok(())
    });

    let blob_stream = StreamBody::new(rx);
    let response = hyper::Response::builder()
        .status(hyper::StatusCode::OK)
        .body(BodyExt::boxed(blob_stream))?;

    let child_result = handle.await?;
    child_result?;

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
        println!("Error: {:?}", err);
        bad_request()
    };
    Ok(res)
}

#[tokio::main]
async fn main() -> Result<()> {
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));

    // We create a TcpListener and bind it to 127.0.0.1:3000
    let listener = TcpListener::bind(addr).await?;

    // We start a loop to continuously accept incoming connections
    loop {
        let (stream, _) = listener.accept().await?;

        // Use an adapter to access something implementing `tokio::io` traits as if they implement
        // `hyper::rt` IO traits.
        let io = TokioIo::new(stream);

        // Spawn a tokio task to serve multiple connections concurrently
        tokio::task::spawn(async move {
            // Finally, we bind the incoming connection to our `hello` service
            if let Err(err) = http1::Builder::new()
                // `service_fn` converts our function in a `Service`
                .serve_connection(io, service_fn(proxy_request))
                .await
            {
                eprintln!("Error serving connection: {:?}", err);
            }
        });
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
