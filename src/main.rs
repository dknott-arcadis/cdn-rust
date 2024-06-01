use azure_core::Pageable;
use azure_storage::prelude::StorageCredentials;
use azure_storage_blobs::blob::operations::GetBlobResponse;
use azure_storage_blobs::prelude::ClientBuilder;
use futures::stream::StreamExt;
use http_body_util::BodyExt;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, body::Body};
use hyper_util::rt::TokioIo;
use std::env;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;

const DEFAULT_AZURE_STORAGE_ACCOUNT: &str = "sacitdevcdn001";

async fn get_blob() -> Pageable<GetBlobResponse, azure_core::Error> {
    let account = env::var("AZURE_STORAGE_ACCOUNT").unwrap_or_else(|_| {
        println!(
            "AZURE_STORAGE_ACCOUNT not set, using default value of {}",
            DEFAULT_AZURE_STORAGE_ACCOUNT
        );
        DEFAULT_AZURE_STORAGE_ACCOUNT.to_owned()
    });

    let container = "";
    let blob_name = "";

    let storage_credentials = StorageCredentials::token_credential(get_azure_credentials());
    let blob_client =
        ClientBuilder::new(account, storage_credentials).blob_client(container, blob_name);

    blob_client.get().into_stream()
    // let mut stream = blob_client.get().into_stream();
    // while let Some(value) = stream.next().await {
    //     let mut body = value?.data;
    //     // For each response, we stream the body instead of collecting it all
    //     // into one large allocation.
    //     while let Some(value) = body.next().await {
    //         let value = value?;
    //         result.extend(&value);
    //     }
    // }
}

/// Creates a `DefaultAzureCredential` by default with default options.
/// If `AZURE_CREDENTIAL_KIND` environment variable is set, it creates a `SpecificAzureCredential` with default options
fn get_azure_credentials() -> Arc<dyn azure_core::auth::TokenCredential> {
    azure_identity::create_credential().expect("Unable to create Azure credentials!")
}

async fn proxy_request(
    req: Request<hyper::body::Incoming>,
) -> Result<hyper::Response<reqwest::Body>, reqwest::Error> {
    // let creds = get_azure_credentials();
    // let scopes: &[&str] = &["openid"];
    // let token = creds.get_token(scopes).await.unwrap();
    // println!("token: {:?}", token);
    let stream2 = get_blob().await.map(|p| p.unwrap().data);
    let body = http_body_util::StreamBody::new(stream2);
    // while let Some(value) = stream.next().await {
    //     let body = http_body_util::StreamBody::new(value.unwrap());
    // }

    let uri_string = format!("https://download.geofabrik.de{}", req.uri().path());
    let r = reqwest::get(uri_string).await?;
    let mut res: hyper::Response<reqwest::Body> = r.into();
    res.headers_mut().remove("server");
    Ok(res)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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
