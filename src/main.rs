use azure_core::Pageable;
use azure_storage::prelude::StorageCredentials;
use azure_storage_blobs::blob;
use azure_storage_blobs::blob::operations::GetBlobResponse;
use azure_storage_blobs::prelude::ClientBuilder;
use futures::channel::mpsc;
use futures::stream::{self, StreamExt};
use futures::{SinkExt, Stream, TryStreamExt};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, StreamBody};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{body::Body, Request, body::Frame};
use hyper_util::rt::TokioIo;
use std::env;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use bytes::Bytes;
use anyhow::Result;

const DEFAULT_AZURE_STORAGE_ACCOUNT: &str = "sacitdevdev01";

type BlobPageResponse = Frame<Bytes>;
type BlobResponseStream = StreamBody<azure_core::Result<BlobPageResponse>>;
type ByteStream = azure_core::Result<bytes::Bytes>;
type ChannelPayload = azure_core::Result<Frame<Bytes>>;

// fn get_page_data(page: azure_core::Result<GetBlobResponse>) -> BlobResponseStream {
//     let c = page.map(move |d| d.data).unwrap();
//     let c2: stream::Map<azure_core::ResponseBody, impl FnMut(Result<Bytes, azure_core::Error>) -> Frame<Bytes>> = c.map(|d| Frame::data(d.unwrap()));
//     // let c2 = c.map(|b| hyper::body::Frame::data(b.unwrap()));
//     http_body_util::StreamBody::new(c2)
//     // page.map(move |d| d.data).unwrap().map(|d| http_body_util::StreamBody::new(d))
// }

// fn get_blob_stream(pageable: Pageable<GetBlobResponse, azure_core::Error>) -> impl Stream<Item = ByteStream> {
//    pageable.flat_map(move |f| get_page_data(f))
// }

async fn get_blob(mut tx: futures::channel::mpsc::Sender<ChannelPayload>) -> Result<()> {
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

    
    
    // let stream_blob = move |(mut pages, page): (Pageable<GetBlobResponse, azure_core::Error>, Option<Result<GetBlobResponse, azure_core::Error>>)| {
    //     async move {
    //         let response_body = match page {
    //             Some(page_data) => Some(page_data.unwrap().data),
    //             None => match pages.next().await {
    //                 Some(page_data) => Some(page_data.unwrap().data),
    //                 None => None
    //             }
    //         };
    //         let chunk_data = match response_body {
    //             Some(mut page_data) => page_data.next().await,
    //             None => None
    //         };
            
    //         match chunk_data {
    //             Some(data) => Some((data, (pages, page, response_body))),
    //             None => None
    //         }

    //         // if page.is_some() {
    //         //     let mut body = page.unwrap().data;
    //         //     let next_chunk = body.next().await;
    //         // } else {
    //         //     None
    //         // }
    //     }
    // };

    // let get_page_contents: dyn Fn(Result<GetBlobResponse, azure_core::Error>) -> Result<azure_core::ResponseBody, azure_core::Error> = move |page: azure_core::Result<GetBlobResponse>| {
    //     page.map(|p| p.data)
    // };

    // let s = blob_client.get().into_stream().flat_map(|f| {
    //     get_page_data(f)
    // });

    let mut pageable = blob_client.get().into_stream();
    while let Some(value) = pageable.next().await {
        let body = value.map(|d| d.data).ok();
         match body {
            Some(mut body) => {
                while let Some(value) = body.next().await {
                    let value = value;
                    tx.send(value.map(|d| Frame::data(d))).await?;
                }
            },
            None => ()
        };
    }

    Ok(())

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

async fn try_get_blob(
    req: Request<hyper::body::Incoming>,
) -> Result<hyper::Response<http_body_util::combinators::BoxBody<bytes::Bytes, azure_core::Error>>> {
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

fn bad_request() -> hyper::Response<BoxBody<bytes::Bytes, azure_core::Error>> {
    hyper::Response::builder()
        .status(hyper::StatusCode::BAD_REQUEST)
        .body(empty())
        .unwrap()
}

async fn proxy_request(req: Request<hyper::body::Incoming>) -> Result<hyper::Response<http_body_util::combinators::BoxBody<bytes::Bytes, azure_core::Error>>> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_env() {
        env::var("AZURE_CREDENTIAL_KIND")
            .expect("Unable to get environment variable AZURE_CREDENTIAL_KIND");
    }

    #[tokio::test]
    async fn get_azure_credentials_test() -> azure_core::Result<()> {
        let creds = get_azure_credentials();
        let scopes = &["https://management.azure.com/.default"];
        let _token = creds.get_token(scopes).await.expect("Error getting token");
        Ok(())
    }
}
