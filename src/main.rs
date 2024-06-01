use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::Request;
use hyper_util::rt::TokioIo;
use std::net::SocketAddr;
use tokio::net::TcpListener;

async fn proxy_request(
    req: Request<hyper::body::Incoming>,
) -> Result<hyper::Response<reqwest::Body>, reqwest::Error> {
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
