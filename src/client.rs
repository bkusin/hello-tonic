use compute::worker_pool_client::WorkerPoolClient;
use compute::Empty;

pub mod compute {
    tonic::include_proto!("compute");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = WorkerPoolClient::connect("http://[::1]:3000").await?;

    let request = tonic::Request::new(Empty {} );

    let response = client.register(request).await?;

    println!("RESPONSE={:?}", response);

    Ok(())
}