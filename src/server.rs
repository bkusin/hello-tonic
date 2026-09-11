use std::sync::atomic::{AtomicU32, Ordering::Relaxed};
use std::sync:: Arc;
use std::collections::HashMap;
use std::pin::Pin;

use tonic::{transport::Server, Request, Response, Status};
use tokio::sync::{mpsc::*, RwLock};
use tokio_stream::wrappers::UnboundedReceiverStream;

use compute::worker_pool_server::{WorkerPool, WorkerPoolServer};
use compute::{Empty, WorkPayload, WorkResponse};

pub mod compute {
    tonic::include_proto!("compute"); 
}

// type TaskResult = Result<WorkPayload, Status>;
type TaskSender = UnboundedSender<Result<WorkPayload, Status>>;
// type ResultStream = Pin<Box<Stream<Item = WorkResult> + Send>>;

#[derive(Debug, Default)]
pub struct WorkerPoolManager {
    client_id: AtomicU32,
    clients: Arc<RwLock<HashMap<u32, TaskSender>>>,
}


impl WorkerPoolManager {
     fn assign_work(&self, payload: &str) {
     //   todo!("not implemented. Divide the work among the clients")

        /*
            in this example, the order of results received is not meaningful so just accumlate them as soon as we get them
         */

        let cloned_clients = Arc::clone(&self.clients);
        let payload = Arc::<str>::from(payload);

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                {
                    let lock = cloned_clients.read().await;
                    for sender in lock.values() {
                        if sender.send(Ok(WorkPayload{payload: payload.to_string()})).is_err() {
                        // TODO if send result is an error, drop the client from the map (will need write lock)
                        println!("Can't send task to client");
                        }
                    }
                }
            }
        });
    }
}

#[tonic::async_trait]
impl WorkerPool for WorkerPoolManager {
    type RegisterStream = UnboundedReceiverStream<Result<WorkPayload, Status>>;

    async fn register(&self, request: Request<Empty>) -> Result<Response<Self::RegisterStream>, Status> {
            
        let id = self.client_id.fetch_add(1, Relaxed);
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Result<WorkPayload, Status>>();

        let mut write_guard = self.clients.write().await;
        write_guard.insert(id, tx);

        let output_stream = UnboundedReceiverStream::new(rx);

        // TODO: Error handling if this stream is dropped

        println!("Registered client {}", id);

        Ok(Response::new(output_stream)) 
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:3000".parse()?;
    let manager = WorkerPoolManager::default();
    manager.assign_work("This is a test!");

    Server::builder()
        .add_service(WorkerPoolServer::new(manager))
        .serve(addr)
        .await?;

    Ok(())
}