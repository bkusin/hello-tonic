use std::sync::atomic::{AtomicU32, Ordering::Relaxed};
use std::collections::HashMap;

use tonic::{transport::Server, Request, Response, Status};
use tokio::sync::{mpsc, RwLock};

use compute::worker_pool_server::{WorkerPool, WorkerPoolServer};
use compute::{EmptyMessage, RegisterRequest, WorkPayload, WorkResult};

pub mod hello_world {
    tonic::include_proto!("compute"); 
}

type TaskSender = UnboundedSender<Result<WorkPayload, Status>>;

#[derive(Debug, Default)]
pub struct WorkerPoolManager {
    client_id: AtomicU32,
    clients: Arc<RwLock<HashMap<AtomicU32, UnboundedSender<TaskSender>>>>,
}

#[tonic::async_trait]
impl WorkerPool for WorkerPoolManager {
    async fn register(&self, request: Request<Empty>) -> Result<Response<Empty>, Status> {
        
        let id = self.client_id.fetch_add(1, Relaxed);
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<String>();

        let write_guard = self.clients.write().await;
        write_guard.insert(id, tx);

        Ok(Response::new(Empty)) 
    }

    async fn assign_work(&self, payload: &str) {
        todo!("not implemented. Divide the work among the clients")

        /*
            in this example, the order of results received is not meaningful so just accumlate them as soon as we get them
         */
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:3000".parse()?;
    let manager = WorkerPoolManager::default();

    Server::builder()
        .add_service(WorkerPoolServer::new(manager))
        .serve(addr)
        .await?;

    Ok(())
}