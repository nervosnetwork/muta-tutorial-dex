use serde::{Deserialize, Serialize};

use protocol::types::Hash;

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct StorePayload {
    pub message: String,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct StoreResponse {
    pub id: Hash,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct GetContentPayload {
    pub id: Hash,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct GetContentResponse {
    pub message: String,
}
