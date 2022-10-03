use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum KeyValueCommand {
    Set { key: String, value: String },
    Get { key: String },
}
