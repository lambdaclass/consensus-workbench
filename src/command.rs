use clap::Parser;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[derive(Parser)]
#[clap()]
pub enum Command {
    Set { key: String, value: String },
    Get { key: String },
}
