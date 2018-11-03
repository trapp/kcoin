extern crate jsonrpc_minihttp_server;

use ::errors;

use jsonrpc_minihttp_server::jsonrpc_core::*;
use storage::SqliteStorage;

pub fn chain_height(storage: &SqliteStorage) -> Result<Value> {
    debug!("Received call to chain_height");

    let height = storage.block_height().map_err(|_| {
        errors::no_block_found()
    })?;
    let result = json!({"height": height});
    Ok(result)
}