extern crate jsonrpc_minihttp_server;

use block;
use storage::SqliteStorage;
use kcoin::Network;
use storage;
use jsonrpc_minihttp_server::jsonrpc_core::*;

pub fn regtest_generate(storage: &SqliteStorage, block_size: u64, network: &Network) -> Result<Value> {
    debug!("Received call to regtest_generate");
    block::generate(storage, block_size, network).map_err(internal_error)?;
    let ok = json!({});
    Ok(ok)
}

fn internal_error(e: storage::Error) -> Error {
    println!("internal error {:?}", e);
    Error::internal_error()
}
