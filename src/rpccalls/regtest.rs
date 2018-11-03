extern crate jsonrpc_minihttp_server;

use jsonrpc_minihttp_server::jsonrpc_core::*;

pub fn regtest_generate(params: serde_json::Map<String, Value>) -> Result<Value> {
    debug!("Received call to regtest_generate");
    let result = json!({});
    Ok(result)
}