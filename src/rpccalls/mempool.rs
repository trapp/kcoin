extern crate jsonrpc_minihttp_server;

use ::errors;

use jsonrpc_minihttp_server::jsonrpc_core::*;
use storage::SqliteStorage;
use storage;
use kcoin::Bech32Address;
use kcoin::Network;

pub fn mempool_get_stats(storage: &SqliteStorage, network: &Network) -> Result<Value> {
    debug!("Received call to mempool_getStats");

    let stats = storage.mempool_get_stats(network).map_err(internal_error)?;

    let result = json!({
        "count": stats.count,
        "min_fee": stats.min_fee,
        "max_fee": stats.max_fee,
        "avg_fee": stats.avg_fee
    });
    Ok(result)
}

pub fn mempool_get_transactions(storage: &SqliteStorage, network: &Network, params: serde_json::Map<String, Value>) -> Result<Value> {
    debug!("Received call to mempool_getTransactions");

    let after_seen = match params.get("after_seen") {
        Some(v) => Some(v.as_u64().ok_or(Error::invalid_params("invalid after_seen"))? as u32),
        None => None
    };

    let to = match params.get("to") {
        Some(v) => Some(Bech32Address::new(v.as_str().ok_or(Error::invalid_params("invalid to"))?, network.clone()).map_err(|_| Error::invalid_params("invalid to"))?),
        None => None
    };

    let from = match params.get("from") {
        Some(v) => Some(Bech32Address::new(v.as_str().ok_or(Error::invalid_params("invalid from"))?, network.clone()).map_err(|_| Error::invalid_params("invalid from"))?),
        None => None
    };

    let limit = match params.get("limit") {
        Some(v) => v.as_u64().ok_or(Error::invalid_params("invalid after_height"))? as u32,
        None => 100
    };

    let txs = storage.mempool_get_transactions(after_seen, from, to, limit, network).map_err(internal_error)?;

    let result = json!(txs);
    Ok(result)
}

pub fn mempool_get_transaction_by_hash(storage: &SqliteStorage, network: &Network, params: serde_json::Map<String, Value>) -> Result<Value> {
    debug!("Received call to mempool_getTransactionByHash");

    let hash = params
        .get("hash")
        .ok_or(Error::invalid_params("hash missing"))?
        .as_str()
        .ok_or(Error::invalid_params("invalid hash"))?;

    let tx = storage.mempool_get_transaction_by_hash(network, hash).map_err(|e| {
        match e {
            storage::Error::NotFound => errors::not_found(),
            _ => Error::internal_error()
        }
    })?;

    let result = json!(tx);
    Ok(result)
}

fn internal_error(e: storage::Error) -> Error {
    println!("internal error {:?}", e);
    Error::internal_error()
}
