extern crate jsonrpc_minihttp_server;

use ::errors;

use jsonrpc_minihttp_server::jsonrpc_core::*;
use storage::SqliteStorage;
use storage;
use kcoin::Bech32Address;
use kcoin::Network;

pub fn chain_height(storage: &SqliteStorage) -> Result<Value> {
    debug!("Received call to chain_height");

    let height = storage.block_height().map_err(|_| {
        errors::no_block_found()
    })?;
    let result = json!({"height": height});
    Ok(result)
}

pub fn chain_get_block_by_height(storage: &SqliteStorage, params: serde_json::Map<String, Value>, network: &Network) -> Result<Value> {
    debug!("Received call to chain_block");

    let height = params
        .get("height")
        .ok_or(Error::invalid_params("height missing"))?
        .as_u64()
        .ok_or(Error::invalid_params("invalid height"))?;

    let block = storage.block_get_by_height(height as u32).map_err(|e| {
        match e {
            storage::Error::NotFound => errors::not_found(),
            _ => Error::internal_error()
        }
    })?;

    let txs = storage.chain_get_transactions(Some(height as u32), None, None, None, 10000, network).map_err(internal_error)?;

    let result = json!({
        "height": block.height,
        "hash": block.hash,
        "time": block.time,
        "txs": txs
    });
    Ok(result)
}

pub fn chain_get_transactions(storage: &SqliteStorage, network: &Network, params: serde_json::Map<String, Value>) -> Result<Value> {
    debug!("Received call to chain_getTransactions");

    let height = match params.get("height") {
        Some(v) => Some(v.as_u64().ok_or(Error::invalid_params("invalid height"))? as u32),
        None => None
    };

    let after_height = match params.get("after_height") {
        Some(v) => Some(v.as_u64().ok_or(Error::invalid_params("invalid after_height"))? as u32),
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
        Some(v) => v.as_u64().ok_or(Error::invalid_params("invalid limit"))? as u32,
        None => 100
    };

    let txs = storage.chain_get_transactions(height, after_height, from, to, limit, network).map_err(internal_error)?;

    let result = json!(txs);
    Ok(result)
}

pub fn chain_get_transaction_by_hash(storage: &SqliteStorage, network: &Network, params: serde_json::Map<String, Value>) -> Result<Value> {
    debug!("Received call to chain_getTransactionByHash");

    let hash = params
        .get("hash")
        .ok_or(Error::invalid_params("hash missing"))?
        .as_str()
        .ok_or(Error::invalid_params("invalid hash"))?;

    let tx = storage.chain_get_transaction_by_hash(network, hash).map_err(|e| {
        match e {
            storage::Error::NotFound => errors::not_found(),
            _ => Error::internal_error()
        }
    })?;

    let result = json!(tx);
    Ok(result)
}

pub fn chain_address_info(storage: &SqliteStorage, params: serde_json::Map<String, Value>, network: &Network) -> Result<Value> {
    debug!("Received call to chain_addressInfo");

    let address = Bech32Address::new(
        params
        .get("address")
        .ok_or(Error::invalid_params("address missing"))?
        .as_str()
        .ok_or(Error::invalid_params("invalid address"))?,
        network.clone()
    ).map_err(|_| Error::invalid_params("invalid address"))?;

    let nonce_mined = storage.address_nonce_mined(&address).map_err(|_| {
        Error::internal_error()
    })?;
    let nonce_mempool = storage.address_nonce_mempool(&address).map_err(|_| {
        Error::internal_error()
    })?;
    let last_nonce = nonce_mempool.or(nonce_mined);
    let next_nonce = match last_nonce {
        Some(n) => n + 1,
        None => 0
    };

    let balances = storage.address_get_balances(&address).map_err(|_| {
        Error::internal_error()
    })?;
    let mut balance_result = serde_json::Map::new();
    for balance in balances.iter() {
        balance_result.insert(balance.coin.clone(), json!(balance.balance));
    }

    let reserved_balances = storage.address_get_reserved_balances(&address).map_err(|_| {
        Error::internal_error()
    })?;
    let mut reserved_result = serde_json::Map::new();
    for balance in reserved_balances.iter() {
        reserved_result.insert(balance.coin.clone(), json!(balance.balance));
    }

    Ok(json!({
        "next_nonce": next_nonce,
        "balances": balance_result,
        "reserved_balances": reserved_result
    }))
}

fn internal_error(e: storage::Error) -> Error {
    println!("internal error {:?}", e);
    Error::internal_error()
}
