extern crate jsonrpc_minihttp_server;

use ::errors;

use jsonrpc_minihttp_server::jsonrpc_core::*;
use storage::SqliteStorage;

use ::kcoin::Network;
use ::tx::TransactionEnvelope;
use ::storage;

const MEMPOOL_SIZE: u64 = 10;

pub fn tx_send(storage: &SqliteStorage, network: &Network, params: serde_json::Map<String, Value>) -> Result<Value> {
    debug!("Received call to tx_send");
    println!("{:?}", params);
    let tx = TransactionEnvelope::from_json(params, network).map_err(|e| {
        match e {
            ::tx::Error::InvalidField {field} => {
                Error::invalid_params(format!("invalid parameter {}", field))
            },
            ::tx::Error::MissingField {field} => {
                Error::invalid_params(format!("missing parameter {}", field))
            },
            _ => Error::internal_error()
        }
    })?;
    println!("{:?}", tx);

    if storage.mempool_exists(&tx.hash).map_err(internal_error)? == true {
        return Err(errors::tx_known());
    }

    let nonce_chain = storage.address_nonce_mined(&tx.tx.from).map_err(internal_error)?;
    println!("address chain nonce {:?}", nonce_chain);

    let nonce_mempool = storage.address_nonce_mempool(&tx.tx.from).map_err(internal_error)?;
    println!("address chain mempool {:?}", nonce_mempool);

    let last_nonce = nonce_mempool.or(nonce_chain);
    let next_nonce = match last_nonce {
        Some(n) => n + 1,
        None => 0
    };
    // mempool_min is the nonce at which the mempool starts for this address, inclusive
    let mempool_min = match nonce_chain {
        None => 0,
        Some(v) => v + 1
    };

    println!("last nonce {:?} next nonce {:?} mempool_min {:?}", last_nonce, next_nonce, mempool_min);

    // Deny when the new tx would leave a nonce gap
    if tx.tx.nonce > next_nonce {
        return Err(errors::nonce_gap());
    }

    if let Some(n) = nonce_chain {
        // Deny when the new tx has an already mined nonce
        if tx.tx.nonce <= n {
            return Err(errors::nonce_used());
        }
    }

    // Consider replace when nonce is the same as the one of a mempool tx
    if tx.tx.nonce >= mempool_min && tx.tx.nonce < next_nonce {
        // nonce matches an existing mempool tx. try to replace it if fee is higher.
        let current_tx = storage.mempool_get_by_nonce(&tx.tx.from, tx.tx.nonce, network).map_err(internal_error)?;
        if current_tx.tx.fee >= tx.tx.fee {
            return Err(errors::fee_too_low_to_replace());
        }
        // delete existing tx from mempool. adding the new one happens after this if statement.
        storage.mempool_remove(&current_tx.hash).map_err(internal_error)?;
        storage.mempool_add(&tx).map_err(internal_error)?;
        println!("replaced");
        return Ok(json!({}));
    }

    let mempool_count = storage.mempool_count().map_err(internal_error)?;
    println!("{:?}", mempool_count);
    if mempool_count >= MEMPOOL_SIZE {
        // Mempool is full. See if it's worth it to evict another tx for this one.
        // To calculate this, we take the sum of all fees per address currently in the
        // mempool and compare that to the new tx's fee. If the fee value of the new tx is
        // higher than the lowest fee-sum, the address with the lowest fee-sum gets evicted.
        // Evicting means we remove all transactions in the mempool for this address.
        let res = match storage.mempool_lowest_fee_sum(&tx.tx.from.address) {
            Ok(v) => Ok(Some(v)),
            Err(e) => {
                match e {
                    storage::Error::NotFound => Ok(None),
                    _ => Err(e)
                }
            }
        };

        match res.map_err(internal_error)? {
            Some((lowest_fee_sum, tx_count, address)) => {
                if tx.tx.fee > lowest_fee_sum {
                    println!("evicting {:?} transactions from {:?} from mempool for {:?}. old_amount={:?} new_amount={:?}", tx_count, address, tx.tx.from.address, lowest_fee_sum, tx.tx.fee);
                    storage.mempool_evict(&address).map_err(internal_error)?;
                } else {
                    println!("won't evict {:?} transactions from {:?} from mempool for {:?}. old_amount={:?} new_amount={:?}", tx_count, address, tx.tx.from.address, lowest_fee_sum, tx.tx.fee);
                    return Err(errors::mempool_full());
                }
            }
            None => {
                // Apparently the mempool is completely filled with transactions of this sender
                // himself. Since he can't evict his own lower-nonce transactions deny his new
                // transaction.
                return Err(errors::mempool_full_own_txs());
            }
        };
    }

    storage.mempool_add(&tx).map_err(internal_error)?;

    let result = json!({});
    Ok(result)
}

fn internal_error(e: storage::Error) -> Error {
    println!("internal error {:?}", e);
    Error::internal_error()
}
