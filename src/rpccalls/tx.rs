extern crate jsonrpc_minihttp_server;

use ::errors;

use jsonrpc_minihttp_server::jsonrpc_core::*;
use storage::SqliteStorage;

use kcoin;
use ::kcoin::Network;
use ::tx::TransactionEnvelope;
use ::storage;

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

    let coin_exists = storage.coin_exists(&tx.tx.coin).map_err(internal_error)?;
    println!("coin exists {:?}", coin_exists);
    if !coin_exists && tx.tx.fee < kcoin::NEW_COIN_FEE {
        return Err(errors::fee_too_low());
    }

    let address_balance = match storage.address_get_balance(&tx.tx.from.address, &tx.tx.coin).map_err(internal_error)? {
        Some(v) => v,
        None => 0
    };
    println!("address balance {:?}", address_balance);

    let reserved_balance = match storage.address_get_reserved_balance(&tx.tx.from.address, &tx.tx.coin).map_err(internal_error)? {
        Some(v) => v,
        None => 0
    };
    println!("reserved balance {:?}", reserved_balance);

    let knc_address_balance = match tx.tx.coin.as_ref() {
        "KCN" => address_balance,
        _ => {
            match storage.address_get_balance(&tx.tx.from.address, "KCN").map_err(internal_error)? {
                Some(v) => v,
                None => 0
            }
        }
    };
    println!("knc address balance {:?}", knc_address_balance);

    let knc_reserved_balance = match tx.tx.coin.as_ref() {
        "KCN" => reserved_balance,
        _ => {
            match storage.address_get_reserved_balance(&tx.tx.from.address, "KCN").map_err(internal_error)? {
                Some(v) => v,
                None => 0
            }
        }
    };
    println!("knc reserved balance {:?}", knc_reserved_balance);

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

        // check if he has enough balance if we replace the tx with the new one.
        if (&tx.tx.coin == "KCN" && address_balance < reserved_balance - current_tx.tx.fee - current_tx.tx.amount + tx.tx.fee + tx.tx.amount)
                || (coin_exists && &tx.tx.coin != "KCN" && address_balance < reserved_balance - current_tx.tx.amount + tx.tx.amount
                    || knc_address_balance < knc_reserved_balance - current_tx.tx.fee + tx.tx.fee
                )
             {
            return Err(errors::insufficient_balance());
        }

        // delete existing tx from mempool. adding the new one happens after this if statement.
        storage.mempool_remove(&current_tx.hash).map_err(internal_error)?;
        storage.mempool_add(&tx).map_err(internal_error)?;
        println!("replaced");
        return Ok(json!({}));
    }

    let mempool_count = storage.mempool_count().map_err(internal_error)?;
    println!("mempool count {:?}", mempool_count);
    if mempool_count >= ::kcoin::MEMPOOL_SIZE.into() {
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
                // himself. Since he can't evict his own lower-nonce transactions without creating
                // a gap, deny his new transaction.
                return Err(errors::mempool_full_own_txs());
            }
        };
    }

    // check balance
    if (&tx.tx.coin == "KCN" && address_balance < reserved_balance + tx.tx.fee + tx.tx.amount)
        || (coin_exists && &tx.tx.coin != "KCN" && address_balance < reserved_balance + tx.tx.amount
                || knc_address_balance < knc_reserved_balance + tx.tx.fee
            )
         {
        return Err(errors::insufficient_balance());
    }

    println!("balance comp {:?} {:?}", &tx.tx.coin == "KCN" && address_balance < reserved_balance + tx.tx.fee + tx.tx.amount, &tx.tx.coin != "KCN" && address_balance < reserved_balance + tx.tx.amount
        && knc_address_balance < knc_reserved_balance + tx.tx.fee);

    storage.mempool_add(&tx).map_err(internal_error)?;

    let result = json!({});
    Ok(result)
}

fn internal_error(e: storage::Error) -> Error {
    println!("internal error {:?}", e);
    Error::internal_error()
}
