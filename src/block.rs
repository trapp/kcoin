
use storage::Error;
use storage::SqliteStorage;
use kcoin::Network;
use time::Timespec;
use tx::{MinedTx, TransactionEnvelope};
use time;
use sha2::{Sha256, Digest};
use std::vec::Vec;
use hex;
use rusqlite::Connection;

#[derive(Debug)]
pub struct Block {
    pub height: u32,
    pub hash: String,
    pub time: Timespec
}

pub fn generate(storage: &SqliteStorage, network: &Network) -> Result<(), Error> {
    let conn = storage.get_conn()?;
    println!("gen block");
    let txs = storage.mempool_get_block_candidates(network)?;

    if txs.len() == 0 {
        // No transactions available. No need for a block.
        return Ok(());
    }

    // We have some txs, let's make a block.
    // They are already sorted to maximize the fee revenue.
    // We just need to update the balances accordingly.
    println!("found {:?} pending transactions. will craft block", txs.len());
    storage.start_transaction(&conn)?;
    match generate_with_conn(storage, &conn, &txs, network) {
        Ok(_) => {
            storage.commit_transaction(&conn)?;
            Ok(())
        },
        Err(e) => {
            storage.rollback_transaction(&conn)?;
            Err(e)
        }
    }
}

fn generate_with_conn(storage: &SqliteStorage, conn: &Connection, txs: &Vec<TransactionEnvelope>, network: &Network) -> Result<(), Error> {
    let height = storage.block_height()? + 1;

    let mut hash_result: Vec<u8> = Vec::new();
    for (i, tx) in txs.iter().enumerate() {
        println!("{:?}", tx);
        storage.transaction_insert_with_conn(&conn, height, i as u32, tx)?;
        storage.mempool_remove_with_conn(&conn, &tx.hash);
        hash_result.extend(hex::decode(&tx.hash).map_err(|e| Error::InternalError)?);
        hash_result = hash(&hash_result);
        println!("hash step {:?}", hex::encode(&hash_result));
    }

    // sanity balance check
    storage.balance_sanity_check_with_conn(&conn)?;

    let time = time::get_time();
    let addition_json = json!({
        "height": height,
        "time": time.sec
    });

    println!("addition {:?}", addition_json.to_string());
    let addition_hash = hash(addition_json.to_string().as_bytes());
    println!("addition hash {:?}", hex::encode(&addition_hash));

    hash_result.extend(&addition_hash);
    hash_result = hash(&hash_result);

    let hash = hex::encode(hash_result);
    let block = Block {height: height, hash: hash, time};
    println!("autocommit before adding block {:?}", conn.is_autocommit());
    storage.block_add_with_conn(&conn, &block)?;
    println!("block added {:?}", block);
    Ok(())
}

fn hash(bytes: &[u8]) -> Vec<u8> {
    let mut hasher = Sha256::default();
    hasher.input(bytes);
    hasher.result().to_vec()
}
