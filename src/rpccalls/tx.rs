extern crate jsonrpc_minihttp_server;

use ::errors;
use super::get_string;

use jsonrpc_minihttp_server::jsonrpc_core::*;
use storage::SqliteStorage;

use std::time::SystemTime;
use futures;

use ::kcoin::Bech32Address;
use ::kcoin::Network;
use ed25519_dalek::{PublicKey, Signature};
use sha2::{Sha512, Sha256, Digest};
use serde_derive;
use serde_json;
use std::vec::Vec;
use hex;

#[derive(Debug, Serialize)]
pub struct Transaction {
    amount: u64,
    coin: String,
    fee: u64,
    from: Bech32Address,
    memo: String,
    nonce: u64,
    to: Bech32Address
}

impl Transaction {
    pub fn signature_data(&self) -> Result<Vec<u8>> {
        let json = json!({
            "amount": self.amount,
            "coin": self.coin,
            "fee": self.fee,
            "from": self.from.address,
            "memo": self.memo,
            "nonce": self.nonce,
            "to": self.to.address
        });
        let str = serde_json::to_string(&json).map_err(|_| Error::internal_error())?;
        let bytes = str.as_bytes();
        let mut hasher = Sha256::default();
        hasher.input(bytes);
        let result = hasher.result();
        Ok(
            result.to_vec()
        )
    }
}

#[derive(Debug)]
pub struct TransactionEnvelope {
    hash: String,
    signature: String,
    tx: Transaction
}

impl TransactionEnvelope {
    pub fn from_json(json: serde_json::Map<String, Value>, network: &Network) -> Result<Self> {
        let hash = json
            .get("hash")
            .ok_or(Error::invalid_params("missing parameter `hash`"))?
            .as_str()
            .ok_or(Error::invalid_params("invalid parameter `hash`"))?
            .to_owned();
        let signature = json
            .get("signature")
            .ok_or(Error::invalid_params("missing parameter `signature`"))?
            .as_str()
            .ok_or(Error::invalid_params("invalid parameter `signature`"))?
            .to_owned();
        let tx = json
            .get("tx")
            .ok_or(Error::invalid_params("missing parameter `tx`"))?
            .as_object()
            .ok_or(Error::invalid_params("invalid parameter `tx`"))?;
        let amount = tx
            .get("amount")
            .ok_or(Error::invalid_params("missing parameter `tx.amount`"))?
            .as_u64()
            .ok_or(Error::invalid_params("invalid parameter `tx.amount`"))?;
        let coin = tx
            .get("coin")
            .ok_or(Error::invalid_params("missing parameter `tx.coin`"))?
            .as_str()
            .ok_or(Error::invalid_params("invalid parameter `tx.coin`"))?
            .to_owned();
        let fee = tx
            .get("fee")
            .ok_or(Error::invalid_params("missing parameter `tx.fee`"))?
            .as_u64()
            .ok_or(Error::invalid_params("invalid parameter `tx.fee`"))?;
        let from = Bech32Address::new(
            tx
            .get("from")
            .ok_or(Error::invalid_params("missing parameter `tx.from`"))?
            .as_str()
            .ok_or(Error::invalid_params("invalid parameter `tx.from`"))?,
            network.clone()
        ).map_err(|_| Error::invalid_params("invalid parameter `tx.from`"))?;
        let memo = tx
            .get("memo")
            .ok_or(Error::invalid_params("missing parameter `tx.memo`"))?
            .as_str()
            .ok_or(Error::invalid_params("invalid parameter `tx.memo`"))?
            .to_owned();
        let nonce = tx
            .get("nonce")
            .ok_or(Error::invalid_params("missing parameter `tx.nonce`"))?
            .as_u64()
            .ok_or(Error::invalid_params("invalid parameter `tx.nonce`"))?;
        let to = Bech32Address::new(
            tx
            .get("to")
            .ok_or(Error::invalid_params("missing parameter `tx.to`"))?
            .as_str()
            .ok_or(Error::invalid_params("invalid parameter `tx.to`"))?,
            network.clone()
        ).map_err(|_| Error::invalid_params("invalid parameter `tx.from`"))?;
        let envelope = TransactionEnvelope {
            hash: hash,
            signature: signature,
            tx: Transaction {
                amount,
                coin,
                fee,
                from,
                memo,
                nonce,
                to
            }
        };

        match envelope.verify() {
            true => Ok(envelope),
            false => Err(Error::invalid_params("invalid signature"))
        }
    }

    pub fn verify(&self) -> bool {
        let public_key = match self.tx.from.public_key() {
            Ok(t) => t,
            Err(_) => {
                println!("couldn't fetch pub key from from");
                return false;
            }
        };
        println!("public key {:?}", public_key);
        println!("signature str {:?}", self.signature);
        let signature_as_bytes = match hex::decode(&self.signature) {
            Ok(t) => t,
            Err(_) => {
                return false;
            }
        };
        println!("signature bytes {:?}", signature_as_bytes);
        let signature = match Signature::from_bytes(&signature_as_bytes) {
            Ok(t) => t,
            Err(e) => {
                println!("{:?}", e);
                return false;
            }
        };
        println!("signature parsed {:?}", signature);
        let signature_data = match self.tx.signature_data() {
            Ok(t) => t,
            Err(e) => {
                println!("{:?}", e);
                return false;
            }
        };
        println!("signature_data {:?} {}", signature_data, signature_data.len());
        match public_key.verify::<Sha512>(&signature_data, &signature) {
            Ok(_) => true,
            Err(e) => {
                println!("{:?}", e);
                false
            }
        }
    }
}

pub fn tx_send(storage: &SqliteStorage, network: &Network, params: serde_json::Map<String, Value>) -> Result<Value> {
    debug!("Received call to tx_send");
    println!("{:?}", params);
    let tx = TransactionEnvelope::from_json(params, network);
    println!("{:?}", tx);
    let result = json!({"hash": "abc".to_owned()});
    Ok(result)
}