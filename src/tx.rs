use ed25519_dalek::{Signature};
use sha2::{Sha512, Sha256, Digest};
use serde_json;
use serde_json::{Value, Map};
use ::kcoin::Bech32Address;
use ::kcoin::Network;
use time::Timespec;
use hex;
use time;

#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "internal error")]
    InternalError,
    #[fail(display = "invalid field {}", field)]
    InvalidField {
        field: String
    },
    #[fail(display = "missing field {}", field)]
    MissingField {
        field: String
    },
}

#[derive(Debug, Serialize)]
pub struct Transaction {
    pub amount: u64,
    pub coin: String,
    pub fee: u64,
    pub from: Bech32Address,
    pub memo: String,
    pub nonce: u64,
    pub to: Bech32Address,
}

impl Transaction {
    pub fn signature_data(&self) -> Result<Vec<u8>, Error> {
        let json = json!({
        "amount": self.amount,
        "coin": self.coin,
        "fee": self.fee,
        "from": self.from.address,
        "memo": self.memo,
        "nonce": self.nonce,
        "to": self.to.address
    });
        let str = serde_json::to_string(&json).map_err(|_| Error::InternalError)?;
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
    pub hash: String,
    pub signature: String,
    pub seen: Timespec,
    pub tx: Transaction
}

impl TransactionEnvelope {

    fn field_as_str<'a>(json: &'a Map<String, Value>, field: &str) -> Result<&'a str, Error> {
        json.get(field)
            .ok_or(Error::MissingField { field: field.to_owned() })?
            .as_str()
            .ok_or(Error::InvalidField { field: field.to_owned() })
    }

    fn field_as_object<'a>(json: &'a Map<String, Value>, field: &str) -> Result<&'a Map<String, Value>, Error> {
        json.get(field)
            .ok_or(Error::MissingField { field: field.to_owned() })?
            .as_object()
            .ok_or(Error::InvalidField { field: field.to_owned() })
    }

    fn field_as_u64(json: &Map<String, Value>, field: &str) -> Result<u64, Error> {
        json.get(field)
            .ok_or(Error::MissingField { field: field.to_owned() })?
            .as_u64()
            .ok_or(Error::InvalidField { field: field.to_owned() })
    }

    fn field_as_address(json: &Map<String, Value>, network: &Network, field: &str) -> Result<Bech32Address, Error> {
        Bech32Address::new(
            TransactionEnvelope::field_as_str(&json, field)?,
            network.clone(),
        ).map_err(|_| Error::InvalidField { field: field.to_owned() })
    }

    pub fn from_json(json: serde_json::Map<String, Value>, network: &Network) -> Result<Self, Error> {
        let hash = TransactionEnvelope::field_as_str(&json, "hash")?.to_owned();
        let signature = TransactionEnvelope::field_as_str(&json, "signature")?.to_owned();
        let tx = TransactionEnvelope::field_as_object(&json, "tx")?;
        let amount = TransactionEnvelope::field_as_u64(&tx, "amount")?;
        let coin = TransactionEnvelope::field_as_str(&tx, "coin")?.to_owned();
        let fee = TransactionEnvelope::field_as_u64(&tx, "fee")?;
        let from = TransactionEnvelope::field_as_address(&tx, network, "from")?;
        let memo = TransactionEnvelope::field_as_str(&tx, "memo")?.to_owned();
        let nonce = TransactionEnvelope::field_as_u64(&tx, "nonce")?;
        let to = TransactionEnvelope::field_as_address(&tx, network, "to")?;
        let envelope = TransactionEnvelope {
            hash: hash,
            signature: signature,
            seen: time::get_time(),
            tx: Transaction {
                amount,
                coin,
                fee,
                from,
                memo,
                nonce,
                to,
            },
        };

        match envelope.verify() {
            true => Ok(envelope),
            false => Err(Error::InvalidField {field: "signature".to_owned()})
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