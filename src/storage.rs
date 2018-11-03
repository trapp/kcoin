use std::fs;
use std::path::Path;
use rusqlite::{NO_PARAMS};
use r2d2_sqlite::SqliteConnectionManager;
use r2d2::{Pool, PooledConnection};
use std::time::{SystemTime};
use std::collections::HashMap;
use kcoin::Bech32Address;
use rusqlite;
use rusqlite::Row;
use ::tx::{TransactionEnvelope, Transaction};
use ::kcoin::Network;
use std::convert::From;
use time::Timespec;

#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "cannot open db")]
    CannotOpenDb,
    #[fail(display = "cannot claim a db connection")]
    CannotClaimDbConnection,
    #[fail(display = "datadir not writeable")]
    DataDirNotWriteable,
    #[fail(display = "cannot create db schema: {}", message)]
    CannotCreateSchema {
        message: String
    },
    #[fail(display = "cannot query db: {}", message)]
    QueryError {
        message: String
    },
    #[fail(display = "internal error")]
    InternalError,
    #[fail(display = "not found")]
    NotFound
}

impl From<rusqlite::Error> for Error {
    fn from(error: rusqlite::Error) -> Self {
        match error {
            rusqlite::Error::QueryReturnedNoRows => Error::NotFound,
            _ => Error::QueryError {message: error.to_string()}
        }
    }
}

impl From<::kcoin::KCoinError> for Error {
    fn from(error: ::kcoin::KCoinError) -> Self {
        Error::QueryError {message: error.to_string()}
    }
}

pub struct SqliteStorage {
    pool: Pool<SqliteConnectionManager>
}

impl SqliteStorage {
    pub fn new(dir: Option<&Path>, knc_address: Bech32Address, knc_supply: u64) -> Result<Self, Error> {
        let manager = match dir {
            Some(s) => {
                if !s.exists() {
                    println!("{:?}", s);
                    fs::create_dir(&s).map_err(|_| Error::DataDirNotWriteable) ?;
                }
                let mut db_file_name = s.to_path_buf();
                db_file_name.push("db.sqlite3");
                let db_path = db_file_name.as_path();

                SqliteConnectionManager::file(db_path)
            },
            None => {
                SqliteConnectionManager::memory()
            }
        };
        let pool = Pool::new(manager).map_err(|_| Error::CannotOpenDb)?;
        let conn = pool.get().map_err(|_| Error::CannotOpenDb)?;
        // create schema
        conn.execute_batch(
            "BEGIN;
                CREATE TABLE IF NOT EXISTS `block` (`height` INTEGER, `hash` TEXT, `time` INTEGER);
                CREATE UNIQUE INDEX IF NOT EXISTS `block_height` ON `block`(`height`);
                CREATE UNIQUE INDEX IF NOT EXISTS `block_hash` ON `block`(`hash`);
                CREATE INDEX IF NOT EXISTS `block_time` ON `block`(`time`);

                CREATE TABLE IF NOT EXISTS `transaction` (`hash` TEXT, `signature` TEXT, `block` INTEGER, `seen` INTEGER, `from` TEXT, `to` TEXT, `coin` TEXT, `amount` BIGINT, `nonce` BIGINT, `fee` BIGINT, `memo` TEXT);
                CREATE UNIQUE INDEX IF NOT EXISTS `tx_hash` ON `transaction`(`hash`);
                CREATE INDEX IF NOT EXISTS `tx_block` ON `transaction`(`block`);
                CREATE INDEX IF NOT EXISTS `tx_from` ON `transaction`(`from`);
                CREATE INDEX IF NOT EXISTS `tx_to` ON `transaction`(`to`);
                CREATE INDEX IF NOT EXISTS `tx_coin` ON `transaction`(`coin`);
                CREATE INDEX IF NOT EXISTS `tx_amount` ON `transaction`(`amount`);
                CREATE UNIQUE INDEX IF NOT EXISTS `tx_from_nonce` ON `transaction`(`from`, `nonce`);
                CREATE INDEX IF NOT EXISTS `tx_fee` ON `transaction`(`fee`);

                CREATE TABLE IF NOT EXISTS `mempool` (`hash` TEXT, `signature` TEXT, `seen` INTEGER, `from` TEXT, `to` TEXT, `coin` TEXT, `amount` BIGINT, `nonce` BIGINT, `fee` BIGINT, `memo` TEXT);
                CREATE UNIQUE INDEX IF NOT EXISTS `mempool_hash` ON `mempool`(`hash`);
                CREATE INDEX IF NOT EXISTS `mempool_from` ON `mempool`(`from`);
                CREATE INDEX IF NOT EXISTS `mempool_to` ON `mempool`(`to`);
                CREATE INDEX IF NOT EXISTS `mempool_coin` ON `mempool`(`coin`);
                CREATE INDEX IF NOT EXISTS `mempool_amount` ON `mempool`(`amount`);
                CREATE INDEX IF NOT EXISTS `mempool_seen` ON `mempool`(`seen`);
                CREATE UNIQUE INDEX IF NOT EXISTS `mempool_from_nonce` ON `mempool`(`from`, `nonce`);
                CREATE INDEX IF NOT EXISTS `mempool_fee` ON `mempool`(`fee`);

                CREATE TABLE IF NOT EXISTS `address_balance` (`address` TEXT, `coin` TEXT, `balance` BIGINT);
                CREATE UNIQUE INDEX IF NOT EXISTS `address_balance_address_coin` ON `address_balance`(`address`, `coin`);
                COMMIT;",
        ).map_err(|e| Error::CannotCreateSchema{message: e.to_string()})?;

        let mut stmt = conn
            .prepare("SELECT count(*) FROM `address_balance` WHERE `coin` = 'KCN'")
            .map_err(|e| Error::QueryError {message: e.to_string()})?;

        let count: u32 = stmt
            .query_row(NO_PARAMS, |row| row.get(0))
            .map_err(|e| Error::QueryError {message: e.to_string()})?;

        if count == 0 {
            conn.execute("INSERT INTO address_balance (`address`, `coin`, `balance`)
                  VALUES (?1, ?2, ?3)",
                     &[knc_address.address, "KCN".to_owned(), (knc_supply * 100000000).to_string()]).unwrap();
        }

        Ok(SqliteStorage {pool})
    }

    pub fn get_conn(&self) -> Result<PooledConnection<SqliteConnectionManager>, Error> {
        self.pool.get().map_err(|_| Error::CannotClaimDbConnection)
    }

    pub fn clone(&self) -> Self {
        SqliteStorage {
            pool: self.pool.clone()
        }
    }

    pub fn block_height(&self) -> Result<u32, Error> {
        let conn = self.pool.get().map_err(|_| Error::CannotOpenDb)?;
        let mut stmt = conn
            .prepare("SELECT `height` FROM `block` ORDER BY height DESC LIMIT 1")
            .map_err(|e| Error::QueryError {message: e.to_string()})?;

        let height: u32 = stmt
            .query_row(NO_PARAMS, |row| row.get(0))
            .map_err(|e| Error::QueryError {message: e.to_string()})?;
        Ok(height)
    }

    pub fn address_nonce_mined(&self, address: &Bech32Address) -> Result<Option<u64>, Error> {
        let conn = self.get_conn()?;
        let mut stmt = conn
            .prepare("SELECT `nonce` FROM `transaction` WHERE `from` = ?1 ORDER BY nonce DESC LIMIT 1")
            .map_err(|e| Error::QueryError {message: e.to_string()})?;

        let nonce: Option<i64> = match stmt
            .query_row(vec![&address.address], |row| row.get(0)) {
            Ok(v) => Ok(Some(v)),
            Err(e) => {
                match e {
                    rusqlite::Error::QueryReturnedNoRows => Ok(None),
                    _ => Err(Error::QueryError {message: e.to_string()})
                }
            }
        }?;

        match nonce {
            Some(v) => Ok(Some(SqliteStorage::i64_to_u64(v)?)),
            None => Ok(None)
        }
    }

    pub fn address_nonce_mempool(&self, address: &Bech32Address) -> Result<Option<u64>, Error> {
        let conn = self.get_conn()?;
        let mut stmt = conn
            .prepare("SELECT `nonce` FROM `mempool` WHERE `from` = ?1 ORDER BY nonce DESC LIMIT 1")
            .map_err(|e| Error::QueryError {message: e.to_string()})?;

        let nonce: Option<i64> = match stmt
            .query_row(vec![&address.address], |row| row.get(0)) {
            Ok(v) => Ok(Some(v)),
            Err(e) => {
                match e {
                    rusqlite::Error::QueryReturnedNoRows => Ok(None),
                    _ => Err(Error::QueryError {message: e.to_string()})
                }
            }
        }?;

        match nonce {
            Some(v) => Ok(Some(SqliteStorage::i64_to_u64(v)?)),
            None => Ok(None)
        }
    }

    pub fn mempool_remove(&self, hash: &str) -> Result<(), Error> {
        let conn = self.get_conn()?;
        conn.execute(
            "DELETE FROM `mempool` WHERE `hash` = ?1",
            &[
                hash
            ],
        ).map_err(|e| {
            println!("{:?}", e);
            Error::QueryError {message: "delete failed".to_owned()}
        })?;
        Ok(())
    }

    pub fn mempool_evict(&self, address: &str) -> Result<(), Error> {
        let conn = self.get_conn()?;
        conn.execute(
            "DELETE FROM `mempool` WHERE `from` = ?1",
            &[
                address
            ],
        ).map_err(|e| {
            println!("{:?}", e);
            Error::QueryError {message: "delete failed".to_owned()}
        })?;
        Ok(())
    }

    pub fn mempool_add(&self, transaction: &TransactionEnvelope) -> Result<(), Error> {
        let conn = self.get_conn()?;
        let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).map_err(|_| Error::InternalError)?.as_secs();
        conn.execute(
            "INSERT INTO mempool (`amount`, `coin`, `fee`, `from`, `hash`, `nonce`, `memo`, `seen`, `signature`, `to`)
                  VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            &[
                &transaction.tx.amount.to_string(),
                &transaction.tx.coin,
                &transaction.tx.fee.to_string(),
                &transaction.tx.from.address,
                &transaction.hash,
                &transaction.tx.nonce.to_string(),
                &transaction.tx.memo,
                &now.to_string(),
                &transaction.signature,
                &transaction.tx.to.address
            ],
        ).map_err(|e| {
            println!("{:?}", e);
            Error::QueryError {message: "insert failed".to_owned()}
        })?;
        Ok(())
    }

    pub fn mempool_count(&self) -> Result<u64, Error> {
        let conn = self.get_conn()?;

        let count : u64 = conn
            .query_row_and_then(
                "SELECT count(*) FROM `mempool`",
                NO_PARAMS,
                |row| {
                    SqliteStorage::i64_to_u64(row.get_checked(0)?)
                })?;

        Ok(count)
    }

    pub fn mempool_lowest_fee_sum(&self, exclude_address: &str) -> Result<(u64, u64, String), Error> {
        let conn = self.get_conn()?;
        let res: Result<(u64, u64, String), Error> = conn
            .query_row_and_then(
                "SELECT sum(`fee`) as total_fee,\
                             count(*) as total_count,\
                             `from`\
                      FROM `mempool`\
                      WHERE `from` <> ?1\
                      GROUP BY `from`\
                      ORDER BY total_fee ASC LIMIT 1",
                &[exclude_address],
                |row| {
                    Ok((
                        SqliteStorage::i64_to_u64(row.get_checked(0)?)?,
                        SqliteStorage::i64_to_u64(row.get_checked(1)?)?,
                        row.get_checked(2)?,
                    ))
                });

        res
    }

    pub fn mempool_get_by_nonce(&self, from: &Bech32Address, nonce: u64, network: &Network) -> Result<TransactionEnvelope, Error> {
        let conn = self.get_conn()?;

        let tx = conn
            .query_row_and_then(
                "SELECT `amount`, `coin`, `fee`, `from`, `hash`, `memo`, `nonce`, `seen`, `signature`, `to` FROM `mempool` WHERE `from` = ?1 AND `nonce` = ?2 LIMIT 1",
                &[&from.address, &nonce.to_string()],
                |row| {
                SqliteStorage::tx_from_row(row, network)
            })?;

        Ok(tx)
    }

    fn tx_from_row(row: &Row, network: &Network) -> Result<TransactionEnvelope, Error> {
        let from: String = row.get_checked(3)?;
        let to: String = row.get_checked(9)?;
        let memo: String = row.get_checked(5)?;
        Ok(TransactionEnvelope {
            hash: row.get_checked(4)?,
            signature: row.get_checked(8)?,
            seen: Timespec::new(row.get_checked(7)?, 0),
            tx: Transaction {
                amount: SqliteStorage::i64_to_u64(row.get_checked(0)?)?,
                coin: row.get_checked(1)?,
                fee: SqliteStorage::i64_to_u64(row.get_checked(2)?)?,
                memo,
                from: Bech32Address::new(&from, network.clone())?,
                to: Bech32Address::new(&to, network.clone())?,
                nonce: SqliteStorage::i64_to_u64(row.get_checked(6)?)?
            }
        })
    }

    pub fn mempool_exists(&self, hash: &str) -> Result<bool, Error> {
        let conn = self.get_conn()?;
        let mut stmt = conn
            .prepare("SELECT count(*) FROM `mempool` WHERE `hash` = ?1 LIMIT 1")
            .map_err(|e| Error::QueryError {message: e.to_string()})?;

        let result: u32 = stmt
            .query_row(&[hash], |row| {
                row.get(0)
            })
            .map_err(|e| {
                Error::QueryError {message: e.to_string()}
            })?;

        Ok(result == 1)
    }

    fn i64_to_u64(num: i64) -> Result<u64, Error> {
        if num < 0 {
            Err(Error::QueryError {message: "Invalid number found".to_owned()})
        } else {
            Ok(num as u64)
        }
    }
}
