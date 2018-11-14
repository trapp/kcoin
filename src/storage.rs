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
use tx::{TransactionEnvelope, Transaction, MinedTx};
use block::Block;
use ::kcoin::Network;
use std::convert::From;
use time::Timespec;

#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "cannot open db")]
    CannotOpenDb,
    #[fail(display = "cannot start transaction {}", message)]
    CannotStartTransaction {
        message: String
    },
    #[fail(display = "cannot commit transaction {}", message)]
    CannotCommitTransaction {
        message: String
    },
    #[fail(display = "cannot rollback transaction {}", message)]
    CannotRollbackTransaction {
        message: String
    },
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
        println!("converting {:?}", error);
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
    pool: Pool<SqliteConnectionManager>,
    knc_address: Bech32Address
}

pub struct Balance {
    pub coin: String,
    pub balance: u64
}

pub struct MempoolStats {
    pub count: u32,
    pub min_fee: u32,
    pub max_fee: u32,
    pub avg_fee: u32
}

impl SqliteStorage {
    pub fn new(dir: &Path, regtest: bool, knc_address: Bech32Address, knc_supply: u64) -> Result<Self, Error> {
        let manager = match regtest {
            false => {
                if !dir.exists() {
                    println!("{:?}", dir);
                    fs::create_dir(&dir).map_err(|_| Error::DataDirNotWriteable) ?;
                }
                let mut db_file_name = dir.to_path_buf();
                db_file_name.push("db.sqlite3");
                let db_path = db_file_name.as_path();

                SqliteConnectionManager::file(db_path)
            },
            true => {
                if !dir.exists() {
                    println!("{:?}", dir);
                    fs::create_dir(&dir).map_err(|_| Error::DataDirNotWriteable) ?;
                }
                let mut db_file_name = dir.to_path_buf();
                db_file_name.push("db-regtest.sqlite3");
                let db_path = db_file_name.as_path();

                SqliteConnectionManager::file(db_path)
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

                CREATE TABLE IF NOT EXISTS `transaction` (`hash` TEXT, `signature` TEXT, `block` INTEGER, `index` INTEGER, `seen` INTEGER, `from` TEXT, `to` TEXT, `coin` TEXT, `amount` BIGINT, `nonce` BIGINT, `fee` BIGINT, `memo` TEXT);
                CREATE UNIQUE INDEX IF NOT EXISTS `tx_hash` ON `transaction`(`hash`);
                CREATE INDEX IF NOT EXISTS `tx_block` ON `transaction`(`block`);
                CREATE INDEX IF NOT EXISTS `tx_index` ON `transaction`(`index`);
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
                     &[&knc_address.address, "KCN", &(knc_supply * 100000000).to_string()]).unwrap();
        }

        Ok(SqliteStorage {pool, knc_address: knc_address})
    }

    pub fn get_conn(&self) -> Result<PooledConnection<SqliteConnectionManager>, Error> {
        self.pool.get().map_err(|_| Error::CannotClaimDbConnection)
    }

    pub fn clone(&self) -> Self {
        SqliteStorage {
            pool: self.pool.clone(),
            knc_address: self.knc_address.clone()
        }
    }

    pub fn block_height(&self) -> Result<u32, Error> {
        let conn = self.get_conn()?;
        let mut stmt = conn
            .prepare("SELECT `height` FROM `block` ORDER BY height DESC LIMIT 1")
            .map_err(|e| Error::QueryError {message: e.to_string()})?;

        let height: u32 = match stmt.query_row(NO_PARAMS, |row| row.get(0)) {
            Ok(v) => v,
            Err(e) => {
                match e {
                    rusqlite::Error::QueryReturnedNoRows => 0,
                    _ => return Err(Error::QueryError {message: e.to_string()})
                }
            }
        };
        Ok(height)
    }

    pub fn block_get_by_height(&self, height: u32) -> Result<Block, Error> {
        let conn = self.get_conn()?;
        let mut stmt = conn
            .prepare("SELECT `height`, `hash`, `time` FROM `block` WHERE `height` = ?1 LIMIT 1")
            .map_err(|e| Error::QueryError {message: e.to_string()})?;

        let block = conn.query_row_and_then(
            "SELECT `height`, `hash`, `time` FROM `block` WHERE `height` = ?1 LIMIT 1",
            &[height],
            |row| -> Result<Block, Error> {
                Ok(Block {
                    height: row.get_checked(0)?,
                    hash: row.get_checked(1)?,
                    time: row.get_checked(2)?
                })
            }
        )?;

        Ok(block)
    }

    pub fn chain_get_transactions(&self, height: Option<u32>, after_height: Option<u32>, from: Option<Bech32Address>, to: Option<Bech32Address>, limit: u32, network: &Network) -> Result<Vec<MinedTx>, Error> {
        let conn = self.get_conn()?;

        // `hash` TEXT, `signature` TEXT, `block` INTEGER, `index` INTEGER, `seen` INTEGER,
        // `from` TEXT, `to` TEXT, `coin` TEXT, `amount` BIGINT, `nonce` BIGINT, `fee` BIGINT,
        // `memo` TEXT

        let mut whereVec = Vec::new();
        let mut params = Vec::new();
        params.push(limit.to_string());
        match height {
            Some(v) => {
                let q = format!("`block` = ?{}", whereVec.len() + 2);
                whereVec.push(q);
                params.push(v.to_string());
            },
            None => {}
        }

        match from {
            Some(v) => {
                let q = format!("`from` = ?{}", whereVec.len() + 2);
                whereVec.push(q);
                params.push(v.address);
            },
            None => {}
        }

        match to {
            Some(v) => {
                let q = format!("`to` = ?{}", whereVec.len() + 2);
                whereVec.push(q);
                params.push(v.address);
            },
            None => {}
        }

        match after_height {
            Some(v) => {
                let q = format!("`block` > ?{}", whereVec.len() + 2);
                whereVec.push(q);
                params.push(v.to_string());
            },
            None => {}
        }

        if whereVec.len() == 0 {
            whereVec.push("1".to_owned());
        }

        // "SELECT `amount`, `coin`, `fee`, `from`, `hash`, `memo`, `nonce`, `seen`, `signature`, `to` FROM `mempool` WHERE `from` = ?1 AND `nonce` = ?2 LIMIT 1",

        let query = format!("SELECT `amount`, `coin`, `fee`, `from`, `hash`, `memo`, `nonce`, `seen`, `signature`, `to`, `block`, `index` \
                  FROM `transaction` \
                  WHERE {} \
                  ORDER BY `index` ASC \
                  LIMIT ?1", whereVec.join(" AND "));

        let mut stmt = conn
            .prepare(&query)
            .map_err(|e| Error::QueryError {message: e.to_string()})?;

        let rows = stmt
            .query_and_then(
                &params,
                |row| -> Result<MinedTx, Error> {
                    Ok(MinedTx {
                        block: row.get_checked(10)?,
                        index: row.get_checked(11)?,
                        tx_envelope: SqliteStorage::tx_from_row(row, network)?
                    })
                })?;

        let mut txs = Vec::new();
        for tx in rows {
            txs.push(tx?);
        }
        Ok(txs)
    }

    pub fn mempool_get_transactions(&self, after_seen: Option<u32>, from: Option<Bech32Address>, to: Option<Bech32Address>, limit: u32, network: &Network) -> Result<Vec<TransactionEnvelope>, Error> {
        let conn = self.get_conn()?;

        let mut whereVec = Vec::new();
        let mut params = Vec::new();
        params.push(limit.to_string());

        match from {
            Some(v) => {
                let q = format!("`from` = ?{}", whereVec.len() + 2);
                whereVec.push(q);
                params.push(v.address);
            },
            None => {}
        }

        match to {
            Some(v) => {
                let q = format!("`to` = ?{}", whereVec.len() + 2);
                whereVec.push(q);
                params.push(v.address);
            },
            None => {}
        }

        match after_seen {
            Some(v) => {
                let q = format!("`seen` > ?{}", whereVec.len() + 2);
                whereVec.push(q);
                params.push(v.to_string());
            },
            None => {}
        }

        if whereVec.len() == 0 {
            whereVec.push("1".to_owned());
        }

        let query = format!("SELECT `amount`, `coin`, `fee`, `from`, `hash`, `memo`, `nonce`, `seen`, `signature`, `to` \
                  FROM `mempool` \
                  WHERE {} \
                  ORDER BY `seen` ASC \
                  LIMIT ?1", whereVec.join(" AND "));

        let mut stmt = conn
            .prepare(&query)
            .map_err(|e| Error::QueryError {message: e.to_string()})?;

        let rows = stmt
            .query_and_then(
                &params,
                |row| -> Result<TransactionEnvelope, Error> {
                    Ok(SqliteStorage::tx_from_row(row, network)?)
                })?;

        let mut txs = Vec::new();
        for tx in rows {
            txs.push(tx?);
        }
        Ok(txs)
    }

    pub fn chain_get_transaction_by_hash(&self, network: &Network, hash: &str) -> Result<MinedTx, Error> {
        let conn = self.get_conn()?;

        let query = format!("SELECT `amount`, `coin`, `fee`, `from`, `hash`, `memo`, `nonce`, `seen`, `signature`, `to`, `block`, `index` \
                  FROM `transaction` \
                  WHERE hash = ?");

        let mut stmt = conn
            .prepare(&query)
            .map_err(|e| Error::QueryError {message: e.to_string()})?;

        let rows = stmt
            .query_and_then(
                &[hash],
                |row| -> Result<MinedTx, Error> {
                    Ok(MinedTx {
                        block: row.get_checked(10)?,
                        index: row.get_checked(11)?,
                        tx_envelope: SqliteStorage::tx_from_row(row, network)?
                    })
                })?;

        for tx in rows {
            return Ok(tx?);
        }
        Err(Error::NotFound)
    }

    pub fn mempool_get_transaction_by_hash(&self, network: &Network, hash: &str) -> Result<TransactionEnvelope, Error> {
        let conn = self.get_conn()?;

        let query = format!("SELECT `amount`, `coin`, `fee`, `from`, `hash`, `memo`, `nonce`, `seen`, `signature`, `to` \
                  FROM `mempool` \
                  WHERE hash = ?");

        let mut stmt = conn
            .prepare(&query)
            .map_err(|e| Error::QueryError {message: e.to_string()})?;

        let rows = stmt
            .query_and_then(
                &[hash],
                |row| -> Result<TransactionEnvelope, Error> {
                    Ok(SqliteStorage::tx_from_row(row, network)?)
                })?;

        for tx in rows {
            return Ok(tx?);
        }
        Err(Error::NotFound)
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

    pub fn mempool_remove_with_conn(&self, conn: &rusqlite::Connection, hash: &str) -> Result<(), Error> {
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
            seen: row.get_checked(7)?,
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

    pub fn balance_sanity_check_with_conn(&self, conn: &rusqlite::Connection) -> Result<(), Error> {
        let mut stmt = conn
            .prepare("SELECT count(*) FROM `address_balance` WHERE `balance` < 0 LIMIT 1")
            .map_err(|e| Error::QueryError {message: e.to_string()})?;

        let result: u32 = stmt
            .query_row(NO_PARAMS, |row| {
                row.get(0)
            })
            .map_err(|e| {
                Error::QueryError {message: e.to_string()}
            })?;

        if result > 0 {
            Err(Error::InternalError)
        } else {
            Ok(())
        }
    }

    pub fn mempool_get_block_candidates(&self, block_size: u64, network: &Network) -> Result<Vec<TransactionEnvelope>, Error> {
        let conn = self.get_conn()?;
        let mut stmt = conn
            .prepare("SELECT `amount`, `coin`, `fee`, `from`, `hash`, `memo`, `nonce`, `seen`, `signature`, `to` \
                      FROM `mempool` m \
                      ORDER BY (nonce - ifnull((select nonce from `transaction` t where m.`from` = t.`from` order by nonce desc limit 1), 0)) ASC, fee DESC \
                      LIMIT ?1")
            .map_err(|e| Error::QueryError {message: e.to_string()})?;

        //select * from mempool m order by (nonce - ifnull((select nonce from `transaction` t where m."from" = t."from" order by nonce desc limit 1), 0)) ASC, fee desc;
        let rows = stmt
            .query_and_then(
                &[block_size as u32],
                |row| {
                    SqliteStorage::tx_from_row(row, network)
                })?;

        let mut txs = Vec::new();
        for tx in rows {
            txs.push(tx?);
        }
        Ok(txs)
    }

    pub fn mempool_get_stats(&self, network: &Network) -> Result<MempoolStats, Error> {
        let conn = self.get_conn()?;

        conn
            .query_row_and_then(
                "SELECT COUNT(*), IFNULL(MIN(fee), 0), IFNULL(MAX(fee),0), IFNULL(AVG(fee),0) FROM `mempool` m",
                NO_PARAMS,
                |row| {
                    Ok(MempoolStats {
                        count: row.get_checked(0)?,
                        min_fee: row.get_checked(1)?,
                        max_fee: row.get_checked(2)?,
                        avg_fee: row.get_checked(3)?
                    })
                })
    }

    pub fn start_transaction(&self, conn: &rusqlite::Connection) -> Result<(), Error> {
        println!("start tx {:?}", conn.is_autocommit());
        let res = conn.execute_batch("BEGIN DEFERRED").map_err(|e| Error::CannotStartTransaction { message: e.to_string() });
        println!("start tx {:?}", conn.is_autocommit());
        res
    }

    pub fn commit_transaction(&self, conn: &rusqlite::Connection) -> Result<(), Error> {
        conn.execute_batch("COMMIT").map_err(|e| Error::CannotCommitTransaction { message: e.to_string() })
    }

    pub fn rollback_transaction(&self, conn: &rusqlite::Connection) -> Result<(), Error> {
        conn.execute_batch("ROLLBACK").map_err(|e| Error::CannotRollbackTransaction { message: e.to_string() })
    }

    pub fn transaction_insert_with_conn(&self, conn: &rusqlite::Connection, block: u32, index: u32, transaction: &TransactionEnvelope) -> Result<(), Error> {
        // `hash` TEXT, `signature` TEXT, `block` INTEGER, `seen` INTEGER, `from` TEXT, `to` TEXT,
        //`coin` TEXT, `amount` BIGINT, `nonce` BIGINT, `fee` BIGINT, `memo` TEXT);

        println!("inserting tx {:?}", transaction);
        println!("autocommit {:?}", conn.is_autocommit());
        let tx_inserted_rows = conn.execute(
            "INSERT INTO `transaction` (`hash`, `signature`, `block`, `index`, `seen`, `from`, `to`, `coin`, `amount`, `nonce`, `fee`, `memo`)
                  VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            &[
                &transaction.hash,
                &transaction.signature,
                &block.to_string(),
                &index.to_string(),
                &transaction.seen.to_string(),
                &transaction.tx.from.address.to_string(),
                &transaction.tx.to.address.to_string(),
                &transaction.tx.coin.to_string(),
                &transaction.tx.amount.to_string(),
                &transaction.tx.nonce.to_string(),
                &transaction.tx.fee.to_string(),
                &transaction.tx.memo.to_string()
            ],
        ).map_err(|e| {
            println!("{:?}", e);
            Error::QueryError {message: "insert tx failed".to_owned()}
        })?;
        if tx_inserted_rows == 0 {
            return Err(Error::QueryError {message: "unable to insert tx".to_owned()});
        }

        println!("autocommit after inserting tx {:?}", conn.is_autocommit());

        // Deduct fee from balance of sender
        let fee_from_changed = conn.execute(
            "UPDATE `address_balance` SET `balance` = `balance` - ?1 WHERE `address` = ?2 AND `coin` = 'KCN'",
            &[
                &(transaction.tx.fee).to_string(),
                &transaction.tx.from.address.to_string()
            ],
        ).map_err(|e| {
            println!("{:?}", e);
            Error::QueryError {message: "unable to deduct fee from senders balance".to_owned()}
        })?;
        if fee_from_changed == 0 {
            return Err(Error::QueryError {message: "unable to deduct fee from senders balance".to_owned()});
        }

        // Deduct amount from balance of sender
        let from_changed = conn.execute(
            "UPDATE `address_balance` SET `balance` = `balance` - ?1 WHERE `address` = ?2 AND `coin` = ?3",
            &[
                &(transaction.tx.amount).to_string(),
                &transaction.tx.from.address.to_string(),
                &transaction.tx.coin.to_string()
            ],
        ).map_err(|e| {
            println!("{:?}", e);
            Error::QueryError {message: "insert tx failed".to_owned()}
        })?;
        if from_changed == 0 && self.coin_exists_in_chain_with_conn(&conn, &transaction.tx.coin)? {
            return Err(Error::QueryError {message: "sender has not enough balance".to_owned()});
        }

        println!("autocommit after updating sender balance {:?}", conn.is_autocommit());

        // Add amount to receivers balance
        match self.address_get_balance_with_conn(&conn, &transaction.tx.to.address, &transaction.tx.coin)? {
            Some(_) => {
                let to_changed = conn.execute(
                    "UPDATE `address_balance` SET `balance` = `balance` + ?1 WHERE `address` = ?2 AND `coin` = ?3",
                    &[
                        &transaction.tx.amount.to_string(),
                        &transaction.tx.to.address.to_string(),
                        &transaction.tx.coin.to_string()
                    ],
                ).map_err(|e| {
                    println!("{:?}", e);
                    Error::QueryError {message: "insert tx failed".to_owned()}
                })?;

                if to_changed == 0 {
                    return Err(Error::QueryError {message: "Unable to update receiver balance".to_owned()});
                }
            },
            None => {
                let to_changed = conn.execute(
                    "INSERT INTO `address_balance` (`address`, `coin`, `balance`) VALUES (?1, ?2, ?3)",
                    &[
                        &transaction.tx.to.address,
                        &transaction.tx.coin,
                        &transaction.tx.amount.to_string()
                    ],
                ).map_err(|e| {
                    println!("{:?}", e);
                    Error::QueryError {message: "insert receiver balance failed".to_owned()}
                })?;

                if to_changed == 0 {
                    return Err(Error::QueryError {message: "Unable to insert receiver balance".to_owned()});
                }
            }
        }

        println!("autocommit after updating receiver balance {:?}", conn.is_autocommit());

        let master_changed = conn.execute(
            "UPDATE `address_balance` SET `balance` = `balance` + ?1 WHERE `address` = ?2 AND `coin` = 'KCN'",
            &[
                &transaction.tx.fee.to_string(),
                &self.knc_address.address
            ],
        ).map_err(|e| {
            println!("{:?}", e);
            Error::QueryError {message: "update balance for master failed".to_owned()}
        })?;

        if master_changed == 0 {
            return Err(Error::QueryError {message: "Unable to update kcn owner balance".to_owned()});
        }

        println!("autocommit after updating master balance {:?}", conn.is_autocommit());

        Ok(())
    }

    pub fn address_get_balance(&self, address: &str, coin: &str) -> Result<Option<u64>, Error> {
        let conn = self.get_conn()?;
        self.address_get_balance_with_conn(&conn, address, coin)
    }

    pub fn address_get_balance_with_conn(&self, conn: &rusqlite::Connection, address: &str, coin: &str) -> Result<Option<u64>, Error> {
        match conn
            .query_row_and_then(
                "SELECT balance FROM `address_balance` where `address` = ?1 and `coin` = ?2",
                &[address, coin],
                |row| {
                    SqliteStorage::i64_to_u64(row.get_checked(0)?)
                }) {
            Ok(v) => Ok(Some(v)),
            Err(e) => {
                match e {
                    Error::NotFound => Ok(None),
                    _ => Err(Error::QueryError {message: e.to_string()})
                }
            }
        }
    }

    pub fn address_get_balances(&self, address: &Bech32Address) -> Result<Vec<Balance>, Error> {
        let conn = self.get_conn()?;

        let mut stmt = conn
            .prepare("SELECT coin, balance FROM `address_balance` where `address` = ?1")
            .map_err(|e| Error::QueryError {message: e.to_string()})?;

        let rows = stmt
            .query_and_then(
                &[&address.address],
                |row| -> Result<Balance, Error> {
                    Ok(Balance {
                        coin: row.get_checked(0)?,
                        balance: SqliteStorage::i64_to_u64(row.get_checked(1)?)?
                    })
                })?;

        let mut results = Vec::new();
        for result in rows {
            results.push(result?);
        }
        Ok(results)
    }

    pub fn address_get_reserved_balances(&self, address: &Bech32Address) -> Result<Vec<Balance>, Error> {
        let conn = self.get_conn()?;

        let mut stmt = conn
            .prepare("SELECT `coin`, SUM(`amount`) + SUM(`fee`) as `reserved` FROM `mempool` where `from` = ?1 GROUP BY `coin`")
            .map_err(|e| Error::QueryError {message: e.to_string()})?;

        let rows = stmt
            .query_and_then(
                &[&address.address],
                |row| -> Result<Balance, Error> {
                    Ok(Balance {
                        coin: row.get_checked(0)?,
                        balance: SqliteStorage::i64_to_u64(row.get_checked(1)?)?
                    })
                })?;

        let mut results = Vec::new();
        for result in rows {
            results.push(result?);
        }
        Ok(results)
    }

    pub fn address_get_reserved_balance(&self, address: &str, coin: &str) -> Result<Option<u64>, Error> {
        let conn = self.get_conn()?;

        match conn
            .query_row_and_then(
                "SELECT SUM(`amount`) + SUM(`fee`) as `reserved` FROM `mempool` where `from` = ?1 and `coin` = ?2 GROUP BY `from`, `coin` LIMIT 1",
                &[address, coin],
                |row| {
                    SqliteStorage::i64_to_u64(row.get_checked(0)?)
                }) {
            Ok(v) => Ok(Some(v)),
            Err(e) => {
                match e {
                    Error::NotFound => Ok(None),
                    _ => Err(Error::QueryError {message: e.to_string()})
                }
            }
        }
    }

    pub fn coin_exists(&self, coin: &str) -> Result<bool, Error> {
        Ok(self.coin_exists_in_chain(coin)? || self.coin_exists_in_mempool(coin)?)
    }

    pub fn coin_exists_in_chain_with_conn(&self, conn: &rusqlite::Connection, coin: &str) -> Result<bool, Error> {
        match conn
            .query_row_and_then(
                "SELECT `balance` from `address_balance` a where a.coin = ?1 LIMIT 1",
                &[coin],
                |row| {
                    // value doesn't matter
                    Ok(false)
                }) {
            Ok(v) => Ok(true),
            Err(e) => {
                match e {
                    Error::NotFound => Ok(false),
                    _ => Err(Error::QueryError {message: e.to_string()})
                }
            }
        }
    }

    pub fn coin_exists_in_chain(&self, coin: &str) -> Result<bool, Error> {
        let conn = self.get_conn()?;
        self.coin_exists_in_chain_with_conn(&conn, coin)
    }

    pub fn coin_exists_in_mempool(&self, coin: &str) -> Result<bool, Error> {
        let conn = self.get_conn()?;

        match conn
            .query_row_and_then(
                "SELECT `amount` from `mempool` m where m.coin = ?1 LIMIT 1",
                &[coin],
                |row| {
                    // value doesn't matter
                    Ok(false)
                }) {
            Ok(v) => Ok(true),
            Err(e) => {
                match e {
                    Error::NotFound => Ok(false),
                    _ => Err(Error::QueryError {message: e.to_string()})
                }
            }
        }
    }

    pub fn block_add_with_conn(&self, conn: &rusqlite::Connection, block: &Block) -> Result<(), Error> {
        let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).map_err(|_| Error::InternalError)?.as_secs();
        conn.execute(
            "INSERT INTO `block` (`height`, `hash`, `time`)
                  VALUES (?1, ?2, ?3)",
            &[
                &block.height.to_string(),
                &block.hash,
                &block.time.to_string()
            ],
        ).map_err(|e| {
            println!("{:?}", e);
            Error::QueryError {message: "insert block failed".to_owned()}
        })?;
        Ok(())
    }

    fn i64_to_u64(num: i64) -> Result<u64, Error> {
        if num < 0 {
            Err(Error::QueryError {message: "Invalid number found".to_owned()})
        } else {
            Ok(num as u64)
        }
    }
}
