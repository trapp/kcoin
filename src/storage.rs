use std::fs;
use std::path::Path;
use rusqlite::{Connection, NO_PARAMS};
use r2d2_sqlite::SqliteConnectionManager;
use r2d2::{Pool, PooledConnection};
use std::str::FromStr;
use std::time;
use std::thread;
use futures;
use std::time::{SystemTime, UNIX_EPOCH};
use std::collections::HashMap;
use kcoin::Bech32Address;

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
    }
}

pub struct Address {
    address: String,
    nonce: u32,
    balance: HashMap<String, u64>
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
                CREATE TABLE IF NOT EXISTS `block` (`height` INTEGER, `hash` TEXT);
                CREATE UNIQUE INDEX IF NOT EXISTS `block_height` ON `block`(`height`);
                CREATE UNIQUE INDEX IF NOT EXISTS `block_hash` ON `block`(`hash`);
                CREATE TABLE IF NOT EXISTS `transaction` (`hash` TEXT, `signature` TEXT, `block` INTEGER, `from` TEXT, `to` TEXT, `coin` TEXT, `amount` BIGINT, `nonce` BIGINT);
                CREATE UNIQUE INDEX IF NOT EXISTS `tx_hash` ON `transaction`(`hash`);
                CREATE INDEX IF NOT EXISTS `tx_block` ON `transaction`(`block`);
                CREATE INDEX IF NOT EXISTS `tx_from` ON `transaction`(`from`);
                CREATE INDEX IF NOT EXISTS `tx_to` ON `transaction`(`to`);
                CREATE INDEX IF NOT EXISTS `tx_coin` ON `transaction`(`coin`);
                CREATE INDEX IF NOT EXISTS `tx_amount` ON `transaction`(`amount`);
                CREATE INDEX IF NOT EXISTS `tx_from_nonce` ON `transaction`(`from`, `nonce`);
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
                     &[knc_address.address, "KCN".to_owned(), knc_supply.to_string()]).unwrap();
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
}