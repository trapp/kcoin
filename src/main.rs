#[macro_use] extern crate serde_derive;
#[macro_use] extern crate serde_json;
#[macro_use] extern crate log;
#[macro_use] extern crate failure;
#[macro_use] extern crate lazy_static;
extern crate serde;
extern crate hex;
extern crate jsonrpc_minihttp_server;
extern crate clap;
extern crate env_logger;
extern crate rusqlite;
extern crate dirs;
extern crate r2d2;
extern crate r2d2_sqlite;
extern crate futures;
extern crate bech32;
extern crate ed25519_dalek;
extern crate sha2;
extern crate time;
extern crate regex;

mod rpccalls;
mod errors;
mod storage;
mod kcoin;
mod tx;
mod block;

fn main() {
    match kcoin::init() {
        Ok(_) => { println!("done"); },
        Err(e) => {
            println!("{}", e);
            std::process::exit(1);
        }
    }
}
