use clap::{Arg, App};

use std::path::Path;
use std::env;
use log::LevelFilter;
use env_logger::{Builder, Target};
use dirs;
use rpccalls;
use storage;
use serde_json;
use std::num::ParseIntError;
use bech32::{Bech32, convert_bits};
use ed25519_dalek::PublicKey;
use std::thread;
use std::time;
use jsonrpc_minihttp_server::{ServerBuilder, DomainsValidation};
use jsonrpc_minihttp_server::jsonrpc_core::{Params, Value, IoHandler, Compatibility, Error};
use jsonrpc_minihttp_server::cors::AccessControlAllowOrigin;
use block;
use serde::{Serialize, Serializer};

pub const NEW_COIN_FEE: u64 = 1000000000;

#[derive(Debug, Fail)]
pub enum KCoinError {
    #[fail(display = "invalid address")]
    InvalidAddress,
    #[fail(display = "unable to determine home directory")]
    UnableToDetermineHomeDir,
    #[fail(display = "invalid argument `{}`: {}", argument, reason)]
    InvalidArgument {
        argument: String,
        reason: String
    }
}

#[derive(Debug, Clone)]
pub enum Network {
    Mainnet,
    Regtest
}

impl Network {
    fn prefix(&self) -> String {
        match self {
            Network::Mainnet => "kcn".to_owned(),
            Network::Regtest => "ktest".to_owned()
        }
    }
}

#[derive(Debug, Clone)]
pub struct Bech32Address {
    pub address: String,
    pub network: Network
}

impl Serialize for Bech32Address {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.address)
    }
}

impl Bech32Address {

    pub fn new(address: &str, network: Network) -> Result<Self, KCoinError> {
        let bech32_address = Bech32Address { address: address.to_owned(), network };
        match bech32_address.validate() {
            true => Ok(bech32_address),
            false => Err(KCoinError::InvalidAddress)
        }
    }

    pub fn validate(&self) -> bool {
        let c = self.address.parse::<Bech32>();
        match c {
            Ok(address) => {
                address.hrp() == self.network.prefix()
            },
            Err(_) => false
        }
    }

    pub fn public_key(&self) -> Result<PublicKey, KCoinError> {
        let c = self.address.parse::<Bech32>();
        match c {
            Ok(address) => {
                if address.hrp() != self.network.prefix() {
                    return Err(KCoinError::InvalidAddress);
                }
                let data = convert_bits(address.data(), 5, 8, false).map_err(|_| KCoinError::InvalidAddress)?;
                let data_slice = data.as_slice();
                let key = PublicKey::from_bytes(data_slice).map_err(|_| KCoinError::InvalidAddress)?;
                Ok(key)
            },
            Err(_) => Err(KCoinError::InvalidAddress)
        }
    }
}

pub fn init() -> Result<(), KCoinError> {
    let mut builder = Builder::new();
    builder.target(Target::Stdout);
    if env::var("RUST_LOG").is_ok() {
        builder.parse(&env::var("RUST_LOG").unwrap());
    } else {
        builder.filter(None, LevelFilter::Info);
    }
    builder.init();

    // get home dir
    let mut default_data_dir = dirs::home_dir().ok_or(KCoinError::UnableToDetermineHomeDir)?;
    default_data_dir.push(".kcoin");

    let default_data_dir_str = default_data_dir.to_string_lossy();

    // Parse command line args
    let matches = App::new("kcoin")
        .version("0.1.0")
        .arg(Arg::with_name("rpc-host")
            .short("h")
            .long("rpc-host")
            .value_name("IP/HOSTNAME")
            .help("Hostname or Ip address the rpc server will listen on")
            .takes_value(true)
            .default_value("127.0.0.1"))
        .arg(Arg::with_name("rpc-port")
            .short("p")
            .long("rpc-port")
            .value_name("PORT")
            .help("Port the rpc server will listen on")
            .takes_value(true)
            .default_value("3030"))
        .arg(Arg::with_name("datadir")
            .short("d")
            .long("datadir")
            .value_name("PATH")
            .help("Directory kcoin will use to store its data")
            .takes_value(true)
            .default_value(&default_data_dir_str))
        .arg(Arg::with_name("kcn-address")
            .short("a")
            .long("kcn-address")
            .value_name("ADDRESS")
            .help("Owner of the initial KCN supply")
            .takes_value(true)
            .required(true))
        .arg(Arg::with_name("kcn-supply")
            .short("s")
            .long("kcn-supply")
            .value_name("AMOUNT")
            .help("How many KCNs get created initially")
            .takes_value(true)
            .default_value("100000000"))
        .arg(Arg::with_name("block-time")
            .short("t")
            .long("block-time")
            .value_name("SECONDS")
            .help("After how many seconds a new block should get crafted")
            .takes_value(true)
            .default_value("60"))
        .arg(Arg::with_name("mempool-size")
            .short("m")
            .long("mempool-size")
            .value_name("NUMBER")
            .help("How many transactions the mempool can fit")
            .takes_value(true)
            .default_value("5000"))
        .arg(Arg::with_name("block-size")
            .short("b")
            .long("block-size")
            .value_name("NUMBER")
            .help("How many transactions the each block can fit")
            .takes_value(true)
            .default_value("100"))
        .arg(Arg::with_name("regtest")
            .short("r")
            .long("regtest")
            .help("Enables regtest mode. Regtest mode disables automatic block generation and allows generating blocks on demand by invoking the generate rpc call."))
        .get_matches();

    let mut io = IoHandler::with_compatibility(Compatibility::V2);

    let host = matches.value_of("rpc-host").unwrap_or_default();
    debug!("Value for host: {}", host);

    let port = matches.value_of("rpc-port").unwrap_or_default();
    debug!("Value for port: {}", port);

    let datadir = matches.value_of("datadir").unwrap_or_default();
    debug!("Value for datadir: {}", datadir);

    let regtest = matches.is_present("regtest");
    debug!("Value for regtest: {}", regtest);

    let network = match regtest {
        false => Network::Mainnet,
        true => Network::Regtest
    };

    debug!("{:?}", matches.value_of("kcn-address"));
    let kcn_address = Bech32Address::new(matches.value_of("kcn-address").unwrap(), network.clone()).map_err(|e| KCoinError::InvalidArgument { argument: "kcn-address".to_owned(), reason: e.to_string()})?;
    debug!("Value for kcn-address: {:?}", kcn_address);

    let kcn_supply: u64 = matches.value_of("kcn-supply").unwrap_or_default().parse().map_err(|e: ParseIntError| KCoinError::InvalidArgument{ argument: "kcn-supply".to_owned(), reason: e.to_string()})?;
    debug!("Value for kcn-supply: {}", kcn_supply);

    let block_time: u64 = matches.value_of("block-time").unwrap_or_default().parse().map_err(|e: ParseIntError| KCoinError::InvalidArgument{ argument: "block-time".to_owned(), reason: e.to_string()})?;
    debug!("Value for block-time: {}", block_time);

    let block_size: u64 = matches.value_of("block-size").unwrap_or_default().parse().map_err(|e: ParseIntError| KCoinError::InvalidArgument{ argument: "block-size".to_owned(), reason: e.to_string()})?;
    debug!("Value for block-size: {}", block_size);

    let mempool_size: u64 = matches.value_of("mempool-size").unwrap_or_default().parse().map_err(|e: ParseIntError| KCoinError::InvalidArgument{ argument: "mempool-size".to_owned(), reason: e.to_string()})?;
    debug!("Value for mempool-size: {}", mempool_size);

    let storage = storage::SqliteStorage::new(&Path::new(datadir), regtest, kcn_address, kcn_supply).unwrap();

    if regtest == true {
        info!("Regtest mode enabled. Automated block production has been disabled.");

        let block_gen_storage = storage.clone();
        let network_clone = network.clone();
        let block_size_clone = block_size;
        io.add_method("regtest_generate", move |_| {
            rpccalls::regtest::regtest_generate(&block_gen_storage, block_size_clone, &network_clone)
        });
    }

    {
        let storage_clone = storage.clone();
        io.add_method("chain_getHeight", move |_| {
            rpccalls::chain::chain_height(&storage_clone)
        });
    }

    {
        let storage_clone = storage.clone();
        let network_clone = network.clone();
        io.add_method("chain_getBlockByHeight", move |params| {
            rpccalls::chain::chain_get_block_by_height(&storage_clone, param_map(params)?, &network_clone)
        });
    }

    {
        let storage_clone = storage.clone();
        let network_clone = network.clone();
        io.add_method("chain_addressInfo", move |params| {
            rpccalls::chain::chain_address_info(&storage_clone, param_map(params)?, &network_clone)
        });
    }

    {
        let storage_clone = storage.clone();
        let network_clone = network.clone();
        io.add_method("mempool_getTransactions", move |params| {
            rpccalls::mempool::mempool_get_transactions(&storage_clone, &network_clone, param_map(params)?)
        });
    }

    {
        let storage_clone = storage.clone();
        let network_clone = network.clone();
        io.add_method("mempool_getTransactionByHash", move |params| {
            rpccalls::mempool::mempool_get_transaction_by_hash(&storage_clone, &network_clone, param_map(params)?)
        });
    }

    {
        let storage_clone = storage.clone();
        let network_clone = network.clone();
        io.add_method("chain_getTransactions", move |params| {
            rpccalls::chain::chain_get_transactions(&storage_clone, &network_clone, param_map(params)?)
        });
    }

    {
        let storage_clone = storage.clone();
        let network_clone = network.clone();
        io.add_method("chain_getTransactionByHash", move |params| {
            rpccalls::chain::chain_get_transaction_by_hash(&storage_clone, &network_clone, param_map(params)?)
        });
    }

    {
        let storage_clone = storage.clone();
        let network_clone = network.clone();
        io.add_method("mempool_getStats", move |_| {
            rpccalls::mempool::mempool_get_stats(&storage_clone, &network_clone)
        });
    }

    {
        let storage_clone = storage.clone();
        let network_clone = network.clone();
        let mempool_size_clone = mempool_size;
        io.add_method("tx_send", move |params| {
            rpccalls::tx::tx_send(&storage_clone, &network_clone, mempool_size_clone, param_map(params)?)
        });
    }


    if regtest == false {
        let block_gen_storage = storage.clone();
        let network_clone = network.clone();
        let block_size_clone = block_size;
        let block_gen_thread = thread::spawn(move || {
            loop {
                match block::generate(&block_gen_storage, block_size_clone, &network) {
                    Ok(_) => {},
                    Err(e) => {
                        println!("Error during block production: {:?}", e);
                    }
                }

                thread::sleep(time::Duration::from_millis((block_time * 1000).into()));
            }
        });
    }

    let listen = format!("{}:{}", host, port);
    let server = ServerBuilder::new(io)
        .cors(DomainsValidation::AllowOnly(vec![AccessControlAllowOrigin::Null]))
        .threads(4)
        .start_http(&listen.parse().unwrap())
        .expect("Unable to start RPC server");
    info!("Listening on {}", listen);
    server.wait().unwrap();
    Ok(())
}

fn param_map(params: Params) -> Result<serde_json::Map<String, Value>, Error> {
    match params {
        Params::Map(m) => Ok(m),
        _ => Err(Error::invalid_params("Params not an object"))
    }
}
