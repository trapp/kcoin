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

#[derive(Debug, Serialize)]
pub struct Bech32Address {
    pub address: String,
    #[serde(skip)]
    pub network: Network
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
                //let data = Vec<u8>::from_base32(address.data());
                println!("data {:?}", address.data());
                let data = convert_bits(address.data(), 5, 8, false).map_err(|_| KCoinError::InvalidAddress)?;
                println!("data converted {:?}", address.data());
                let data_slice = data.as_slice();
                println!("data as slice {:?}", data_slice);
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
    if regtest == true {
        info!("Regtest mode enabled. Automated block production has been disabled.");
        io.add_method("regtest_generate", move |params| {
            rpccalls::regtest::regtest_generate(param_map(params)?)
        });
    }

    let network = match regtest {
        false => Network::Mainnet,
        true => Network::Regtest
    };

    let kcn_address = Bech32Address::new(matches.value_of("kcn-address").unwrap(), network.clone()).map_err(|e| KCoinError::InvalidArgument { argument: "kcn-address".to_owned(), reason: e.to_string()})?;
    debug!("Value for kcn-address: {:?}", kcn_address);

    let kcn_supply: u64 = matches.value_of("kcn-supply").unwrap_or_default().parse().map_err(|e: ParseIntError| KCoinError::InvalidArgument{ argument: "kcn-supply".to_owned(), reason: e.to_string()})?;
    debug!("Value for kcn-supply: {}", kcn_supply);

    let storage = match regtest {
        true => {
            storage::SqliteStorage::new(None, kcn_address, kcn_supply).unwrap()
        },
        false => {
            storage::SqliteStorage::new(Some(&Path::new(datadir)), kcn_address, kcn_supply).unwrap()
        }
    };

    {
        let storage_clone = storage.clone();
        io.add_method("chain_height", move |_| {
            rpccalls::chain::chain_height(&storage_clone)
        });
    }

    {
        let storage_clone = storage.clone();
        let network_clone = network.clone();
        io.add_method("tx_send", move |params| {
            rpccalls::tx::tx_send(&storage_clone, &network_clone, param_map(params)?)
        });
    }

    {
        let block_gen_storage = storage.clone();
        let block_gen_thread = thread::spawn(move || {
            const BLOCK_SIZE: u64 = 2;
            loop {
                let ten_seconds = time::Duration::from_millis(30000);
                thread::sleep(ten_seconds);

                println!("gen block");

                // fetch mempool txs ordered by fee limit block size

                //
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
