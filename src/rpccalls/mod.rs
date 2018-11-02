extern crate jsonrpc_minihttp_server;

pub mod regtest;
pub mod chain;
pub mod tx;
use jsonrpc_minihttp_server::jsonrpc_core::*;

fn get_string<'a>(params: &'a serde_json::Map<String, Value>, name: &str) -> Result<&'a str> {
    params.get(name)
        .ok_or(Error::invalid_params("Missing parameter: ".to_owned() + name))?
        .as_str()
        .ok_or(Error::invalid_params("Not a string: ".to_owned() + name))
}