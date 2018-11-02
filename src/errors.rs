use jsonrpc_minihttp_server::jsonrpc_core::*;

pub fn could_not_execute_docker_compose() -> Error {
    jsonrpc_error("Could not execute docker-compose", -33002, None)
}

pub fn reset_failed() -> Error {
    jsonrpc_error("Reset failed", -33003, None)
}

pub fn query_error() -> Error {
    jsonrpc_error("Query Error", -33004, None)
}

pub fn no_block_found() -> Error {
    jsonrpc_error("No block found", -33005, None)
}

pub fn jsonrpc_error(message: &str, code: i64, data: Option<Value>) -> Error {
    Error {
        message: message.to_string(),
        code: ErrorCode::ServerError(code),
        data
    }
}

