use jsonrpc_minihttp_server::jsonrpc_core::*;



pub fn no_block_found() -> Error {
    jsonrpc_error("No block found", -33005, None)
}
pub fn nonce_gap() -> Error {
    jsonrpc_error("Non-sequential nonce", -33006, None)
}
pub fn nonce_used() -> Error { jsonrpc_error("Nonce has been used already", -33007, None) }
pub fn tx_known() -> Error { jsonrpc_error("Transaction is already in the mempool", -33008, None) }
pub fn fee_too_low_to_replace() -> Error { jsonrpc_error("Fee too low to replace transaction in the mempool", -33009, None) }
pub fn mempool_full() -> Error { jsonrpc_error("Mempool is full and fee is too low to evict another tx", -33010, None) }
pub fn mempool_full_own_txs() -> Error { jsonrpc_error("Mempool is filled exclusively with your transactions. Wait until some get mined.", -33011, None) }
pub fn insufficient_balance() -> Error { jsonrpc_error("Insufficient balance", -33012, None) }
pub fn fee_too_low() -> Error { jsonrpc_error("Fee too low", -33013, None) }

pub fn jsonrpc_error(message: &str, code: i64, data: Option<Value>) -> Error {
    Error {
        message: message.to_string(),
        code: ErrorCode::ServerError(code),
        data
    }
}
