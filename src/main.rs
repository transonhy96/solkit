use std::{
    collections::HashMap,
    str::FromStr,
    sync::{Arc, Mutex},
};

use solana_program::{
    bpf_loader, bpf_loader_upgradeable, pubkey::Pubkey, system_program, sysvar::is_sysvar_id, vote,
};
use solana_sdk::native_loader;
use solana_transaction_status::{
    parse_accounts::ParsedAccount, EncodedConfirmedBlock, EncodedTransaction, UiMessage,
    UiParsedMessage, UiTransactionEncoding, UiTransactionStatusMeta,
};
const SYSTEM_PROGS: [Pubkey; 5] = [
    system_program::ID,
    bpf_loader::ID,
    bpf_loader_upgradeable::ID,
    vote::program::ID,
    native_loader::ID,
];
pub struct Processor {
    native_tx_vol: Arc<Mutex<HashMap<String, u64>>>,
}

impl Processor {
    pub fn new() -> Self {
        Processor {
            native_tx_vol: Arc::new(Mutex::new(HashMap::new())),
        }
    }
    pub fn process_block(&self, b: &EncodedConfirmedBlock) {
        if b.transactions.len() < 1 {
            return;
        }

        for (_, tx) in b.transactions.iter().enumerate() {
            let meta = match &tx.meta {
                Some(val) => val,
                _ => {
                    continue;
                }
            };
            match &tx.transaction {
                EncodedTransaction::Json(ui_tx) => {
                    let msg = &ui_tx.message;
                    match msg {
                        // will be Parsed cause get_block_with_encoding(UiTransactionEncoding::JsonParsed)
                        UiMessage::Parsed(p) => self.process_tx(p, meta),
                        _ => {
                            continue;
                        }
                    }
                }
                _ => {
                    continue;
                }
            }
        }
    }
    pub fn process_tx(&self, parsed_tx: &UiParsedMessage, meta: &UiTransactionStatusMeta) {
        let target_pks: Vec<&ParsedAccount> = parsed_tx
            .account_keys
            .iter()
            .filter(|&s| !self.is_sys_pks(&s.pubkey))
            .collect();

        if target_pks.len() > 0 {
            for (_, acc) in target_pks.iter().enumerate() {
                let i = parsed_tx
                    .account_keys
                    .iter()
                    .position(|s| s.pubkey == acc.pubkey);
                match i {
                    Some(v) => {
                        let pre_bal = meta.pre_balances.get(v).unwrap();
                        let post_bal = meta.post_balances.get(v).unwrap();
                        let vol = if post_bal > pre_bal {
                            post_bal - pre_bal
                        } else {
                            pre_bal - post_bal
                        };
                        let mut native_tx = self.native_tx_vol.lock().unwrap();
                        let k = acc.pubkey.to_string();
                        if native_tx.contains_key::<String>(&k) {
                            let old_vol = native_tx.get_mut(&k).unwrap();
                            *old_vol += vol;
                        } else {
                            native_tx.insert(k, vol);
                        }
                    }
                    _ => {
                        continue;
                    }
                }
            }
        }
    }
    pub fn is_sys_pks(&self, pk: &String) -> bool {
        if SYSTEM_PROGS.map(|s| s.to_string()).contains(pk) {
            return true;
        }
        let pubkey = Pubkey::from_str(pk).expect("Incorrect pubkey");
        return is_sysvar_id(&pubkey);
    }
}
#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() {
    let client = solana_client::rpc_client::RpcClient::new(
        "https://api-mainnet-beta.renec.foundation:8899/",
    );
    let latest_block_height = client.get_block_height().unwrap();
    println!("Latest block height : {}", latest_block_height);``
    const GENESIS_BLOCK_HEIGHT: u64 = 85_636_586;

    let processor = Arc::new(Mutex::new(Processor::new()));

    let mut current_scan_block_height = GENESIS_BLOCK_HEIGHT;
    const BATCH: u64 = 100;

    while current_scan_block_height < latest_block_height {
        let mut handles = vec![];
        for _ in 0..1000 {
            let mut scan_block = current_scan_block_height;
            let end_scan_block = BATCH + current_scan_block_height;
            let process = Arc::clone(&processor);

            let handle = tokio::spawn(async move {
                let client = solana_client::rpc_client::RpcClient::new(
                    "https://api-mainnet-beta.renec.foundation:8899/",
                );
                while scan_block < end_scan_block {
                    let block = client
                        .get_block_with_encoding(scan_block, UiTransactionEncoding::JsonParsed)
                        .unwrap();
                    //println!("block: {}", serde_json::to_string_pretty(&block).unwrap());
                    //println!("Processing block {} on thread : {}", scan_block, i);
                    process.lock().unwrap().process_block(&block);
                    scan_block += 1;
                }
                println!(
                    "Data {}",
                    serde_json::to_string_pretty(&process.lock().unwrap().native_tx_vol).unwrap()
                );
            });
            current_scan_block_height = end_scan_block;
            println!("Processing block {}", current_scan_block_height);
            handles.push(handle);
        }
        futures::future::join_all(handles).await;
    }
}
