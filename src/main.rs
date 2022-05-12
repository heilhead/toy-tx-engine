mod account;
mod engine;
mod input;
mod transaction;

use crate::engine::Engine;
use crate::input::InputStream;
use anyhow::Context;
use clap::{arg, command};
use rust_decimal::Decimal;
use serde::Serialize;

/// Processes all transactions from the input stream. Bad transactions do not stop the processing,
/// and all errors are printed to `stderr`.
fn process_transactions(engine: &mut Engine, input: InputStream) {
    for data in input {
        // In the interests of time, we just print the errors to `stderr` without any fancy logging.
        match data {
            Ok(data) => {
                if let Err(err) = engine.process_transaction(&data) {
                    eprintln!("Error processing transaction: Transaction={data:?} Error={err}");
                }
            }

            Err(err) => eprintln!("Error decoding transaction: {err}"),
        }
    }
}

/// An intermediate representation of account data for serialization purposes. As it turned out,
/// `csv` crate doesn't supported nested struct serialization (even with serde's `flatten` switch).
#[derive(Serialize)]
struct AccountInfo {
    client: u16,
    available: Decimal,
    held: Decimal,
    total: Decimal,
    locked: bool,
}

/// Dumps account data to `stdout`.
fn dump_account_data(engine: &Engine) -> anyhow::Result<()> {
    let mut writer = csv::Writer::from_writer(std::io::stdout());

    for account in engine.accounts() {
        let balance = account.balance();

        writer.serialize(AccountInfo {
            client: account.id(),
            available: balance.available(),
            held: balance.held(),
            total: balance.total(),
            locked: account.locked(),
        })?;
    }

    writer.flush()?;

    Ok(())
}

fn main() -> anyhow::Result<()> {
    let args = command!()
        .arg_required_else_help(true)
        .arg(arg!(<INPUT_FILE> "Path to input CSV file"))
        .get_matches();

    let input_path = args.value_of("INPUT_FILE").unwrap();
    let input = InputStream::from_file(input_path).context("Failed to create input stream")?;
    let mut engine = Engine::new();

    process_transactions(&mut engine, input);
    dump_account_data(&engine)?;

    Ok(())
}
