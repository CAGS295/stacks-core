// Copyright (C) 2013-2020 Blockstack PBC, a public benefit corporation
// Copyright (C) 2020-2024 Stacks Open Internet Foundation
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

//! Ping utils
use std::fs::File;
use std::io::Read;
use std::io::Write as WriteT;
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

use crate::client::{ClientError, StacksClient, SIGNER_SLOTS_PER_USER};
use crate::{config::Network, utils::build_stackerdb_contract};
use blockstack_lib::chainstate::stacks::StacksTransaction;
use blockstack_lib::chainstate::stacks::StacksTransactionSigner;
use blockstack_lib::chainstate::stacks::TransactionAnchorMode;
use blockstack_lib::chainstate::stacks::TransactionAuth;
use blockstack_lib::chainstate::stacks::TransactionPostConditionMode;
use blockstack_lib::{
    chainstate::stacks::{TransactionPayload, TransactionSmartContract},
    util_lib::strings::StacksString,
};
use clarity::vm::types::QualifiedContractIdentifier;
use clarity::vm::types::{PrincipalData, StandardPrincipalData};
use clarity::vm::ContractName;
use reqwest::blocking::Client;
use reqwest::StatusCode;
use stacks_common::{
    address::AddressHashMode,
    types::chainstate::{StacksAddress, StacksPrivateKey, StacksPublicKey},
};

#[derive(clap::Subcommand, Debug)]

/// Ping subcommands
pub enum PingSubcommands {
    /// Generate a simple stackerDB contract.
    /// This command can be used to generate a simple stackerDB contract.
    /// A shared seed can be used to generate keys deterministically.
    /// DO NOT USE this in production.
    /// Don't hold funds on this accounts. Anyone with the shared seed can deterministically generate the signer's secret keys.
    GenerateContract(GenerateContractArgs),
    /// Publish a stackerDB contract,
    PublishContract(PublishContractArgs),
}

impl PingSubcommands {
    /// Handle any subcommand
    pub fn handle(&self) {
        match self {
            PingSubcommands::GenerateContract(args) => args.handle(),
            PingSubcommands::PublishContract(args) => args.handle(),
        }
    }
}

#[derive(clap::Args, Debug)]
/// You can provide either existing [signers] addresses or generate new ones based on a [seed]
/// and the specified number of signers [num_signers].
pub struct GenerateContractArgs {
    // output file e.g. ./stackerDB.clar
    save_to_file: PathBuf,
    #[clap(value_parser = PrincipalData::parse_standard_principal, long, value_delimiter= ',',conflicts_with_all=["seed","num_signers","network"])]
    /// A list of signers' addresses e.g. SP2E7G2V8QAJ9KS1DMHYNMBWFWY2EHGGYTGRTH12B,SP1NHW9S3XP1937EX5WTJSF599YPZRB0H85W1WCP0
    signers: Vec<StandardPrincipalData>,
    /// chunk-size for the contract
    #[clap(short, long, default_value = "4096")]
    chunk_size: u32,
    #[clap(long, requires_all = ["num_signers","network"])]
    seed: Option<String>,
    #[clap(long, requires_all = ["seed","network"])]
    num_signers: Option<u32>,
    #[clap(long, requires_all = ["seed","num_signers"])]
    network: Option<Network>,
}

impl GenerateContractArgs {
    fn handle(&self) {
        let mut file = File::create(&self.save_to_file).unwrap();
        let addresses: Vec<StacksAddress> =
        // Use the signers provided
        if !self.signers.is_empty() {
            self.signers
                .clone()
                .into_iter()
                .map(StacksAddress::from)
                .collect()
        } else
        //generate new signers from a seed, expect all signers' conflicting options to be Some()
        {
            (0..self.num_signers.unwrap()).map(|i|
            to_stacks_address(self.network.as_ref().unwrap(), &private_key_from_seed(self.seed.as_ref().unwrap(), i))
            ).collect()
        };

        let contract =
            build_stackerdb_contract(addresses.as_slice(), SIGNER_SLOTS_PER_USER, self.chunk_size);
        file.write_all(contract.as_bytes()).unwrap();
        println!("New stackerdb contract written to {:?}", self.save_to_file);
    }
}

fn to_stacks_address(network: &Network, pkey: &StacksPrivateKey) -> StacksAddress {
    let address_version = network.to_address_version();
    StacksAddress::from_public_keys(
        address_version,
        &AddressHashMode::from_version(address_version),
        1,
        &vec![StacksPublicKey::from_private(pkey)],
    )
    .unwrap()
}

#[derive(clap::Args, Debug)]
/// Once you have generated a contract, publish it.
pub struct PublishContractArgs {
    #[clap(long)]
    source_file: PathBuf,
    #[clap(long, short)]
    contract_name: String,
    #[clap(value_enum, long)]
    network: Network,
    #[clap(long, short)]
    stacks_private_key: String,
    #[clap(long, short)]
    nonce: u64,
    #[clap(long, short)]
    fee: u64,
    #[clap(long)]
    /// e.g. http://localhost:20443
    host: String,
}

impl PublishContractArgs {
    fn handle(&self) {
        let pkey = StacksPrivateKey::from_hex(&self.stacks_private_key).unwrap();
        let contract_name = ContractName::try_from(self.contract_name.clone()).unwrap();

        let tx = {
            let payload = {
                let code_body = {
                    let mut contract = String::new();
                    File::open(&self.source_file)
                        .unwrap()
                        .read_to_string(&mut contract)
                        .unwrap();

                    StacksString::from_str(contract.as_str()).unwrap()
                };

                TransactionPayload::SmartContract(
                    TransactionSmartContract {
                        name: contract_name.clone(),
                        code_body,
                    },
                    None,
                )
            };

            let auth = {
                let mut auth = TransactionAuth::from_p2pkh(&pkey).unwrap();
                auth.set_origin_nonce(self.nonce);
                auth.set_tx_fee(self.fee);
                auth
            };

            let mut unsinged_tx =
                StacksTransaction::new(self.network.to_transaction_version(), auth, payload);
            unsinged_tx.chain_id = self.network.to_chain_id();
            unsinged_tx.post_condition_mode = TransactionPostConditionMode::Allow;
            unsinged_tx.anchor_mode = TransactionAnchorMode::OnChainOnly;

            let mut signer = StacksTransactionSigner::new(&unsinged_tx);

            signer.sign_origin(&pkey).unwrap();
            signer.get_tx().unwrap()
        };

        let client = Client::new();

        StacksClient::submit_tx(&tx, &client, &self.host).unwrap();

        let principal = {
            let address = to_stacks_address(&self.network, &pkey);
            StandardPrincipalData::from(address)
        };

        while matches!(
            StacksClient::get_contract_source(
                &self.host,
                &principal.clone(),
                &self.contract_name,
                &client,
            )
            .map(|_| {
                println!(
                    "Contract {} published successfully",
                    QualifiedContractIdentifier::new(principal.clone(), contract_name.clone())
                )
            }),
            Err(ClientError::RequestFailure(StatusCode::NOT_FOUND))
        ) {
            thread::sleep(Duration::from_millis(500));
        }
    }
}

fn private_key_from_seed(seed: &str, signer_id: u32) -> StacksPrivateKey {
    StacksPrivateKey::from_seed(format!("{signer_id}{}", seed).as_bytes())
}

#[cfg(test)]
mod test {
    use stacks_common::types::Address;

    use super::*;

    #[test]
    fn sane_to_stacks_address() {
        let address = to_stacks_address(
            &Network::Mainnet,
            &StacksPrivateKey::from_hex(
                "d06a21eb4127872d0a96a3261437f5f932f3cdb98cd651396892f026b8a7542c01",
            )
            .unwrap(),
        );
        assert_eq!(
            address,
            StacksAddress::from_string("SP23M92VQE6452BXRGDMEBRM1WPDCJXAA5T3WYE17").unwrap()
        )
    }

    #[test]
    fn different_private_key_per_signer() {
        let seed = "secret";
        let a = private_key_from_seed(seed, 0);
        let b = private_key_from_seed(seed, 1);
        assert_ne!(a, b);
    }
}
