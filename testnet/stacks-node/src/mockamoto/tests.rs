use std::thread;
use std::time::{Duration, Instant};

use clarity::boot_util::boot_code_addr;
use clarity::vm::costs::ExecutionCost;
use clarity::vm::Value;
use rand_core::OsRng;
use stacks::chainstate::burn::db::sortdb::SortitionDB;
use stacks::chainstate::nakamoto::NakamotoChainState;
use stacks::chainstate::stacks::boot::POX_4_NAME;
use stacks::chainstate::stacks::db::StacksChainState;
use stacks_common::types::chainstate::{StacksAddress, StacksPrivateKey};
use stacks_common::types::StacksEpochId;
use stacks_common::util::hash::to_hex;
use wsts::curve::point::Point;
use wsts::curve::scalar::Scalar;

use super::MockamotoNode;
use crate::config::{EventKeyType, EventObserverConfig};
use crate::neon_node::PeerThread;
use crate::tests::neon_integrations::test_observer;
use crate::tests::{make_contract_call, make_stacks_transfer, to_addr};
use crate::{Config, ConfigFile};

#[test]
fn observe_100_blocks() {
    let mut conf = Config::from_config_file(ConfigFile::mockamoto()).unwrap();
    conf.node.mockamoto_time_ms = 10;

    let submitter_sk = StacksPrivateKey::from_seed(&[1]);
    let submitter_addr = to_addr(&submitter_sk);
    conf.add_initial_balance(submitter_addr.to_string(), 1_000);
    let recipient_addr = StacksAddress::burn_address(false).into();

    test_observer::spawn();
    let observer_port = test_observer::EVENT_OBSERVER_PORT;
    conf.events_observers.insert(EventObserverConfig {
        endpoint: format!("localhost:{observer_port}"),
        events_keys: vec![EventKeyType::AnyEvent],
    });

    let mut mockamoto = MockamotoNode::new(&conf).unwrap();
    let globals = mockamoto.globals.clone();

    let mut mempool = PeerThread::connect_mempool_db(&conf);
    let (mut chainstate, _) = StacksChainState::open(
        conf.is_mainnet(),
        conf.burnchain.chain_id,
        &conf.get_chainstate_path_str(),
        None,
    )
    .unwrap();
    let burnchain = conf.get_burnchain();
    let sortdb = burnchain.open_sortition_db(true).unwrap();

    let start = Instant::now();

    let node_thread = thread::Builder::new()
        .name("mockamoto-main".into())
        .spawn(move || mockamoto.run())
        .expect("FATAL: failed to start mockamoto main thread");

    // make a transfer tx to test that the mockamoto miner picks up txs from the mempool
    let transfer_tx = make_stacks_transfer(&submitter_sk, 0, 10, &recipient_addr, 100);
    let transfer_tx_hex = format!("0x{}", to_hex(&transfer_tx));

    // complete within 2 minutes or abort
    let completed = loop {
        if Instant::now().duration_since(start) > Duration::from_secs(120) {
            break false;
        }
        let latest_block = test_observer::get_blocks().pop();
        thread::sleep(Duration::from_secs(1));
        let Some(ref latest_block) = latest_block else {
            info!("No block observed yet!");
            continue;
        };
        let stacks_block_height = latest_block.get("block_height").unwrap().as_u64().unwrap();
        info!("Block height observed: {stacks_block_height}");

        if stacks_block_height == 1 {
            let tip = NakamotoChainState::get_canonical_block_header(chainstate.db(), &sortdb)
                .unwrap()
                .unwrap();
            mempool
                .submit_raw(
                    &mut chainstate,
                    &sortdb,
                    &tip.consensus_hash,
                    &tip.anchored_header.block_hash(),
                    transfer_tx.clone(),
                    &ExecutionCost::max_value(),
                    &StacksEpochId::Epoch30,
                )
                .unwrap();
        }

        if stacks_block_height >= 100 {
            break true;
        }
    };

    globals.signal_stop();

    let transfer_tx_included = test_observer::get_blocks()
        .into_iter()
        .find(|block_json| {
            block_json["transactions"]
                .as_array()
                .unwrap()
                .iter()
                .find(|tx_json| tx_json["raw_tx"].as_str() == Some(&transfer_tx_hex))
                .is_some()
        })
        .is_some();

    assert!(
        transfer_tx_included,
        "Mockamoto node failed to include the transfer tx"
    );

    assert!(
        completed,
        "Mockamoto node failed to produce and announce 100 blocks before timeout"
    );
    node_thread
        .join()
        .expect("Failed to join node thread to exit");
}

#[test]
fn observe_set_aggregate_tx() {
    let mut conf = Config::from_config_file(ConfigFile::mockamoto()).unwrap();
    conf.node.mockamoto_time_ms = 10;

    let submitter_sk = StacksPrivateKey::from_seed(&[1]);
    let submitter_addr = to_addr(&submitter_sk);
    conf.add_initial_balance(submitter_addr.to_string(), 1_000);

    test_observer::spawn();
    let observer_port = test_observer::EVENT_OBSERVER_PORT;
    conf.events_observers.push(EventObserverConfig {
        endpoint: format!("localhost:{observer_port}"),
        events_keys: vec![EventKeyType::AnyEvent],
    });

    let mut mockamoto = MockamotoNode::new(&conf).unwrap();

    let globals = mockamoto.globals.clone();

    let mut mempool = PeerThread::connect_mempool_db(&conf);
    let (mut chainstate, _) = StacksChainState::open(
        conf.is_mainnet(),
        conf.burnchain.chain_id,
        &conf.get_chainstate_path_str(),
        None,
    )
    .unwrap();
    let burnchain = conf.get_burnchain();
    let sortdb = burnchain.open_sortition_db(true).unwrap();
    let sortition_tip = SortitionDB::get_canonical_burn_chain_tip(mockamoto.sortdb.conn()).unwrap();

    let start = Instant::now();
    // Get a reward cycle to compare against
    let reward_cycle = mockamoto
        .sortdb
        .pox_constants
        .block_height_to_reward_cycle(
            mockamoto.sortdb.first_block_height,
            sortition_tip.block_height,
        )
        .expect(
            format!(
                "Failed to determine reward cycle of block height: {}",
                sortition_tip.block_height
            )
            .as_str(),
        );

    let node_thread = thread::Builder::new()
        .name("mockamoto-main".into())
        .spawn(move || {
            mockamoto.run();
            let aggregate_key_block_header = NakamotoChainState::get_canonical_block_header(
                mockamoto.chainstate.db(),
                &mockamoto.sortdb,
            )
            .unwrap()
            .unwrap();
            // Get the aggregate public key to later verify that it was set correctly
            mockamoto
                .chainstate
                .get_aggregate_public_key_pox_4(
                    &mockamoto.sortdb,
                    &aggregate_key_block_header.index_block_hash(),
                    reward_cycle,
                )
                .unwrap()
        })
        .expect("FATAL: failed to start mockamoto main thread");

    // Create a "set-aggregate-public-key" tx to verify it sets correctly
    let mut rng = OsRng::default();
    let x = Scalar::random(&mut rng);
    let random_key = Point::from(x);

    let aggregate_public_key = Value::buff_from(random_key.compress().data.to_vec())
        .expect("Failed to serialize aggregate public key");
    let aggregate_tx = make_contract_call(
        &submitter_sk,
        0,
        10,
        &boot_code_addr(false),
        POX_4_NAME,
        "set-aggregate-public-key",
        &[Value::UInt(u128::from(reward_cycle)), aggregate_public_key],
    );
    let aggregate_tx_hex = format!("0x{}", to_hex(&aggregate_tx));

    // complete within 5 seconds or abort (we are only observing one block)
    let completed = loop {
        if Instant::now().duration_since(start) > Duration::from_secs(5) {
            break false;
        }
        let latest_block = test_observer::get_blocks().pop();
        thread::sleep(Duration::from_secs(1));
        let Some(ref latest_block) = latest_block else {
            info!("No block observed yet!");
            continue;
        };
        let stacks_block_height = latest_block.get("block_height").unwrap().as_u64().unwrap();
        info!("Block height observed: {stacks_block_height}");

        // Submit the aggregate tx for processing to update the aggregate public key
        let tip = NakamotoChainState::get_canonical_block_header(chainstate.db(), &sortdb)
            .unwrap()
            .unwrap();
        mempool
            .submit_raw(
                &mut chainstate,
                &sortdb,
                &tip.consensus_hash,
                &tip.anchored_header.block_hash(),
                aggregate_tx.clone(),
                &ExecutionCost::max_value(),
                &StacksEpochId::Epoch30,
            )
            .unwrap();
        break true;
    };

    globals.signal_stop();

    let aggregate_key = node_thread
        .join()
        .expect("Failed to join node thread to exit");

    // Did we set and retrieve the aggregate key correctly?
    assert_eq!(aggregate_key.unwrap(), random_key);

    let aggregate_tx_included = test_observer::get_blocks()
        .into_iter()
        .find(|block_json| {
            block_json["transactions"]
                .as_array()
                .unwrap()
                .iter()
                .find(|tx_json| tx_json["raw_tx"].as_str() == Some(&aggregate_tx_hex))
                .is_some()
        })
        .is_some();

    assert!(
        aggregate_tx_included,
        "Mockamoto node failed to include the aggregate tx"
    );

    assert!(
        completed,
        "Mockamoto node failed to produce and announce its block before timeout"
    );
}
