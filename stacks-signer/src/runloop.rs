use std::collections::VecDeque;
use std::sync::mpsc::Sender;
use std::time::Duration;

use libsigner::{SignerRunLoop, StackerDBChunksEvent};
use slog::{slog_debug, slog_error, slog_info, slog_warn};
use stacks_common::codec::{read_next, StacksMessageCodec};
use stacks_common::{debug, error, info, warn};
use wsts::common::MerkleRoot;
use wsts::curve::ecdsa;
use wsts::net::{Message, Packet, Signable};
use wsts::state_machine::coordinator::frost::Coordinator as FrostCoordinator;
use wsts::state_machine::coordinator::{Config as CoordinatorConfig, Coordinator};
use wsts::state_machine::signer::Signer;
use wsts::state_machine::{OperationResult, PublicKeys};
use wsts::v2;

use crate::client::{
    retry_with_exponential_backoff, ClientError, StackerDB, StackerDBMessage, StacksClient,
};
use crate::config::Config;

/// Which operation to perform
#[derive(PartialEq, Clone)]
pub enum RunLoopCommand {
    /// Generate a DKG aggregate public key
    Dkg,
    /// Sign a message
    Sign {
        /// The bytes to sign
        message: Vec<u8>,
        /// Whether to make a taproot signature
        is_taproot: bool,
        /// Taproot merkle root
        merkle_root: Option<MerkleRoot>,
    },
}

/// The RunLoop state
#[derive(PartialEq, Debug)]
pub enum State {
    // TODO: Uninitialized should indicate we need to replay events/configure the signer
    /// The runloop signer is uninitialized
    Uninitialized,
    /// The runloop is idle
    Idle,
    /// The runloop is executing a DKG round
    Dkg,
    /// The runloop is executing a signing round
    Sign,
}

/// The runloop for the stacks signer
pub struct RunLoop<C> {
    /// The timeout for events
    pub event_timeout: Duration,
    /// The coordinator for inbound messages
    pub coordinator: C,
    /// The signing round used to sign messages
    // TODO: update this to use frost_signer directly instead of the frost signing round
    // See: https://github.com/stacks-network/stacks-blockchain/issues/3913
    pub signing_round: Signer<v2::Signer>,
    /// The stacks node client
    pub stacks_client: StacksClient,
    /// The stacker db client
    pub stackerdb: StackerDB,
    /// Received Commands that need to be processed
    pub commands: VecDeque<RunLoopCommand>,
    /// The current state
    pub state: State,
}

impl<C: Coordinator> RunLoop<C> {
    /// Initialize the signer, reading the stacker-db state and setting the aggregate public key
    fn initialize(&mut self) -> Result<(), ClientError> {
        // TODO: update to read stacker db to get state.
        // Check if the aggregate key is set in the pox contract
        if let Some(key) = self.stacks_client.get_aggregate_public_key()? {
            debug!("Aggregate public key is set: {:?}", key);
            self.coordinator.set_aggregate_public_key(Some(key));
        } else {
            // Update the state to IDLE so we don't needlessy requeue the DKG command.
            let (coordinator_id, _) = calculate_coordinator(&self.signing_round.public_keys);
            if coordinator_id == self.signing_round.signer_id
                && self.commands.front() != Some(&RunLoopCommand::Dkg)
            {
                self.commands.push_front(RunLoopCommand::Dkg);
            }
        }
        self.state = State::Idle;
        Ok(())
    }

    /// Execute the given command and update state accordingly
    /// Returns true when it is successfully executed, else false
    fn execute_command(&mut self, command: &RunLoopCommand) -> bool {
        match command {
            RunLoopCommand::Dkg => {
                info!("Starting DKG");
                match self.coordinator.start_dkg_round() {
                    Ok(msg) => {
                        let ack = self
                            .stackerdb
                            .send_message_with_retry(self.signing_round.signer_id, msg.into());
                        debug!("ACK: {:?}", ack);
                        self.state = State::Dkg;
                        true
                    }
                    Err(e) => {
                        error!("Failed to start DKG: {:?}", e);
                        warn!("Resetting coordinator's internal state.");
                        self.coordinator.reset();
                        false
                    }
                }
            }
            RunLoopCommand::Sign {
                message,
                is_taproot,
                merkle_root,
            } => {
                info!("Signing message: {:?}", message);
                match self
                    .coordinator
                    .start_signing_round(message, *is_taproot, *merkle_root)
                {
                    Ok(msg) => {
                        let ack = self
                            .stackerdb
                            .send_message_with_retry(self.signing_round.signer_id, msg.into());
                        debug!("ACK: {:?}", ack);
                        self.state = State::Sign;
                        true
                    }
                    Err(e) => {
                        error!("Failed to start signing message: {:?}", e);
                        warn!("Resetting coordinator's internal state.");
                        self.coordinator.reset();
                        false
                    }
                }
            }
        }
    }

    /// Attempt to process the next command in the queue, and update state accordingly
    fn process_next_command(&mut self) {
        match self.state {
            State::Uninitialized => {
                debug!(
                    "Signer is uninitialized. Waiting for aggregate public key from stacks node..."
                );
            }
            State::Idle => {
                if let Some(command) = self.commands.pop_front() {
                    while !self.execute_command(&command) {
                        warn!("Failed to execute command. Retrying...");
                    }
                } else {
                    debug!("Nothing to process. Waiting for command...");
                }
            }
            State::Dkg | State::Sign => {
                // We cannot execute the next command until the current one is finished...
                // Do nothing...
                debug!("Waiting for operation to finish");
            }
        }
    }

    /// Process the event as a miner message from the miner stacker-db
    fn process_event_miner(&mut self, event: &StackerDBChunksEvent) {
        // Determine the current coordinator id and public key for verification
        let (coordinator_id, _coordinator_public_key) =
            calculate_coordinator(&self.signing_round.public_keys);
        event.modified_slots.iter().for_each(|chunk| {
            let mut ptr = &chunk.data[..];
            let Some(stacker_db_message) = read_next::<StackerDBMessage, _>(&mut ptr).ok() else {
                warn!("Received an unrecognized message type from .miners stacker-db slot id {}: {:?}", chunk.slot_id, ptr);
                return;
            };
            match stacker_db_message {
                StackerDBMessage::Packet(_packet) => {
                    // We should never actually be receiving packets from the miner stacker-db.
                    warn!(
                        "Received a packet from the miner stacker-db. This should never happen..."
                    );
                }
                StackerDBMessage::Block(block) => {
                    // Received a block proposal from the miner.
                    // If the signer is the coordinator, then trigger a Signing round for the block
                    if coordinator_id == self.signing_round.signer_id {
                        let is_valid_block =  self.stacks_client.is_valid_nakamoto_block(&block).unwrap_or_else(|e| {
                            warn!("Failed to validate block: {:?}", e);
                            false
                        });
                        // Don't bother triggering a signing round for the block if it is invalid
                        if !is_valid_block {
                            warn!("Received an invalid block proposal from the miner. Ignoring block proposal: {:?}", block);
                            return;
                        }

                        // TODO: dependent on https://github.com/stacks-network/stacks-core/issues/4018
                        // let miner_public_key = self.stacks_client.get_miner_public_key().expect("Failed to get miner public key. Cannot verify blocks.");
                        // let Some(block_miner_public_key) = block.header.recover_miner_pk() else {
                        //     warn!("Failed to recover miner public key from block. Ignoring block proposal: {:?}", block);
                        //     return;
                        // };
                        // if block_miner_public_key != miner_public_key {
                        //     warn!("Received a block proposal signed with an invalid miner public key. Ignoring block proposal: {:?}.", block);
                        //     return;
                        // }

                        // This is a block proposal from the miner. Trigger a signing round for it.
                        self.commands.push_back(RunLoopCommand::Sign {
                            message: block.serialize_to_vec(),
                            is_taproot: false,
                            merkle_root: None,
                        });
                    }
                }
            }
        });
    }

    /// Process the event as a signer message from the signer stacker-db
    fn process_event_signer(&mut self, event: &StackerDBChunksEvent) -> Vec<OperationResult> {
        // Determine the current coordinator id and public key for verification
        let (coordinator_id, coordinator_public_key) =
            calculate_coordinator(&self.signing_round.public_keys);
        // Filter out invalid messages
        let inbound_messages: Vec<Packet> = event
            .modified_slots
            .iter()
            .filter_map(|chunk| {
                let mut ptr = &chunk.data[..];
                let Some(stacker_db_message) = read_next::<StackerDBMessage, _>(&mut ptr).ok() else {
                    warn!("Received an unrecognized message type from .signers stacker-db slot id {}: {:?}", chunk.slot_id, ptr);
                    return None;
                };
                match stacker_db_message {
                    StackerDBMessage::Packet(packet) => {
                        if verify_msg(
                            &packet,
                            &self.signing_round.public_keys,
                            coordinator_public_key,
                        ) {
                            Some(packet)
                        } else {
                            None
                        }
                    }
                    StackerDBMessage::Block(_block) => {
                        // Blocks are meant to be read by observing miners. Ignore them.
                        None
                    }
                }
            })
            .collect();
        // First process all messages as a signer
        // TODO: deserialize the packet into a block and verify its contents
        let mut outbound_messages = self
            .signing_round
            .process_inbound_messages(&inbound_messages)
            .unwrap_or_default();
        let mut operation_results = vec![];
        // If the signer is the coordinator, then next process the message as the coordinator
        if self.signing_round.signer_id == coordinator_id {
            let (messages, results) = self
                .coordinator
                .process_inbound_messages(&inbound_messages)
                .unwrap_or_default();
            outbound_messages.extend(messages);
            operation_results.extend(results);
        }
        debug!(
            "Sending {} messages to other stacker-db instances.",
            outbound_messages.len()
        );
        for msg in outbound_messages {
            let ack = self
                .stackerdb
                .send_message_with_retry(self.signing_round.signer_id, msg.into());
            if let Ok(ack) = ack {
                debug!("ACK: {:?}", ack);
            } else {
                warn!("Failed to send message to stacker-db instance: {:?}", ack);
            }
        }
        operation_results
    }
}

impl From<&Config> for RunLoop<FrostCoordinator<v2::Aggregator>> {
    /// Creates new runloop from a config
    fn from(config: &Config) -> Self {
        // TODO: this should be a config option
        // See: https://github.com/stacks-network/stacks-blockchain/issues/3914
        let threshold = ((config.signer_ids_public_keys.key_ids.len() * 7) / 10)
            .try_into()
            .unwrap();
        let total_signers = config
            .signer_ids_public_keys
            .signers
            .len()
            .try_into()
            .unwrap();
        let total_keys = config
            .signer_ids_public_keys
            .key_ids
            .len()
            .try_into()
            .unwrap();
        let key_ids = config
            .signer_key_ids
            .get(&config.signer_id)
            .unwrap()
            .iter()
            .map(|i| i - 1) // Signer::new (unlike Signer::from) doesn't do this
            .collect::<Vec<u32>>();
        let coordinator_config = CoordinatorConfig {
            threshold,
            num_signers: total_signers,
            num_keys: total_keys,
            message_private_key: config.message_private_key,
            ..Default::default()
        };
        let coordinator = FrostCoordinator::new(coordinator_config);
        let signing_round = Signer::new(
            threshold,
            total_signers,
            total_keys,
            config.signer_id,
            key_ids,
            config.message_private_key,
            config.signer_ids_public_keys.clone(),
        );
        let stacks_client = StacksClient::from(config);
        let stackerdb = StackerDB::from(config);
        RunLoop {
            event_timeout: config.event_timeout,
            coordinator,
            signing_round,
            stacks_client,
            stackerdb,
            commands: VecDeque::new(),
            state: State::Uninitialized,
        }
    }
}

impl<C: Coordinator> SignerRunLoop<Vec<OperationResult>, RunLoopCommand> for RunLoop<C> {
    fn set_event_timeout(&mut self, timeout: Duration) {
        self.event_timeout = timeout;
    }

    fn get_event_timeout(&self) -> Duration {
        self.event_timeout
    }

    fn run_one_pass(
        &mut self,
        event: Option<StackerDBChunksEvent>,
        cmd: Option<RunLoopCommand>,
        res: Sender<Vec<OperationResult>>,
    ) -> Option<Vec<OperationResult>> {
        info!(
            "Running one pass for signer ID# {}. Current state: {:?}",
            self.signing_round.signer_id, self.state
        );
        if let Some(command) = cmd {
            self.commands.push_back(command);
        }
        if self.state == State::Uninitialized {
            let request_fn = || self.initialize().map_err(backoff::Error::transient);
            retry_with_exponential_backoff(request_fn)
                .expect("Failed to connect to initialize due to timeout. Stacks node may be down.");
        }
        // Process any arrived events
        if let Some(event) = event {
            if event.contract_id == *self.stackerdb.miners_contract_id() {
                self.process_event_miner(&event);
            } else if event.contract_id == *self.stackerdb.signers_contract_id() {
                let operation_results = self.process_event_signer(&event);
                let nmb_results = operation_results.len();
                if nmb_results > 0 {
                    // We finished our command. Update the state
                    self.state = State::Idle;
                    match res.send(operation_results) {
                        Ok(_) => debug!("Successfully sent {} operation result(s)", nmb_results),
                        Err(e) => {
                            warn!("Failed to send operation results: {:?}", e);
                        }
                    }
                }
            } else {
                warn!(
                    "Received event from unknown contract ID: {}",
                    event.contract_id
                );
            }
        }
        // The process the next command
        // Must be called AFTER processing the event as the state may update to IDLE due to said event.
        self.process_next_command();
        None
    }
}

/// Helper function for determining the coordinator public key given the the public keys
fn calculate_coordinator(public_keys: &PublicKeys) -> (u32, &ecdsa::PublicKey) {
    // TODO: do some sort of VRF here to calculate the public key
    // See: https://github.com/stacks-network/stacks-blockchain/issues/3915
    // Mockamato just uses the first signer_id as the coordinator for now
    (0, public_keys.signers.get(&0).unwrap())
}

/// TODO: this should not be here.
/// Temporary copy paste from frost-signer
/// See: https://github.com/stacks-network/stacks-blockchain/issues/3913
fn verify_msg(
    m: &Packet,
    public_keys: &PublicKeys,
    coordinator_public_key: &ecdsa::PublicKey,
) -> bool {
    match &m.msg {
        Message::DkgBegin(msg) | Message::DkgPrivateBegin(msg) => {
            if !msg.verify(&m.sig, coordinator_public_key) {
                warn!("Received a DkgPrivateBegin message with an invalid signature.");
                return false;
            }
        }
        Message::DkgEnd(msg) => {
            if let Some(public_key) = public_keys.signers.get(&msg.signer_id) {
                if !msg.verify(&m.sig, public_key) {
                    warn!("Received a DkgPublicEnd message with an invalid signature.");
                    return false;
                }
            } else {
                warn!(
                    "Received a DkgPublicEnd message with an unknown id: {}",
                    msg.signer_id
                );
                return false;
            }
        }
        Message::DkgPublicShares(msg) => {
            if let Some(public_key) = public_keys.signers.get(&msg.signer_id) {
                if !msg.verify(&m.sig, public_key) {
                    warn!("Received a DkgPublicShares message with an invalid signature.");
                    return false;
                }
            } else {
                warn!(
                    "Received a DkgPublicShares message with an unknown id: {}",
                    msg.signer_id
                );
                return false;
            }
        }
        Message::DkgPrivateShares(msg) => {
            // Private shares have key IDs from [0, N) to reference IDs from [1, N]
            // in Frost V4 to enable easy indexing hence ID + 1
            // TODO: Once Frost V5 is released, this off by one adjustment will no longer be required
            if let Some(public_key) = public_keys.signers.get(&msg.signer_id) {
                if !msg.verify(&m.sig, public_key) {
                    warn!("Received a DkgPrivateShares message with an invalid signature from signer_id {} key {}", msg.signer_id, &public_key);
                    return false;
                }
            } else {
                warn!(
                    "Received a DkgPrivateShares message with an unknown id: {}",
                    msg.signer_id
                );
                return false;
            }
        }
        Message::NonceRequest(msg) => {
            if !msg.verify(&m.sig, coordinator_public_key) {
                warn!("Received a NonceRequest message with an invalid signature.");
                return false;
            }
        }
        Message::NonceResponse(msg) => {
            if let Some(public_key) = public_keys.signers.get(&msg.signer_id) {
                if !msg.verify(&m.sig, public_key) {
                    warn!("Received a NonceResponse message with an invalid signature.");
                    return false;
                }
            } else {
                warn!(
                    "Received a NonceResponse message with an unknown id: {}",
                    msg.signer_id
                );
                return false;
            }
        }
        Message::SignatureShareRequest(msg) => {
            if !msg.verify(&m.sig, coordinator_public_key) {
                warn!("Received a SignatureShareRequest message with an invalid signature.");
                return false;
            }
        }
        Message::SignatureShareResponse(msg) => {
            if let Some(public_key) = public_keys.signers.get(&msg.signer_id) {
                if !msg.verify(&m.sig, public_key) {
                    warn!("Received a SignatureShareResponse message with an invalid signature.");
                    return false;
                }
            } else {
                warn!(
                    "Received a SignatureShareResponse message with an unknown id: {}",
                    msg.signer_id
                );
                return false;
            }
        }
    }
    true
}
