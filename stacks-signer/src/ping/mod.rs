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

mod periodic_pinger;

use std::{fmt, fmt::Debug};

use libstackerdb::StackerDBChunkData;
pub use periodic_pinger::{PeriodicPinger, PingStopHandle};
use rand_core::{OsRng, RngCore};
use serde_derive::{Deserialize, Serialize};
use slog::slog_warn;
use stacks_common::warn;

use crate::client::{SignerMessage, PING_SLOT_ID, SIGNER_SLOTS_PER_USER};

/// Is an incoming slot update a ping::Packet?
/// Use it to filter out other slots.
pub fn is_ping_slot(slot_id: u32) -> bool {
    let Some(v) = slot_id.checked_sub(PING_SLOT_ID) else {
        return false;
    };

    v % SIGNER_SLOTS_PER_USER == 0
}

/// What is written to the ping slot.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum Packet {
    /// Outgoing
    Ping(Ping),
    /// Incoming
    Pong(Pong),
}

#[derive(Serialize, Deserialize, PartialEq, Clone)]
/// A ping in a slot means someone has requested you to push a Pong into your slot.
pub struct Ping {
    id: u64,
    payload: Vec<u8>,
}
#[derive(Serialize, Deserialize, PartialEq, Clone)]
/// A pong in a slot means someone has responded to an RTT request.
pub struct Pong {
    id: u64,
    payload: Vec<u8>,
}

impl From<Pong> for Packet {
    fn from(value: Pong) -> Self {
        Self::Pong(value)
    }
}

impl From<Ping> for Packet {
    fn from(value: Ping) -> Self {
        Self::Ping(value)
    }
}

impl Ping {
    /// Uniquely identify the RTT request
    pub fn new(payload_size: usize) -> Self {
        let mut payload = Vec::with_capacity(payload_size);
        OsRng.fill_bytes(payload.as_mut_slice());
        Ping {
            id: OsRng.next_u64(),
            payload,
        }
    }

    /// Pong receives its fields from a ping.
    pub fn pong(self) -> Pong {
        Pong {
            id: self.id,
            payload: self.payload,
        }
    }

    /// getter
    pub fn id(&self) -> u64 {
        self.id
    }
}

impl Pong {
    /// getter
    pub fn id(&self) -> u64 {
        self.id
    }
}

impl Debug for Ping {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Ping").field("id", &self.id).finish()
    }
}

impl Debug for Pong {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pong").field("id", &self.id).finish()
    }
}

impl Packet {
    /// Whether a Packet needs to be processed or skipped
    pub fn verify_packet(chunk: &StackerDBChunkData, signer_id: u32) -> Option<Result<Self, ()>> {
        if !is_ping_slot(chunk.slot_id) {
            return None;
        }

        let msg: SignerMessage = bincode::deserialize(&chunk.data)
            .map_err(|e| {
                warn!("Failed to deserialize ping slot {}: {e}", chunk.slot_id);
            })
            .ok()?;

        let self_slot_id = msg.slot_id(signer_id);

        let SignerMessage::Ping(packet) = msg else {
            return None;
        };

        // don't process your own events
        if self_slot_id == chunk.slot_id {
            return Some(Err(()));
        }

        Some(Ok(packet))
    }
}

#[cfg(test)]
mod tests {
    use std::vec;

    use super::*;
    use crate::client::SignerMessage;
    use assert_matches::assert_matches;
    use stacks_common::util::secp256k1::MessageSignature;

    #[test]
    fn same_slot_for_ping_pong() {
        let ping_packet: SignerMessage = Ping::new(0).into();
        assert_eq!(
            ping_packet.slot_id(1),
            SignerMessage::from(Pong {
                id: 2,
                payload: vec![]
            })
            .slot_id(1)
        );
    }

    #[test]
    fn sane_is_ping_slot() {
        assert!(!is_ping_slot(0));
        assert!(!is_ping_slot(1));
        assert!(!is_ping_slot(SIGNER_SLOTS_PER_USER));
        assert!(is_ping_slot(SIGNER_SLOTS_PER_USER + PING_SLOT_ID));
        assert!(is_ping_slot(PING_SLOT_ID));
    }

    #[test]
    fn debug_skips_load_field() {
        let ping = Ping::new(1);
        let ping_string = format!("{ping:?}");
        let _p = &ping.payload;

        assert!(!ping_string.contains("payload"));
    }

    #[test]
    fn sane_verify_packet() {
        // Ignore your own messages
        let mut signer_id = 0;
        let mut chunk = StackerDBChunkData {
            // Not ping slot
            slot_id: 0,
            slot_version: 0,
            sig: MessageSignature::empty(),
            // Not a ping packet
            data: vec![],
        };
        // Not ping slot
        assert!(Packet::verify_packet(&chunk, signer_id).is_none());
        chunk.slot_id = PING_SLOT_ID;
        // Not a ping packet
        assert!(Packet::verify_packet(&chunk, signer_id).is_none());
        let msg: SignerMessage = Ping::new(0).into();
        chunk.data = bincode::serialize(&msg).unwrap();
        // Ignore your own messages
        assert_matches!(Packet::verify_packet(&chunk, signer_id), Some(packet) => {
            assert!(packet.is_err());
        });
        signer_id += 1;
        assert_matches!(Packet::verify_packet(&chunk, signer_id), Some(packet) => {
            assert!(packet.is_ok());
        });
    }
}
