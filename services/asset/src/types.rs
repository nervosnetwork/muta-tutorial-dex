use bytes::Bytes;
use serde::{Deserialize, Serialize};

use protocol::fixed_codec::{FixedCodec, FixedCodecError};
use protocol::types::{Address, Hash};
use protocol::ProtocolResult;

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct InitGenesisPayload {
    pub id: Hash,
    pub name: String,
    pub symbol: String,
    pub supply: u64,
    pub issuer: Address,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
pub struct Asset {
    pub id: Hash,
    pub name: String,
    pub symbol: String,
    pub supply: u64,
    pub issuer: Address,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Default)]
pub struct Balance {
    pub current: u64,
    pub locked: u64,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct CreateAssetPayload {
    pub name: String,
    pub symbol: String,
    pub supply: u64,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct GetAssetPayload {
    pub id: Hash,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct GetBalancePayload {
    pub asset_id: Hash,
    pub user: Address,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct GetBalanceResponse {
    pub asset_id: Hash,
    pub balance: Balance,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct ModifyBalancePayload {
    pub asset_id: Hash,
    pub user: Address,
    pub value: u64,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct TransferPayload {
    pub asset_id: Hash,
    pub to: Address,
    pub value: u64,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct TransferEvent {
    pub asset_id: Hash,
    pub from: Address,
    pub to: Address,
    pub value: u64,
}

impl rlp::Decodable for Asset {
    fn decode(rlp: &rlp::Rlp) -> Result<Self, rlp::DecoderError> {
        Ok(Self {
            id: rlp.at(0)?.as_val()?,
            name: rlp.at(1)?.as_val()?,
            symbol: rlp.at(2)?.as_val()?,
            supply: rlp.at(3)?.as_val()?,
            issuer: rlp.at(4)?.as_val()?,
        })
    }
}

impl rlp::Encodable for Asset {
    fn rlp_append(&self, s: &mut rlp::RlpStream) {
        s.begin_list(5)
            .append(&self.id)
            .append(&self.name)
            .append(&self.symbol)
            .append(&self.supply)
            .append(&self.issuer);
    }
}

impl FixedCodec for Asset {
    fn encode_fixed(&self) -> ProtocolResult<Bytes> {
        Ok(Bytes::from(rlp::encode(self)))
    }

    fn decode_fixed(bytes: Bytes) -> ProtocolResult<Self> {
        Ok(rlp::decode(bytes.as_ref()).map_err(FixedCodecError::from)?)
    }
}

impl rlp::Decodable for Balance {
    fn decode(rlp: &rlp::Rlp) -> Result<Self, rlp::DecoderError> {
        Ok(Self {
            current: rlp.at(0)?.as_val()?,
            locked: rlp.at(1)?.as_val()?,
        })
    }
}

impl rlp::Encodable for Balance {
    fn rlp_append(&self, s: &mut rlp::RlpStream) {
        s.begin_list(2).append(&self.current).append(&self.locked);
    }
}

impl FixedCodec for Balance {
    fn encode_fixed(&self) -> ProtocolResult<Bytes> {
        Ok(Bytes::from(rlp::encode(self)))
    }

    fn decode_fixed(bytes: Bytes) -> ProtocolResult<Self> {
        Ok(rlp::decode(bytes.as_ref()).map_err(FixedCodecError::from)?)
    }
}
