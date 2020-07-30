use std::cmp::Ordering;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use protocol::fixed_codec::{FixedCodec, FixedCodecError};
use protocol::types::{Address, Hash};
use protocol::ProtocolResult;

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct GenesisPayload {
    pub order_validity: u64,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct Trade {
    pub id: Hash,
    pub base_asset: Hash,
    pub counter_party: Hash,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
pub struct AddTradePayload {
    pub base_asset: Hash,
    pub counter_party: Hash,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default)]
pub struct GetTradesResponse {
    pub trades: Vec<Trade>,
}

#[derive(Deserialize, Serialize, Eq, PartialEq, Clone, Default)]
pub struct Order {
    pub trade_id: Hash,
    pub tx_hash: Hash,
    pub kind: OrderKind,
    pub price: u64,
    pub amount: u64,
    pub height: u64,
    pub user: Address,
    pub expiry: u64,
    pub status: OrderStatus,
    pub deals: Vec<Deal>,
}

#[derive(Deserialize, Serialize, Clone, Debug, Eq, PartialEq)]
pub enum OrderKind {
    Buy,
    Sell,
}

impl Default for OrderKind {
    fn default() -> Self {
        OrderKind::Buy
    }
}

#[derive(Deserialize, Serialize, Clone, Debug, Eq, PartialEq)]
pub enum OrderStatus {
    Fresh,
    Partial(u64),
    Full,
}

impl Default for OrderStatus {
    fn default() -> Self {
        OrderStatus::Fresh
    }
}

#[derive(Deserialize, Serialize, Clone, Debug, Eq, PartialEq)]
pub struct Deal {
    pub price: u64,
    pub amount: u64,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub enum DealStatus {
    Dealing,
    Dealt,
}

impl Default for DealStatus {
    fn default() -> Self {
        Self::Dealing
    }
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct OrderPayload {
    pub trade_id: Hash,
    pub kind: OrderKind,
    pub price: u64,
    pub amount: u64,
    pub expiry: u64,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct GetOrderPayload {
    pub tx_hash: Hash,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default)]
pub struct GetOrderResponse {
    pub trade_id: Hash,
    pub tx_hash: Hash,
    pub kind: OrderKind,
    pub price: u64,
    pub amount: u64,
    pub height: u64,
    pub user: Address,
    pub expiry: u64,
    pub order_status: OrderStatus,
    pub deal_status: DealStatus,
    pub deals: Vec<Deal>,
}

impl GetOrderResponse {
    pub fn from_order(order: &Order, status: DealStatus) -> Self {
        Self {
            trade_id: order.trade_id.clone(),
            tx_hash: order.tx_hash.clone(),
            kind: order.kind.clone(),
            price: order.price,
            amount: order.amount,
            height: order.height,
            user: order.user.clone(),
            expiry: order.expiry,
            order_status: order.status.clone(),
            deal_status: status,
            deals: order.deals.clone(),
        }
    }
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct ModifyAssetPayload {
    pub asset_id: Hash,
    pub user: Address,
    pub value: u64,
}

impl rlp::Encodable for Trade {
    fn rlp_append(&self, s: &mut rlp::RlpStream) {
        s.begin_list(3)
            .append(&self.id)
            .append(&self.base_asset)
            .append(&self.counter_party);
    }
}

impl rlp::Decodable for Trade {
    fn decode(r: &rlp::Rlp) -> Result<Self, rlp::DecoderError> {
        if !r.is_list() && r.size() != 3 {
            return Err(rlp::DecoderError::RlpIncorrectListLen);
        }

        let id = rlp::decode(r.at(0)?.as_raw())?;
        let base_asset = rlp::decode(r.at(1)?.as_raw())?;
        let counter_party = rlp::decode(r.at(2)?.as_raw())?;

        Ok(Trade {
            id,
            base_asset,
            counter_party,
        })
    }
}

impl FixedCodec for Trade {
    fn encode_fixed(&self) -> ProtocolResult<Bytes> {
        Ok(Bytes::from(rlp::encode(self)))
    }

    fn decode_fixed(bytes: Bytes) -> ProtocolResult<Self> {
        Ok(rlp::decode(bytes.as_ref()).map_err(FixedCodecError::from)?)
    }
}

impl rlp::Encodable for Order {
    fn rlp_append(&self, s: &mut rlp::RlpStream) {
        s.begin_list(11)
            .append(&self.trade_id)
            .append(&self.tx_hash);
        match self.kind {
            OrderKind::Buy => s.append(&1u64),
            OrderKind::Sell => s.append(&2u64),
        };

        s.append(&self.price)
            .append(&self.amount)
            .append(&self.height)
            .append(&self.user)
            .append(&self.expiry);

        match self.status {
            OrderStatus::Fresh => s.append(&0u64).append(&0u64),
            OrderStatus::Partial(v) => s.append(&1u64).append(&v),
            OrderStatus::Full => s.append(&2u64).append(&0u64),
        };

        s.append_list(&self.deals);
    }
}

impl rlp::Decodable for Order {
    fn decode(r: &rlp::Rlp) -> Result<Self, rlp::DecoderError> {
        if !r.is_list() && r.size() != 11 {
            return Err(rlp::DecoderError::RlpIncorrectListLen);
        }

        let trade_id = rlp::decode(r.at(0)?.as_raw())?;
        let tx_hash = rlp::decode(r.at(1)?.as_raw())?;
        let kind = match r.at(2)?.as_val::<u64>()? {
            1 => OrderKind::Buy,
            2 => OrderKind::Sell,
            _ => unreachable!(),
        };

        let price = r.at(3)?.as_val::<u64>()?;
        let amount = r.at(4)?.as_val::<u64>()?;
        let height = r.at(5)?.as_val::<u64>()?;
        let user = rlp::decode(r.at(6)?.as_raw())?;
        let expiry = r.at(7)?.as_val::<u64>()?;
        let status = match r.at(8)?.as_val::<u64>()? {
            0 => OrderStatus::Fresh,
            1 => OrderStatus::Partial(r.at(9)?.as_val::<u64>()?),
            2 => OrderStatus::Full,
            _ => unreachable!(),
        };

        let deals: Vec<Deal> = rlp::decode_list(r.at(10)?.as_raw());

        Ok(Order {
            trade_id,
            tx_hash,
            kind,
            price,
            amount,
            height,
            user,
            expiry,
            status,
            deals,
        })
    }
}

impl FixedCodec for Order {
    fn encode_fixed(&self) -> ProtocolResult<Bytes> {
        Ok(Bytes::from(rlp::encode(self)))
    }

    fn decode_fixed(bytes: Bytes) -> ProtocolResult<Self> {
        Ok(rlp::decode(bytes.as_ref()).map_err(FixedCodecError::from)?)
    }
}

impl rlp::Encodable for Deal {
    fn rlp_append(&self, s: &mut rlp::RlpStream) {
        s.begin_list(2).append(&self.price).append(&self.amount);
    }
}

impl rlp::Decodable for Deal {
    fn decode(r: &rlp::Rlp) -> Result<Self, rlp::DecoderError> {
        if !r.is_list() && r.size() != 2 {
            return Err(rlp::DecoderError::RlpIncorrectListLen);
        }

        let price = r.at(0)?.as_val::<u64>()?;
        let amount = r.at(1)?.as_val::<u64>()?;

        Ok(Deal { price, amount })
    }
}

impl FixedCodec for Deal {
    fn encode_fixed(&self) -> ProtocolResult<Bytes> {
        Ok(Bytes::from(rlp::encode(self)))
    }

    fn decode_fixed(bytes: Bytes) -> ProtocolResult<Self> {
        Ok(rlp::decode(bytes.as_ref()).map_err(FixedCodecError::from)?)
    }
}

impl PartialOrd for Order {
    fn partial_cmp(&self, other: &Order) -> Option<Ordering> {
        match (self.kind.clone(), other.kind.clone()) {
            (OrderKind::Sell, OrderKind::Sell) => {
                if self.price > other.price {
                    Some(Ordering::Less)
                } else if self.price < other.price {
                    Some(Ordering::Greater)
                } else {
                    Some(self.height.cmp(&other.height))
                }
            }
            (OrderKind::Buy, OrderKind::Buy) => {
                if self.price > other.price {
                    Some(Ordering::Greater)
                } else if self.price < other.price {
                    Some(Ordering::Less)
                } else {
                    Some(self.height.cmp(&other.height))
                }
            }
            _ => None,
        }
    }
}

impl Ord for Order {
    fn cmp(&self, other: &Order) -> Ordering {
        self.partial_cmp(other).expect("unreachable")
    }
}
