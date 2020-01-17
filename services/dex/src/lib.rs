#[cfg(test)]
mod tests;
mod types;

use std::cell::RefCell;
use std::rc::Rc;

use derive_more::{Display, From};
use bytes::Bytes;

use binding_macro::{cycles, genesis, hook_after, read, service, write};
use protocol::traits::{
    ExecutorParams, ServiceSDK, StoreArray, StoreMap, StoreUint64,
};
use protocol::types::{Address, Hash, ServiceContext, ServiceContextParams};
use protocol::{ProtocolError, ProtocolErrorKind, ProtocolResult};

use crate::types::{
    DealStatus, GenesisPayload, GetOrderResponse, ModifyAssetPayload, Order, OrderKind, GetOrderPayload,
    OrderPayload, OrderStatus, Trade, GetTradesResponse, Deal
};

const DEX: Bytes = Bytes::from_static(b"dex");

pub struct DexService<SDK: ServiceSDK> {
    sdk: SDK,
    trades: Box<dyn StoreArray<Trade>>,
    buy_orders: Box<dyn StoreMap<Hash, Order>>,
    sell_orders: Box<dyn StoreMap<Hash, Order>>,
    history_orders: Box<dyn StoreMap<Hash, Order>>,
    valid_limit: Box<dyn StoreUint64>,
}

#[service]
impl<SDK: 'static + ServiceSDK> DexService<SDK> {
    pub fn new(mut sdk: SDK) -> ProtocolResult<Self> {
        let trades: Box<dyn StoreArray<Trade>> = sdk.alloc_or_recover_array("trades")?;
        let buy_orders: Box<dyn StoreMap<Hash, Order>> = sdk.alloc_or_recover_map("buy_orders")?;
        let sell_orders: Box<dyn StoreMap<Hash, Order>> =
            sdk.alloc_or_recover_map("sell_orders")?;
        let history_orders: Box<dyn StoreMap<Hash, Order>> =
            sdk.alloc_or_recover_map("history_orders")?;
        let valid_limit: Box<dyn StoreUint64> = sdk.alloc_or_recover_uint64("valid_limit")?;

        Ok(Self {
            sdk,
            trades,
            buy_orders,
            sell_orders,
            history_orders,
            valid_limit,
        })
    }

    #[genesis]
    fn genesis(&mut self, payload: GenesisPayload) -> ProtocolResult<()> {
        self.valid_limit.set(payload.valid_limit)
    }

    #[cycles(210_00)]
    #[write]
    fn add_trade(&mut self, ctx: ServiceContext, payload: Trade) -> ProtocolResult<Trade> {
        self.trades.push(payload.clone())?;

        Ok(payload)
    }

    #[read]
    fn get_trades(&self, _ctx: ServiceContext) -> ProtocolResult<GetTradesResponse> {
        let mut trades = Vec::<Trade>::new();
        for (_, trade) in self.trades.iter() {
            &trades.push(trade);
        }

        Ok(GetTradesResponse{
            trades,
        })
    }

    #[cycles(210_00)]
    #[write]
    fn order(&mut self, ctx: ServiceContext, payload: OrderPayload) -> ProtocolResult<()> {
        if payload.expiry > ctx.get_current_epoch_id() + self.valid_limit.get()? {
            return Err(DexError::ExpiryExceed.into());
        }

        let order = Order {
            tx_hash: ctx.get_tx_hash().expect("should not fail"),
            kind: payload.kind.clone(),
            price: payload.price,
            amount: payload.amount,
            height: ctx.get_current_epoch_id(),
            user: ctx.get_caller(),
            expiry: payload.expiry,
            status: OrderStatus::Fresh(0),
            deals: Vec::new(),
        };

        match order.kind {
            OrderKind::Buy => {
                let lock_asset_payload = ModifyAssetPayload {
                    asset_id: self.trades.get(0)?.base_asset,
                    user: ctx.get_caller(),
                    value: order.amount*order.price,
                };

                self.lock_asset(lock_asset_payload)?;

                self.buy_orders
                    .insert(ctx.get_tx_hash().expect("should not fail"), order)?
            }
            OrderKind::Sell => {
                let lock_asset_payload = ModifyAssetPayload {
                    asset_id: self.trades.get(0)?.counter_party,
                    user: ctx.get_caller(),
                    value: order.amount,
                };

                self.lock_asset(lock_asset_payload)?;

                self.sell_orders
                    .insert(ctx.get_tx_hash().expect("should not fail"), order)?
            }
        };

        Ok(())
    }

    #[hook_after]
    fn deal(&mut self, params: &ExecutorParams) -> ProtocolResult<()> {
        self.remove_expiry_orders(params.epoch_id)?;

        let mut buy_arr = Vec::<Order>::new();
        for (_, order) in self.buy_orders.iter() {
            buy_arr.push(order);
        }
        buy_arr.sort();

        let mut sell_arr = Vec::<Order>::new();
        for (_, order) in self.sell_orders.iter() {
            sell_arr.push(order);
        }
        sell_arr.sort();

        loop {
            let opt_buy = buy_arr.pop();
            let opt_sell = sell_arr.pop();
            if opt_buy.is_none() || opt_sell.is_none() {
                break;
            }
            let current_buy = opt_buy.unwrap();
            let current_sell = opt_sell.unwrap();
            if current_buy.price < current_sell.price {
                break;
            }
            let deal_price = (current_buy.price + current_sell.price) / 2;

            if current_buy.amount < current_sell.amount {
                let next_sell = self.settle_buyer(deal_price, current_buy.clone(), current_sell.clone())?;
                sell_arr.push(next_sell);
            } else if current_buy.amount > current_sell.amount {
                let next_buy = self.settle_seller(deal_price, current_buy.clone(), current_sell.clone())?;
                buy_arr.push(next_buy);
            } else {
                self.settle_both(deal_price, current_buy.clone(), current_sell.clone())?;
            }
        }

        Ok(())
    }

    fn settle_buyer(&mut self, deal_price: u64, mut current_buy: Order, mut current_sell: Order) -> ProtocolResult<Order> {
        let unlock_buyer = ModifyAssetPayload{
            asset_id: self.trades.get(0)?.base_asset,
            user: current_buy.user.clone(),
            value: current_buy.amount*current_buy.price,
        };
        self.unlock_asset(unlock_buyer)?;

        let add_buyer = ModifyAssetPayload{
            asset_id: self.trades.get(0)?.counter_party,
            user: current_buy.user.clone(),
            value: current_buy.amount,
        };
        self.add_asset(add_buyer)?;

        let sub_buyer = ModifyAssetPayload{
            asset_id: self.trades.get(0)?.base_asset,
            user: current_buy.user.clone(),
            value: current_buy.amount*deal_price,
        };
        self.sub_asset(sub_buyer)?;

        let unlock_seller = ModifyAssetPayload{
            asset_id: self.trades.get(0)?.counter_party,
            user: current_sell.user.clone(),
            value: current_buy.amount,
        };
        self.unlock_asset(unlock_seller)?;

        let add_seller = ModifyAssetPayload{
            asset_id: self.trades.get(0)?.base_asset,
            user: current_sell.user.clone(),
            value: current_buy.amount*deal_price,
        };
        self.add_asset(add_seller)?;

        let sub_seller = ModifyAssetPayload{
            asset_id: self.trades.get(0)?.counter_party,
            user: current_sell.user.clone(),
            value: current_buy.amount,
        };
        self.sub_asset(sub_seller)?;

        let settle_deal = Deal{
            price: deal_price,
            amount: current_buy.amount
        };
        current_buy.status = OrderStatus::Full;
        current_buy.deals.push(settle_deal.clone());

        current_sell.deals.push(settle_deal.clone());
        let mut dealt_amount: u64 = match current_sell.status {
            OrderStatus::Fresh(_) => 0,
            OrderStatus::Partial(v) => v,
            OrderStatus::Full => panic!("should not be full")
        };
        dealt_amount += settle_deal.amount;
        current_sell.status = OrderStatus::Partial(dealt_amount);

        self.buy_orders.remove(&current_buy.tx_hash)?;
        self.history_orders.insert(current_buy.tx_hash.clone(), current_buy)?;

        self.sell_orders.insert(current_sell.tx_hash.clone(), current_sell.clone())?;

        Ok(current_sell)
    }

    fn settle_seller(&mut self, deal_price: u64, mut current_buy: Order, mut current_sell: Order) -> ProtocolResult<Order> {
        let unlock_seller = ModifyAssetPayload{
            asset_id: self.trades.get(0)?.counter_party,
            user: current_sell.user.clone(),
            value: current_sell.amount,
        };
        self.unlock_asset(unlock_seller)?;

        let add_seller = ModifyAssetPayload{
            asset_id: self.trades.get(0)?.base_asset,
            user: current_sell.user.clone(),
            value: current_sell.amount*deal_price,
        };
        self.add_asset(add_seller)?;

        let sub_seller = ModifyAssetPayload{
            asset_id: self.trades.get(0)?.counter_party,
            user: current_sell.user.clone(),
            value: current_sell.amount,
        };
        self.sub_asset(sub_seller)?;

        let unlock_buyer = ModifyAssetPayload{
            asset_id: self.trades.get(0)?.base_asset,
            user: current_buy.user.clone(),
            value: current_sell.amount*deal_price,
        };
        self.unlock_asset(unlock_buyer)?;

        let add_buyer = ModifyAssetPayload{
            asset_id: self.trades.get(0)?.counter_party,
            user: current_buy.user.clone(),
            value: current_sell.amount,
        };
        self.add_asset(add_buyer)?;

        let sub_buyer = ModifyAssetPayload{
            asset_id: self.trades.get(0)?.base_asset,
            user: current_buy.user.clone(),
            value: current_sell.amount*deal_price,
        };
        self.sub_asset(sub_buyer)?;

        let settle_deal = Deal{
            price: deal_price,
            amount: current_sell.amount
        };
        current_sell.status = OrderStatus::Full;
        current_sell.deals.push(settle_deal.clone());

        current_buy.deals.push(settle_deal.clone());
        let mut dealt_amount: u64 = match current_buy.status {
            OrderStatus::Fresh(_) => 0,
            OrderStatus::Partial(v) => v,
            OrderStatus::Full => panic!("should not be full")
        };
        dealt_amount += settle_deal.amount;
        current_buy.status = OrderStatus::Partial(dealt_amount);

        self.sell_orders.remove(&current_sell.tx_hash)?;
        self.history_orders.insert(current_sell.tx_hash.clone(), current_sell)?;

        self.buy_orders.insert(current_buy.tx_hash.clone(), current_buy.clone())?;

        Ok(current_buy)
    }

    fn settle_both(&mut self, deal_price: u64, mut current_buy: Order, mut current_sell: Order) -> ProtocolResult<()> {
        let unlock_seller = ModifyAssetPayload{
            asset_id: self.trades.get(0)?.counter_party,
            user: current_sell.user.clone(),
            value: current_sell.amount,
        };
        self.unlock_asset(unlock_seller)?;

        let add_seller = ModifyAssetPayload{
            asset_id: self.trades.get(0)?.base_asset,
            user: current_sell.user.clone(),
            value: current_sell.amount*deal_price,
        };
        self.add_asset(add_seller)?;

        let sub_seller = ModifyAssetPayload{
            asset_id: self.trades.get(0)?.counter_party,
            user: current_sell.user.clone(),
            value: current_sell.amount,
        };
        self.sub_asset(sub_seller)?;

        let unlock_buyer = ModifyAssetPayload{
            asset_id: self.trades.get(0)?.base_asset,
            user: current_buy.user.clone(),
            value: current_sell.amount*deal_price,
        };
        self.unlock_asset(unlock_buyer)?;

        let add_buyer = ModifyAssetPayload{
            asset_id: self.trades.get(0)?.counter_party,
            user: current_buy.user.clone(),
            value: current_sell.amount,
        };
        self.add_asset(add_buyer)?;

        let sub_buyer = ModifyAssetPayload{
            asset_id: self.trades.get(0)?.base_asset,
            user: current_buy.user.clone(),
            value: current_sell.amount*deal_price,
        };
        self.sub_asset(sub_buyer)?;

        let settle_deal = Deal{
            price: deal_price,
            amount: current_sell.amount
        };
        current_sell.status = OrderStatus::Full;
        current_sell.deals.push(settle_deal.clone());

        current_buy.status = OrderStatus::Full;
        current_buy.deals.push(settle_deal.clone());

        self.sell_orders.remove(&current_sell.tx_hash)?;
        self.history_orders.insert(current_sell.tx_hash.clone(), current_sell)?;

        self.buy_orders.remove(&current_buy.tx_hash)?;
        self.history_orders.insert(current_buy.tx_hash.clone(), current_buy)?;

        Ok(())
    }

    fn lock_asset(&mut self, payload: ModifyAssetPayload) -> ProtocolResult<()> {
        let lock_asset_payload = ModifyAssetPayload {
            asset_id: payload.asset_id.clone(),
            user: payload.user.clone(),
            value: payload.value,
        };

        let payload_str =
            serde_json::to_string(&lock_asset_payload).map_err(DexError::JsonParse)?;

        self.sdk
            .write(&self.get_call_asset_ctx(), Some(DEX.clone()), "asset", "lock", &payload_str)?;

        Ok(())
    }

    fn unlock_asset(&mut self, payload: ModifyAssetPayload) -> ProtocolResult<()> {
        let unlock_asset_payload = ModifyAssetPayload {
            asset_id: payload.asset_id.clone(),
            user: payload.user.clone(),
            value: payload.value,
        };

        let payload_str =
            serde_json::to_string(&unlock_asset_payload).map_err(DexError::JsonParse)?;

        self.sdk
            .write(&self.get_call_asset_ctx(), Some(DEX.clone()), "asset", "unlock", &payload_str)?;

        Ok(())
    }

    fn add_asset(&mut self, payload: ModifyAssetPayload) -> ProtocolResult<()> {
        let add_asset_payload = ModifyAssetPayload {
            asset_id: payload.asset_id.clone(),
            user: payload.user.clone(),
            value: payload.value,
        };

        let payload_str =
            serde_json::to_string(&add_asset_payload).map_err(DexError::JsonParse)?;

        self.sdk
            .write(&self.get_call_asset_ctx(), Some(DEX.clone()), "asset", "add_value", &payload_str)?;

        Ok(())
    }

    fn sub_asset(&mut self, payload: ModifyAssetPayload) -> ProtocolResult<()> {
        let sub_asset_payload = ModifyAssetPayload {
            asset_id: payload.asset_id.clone(),
            user: payload.user.clone(),
            value: payload.value,
        };

        let payload_str =
            serde_json::to_string(&sub_asset_payload).map_err(DexError::JsonParse)?;

        self.sdk
            .write(&self.get_call_asset_ctx(), Some(DEX.clone()), "asset", "sub_value", &payload_str)?;

        Ok(())
    }

    fn get_call_asset_ctx(&self) -> ServiceContext {
        let params = ServiceContextParams {
            tx_hash: None,
            nonce: None,
            cycles_limit: std::u64::MAX,
            cycles_price: std::u64::MAX,
            cycles_used: Rc::new(RefCell::new(0)),
            caller: Address::from_hash(Hash::from_empty()).unwrap(),
            epoch_id: 0,
            timestamp: 0,
            service_name: "".to_owned(),
            service_method: "".to_owned(),
            service_payload: "".to_owned(),
            extra: None,
            events: Rc::new(RefCell::new(vec![])),
        };

        ServiceContext::new(params)
    }

    #[cycles(210_00)]
    #[read]
    fn get_order(&self, ctx: ServiceContext, payload: GetOrderPayload) -> ProtocolResult<GetOrderResponse> {
        if let Ok(order) = self.buy_orders.get(&payload.tx_hash) {
            return Ok(GetOrderResponse::from_order(&order, DealStatus::Dealing));
        } else if let Ok(order) = self.sell_orders.get(&payload.tx_hash) {
            return Ok(GetOrderResponse::from_order(&order, DealStatus::Dealing));
        } else if let Ok(order) = self.history_orders.get(&payload.tx_hash) {
            return Ok(GetOrderResponse::from_order(&order, DealStatus::Dealt));
        }

        Err(DexError::NotFound.into())
    }

    fn remove_expiry_orders(&mut self, current_epoch_id: u64) -> ProtocolResult<()> {
        let mut expiry_buys = Vec::<Hash>::new();
        for (tx_hash, order) in self.buy_orders.iter() {
            if order.expiry < current_epoch_id {
                expiry_buys.push(tx_hash.clone());
            }
        }
        for hash in expiry_buys.iter() {
            self.buy_orders.remove(hash)?;
        }

        let mut expiry_sells = Vec::<Hash>::new();
        for (tx_hash, order) in self.sell_orders.iter() {
            if order.expiry < current_epoch_id {
                expiry_sells.push(tx_hash.clone());
            }
        }
        for hash in expiry_sells.iter() {
            self.sell_orders.remove(hash)?;
        }

        Ok(())
    }
}

#[derive(Debug, Display, From)]
pub enum DexError {
    #[display(fmt = "Parsing payload to json failed {:?}", _0)]
    JsonParse(serde_json::Error),

    #[display(fmt = "Expiry in tx exceed limit")]
    ExpiryExceed,

    #[display(fmt = "Order not found")]
    NotFound,
}

impl std::error::Error for DexError {}

impl From<DexError> for ProtocolError {
    fn from(err: DexError) -> ProtocolError {
        ProtocolError::new(ProtocolErrorKind::Service, Box::new(err))
    }
}
