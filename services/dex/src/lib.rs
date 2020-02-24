#[cfg(test)]
mod tests;
mod types;

use std::cell::RefCell;
use std::rc::Rc;

use bytes::Bytes;
use derive_more::{Display, From};

use binding_macro::{cycles, genesis, hook_after, read, service, write};
use protocol::traits::{ExecutorParams, ServiceSDK, StoreMap, StoreUint64};
use protocol::types::{Address, Hash, ServiceContext, ServiceContextParams};
use protocol::{ProtocolError, ProtocolErrorKind, ProtocolResult};

use crate::types::{
    AddTradePayload, Deal, DealStatus, GenesisPayload, GetOrderPayload, GetOrderResponse,
    GetTradesResponse, ModifyAssetPayload, Order, OrderKind, OrderPayload, OrderStatus, Trade,
};

const ADMISSION_TOKEN: Bytes = Bytes::from_static(b"dex_token");
const TRADES_KEY: &str = "trades";
const BUY_ORDERS_KEY: &str = "buy_orders";
const SELL_ORDERS_KEY: &str = "sell_orders";
const HISTORY_ORDERS_KEY: &str = "history_orders";
const VALIDITY_KEY: &str = "validity";

pub struct DexService<SDK: ServiceSDK> {
    sdk: SDK,
    trades: Box<dyn StoreMap<Hash, Trade>>,
    buy_orders: Box<dyn StoreMap<Hash, Order>>,
    sell_orders: Box<dyn StoreMap<Hash, Order>>,
    history_orders: Box<dyn StoreMap<Hash, Order>>,
    validity: Box<dyn StoreUint64>,
}

#[service]
impl<SDK: 'static + ServiceSDK> DexService<SDK> {
    pub fn new(mut sdk: SDK) -> ProtocolResult<Self> {
        let trades: Box<dyn StoreMap<Hash, Trade>> = sdk.alloc_or_recover_map(TRADES_KEY)?;
        let buy_orders: Box<dyn StoreMap<Hash, Order>> =
            sdk.alloc_or_recover_map(BUY_ORDERS_KEY)?;
        let sell_orders: Box<dyn StoreMap<Hash, Order>> =
            sdk.alloc_or_recover_map(SELL_ORDERS_KEY)?;
        let history_orders: Box<dyn StoreMap<Hash, Order>> =
            sdk.alloc_or_recover_map(HISTORY_ORDERS_KEY)?;
        let validity: Box<dyn StoreUint64> = sdk.alloc_or_recover_uint64(VALIDITY_KEY)?;

        Ok(Self {
            sdk,
            trades,
            buy_orders,
            sell_orders,
            history_orders,
            validity,
        })
    }

    #[genesis]
    fn init_genesis(&mut self, payload: GenesisPayload) -> ProtocolResult<()> {
        self.validity.set(payload.order_validity)
    }

    #[cycles(210_00)]
    #[write]
    fn add_trade(&mut self, ctx: ServiceContext, payload: AddTradePayload) -> ProtocolResult<()> {
        let base_asset = payload.base_asset;
        let counter_party = payload.counter_party;

        if base_asset == counter_party {
            return Err(DexError::IllegalTrade.into());
        }

        let trade_id = if base_asset < counter_party {
            Hash::digest(Bytes::from(base_asset.as_hex() + &counter_party.as_hex()))
        } else {
            Hash::digest(Bytes::from(counter_party.as_hex() + &base_asset.as_hex()))
        };

        if self.trades.contains(&trade_id)? {
            return Err(DexError::TradeExisted.into());
        }

        let trade = Trade {
            id: trade_id.clone(),
            base_asset,
            counter_party,
        };

        self.trades.insert(trade_id, trade.clone())?;
        let event_json = serde_json::to_string(&trade).map_err(DexError::JsonParse)?;
        ctx.emit_event(event_json)
    }

    #[read]
    fn get_trades(&self, _ctx: ServiceContext) -> ProtocolResult<GetTradesResponse> {
        let mut trades = Vec::<Trade>::new();
        for (_, trade) in self.trades.iter() {
            trades.push(trade);
        }

        Ok(GetTradesResponse { trades })
    }

    #[cycles(210_00)]
    #[write]
    fn order(&mut self, ctx: ServiceContext, payload: OrderPayload) -> ProtocolResult<()> {
        let trade_id = payload.trade_id;
        if !self.trades.contains(&trade_id)? {
            return Err(DexError::TradeNotExisted.into());
        }
        if payload.expiry > ctx.get_current_height() + self.validity.get()? {
            return Err(DexError::OrderOverdue.into());
        }

        let order = Order {
            trade_id: trade_id.clone(),
            tx_hash: ctx.get_tx_hash().expect("tx hash should exist"),
            kind: payload.kind.clone(),
            price: payload.price,
            amount: payload.amount,
            height: ctx.get_current_height(),
            user: ctx.get_caller(),
            expiry: payload.expiry,
            status: OrderStatus::Fresh,
            deals: Vec::new(),
        };

        match order.kind {
            OrderKind::Buy => {
                let lock_asset_payload = ModifyAssetPayload {
                    asset_id: self.trades.get(&trade_id)?.base_asset,
                    user: ctx.get_caller(),
                    value: order.amount * order.price,
                };

                self.lock_asset(lock_asset_payload)?;
                self.buy_orders.insert(
                    ctx.get_tx_hash().expect("tx hash should exist"),
                    order.clone(),
                )?
            }
            OrderKind::Sell => {
                let lock_asset_payload = ModifyAssetPayload {
                    asset_id: self.trades.get(&trade_id)?.counter_party,
                    user: ctx.get_caller(),
                    value: order.amount,
                };

                self.lock_asset(lock_asset_payload)?;
                self.sell_orders.insert(
                    ctx.get_tx_hash().expect("tx hash should exist"),
                    order.clone(),
                )?
            }
        };

        let event_json = serde_json::to_string(&order).map_err(DexError::JsonParse)?;
        ctx.emit_event(event_json)
    }

    #[read]
    fn get_order(
        &self,
        _ctx: ServiceContext,
        payload: GetOrderPayload,
    ) -> ProtocolResult<GetOrderResponse> {
        if let Ok(order) = self.buy_orders.get(&payload.tx_hash) {
            return Ok(GetOrderResponse::from_order(&order, DealStatus::Dealing));
        } else if let Ok(order) = self.sell_orders.get(&payload.tx_hash) {
            return Ok(GetOrderResponse::from_order(&order, DealStatus::Dealing));
        } else if let Ok(order) = self.history_orders.get(&payload.tx_hash) {
            return Ok(GetOrderResponse::from_order(&order, DealStatus::Dealt));
        }

        Err(DexError::OrderNotExisted.into())
    }

    #[hook_after]
    fn match_and_deal(&mut self, params: &ExecutorParams) -> ProtocolResult<()> {
        self.remove_expiry_orders(params.height)?;

        let mut buy_queue = Vec::<Order>::new();
        for (_, order) in self.buy_orders.iter() {
            buy_queue.push(order);
        }
        buy_queue.sort();

        let mut sell_queue = Vec::<Order>::new();
        for (_, order) in self.sell_orders.iter() {
            sell_queue.push(order);
        }
        sell_queue.sort();

        loop {
            let opt_buy = buy_queue.pop();
            let opt_sell = sell_queue.pop();
            if opt_buy.is_none() || opt_sell.is_none() {
                break;
            }
            let current_buy = opt_buy.unwrap();
            let current_sell = opt_sell.unwrap();
            if current_buy.price < current_sell.price {
                break;
            }
            let deal_price = (current_buy.price + current_sell.price) / 2;

            let buy_left = match current_buy.status {
                OrderStatus::Fresh => current_buy.amount,
                OrderStatus::Partial(v) => current_buy.amount - v,
                OrderStatus::Full => unreachable!(),
            };

            let sell_left = match current_sell.status {
                OrderStatus::Fresh => current_sell.amount,
                OrderStatus::Partial(v) => current_sell.amount - v,
                OrderStatus::Full => unreachable!(),
            };

            if buy_left < sell_left {
                let next_sell = self.settle_buyer(
                    deal_price,
                    buy_left,
                    current_buy.clone(),
                    current_sell.clone(),
                )?;
                sell_queue.push(next_sell);
            } else if buy_left > sell_left {
                let next_buy = self.settle_seller(
                    deal_price,
                    sell_left,
                    current_buy.clone(),
                    current_sell.clone(),
                )?;
                buy_queue.push(next_buy);
            } else {
                self.settle_both(
                    deal_price,
                    buy_left,
                    current_buy.clone(),
                    current_sell.clone(),
                )?;
            }
        }

        Ok(())
    }

    fn settle_buyer(
        &mut self,
        deal_price: u64,
        deal_amount: u64,
        mut current_buy: Order,
        mut current_sell: Order,
    ) -> ProtocolResult<Order> {
        let trade_id = current_buy.trade_id.clone();
        let unlock_buyer = ModifyAssetPayload {
            asset_id: self.trades.get(&trade_id)?.base_asset,
            user: current_buy.user.clone(),
            value: deal_amount * current_buy.price,
        };
        self.unlock_asset(unlock_buyer)?;

        let add_buyer = ModifyAssetPayload {
            asset_id: self.trades.get(&trade_id)?.counter_party,
            user: current_buy.user.clone(),
            value: deal_amount,
        };
        self.add_value(add_buyer)?;

        let sub_buyer = ModifyAssetPayload {
            asset_id: self.trades.get(&trade_id)?.base_asset,
            user: current_buy.user.clone(),
            value: deal_amount * deal_price,
        };
        self.sub_value(sub_buyer)?;

        let unlock_seller = ModifyAssetPayload {
            asset_id: self.trades.get(&trade_id)?.counter_party,
            user: current_sell.user.clone(),
            value: deal_amount,
        };
        self.unlock_asset(unlock_seller)?;

        let add_seller = ModifyAssetPayload {
            asset_id: self.trades.get(&trade_id)?.base_asset,
            user: current_sell.user.clone(),
            value: deal_amount * deal_price,
        };
        self.add_value(add_seller)?;

        let sub_seller = ModifyAssetPayload {
            asset_id: self.trades.get(&trade_id)?.counter_party,
            user: current_sell.user.clone(),
            value: deal_amount,
        };
        self.sub_value(sub_seller)?;

        let settle_deal = Deal {
            price: deal_price,
            amount: deal_amount,
        };
        current_buy.status = OrderStatus::Full;
        current_buy.deals.push(settle_deal.clone());

        current_sell.deals.push(settle_deal.clone());
        let mut dealt_amount: u64 = match current_sell.status {
            OrderStatus::Fresh => 0,
            OrderStatus::Partial(v) => v,
            OrderStatus::Full => panic!("should not be full"),
        };
        dealt_amount += settle_deal.amount;
        current_sell.status = OrderStatus::Partial(dealt_amount);

        self.buy_orders.remove(&current_buy.tx_hash)?;
        self.history_orders
            .insert(current_buy.tx_hash.clone(), current_buy)?;

        self.sell_orders
            .insert(current_sell.tx_hash.clone(), current_sell.clone())?;

        Ok(current_sell)
    }

    fn settle_seller(
        &mut self,
        deal_price: u64,
        deal_amount: u64,
        mut current_buy: Order,
        mut current_sell: Order,
    ) -> ProtocolResult<Order> {
        let trade_id = current_buy.trade_id.clone();
        let unlock_seller = ModifyAssetPayload {
            asset_id: self.trades.get(&trade_id)?.counter_party,
            user: current_sell.user.clone(),
            value: deal_amount,
        };
        self.unlock_asset(unlock_seller)?;

        let add_seller = ModifyAssetPayload {
            asset_id: self.trades.get(&trade_id)?.base_asset,
            user: current_sell.user.clone(),
            value: deal_amount * deal_price,
        };
        self.add_value(add_seller)?;

        let sub_seller = ModifyAssetPayload {
            asset_id: self.trades.get(&trade_id)?.counter_party,
            user: current_sell.user.clone(),
            value: deal_amount,
        };
        self.sub_value(sub_seller)?;

        let unlock_buyer = ModifyAssetPayload {
            asset_id: self.trades.get(&trade_id)?.base_asset,
            user: current_buy.user.clone(),
            value: deal_amount * current_buy.price,
        };
        self.unlock_asset(unlock_buyer)?;

        let add_buyer = ModifyAssetPayload {
            asset_id: self.trades.get(&trade_id)?.counter_party,
            user: current_buy.user.clone(),
            value: deal_amount,
        };
        self.add_value(add_buyer)?;

        let sub_buyer = ModifyAssetPayload {
            asset_id: self.trades.get(&trade_id)?.base_asset,
            user: current_buy.user.clone(),
            value: deal_amount * deal_price,
        };
        self.sub_value(sub_buyer)?;

        let settle_deal = Deal {
            price: deal_price,
            amount: deal_amount,
        };
        current_sell.status = OrderStatus::Full;
        current_sell.deals.push(settle_deal.clone());

        current_buy.deals.push(settle_deal.clone());
        let mut dealt_amount: u64 = match current_buy.status {
            OrderStatus::Fresh => 0,
            OrderStatus::Partial(v) => v,
            OrderStatus::Full => panic!("should not be full"),
        };
        dealt_amount += settle_deal.amount;
        current_buy.status = OrderStatus::Partial(dealt_amount);

        self.sell_orders.remove(&current_sell.tx_hash)?;
        self.history_orders
            .insert(current_sell.tx_hash.clone(), current_sell)?;

        self.buy_orders
            .insert(current_buy.tx_hash.clone(), current_buy.clone())?;

        Ok(current_buy)
    }

    fn settle_both(
        &mut self,
        deal_price: u64,
        deal_amount: u64,
        mut current_buy: Order,
        mut current_sell: Order,
    ) -> ProtocolResult<()> {
        let trade_id = current_buy.trade_id.clone();
        let unlock_seller = ModifyAssetPayload {
            asset_id: self.trades.get(&trade_id)?.counter_party,
            user: current_sell.user.clone(),
            value: deal_amount,
        };
        self.unlock_asset(unlock_seller)?;

        let add_seller = ModifyAssetPayload {
            asset_id: self.trades.get(&trade_id)?.base_asset,
            user: current_sell.user.clone(),
            value: deal_amount * deal_price,
        };
        self.add_value(add_seller)?;

        let sub_seller = ModifyAssetPayload {
            asset_id: self.trades.get(&trade_id)?.counter_party,
            user: current_sell.user.clone(),
            value: deal_amount,
        };
        self.sub_value(sub_seller)?;

        let unlock_buyer = ModifyAssetPayload {
            asset_id: self.trades.get(&trade_id)?.base_asset,
            user: current_buy.user.clone(),
            value: deal_amount * current_buy.price,
        };
        self.unlock_asset(unlock_buyer)?;

        let add_buyer = ModifyAssetPayload {
            asset_id: self.trades.get(&trade_id)?.counter_party,
            user: current_buy.user.clone(),
            value: deal_amount,
        };
        self.add_value(add_buyer)?;

        let sub_buyer = ModifyAssetPayload {
            asset_id: self.trades.get(&trade_id)?.base_asset,
            user: current_buy.user.clone(),
            value: deal_amount * deal_price,
        };
        self.sub_value(sub_buyer)?;

        let settle_deal = Deal {
            price: deal_price,
            amount: deal_amount,
        };
        current_sell.status = OrderStatus::Full;
        current_sell.deals.push(settle_deal.clone());

        current_buy.status = OrderStatus::Full;
        current_buy.deals.push(settle_deal.clone());

        self.sell_orders.remove(&current_sell.tx_hash)?;
        self.history_orders
            .insert(current_sell.tx_hash.clone(), current_sell)?;

        self.buy_orders.remove(&current_buy.tx_hash)?;
        self.history_orders
            .insert(current_buy.tx_hash.clone(), current_buy)?;

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

        self.sdk.write(
            &self.get_call_asset_ctx(),
            Some(ADMISSION_TOKEN.clone()),
            "asset",
            "lock",
            &payload_str,
        )?;

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

        self.sdk.write(
            &self.get_call_asset_ctx(),
            Some(ADMISSION_TOKEN.clone()),
            "asset",
            "unlock",
            &payload_str,
        )?;

        Ok(())
    }

    fn add_value(&mut self, payload: ModifyAssetPayload) -> ProtocolResult<()> {
        let add_asset_payload = ModifyAssetPayload {
            asset_id: payload.asset_id.clone(),
            user: payload.user.clone(),
            value: payload.value,
        };

        let payload_str = serde_json::to_string(&add_asset_payload).map_err(DexError::JsonParse)?;

        self.sdk.write(
            &self.get_call_asset_ctx(),
            Some(ADMISSION_TOKEN.clone()),
            "asset",
            "add_value",
            &payload_str,
        )?;

        Ok(())
    }

    fn sub_value(&mut self, payload: ModifyAssetPayload) -> ProtocolResult<()> {
        let sub_asset_payload = ModifyAssetPayload {
            asset_id: payload.asset_id.clone(),
            user: payload.user.clone(),
            value: payload.value,
        };

        let payload_str = serde_json::to_string(&sub_asset_payload).map_err(DexError::JsonParse)?;

        self.sdk.write(
            &self.get_call_asset_ctx(),
            Some(ADMISSION_TOKEN.clone()),
            "asset",
            "sub_value",
            &payload_str,
        )?;

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
            height: 0,
            timestamp: 0,
            service_name: "".to_owned(),
            service_method: "".to_owned(),
            service_payload: "".to_owned(),
            extra: None,
            events: Rc::new(RefCell::new(vec![])),
        };

        ServiceContext::new(params)
    }

    fn remove_expiry_orders(&mut self, current_height: u64) -> ProtocolResult<()> {
        let mut expiry_buys = Vec::<(Hash, Order)>::new();
        for (tx_hash, order) in self.buy_orders.iter() {
            if order.expiry < current_height {
                expiry_buys.push((tx_hash.clone(), order.clone()));
            }
        }
        for (hash, order) in expiry_buys.iter() {
            self.buy_orders.remove(hash)?;
            let unlock_amount = match order.status {
                OrderStatus::Fresh => order.amount,
                OrderStatus::Partial(p) => order.amount - p,
                OrderStatus::Full => 0,
            };
            if unlock_amount != 0 {
                let payload = ModifyAssetPayload {
                    asset_id: self.trades.get(&order.trade_id)?.base_asset,
                    user: order.user.clone(),
                    value: unlock_amount,
                };
                self.unlock_asset(payload)?;
            }
            self.history_orders
                .insert(order.tx_hash.clone(), order.clone())?;
        }

        let mut expiry_sells = Vec::<(Hash, Order)>::new();
        for (tx_hash, order) in self.sell_orders.iter() {
            if order.expiry < current_height {
                expiry_sells.push((tx_hash.clone(), order.clone()));
            }
        }
        for (hash, order) in expiry_sells.iter() {
            self.sell_orders.remove(hash)?;
            let unlock_amount = match order.status {
                OrderStatus::Fresh => order.amount,
                OrderStatus::Partial(p) => order.amount - p,
                OrderStatus::Full => 0,
            };
            if unlock_amount != 0 {
                let payload = ModifyAssetPayload {
                    asset_id: self.trades.get(&order.trade_id)?.counter_party,
                    user: order.user.clone(),
                    value: unlock_amount,
                };
                self.unlock_asset(payload)?;
            }
            self.history_orders
                .insert(order.tx_hash.clone(), order.clone())?;
        }

        Ok(())
    }
}

#[derive(Debug, Display, From)]
pub enum DexError {
    #[display(fmt = "Parsing payload to json failed {:?}", _0)]
    JsonParse(serde_json::Error),

    IllegalTrade,

    TradeExisted,

    TradeNotExisted,

    OrderOverdue,

    OrderNotExisted,
}

impl std::error::Error for DexError {}

impl From<DexError> for ProtocolError {
    fn from(err: DexError) -> ProtocolError {
        ProtocolError::new(ProtocolErrorKind::Service, Box::new(err))
    }
}
