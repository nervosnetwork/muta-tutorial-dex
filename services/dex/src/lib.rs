#[cfg(test)]
mod tests;
mod types;

use std::cell::RefCell;
use std::convert::From;
use std::rc::Rc;

use bytes::Bytes;
use derive_more::Display;

use binding_macro::{cycles, genesis, hook_after, read, service, write};
use protocol::traits::{ExecutorParams, ServiceResponse, ServiceSDK, StoreMap, StoreUint64};
use protocol::types::{Address, Hash, ServiceContext, ServiceContextParams};

use crate::types::{
    AddTradePayload, Deal, DealStatus, GenesisPayload, GetOrderPayload, GetOrderResponse,
    GetTradesResponse, ModifyAssetPayload, Order, OrderKind, OrderPayload, OrderStatus, Trade,
};
use asset::types::ModifyBalancePayload;
use asset::AssetFacade;

const ADMISSION_TOKEN: Bytes = Bytes::from_static(b"dex_token");
const TRADES_KEY: &str = "trades";
const BUY_ORDERS_KEY: &str = "buy_orders";
const SELL_ORDERS_KEY: &str = "sell_orders";
const HISTORY_ORDERS_KEY: &str = "history_orders";
const VALIDITY_KEY: &str = "validity";

/*
call a method which returns ServiceResponse.
if the return is ok, get the data,
if the return is error, 'return' it
 */

macro_rules! call_and_parse_service_response {
    ($self: expr, $method: ident) => {{
        let res: ServiceResponse<_> = $self.$method();
        if res.is_error() {
            return ServiceResponse::from_error(res.code, res.error_message);
        } else {
            res.succeed_data
        }
    }};
    ($self: expr, $method: ident, $payload: expr) => {{
        let res: ServiceResponse<_> = $self.$method($payload);
        if res.is_error() {
            return ServiceResponse::from_error(res.code, res.error_message);
        } else {
            res.succeed_data
        }
    }};
}

macro_rules! serde_json_string {
    ($payload: expr) => {
        match serde_json::to_string(&$payload).map_err(DexError::JsonParse) {
            Ok(s) => s,
            Err(e) => return e.into(),
        };
    };
}

macro_rules! check_get_or_return {
    ($service_response:expr) => {{
        if $service_response.is_error() {
            return ServiceResponse::from_error(
                $service_response.code,
                $service_response.error_message,
            );
        } else {
            $service_response.succeed_data
        }
    }};
}

pub struct DexService<SDK: ServiceSDK, A> {
    _sdk: SDK,
    trades: Box<dyn StoreMap<Hash, Trade>>,
    buy_orders: Box<dyn StoreMap<Hash, Order>>,
    sell_orders: Box<dyn StoreMap<Hash, Order>>,
    history_orders: Box<dyn StoreMap<Hash, Order>>,
    validity: Box<dyn StoreUint64>,
    asset: A,
}

// we done have any facade function for DexService cause no one else will call it
pub trait DexFacade {}

impl<SDK: ServiceSDK, A> DexFacade for DexService<SDK, A> {}

#[service]
impl<SDK: 'static + ServiceSDK, A: AssetFacade> DexService<SDK, A> {
    pub fn new(mut sdk: SDK, asset: A) -> Self {
        let trades: Box<dyn StoreMap<Hash, Trade>> = sdk.alloc_or_recover_map(TRADES_KEY);
        let buy_orders: Box<dyn StoreMap<Hash, Order>> = sdk.alloc_or_recover_map(BUY_ORDERS_KEY);
        let sell_orders: Box<dyn StoreMap<Hash, Order>> = sdk.alloc_or_recover_map(SELL_ORDERS_KEY);
        let history_orders: Box<dyn StoreMap<Hash, Order>> =
            sdk.alloc_or_recover_map(HISTORY_ORDERS_KEY);
        let validity: Box<dyn StoreUint64> = sdk.alloc_or_recover_uint64(VALIDITY_KEY);

        Self {
            _sdk: sdk,
            trades,
            buy_orders,
            sell_orders,
            history_orders,
            validity,
            asset,
        }
    }

    #[genesis]
    fn init_genesis(&mut self, payload: GenesisPayload) {
        self.validity.set(payload.order_validity)
    }

    #[cycles(210_00)]
    #[write]
    fn add_trade(&mut self, ctx: ServiceContext, payload: AddTradePayload) -> ServiceResponse<()> {
        let base_asset = payload.base_asset;
        let counter_party = payload.counter_party;

        if base_asset == counter_party {
            return DexError::IllegalTrade.into();
        }

        let trade_id = if base_asset < counter_party {
            Hash::digest(Bytes::from(base_asset.as_hex() + &counter_party.as_hex()))
        } else {
            Hash::digest(Bytes::from(counter_party.as_hex() + &base_asset.as_hex()))
        };

        if self.trades.contains(&trade_id) {
            return DexError::TradeExisted.into();
        }

        let trade = Trade {
            id: trade_id.clone(),
            base_asset,
            counter_party,
        };

        self.trades.insert(trade_id, trade.clone());
        let event_json = serde_json_string!(trade);
        ctx.emit_event("AddTrade".to_owned(), event_json);
        ServiceResponse::from_succeed(())
    }

    #[read]
    fn get_trades(&self, _ctx: ServiceContext) -> ServiceResponse<GetTradesResponse> {
        let mut trades = Vec::<Trade>::new();
        for (_, trade) in self.trades.iter() {
            trades.push(trade);
        }

        ServiceResponse::from_succeed(GetTradesResponse { trades })
    }

    #[cycles(210_00)]
    #[write]
    fn order(&mut self, ctx: ServiceContext, payload: OrderPayload) -> ServiceResponse<()> {
        let trade_id = payload.trade_id;
        if !self.trades.contains(&trade_id) {
            return DexError::TradeNotExisted.into();
        }
        if payload.expiry > ctx.get_current_height() + self.validity.get() {
            return DexError::OrderOverdue.into();
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
                let trade = check_get_or_return!(self.get_trade(trade_id.clone()));

                let lock_asset_payload = ModifyAssetPayload {
                    asset_id: trade.base_asset,
                    user: ctx.get_caller(),
                    value: order.amount * order.price,
                };

                call_and_parse_service_response!(self, lock_asset, lock_asset_payload);
                self.buy_orders.insert(
                    ctx.get_tx_hash().expect("tx hash should exist"),
                    order.clone(),
                )
            }
            OrderKind::Sell => {
                let trade = check_get_or_return!(self.get_trade(trade_id.clone()));

                let lock_asset_payload = ModifyAssetPayload {
                    asset_id: trade.counter_party,
                    user: ctx.get_caller(),
                    value: order.amount,
                };

                call_and_parse_service_response!(self, lock_asset, lock_asset_payload);

                self.sell_orders.insert(
                    ctx.get_tx_hash().expect("tx hash should exist"),
                    order.clone(),
                )
            }
        };

        let event_json = serde_json_string!(order);
        ctx.emit_event("Order".to_owned(), event_json);
        ServiceResponse::from_succeed(())
    }

    #[read]
    fn get_order(
        &self,
        _ctx: ServiceContext,
        payload: GetOrderPayload,
    ) -> ServiceResponse<GetOrderResponse> {
        if let Some(order) = self.buy_orders.get(&payload.tx_hash) {
            return ServiceResponse::from_succeed(GetOrderResponse::from_order(
                &order,
                DealStatus::Dealing,
            ));
        } else if let Some(order) = self.sell_orders.get(&payload.tx_hash) {
            return ServiceResponse::from_succeed(GetOrderResponse::from_order(
                &order,
                DealStatus::Dealing,
            ));
        } else if let Some(order) = self.history_orders.get(&payload.tx_hash) {
            return ServiceResponse::from_succeed(GetOrderResponse::from_order(
                &order,
                DealStatus::Dealt,
            ));
        }

        DexError::OrderNotExisted.into()
    }

    #[hook_after]
    fn match_and_deal(&mut self, params: &ExecutorParams) {
        self.remove_expiry_orders(params.height);

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
                );

                if next_sell.is_error() {
                    continue;
                }
                sell_queue.push(next_sell.succeed_data);
            } else if buy_left > sell_left {
                let next_buy = self.settle_seller(
                    deal_price,
                    sell_left,
                    current_buy.clone(),
                    current_sell.clone(),
                );
                if next_buy.is_error() {
                    continue;
                }
                buy_queue.push(next_buy.succeed_data);
            } else {
                self.settle_both(
                    deal_price,
                    buy_left,
                    current_buy.clone(),
                    current_sell.clone(),
                );
            }
        }
        ()
    }

    fn settle_buyer(
        &mut self,
        deal_price: u64,
        deal_amount: u64,
        mut current_buy: Order,
        mut current_sell: Order,
    ) -> ServiceResponse<Order> {
        let trade_id = current_buy.trade_id.clone();
        let trade = check_get_or_return!(self.get_trade(trade_id.clone()));

        let unlock_buyer = ModifyAssetPayload {
            asset_id: trade.base_asset.clone(),
            user: current_buy.user.clone(),
            value: deal_amount * current_buy.price,
        };
        call_and_parse_service_response!(self, unlock_asset, unlock_buyer);

        let add_buyer = ModifyAssetPayload {
            asset_id: trade.counter_party.clone(),
            user: current_buy.user.clone(),
            value: deal_amount,
        };
        call_and_parse_service_response!(self, add_value, add_buyer);

        let sub_buyer = ModifyAssetPayload {
            asset_id: trade.base_asset.clone(),
            user: current_buy.user.clone(),
            value: deal_amount * deal_price,
        };

        call_and_parse_service_response!(self, sub_value, sub_buyer);

        let unlock_seller = ModifyAssetPayload {
            asset_id: trade.counter_party.clone(),
            user: current_sell.user.clone(),
            value: deal_amount,
        };

        call_and_parse_service_response!(self, unlock_asset, unlock_seller);

        let add_seller = ModifyAssetPayload {
            asset_id: trade.base_asset,
            user: current_sell.user.clone(),
            value: deal_amount * deal_price,
        };

        call_and_parse_service_response!(self, add_value, add_seller);

        let sub_seller = ModifyAssetPayload {
            asset_id: trade.counter_party,
            user: current_sell.user.clone(),
            value: deal_amount,
        };

        call_and_parse_service_response!(self, sub_value, sub_seller);

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

        self.buy_orders.remove(&current_buy.tx_hash);
        self.history_orders
            .insert(current_buy.tx_hash.clone(), current_buy);

        self.sell_orders
            .insert(current_sell.tx_hash.clone(), current_sell.clone());

        ServiceResponse::from_succeed(current_sell)
    }

    fn settle_seller(
        &mut self,
        deal_price: u64,
        deal_amount: u64,
        mut current_buy: Order,
        mut current_sell: Order,
    ) -> ServiceResponse<Order> {
        let trade_id = current_buy.trade_id.clone();
        let trade = check_get_or_return!(self.get_trade(trade_id.clone()));

        let unlock_seller = ModifyAssetPayload {
            asset_id: trade.counter_party.clone(),
            user: current_sell.user.clone(),
            value: deal_amount,
        };

        call_and_parse_service_response!(self, unlock_asset, unlock_seller);

        let add_seller = ModifyAssetPayload {
            asset_id: trade.base_asset.clone(),
            user: current_sell.user.clone(),
            value: deal_amount * deal_price,
        };

        call_and_parse_service_response!(self, add_value, add_seller);

        let sub_seller = ModifyAssetPayload {
            asset_id: trade.counter_party.clone(),
            user: current_sell.user.clone(),
            value: deal_amount,
        };

        call_and_parse_service_response!(self, sub_value, sub_seller);

        let unlock_buyer = ModifyAssetPayload {
            asset_id: trade.base_asset.clone(),
            user: current_buy.user.clone(),
            value: deal_amount * current_buy.price,
        };

        call_and_parse_service_response!(self, unlock_asset, unlock_buyer);

        let add_buyer = ModifyAssetPayload {
            asset_id: trade.counter_party,
            user: current_buy.user.clone(),
            value: deal_amount,
        };

        call_and_parse_service_response!(self, add_value, add_buyer);

        let sub_buyer = ModifyAssetPayload {
            asset_id: trade.base_asset,
            user: current_buy.user.clone(),
            value: deal_amount * deal_price,
        };

        call_and_parse_service_response!(self, sub_value, sub_buyer);

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

        self.sell_orders.remove(&current_sell.tx_hash);
        self.history_orders
            .insert(current_sell.tx_hash.clone(), current_sell);

        self.buy_orders
            .insert(current_buy.tx_hash.clone(), current_buy.clone());

        ServiceResponse::from_succeed(current_buy)
    }

    fn settle_both(
        &mut self,
        deal_price: u64,
        deal_amount: u64,
        mut current_buy: Order,
        mut current_sell: Order,
    ) -> ServiceResponse<()> {
        let trade_id = current_buy.trade_id.clone();
        let trade = check_get_or_return!(self.get_trade(trade_id.clone()));

        let unlock_seller = ModifyAssetPayload {
            asset_id: trade.counter_party.clone(),
            user: current_sell.user.clone(),
            value: deal_amount,
        };
        call_and_parse_service_response!(self, unlock_asset, unlock_seller);

        let add_seller = ModifyAssetPayload {
            asset_id: trade.base_asset.clone(),
            user: current_sell.user.clone(),
            value: deal_amount * deal_price,
        };
        call_and_parse_service_response!(self, add_value, add_seller);

        let sub_seller = ModifyAssetPayload {
            asset_id: trade.counter_party.clone(),
            user: current_sell.user.clone(),
            value: deal_amount,
        };
        call_and_parse_service_response!(self, sub_value, sub_seller);

        let unlock_buyer = ModifyAssetPayload {
            asset_id: trade.base_asset.clone(),
            user: current_buy.user.clone(),
            value: deal_amount * current_buy.price,
        };
        call_and_parse_service_response!(self, unlock_asset, unlock_buyer);

        let add_buyer = ModifyAssetPayload {
            asset_id: trade.counter_party,
            user: current_buy.user.clone(),
            value: deal_amount,
        };
        call_and_parse_service_response!(self, add_value, add_buyer);

        let sub_buyer = ModifyAssetPayload {
            asset_id: trade.base_asset,
            user: current_buy.user.clone(),
            value: deal_amount * deal_price,
        };
        call_and_parse_service_response!(self, sub_value, sub_buyer);

        let settle_deal = Deal {
            price: deal_price,
            amount: deal_amount,
        };
        current_sell.status = OrderStatus::Full;
        current_sell.deals.push(settle_deal.clone());

        current_buy.status = OrderStatus::Full;
        current_buy.deals.push(settle_deal.clone());

        self.sell_orders.remove(&current_sell.tx_hash);
        self.history_orders
            .insert(current_sell.tx_hash.clone(), current_sell);

        self.buy_orders.remove(&current_buy.tx_hash);
        self.history_orders
            .insert(current_buy.tx_hash.clone(), current_buy);

        ServiceResponse::from_succeed(())
    }

    fn lock_asset(&mut self, payload: ModifyAssetPayload) -> ServiceResponse<()> {
        let lock_asset_payload = ModifyBalancePayload {
            asset_id: payload.asset_id.clone(),
            user: payload.user.clone(),
            value: payload.value,
        };

        self.asset
            .lock(self.get_call_asset_ctx(), lock_asset_payload)
    }

    fn unlock_asset(&mut self, payload: ModifyAssetPayload) -> ServiceResponse<()> {
        let unlock_asset_payload = ModifyBalancePayload {
            asset_id: payload.asset_id.clone(),
            user: payload.user.clone(),
            value: payload.value,
        };

        self.asset
            .unlock(self.get_call_asset_ctx(), unlock_asset_payload)
    }

    fn add_value(&mut self, payload: ModifyAssetPayload) -> ServiceResponse<()> {
        let add_asset_payload = ModifyBalancePayload {
            asset_id: payload.asset_id.clone(),
            user: payload.user.clone(),
            value: payload.value,
        };

        self.asset
            .add_value(self.get_call_asset_ctx(), add_asset_payload)
    }

    fn sub_value(&mut self, payload: ModifyAssetPayload) -> ServiceResponse<()> {
        let sub_asset_payload = ModifyBalancePayload {
            asset_id: payload.asset_id.clone(),
            user: payload.user.clone(),
            value: payload.value,
        };

        self.asset
            .sub_value(self.get_call_asset_ctx(), sub_asset_payload)
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
            extra: Some(ADMISSION_TOKEN.clone()),
            events: Rc::new(RefCell::new(vec![])),
        };

        ServiceContext::new(params)
    }

    fn remove_expiry_orders(&mut self, current_height: u64) {
        let mut expiry_buys = Vec::<(Hash, Order)>::new();
        for (tx_hash, order) in self.buy_orders.iter() {
            if order.expiry < current_height {
                expiry_buys.push((tx_hash.clone(), order.clone()));
            }
        }
        for (hash, order) in expiry_buys.iter() {
            self.buy_orders.remove(hash);
            let unlock_amount = match order.status {
                OrderStatus::Fresh => order.amount,
                OrderStatus::Partial(p) => order.amount - p,
                OrderStatus::Full => 0,
            };
            if unlock_amount != 0 {
                let payload = ModifyAssetPayload {
                    asset_id: self.trades.get(&order.trade_id).unwrap().base_asset,
                    user: order.user.clone(),
                    value: unlock_amount,
                };
                self.unlock_asset(payload);
            }
            self.history_orders
                .insert(order.tx_hash.clone(), order.clone());
        }

        let mut expiry_sells = Vec::<(Hash, Order)>::new();
        for (tx_hash, order) in self.sell_orders.iter() {
            if order.expiry < current_height {
                expiry_sells.push((tx_hash.clone(), order.clone()));
            }
        }
        for (hash, order) in expiry_sells.iter() {
            self.sell_orders.remove(hash);
            let unlock_amount = match order.status {
                OrderStatus::Fresh => order.amount,
                OrderStatus::Partial(p) => order.amount - p,
                OrderStatus::Full => 0,
            };
            if unlock_amount != 0 {
                let payload = ModifyAssetPayload {
                    asset_id: self.trades.get(&order.trade_id).unwrap().counter_party,
                    user: order.user.clone(),
                    value: unlock_amount,
                };
                self.unlock_asset(payload);
            }
            self.history_orders
                .insert(order.tx_hash.clone(), order.clone());
        }
    }

    fn get_trade(&self, trade_id: Hash) -> ServiceResponse<Trade> {
        match self.trades.get(&trade_id) {
            Some(trade) => ServiceResponse::from_succeed(trade),
            None => DexError::TradeNotExisted.into(),
        }
    }
}

#[derive(Debug, Display)]
pub enum DexError {
    #[display(fmt = "Parsing payload to json failed {:?}", _0)]
    JsonParse(serde_json::Error),

    IllegalTrade,

    TradeExisted,

    TradeNotExisted,

    OrderOverdue,

    OrderNotExisted,
}

impl DexError {
    fn code(&self) -> u64 {
        match self {
            DexError::JsonParse(_) => 201,
            DexError::IllegalTrade { .. } => 202,
            DexError::TradeExisted { .. } => 203,
            DexError::TradeNotExisted { .. } => 204,
            DexError::OrderOverdue => 205,
            DexError::OrderNotExisted => 206,
        }
    }
}

impl<T: Default> From<DexError> for ServiceResponse<T> {
    fn from(err: DexError) -> ServiceResponse<T> {
        ServiceResponse::from_error(err.code(), err.to_string())
    }
}
