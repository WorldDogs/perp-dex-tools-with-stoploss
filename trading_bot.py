"""
Modular Trading Bot - Supports multiple exchanges
"""

import os
import time
import random
import asyncio
import traceback
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Dict, Optional

from exchanges import ExchangeFactory
from helpers import TradingLogger
from helpers.lark_bot import LarkBot


@dataclass
class TradingConfig:
    """Configuration class for trading parameters."""
    ticker: str
    contract_id: str
    quantity: Decimal
    take_profit: Decimal
    tick_size: Decimal
    direction: str
    max_orders: int
    wait_time: int
    exchange: str
    grid_step: Decimal
    stop_price: Decimal
    pause_price: Decimal
    aster_boost: bool
    max_position_loss: Decimal = field(default=Decimal(-1))
    wait_time_min: Optional[int] = None
    wait_time_max: Optional[int] = None

    @property
    def close_order_side(self) -> str:
        """Get the close order side based on bot direction."""
        return 'buy' if self.direction == "sell" else 'sell'


@dataclass
class OrderMonitor:
    """Thread-safe order monitoring state."""
    order_id: Optional[str] = None
    filled: bool = False
    filled_price: Optional[Decimal] = None
    filled_qty: Decimal = 0.0

    def reset(self):
        """Reset the monitor state."""
        self.order_id = None
        self.filled = False
        self.filled_price = None
        self.filled_qty = 0.0


class TradingBot:
    """Modular Trading Bot - Main trading logic supporting multiple exchanges."""

    def __init__(self, config: TradingConfig):
        self.config = config
        self.logger = TradingLogger(config.exchange, config.ticker, log_to_console=True)

        # Create exchange client
        try:
            self.exchange_client = ExchangeFactory.create_exchange(
                config.exchange,
                config
            )
        except ValueError as e:
            raise ValueError(f"Failed to create exchange client: {e}")

        # Trading state
        self.active_close_orders = []
        self.last_close_orders = 0
        self.last_open_order_time = 0
        self.last_log_time = 0
        self.current_order_status = None
        self.order_filled_event = asyncio.Event()
        self.order_canceled_event = asyncio.Event()
        self.shutdown_requested = False
        self.loop = None
        self.open_trades: Dict[str, Dict[str, Decimal]] = {}
        self.current_wait_target = self._select_wait_interval(Decimal(max(self.config.wait_time, 0)))

        # Register order callback
        self._setup_websocket_handlers()

    async def graceful_shutdown(self, reason: str = "Unknown"):
        """Perform graceful shutdown of the trading bot."""
        self.logger.log(f"Starting graceful shutdown: {reason}", "INFO")
        self.shutdown_requested = True

        try:
            # Disconnect from exchange
            await self.exchange_client.disconnect()
            self.logger.log("Graceful shutdown completed", "INFO")

        except Exception as e:
            self.logger.log(f"Error during graceful shutdown: {e}", "ERROR")

    def _setup_websocket_handlers(self):
        """Setup WebSocket handlers for order updates."""
        def order_update_handler(message):
            """Handle order updates from WebSocket."""
            try:
                # Check if this is for our contract
                if message.get('contract_id') != self.config.contract_id:
                    return

                order_id = message.get('order_id')
                status = message.get('status')
                side = message.get('side', '')
                order_type = message.get('order_type', '')
                size = self._safe_decimal(message.get('size'))
                price = self._safe_decimal(message.get('price'))
                filled_size = self._safe_decimal(message.get('filled_size'))
                if order_type == "OPEN":
                    self.current_order_status = status

                if status == 'FILLED':
                    if order_type == "OPEN":
                        self.order_filled_amount = filled_size
                        # Ensure thread-safe interaction with asyncio event loop
                        if self.loop is not None:
                            self.loop.call_soon_threadsafe(self.order_filled_event.set)
                        else:
                            # Fallback (should not happen after run() starts)
                            self.order_filled_event.set()

                    self.logger.log(f"[{order_type}] [{order_id}] {status} {size} @ {price}", "INFO")
                    if order_type == "CLOSE":
                        self._remove_trade(order_id)
                    self.logger.log_transaction(order_id, side, message.get('size'), message.get('price'), status)
                elif status == "CANCELED":
                    if order_type == "OPEN":
                        self.order_filled_amount = filled_size
                        if self.loop is not None:
                            self.loop.call_soon_threadsafe(self.order_canceled_event.set)
                        else:
                            self.order_canceled_event.set()

                        if self.order_filled_amount > 0:
                            self.logger.log_transaction(order_id, side, self.order_filled_amount, message.get('price'), status)

                    self.logger.log(f"[{order_type}] [{order_id}] {status} {size} @ {price}", "INFO")
                    if order_type == "CLOSE":
                        self._update_trade_quantity(order_id, size, filled_size)
                elif status == "PARTIALLY_FILLED":
                    self.logger.log(f"[{order_type}] [{order_id}] {status} {filled_size} @ {price}", "INFO")
                    if order_type == "CLOSE":
                        self._update_trade_quantity(order_id, size, filled_size)
                else:
                    self.logger.log(f"[{order_type}] [{order_id}] {status} {size} @ {price}", "INFO")

            except Exception as e:
                self.logger.log(f"Error handling order update: {e}", "ERROR")
                self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")

        # Setup order update handler
        self.exchange_client.setup_order_update_handler(order_update_handler)

    @staticmethod
    def _safe_decimal(value) -> Decimal:
        if isinstance(value, Decimal):
            return value
        try:
            if value is None:
                return Decimal(0)
            return Decimal(str(value))
        except (InvalidOperation, ValueError, TypeError):
            return Decimal(0)

    def _register_trade(self, order_id: Optional[str], entry_price, quantity) -> None:
        if not order_id:
            return
        entry = self._safe_decimal(entry_price)
        qty = self._safe_decimal(quantity)
        if qty <= 0:
            return
        self.open_trades[order_id] = {
            'entry_price': entry,
            'quantity': qty
        }

    def _update_trade_quantity(self, order_id: Optional[str], order_size, filled_size) -> None:
        if not order_id or order_id not in self.open_trades:
            return
        size = self._safe_decimal(order_size)
        filled = self._safe_decimal(filled_size)
        remaining = size - filled
        if remaining <= 0:
            self.open_trades.pop(order_id, None)
        else:
            self.open_trades[order_id]['quantity'] = remaining

    def _remove_trade(self, order_id: Optional[str]) -> None:
        if order_id and order_id in self.open_trades:
            self.open_trades.pop(order_id, None)

    def _calculate_wait_time(self) -> Decimal:
        """Calculate wait time between orders."""
        cool_down_time = self.current_wait_target if self.current_wait_target > 0 else self.config.wait_time

        if len(self.active_close_orders) < self.last_close_orders:
            self.last_close_orders = len(self.active_close_orders)
            return 0

        self.last_close_orders = len(self.active_close_orders)
        if len(self.active_close_orders) >= self.config.max_orders:
            return 1

        if self.config.max_orders > 0:
            ratio = len(self.active_close_orders) / self.config.max_orders
        else:
            ratio = 0

        base_wait = self.current_wait_target if self.current_wait_target > 0 else self.config.wait_time

        if ratio >= 2/3:
            cool_down_time = 2 * base_wait
        elif ratio >= 1/3:
            cool_down_time = base_wait
        elif ratio >= 1/6:
            cool_down_time = base_wait / 2
        else:
            cool_down_time = base_wait / 4

        # if the program detects active_close_orders during startup, it is necessary to consider cooldown_time
        if self.last_open_order_time == 0 and len(self.active_close_orders) > 0:
            self.last_open_order_time = time.time()

        if time.time() - self.last_open_order_time > cool_down_time:
            return 0
        else:
            return 1

    def _select_wait_interval(self, base_wait: Decimal) -> float:
        if base_wait <= 0:
            return 0.0

        min_cfg = self.config.wait_time_min
        max_cfg = self.config.wait_time_max

        if min_cfg is None and max_cfg is None:
            return float(base_wait)

        low = float(min_cfg) if min_cfg is not None else float(base_wait)
        high = float(max_cfg) if max_cfg is not None else float(base_wait)

        if low > high:
            low, high = high, low

        low = max(0.0, low)
        high = max(low, high)

        if high == low:
            return low

        return random.uniform(low, high)

    @staticmethod
    def _calculate_loss_percentage(entry_price: Decimal, mark_price: Decimal, direction: str) -> Decimal:
        if entry_price <= 0:
            return Decimal(0)

        if direction == 'buy':
            loss = entry_price - mark_price
        else:
            loss = mark_price - entry_price

        if loss <= 0:
            return Decimal(0)

        return (loss / entry_price) * Decimal(100)

    async def _check_position_loss(self) -> bool:
        if self.config.max_position_loss is None or self.config.max_position_loss <= 0:
            return False

        if not self.open_trades:
            return False

        total_qty = Decimal(0)
        weighted_entry = Decimal(0)

        for trade in self.open_trades.values():
            qty = self._safe_decimal(trade.get('quantity'))
            entry = self._safe_decimal(trade.get('entry_price'))
            total_qty += qty
            weighted_entry += entry * qty

        if total_qty <= 0:
            return False

        avg_entry = weighted_entry / total_qty

        best_bid, best_ask = await self.exchange_client.fetch_bbo_prices(self.config.contract_id)
        if best_bid <= 0 or best_ask <= 0:
            raise ValueError("No bid/ask data available")

        mark_price = best_bid if self.config.direction == 'buy' else best_ask
        loss_pct = self._calculate_loss_percentage(avg_entry, mark_price, self.config.direction)

        if loss_pct >= self.config.max_position_loss:
            await self._trigger_position_loss(total_qty, best_bid, best_ask, loss_pct)
            return True

        return False

    async def _trigger_position_loss(self, total_qty: Decimal, best_bid: Decimal,
                                     best_ask: Decimal, loss_pct: Decimal) -> None:
        self.logger.log(
            f"[MAX-LOSS] Position loss {loss_pct}% reached limit {self.config.max_position_loss}%", "WARNING"
        )

        for order_id in list(self.open_trades.keys()):
            try:
                await self.exchange_client.cancel_order(order_id)
            except Exception as e:
                self.logger.log(f"[MAX-LOSS] Failed to cancel order {order_id}: {e}", "ERROR")

        close_side = self.config.close_order_side
        close_price = best_bid if close_side == 'sell' else best_ask

        stop_result = await self.exchange_client.place_close_order(
            self.config.contract_id,
            total_qty,
            close_price,
            close_side,
            post_only=False
        )

        if stop_result.success:
            self.logger.log(
                f"[MAX-LOSS] Submitted emergency close order {stop_result.order_id} for {total_qty}",
                "WARNING"
            )
        else:
            self.logger.log(
                f"[MAX-LOSS] Failed to submit emergency close order: {stop_result.error_message}",
                "ERROR"
            )

        self.open_trades.clear()

    async def _place_and_monitor_open_order(self) -> bool:
        """Place an order and monitor its execution."""
        try:
            # Reset state before placing order
            self.order_filled_event.clear()
            self.current_order_status = 'OPEN'
            self.order_filled_amount = 0.0

            # Place the order
            order_result = await self.exchange_client.place_open_order(
                self.config.contract_id,
                self.config.quantity,
                self.config.direction
            )

            if not order_result.success:
                self.logger.log(f"Failed to place order: {order_result.error_message}", "ERROR")
                return False

            if order_result.status == 'FILLED':
                return await self._handle_order_result(order_result)
            elif not self.order_filled_event.is_set():
                try:
                    await asyncio.wait_for(self.order_filled_event.wait(), timeout=10)
                except asyncio.TimeoutError:
                    pass

            # Handle order result
            return await self._handle_order_result(order_result)

        except Exception as e:
            self.logger.log(f"Error placing order: {e}", "ERROR")
            self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")
            return False

    async def _handle_order_result(self, order_result) -> bool:
        """Handle the result of an order placement."""
        order_id = order_result.order_id
        filled_price = order_result.price

        if self.order_filled_event.is_set() or order_result.status == 'FILLED':
            if self.config.aster_boost:
                self.last_open_order_time = time.time()
                self.current_wait_target = self._select_wait_interval(Decimal(max(self.config.wait_time, 0)))
                close_order_result = await self.exchange_client.place_market_order(
                    self.config.contract_id,
                    self.config.quantity,
                    self.config.close_order_side
                )
            else:
                self.last_open_order_time = time.time()
                self.current_wait_target = self._select_wait_interval(Decimal(max(self.config.wait_time, 0)))
                # Place close order
                close_side = self.config.close_order_side
                if close_side == 'sell':
                    close_price = filled_price * (1 + self.config.take_profit/100)
                else:
                    close_price = filled_price * (1 - self.config.take_profit/100)

                close_order_result = await self.exchange_client.place_close_order(
                    self.config.contract_id,
                    self.config.quantity,
                    close_price,
                    close_side
                )

                if not close_order_result.success:
                    self.logger.log(f"[CLOSE] Failed to place close order: {close_order_result.error_message}", "ERROR")
                else:
                    tracked_qty = close_order_result.size or self.config.quantity
                    self._register_trade(close_order_result.order_id, filled_price, tracked_qty)

                return True

        else:
            self.order_canceled_event.clear()
            # Cancel the order if it's still open
            self.logger.log(f"[OPEN] [{order_id}] Cancelling order and placing a new order", "INFO")
            try:
                cancel_result = await self.exchange_client.cancel_order(order_id)
                if not cancel_result.success:
                    self.order_canceled_event.set()
                    self.logger.log(f"[CLOSE] Failed to cancel order {order_id}: {cancel_result.error_message}", "ERROR")
                else:
                    self.current_order_status = "CANCELED"

            except Exception as e:
                self.order_canceled_event.set()
                self.logger.log(f"[CLOSE] Error canceling order {order_id}: {e}", "ERROR")

            if self.config.exchange == "backpack":
                self.order_filled_amount = cancel_result.filled_size
            else:
                # Wait for cancel event or timeout
                if not self.order_canceled_event.is_set():
                    try:
                        await asyncio.wait_for(self.order_canceled_event.wait(), timeout=5)
                    except asyncio.TimeoutError:
                        order_info = await self.exchange_client.get_order_info(order_id)
                        self.order_filled_amount = order_info.filled_size

            if self.order_filled_amount > 0:
                close_side = self.config.close_order_side
                if self.config.aster_boost:
                    close_order_result = await self.exchange_client.place_close_order(
                        self.config.contract_id,
                        self.order_filled_amount,
                        close_side
                    )
                else:
                    if close_side == 'sell':
                        close_price = filled_price * (1 + self.config.take_profit/100)
                    else:
                        close_price = filled_price * (1 - self.config.take_profit/100)

                    close_order_result = await self.exchange_client.place_close_order(
                        self.config.contract_id,
                        self.order_filled_amount,
                        close_price,
                        close_side
                    )
                self.last_open_order_time = time.time()
                self.current_wait_target = self._select_wait_interval(Decimal(max(self.config.wait_time, 0)))

                if not close_order_result.success:
                    self.logger.log(f"[CLOSE] Failed to place close order: {close_order_result.error_message}", "ERROR")
                else:
                    tracked_qty = close_order_result.size or self.order_filled_amount
                    self._register_trade(close_order_result.order_id, filled_price, tracked_qty)

            return True

        return False

    async def _log_status_periodically(self):
        """Log status information periodically, including positions."""
        if time.time() - self.last_log_time > 60 or self.last_log_time == 0:
            print("--------------------------------")
            try:
                # Get active orders
                active_orders = await self.exchange_client.get_active_orders(self.config.contract_id)

                # Filter close orders
                self.active_close_orders = []
                for order in active_orders:
                    if order.side == self.config.close_order_side:
                        self.active_close_orders.append({
                            'id': order.order_id,
                            'price': order.price,
                            'size': order.size
                        })

                # Get positions
                position_amt = await self.exchange_client.get_account_positions()

                # Calculate active closing amount
                active_close_amount = sum(
                    Decimal(order.get('size', 0))
                    for order in self.active_close_orders
                    if isinstance(order, dict)
                )

                self.logger.log(f"Current Position: {position_amt} | Active closing amount: {active_close_amount} | "
                                f"Order quantity: {len(self.active_close_orders)}")
                self.last_log_time = time.time()
                # Check for position mismatch
                if abs(position_amt - active_close_amount) > (2 * self.config.quantity):
                    error_message = f"\n\nERROR: [{self.config.exchange.upper()}_{self.config.ticker.upper()}] "
                    error_message += "Position mismatch detected\n"
                    error_message += "###### ERROR ###### ERROR ###### ERROR ###### ERROR #####\n"
                    error_message += "Please manually rebalance your position and take-profit orders\n"
                    error_message += "请手动平衡当前仓位和正在关闭的仓位\n"
                    error_message += f"current position: {position_amt} | active closing amount: {active_close_amount} | "f"Order quantity: {len(self.active_close_orders)}\n"
                    error_message += "###### ERROR ###### ERROR ###### ERROR ###### ERROR #####\n"
                    self.logger.log(error_message, "ERROR")

                    await self._lark_bot_notify(error_message.lstrip())

                    if not self.shutdown_requested:
                        self.shutdown_requested = True

                    mismatch_detected = True
                else:
                    mismatch_detected = False

                return mismatch_detected

            except Exception as e:
                self.logger.log(f"Error in periodic status check: {e}", "ERROR")
                self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")

            print("--------------------------------")

    async def _meet_grid_step_condition(self) -> bool:
        if self.active_close_orders:
            picker = min if self.config.direction == "buy" else max
            next_close_order = picker(self.active_close_orders, key=lambda o: o["price"])
            next_close_price = next_close_order["price"]

            best_bid, best_ask = await self.exchange_client.fetch_bbo_prices(self.config.contract_id)
            if best_bid <= 0 or best_ask <= 0 or best_bid >= best_ask:
                raise ValueError("No bid/ask data available")

            if self.config.direction == "buy":
                new_order_close_price = best_ask * (1 + self.config.take_profit/100)
                if next_close_price / new_order_close_price > 1 + self.config.grid_step/100:
                    return True
                else:
                    return False
            elif self.config.direction == "sell":
                new_order_close_price = best_bid * (1 - self.config.take_profit/100)
                if new_order_close_price / next_close_price > 1 + self.config.grid_step/100:
                    return True
                else:
                    return False
            else:
                raise ValueError(f"Invalid direction: {self.config.direction}")
        else:
            return True

    async def _check_price_condition(self) -> bool:
        stop_trading = False
        pause_trading = False

        if self.config.pause_price == self.config.stop_price == -1:
            return stop_trading, pause_trading

        best_bid, best_ask = await self.exchange_client.fetch_bbo_prices(self.config.contract_id)
        if best_bid <= 0 or best_ask <= 0 or best_bid >= best_ask:
            raise ValueError("No bid/ask data available")

        if self.config.stop_price != -1:
            if self.config.direction == "buy":
                if best_ask >= self.config.stop_price:
                    stop_trading = True
            elif self.config.direction == "sell":
                if best_bid <= self.config.stop_price:
                    stop_trading = True

        if self.config.pause_price != -1:
            if self.config.direction == "buy":
                if best_ask >= self.config.pause_price:
                    pause_trading = True
            elif self.config.direction == "sell":
                if best_bid <= self.config.pause_price:
                    pause_trading = True

        return stop_trading, pause_trading

    async def _lark_bot_notify(self, message: str):
        lark_token = os.getenv("LARK_TOKEN")
        if lark_token:
            async with LarkBot(lark_token) as bot:
                await bot.send_text(message)

    async def run(self):
        """Main trading loop."""
        try:
            self.config.contract_id, self.config.tick_size = await self.exchange_client.get_contract_attributes()

            # Log current TradingConfig
            self.logger.log("=== Trading Configuration ===", "INFO")
            self.logger.log(f"Ticker: {self.config.ticker}", "INFO")
            self.logger.log(f"Contract ID: {self.config.contract_id}", "INFO")
            self.logger.log(f"Quantity: {self.config.quantity}", "INFO")
            self.logger.log(f"Take Profit: {self.config.take_profit}%", "INFO")
            self.logger.log(f"Direction: {self.config.direction}", "INFO")
            self.logger.log(f"Max Orders: {self.config.max_orders}", "INFO")
            self.logger.log(f"Wait Time: {self.config.wait_time}s", "INFO")
            if self.config.wait_time_min is not None or self.config.wait_time_max is not None:
                min_wait = self.config.wait_time_min if self.config.wait_time_min is not None else self.config.wait_time
                max_wait = self.config.wait_time_max if self.config.wait_time_max is not None else self.config.wait_time
                self.logger.log(f"Wait Time Range: {min_wait}s ~ {max_wait}s", "INFO")
            self.logger.log(f"Exchange: {self.config.exchange}", "INFO")
            self.logger.log(f"Grid Step: {self.config.grid_step}%", "INFO")
            self.logger.log(f"Stop Price: {self.config.stop_price}", "INFO")
            self.logger.log(f"Pause Price: {self.config.pause_price}", "INFO")
            self.logger.log(f"Aster Boost: {self.config.aster_boost}", "INFO")
            self.logger.log(f"Max Position Loss: {self.config.max_position_loss}%", "INFO")
            self.logger.log("=============================", "INFO")

            # Capture the running event loop for thread-safe callbacks
            self.loop = asyncio.get_running_loop()
            # Connect to exchange
            await self.exchange_client.connect()

            # Main trading loop
            while not self.shutdown_requested:
                # Update active orders
                active_orders = await self.exchange_client.get_active_orders(self.config.contract_id)

                # Filter close orders
                self.active_close_orders = []
                for order in active_orders:
                    if order.side == self.config.close_order_side:
                        self.active_close_orders.append({
                            'id': order.order_id,
                            'price': order.price,
                            'size': order.size
                        })

                # Periodic logging
                mismatch_detected = await self._log_status_periodically()

                stop_trading, pause_trading = await self._check_price_condition()
                if stop_trading:
                    msg = f"\n\nWARNING: [{self.config.exchange.upper()}_{self.config.ticker.upper()}] \n"
                    msg += "Stopped trading due to stop price\n"
                    await self.graceful_shutdown(msg)
                    await self._lark_bot_notify(msg.lstrip())
                    continue

                if pause_trading:
                    await asyncio.sleep(5)
                    continue

                if not mismatch_detected:
                    try:
                        position_loss_triggered = await self._check_position_loss()
                    except ValueError as e:
                        self.logger.log(f"[MAX-LOSS] Price check failed: {e}", "ERROR")
                        position_loss_triggered = False

                    if position_loss_triggered:
                        await asyncio.sleep(1)
                        continue

                    wait_time = self._calculate_wait_time()

                    if wait_time > 0:
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        meet_grid_step_condition = await self._meet_grid_step_condition()
                        if not meet_grid_step_condition:
                            await asyncio.sleep(1)
                            continue

                        await self._place_and_monitor_open_order()
                        self.last_close_orders += 1

        except KeyboardInterrupt:
            self.logger.log("Bot stopped by user")
            await self.graceful_shutdown("User interruption (Ctrl+C)")
        except Exception as e:
            self.logger.log(f"Critical error: {e}", "ERROR")
            await self.graceful_shutdown(f"Critical error: {e}")
            raise
        finally:
            # Ensure all connections are closed even if graceful shutdown fails
            try:
                await self.exchange_client.disconnect()
            except Exception as e:
                self.logger.log(f"Error disconnecting from exchange: {e}", "ERROR")
