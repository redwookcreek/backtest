import datetime
from zipbird.basic.order import Order, PercentOrder
from zipbird.basic.types import Equity, OpenClose, Positions, StopOrderStatus

import pandas as pd

from zipline import api as zipline_api
from zipline.finance import execution as zipline_execution

from zipbird.replay.order_container import OrderContainer

class DuplicatePendingOrderError(Exception):
    pass

class UnknownFilledOrderError(Exception):
    pass

class MismatchedManagedOrders(Exception):
    pass

class UnhandledOrderException(Exception):
    pass

class PendingOrder:
    def __init__(self, order:Order):
        self.order = order
        self.zipline_order_id = None
    
    def send_order(self, order_api, is_stop_order):
        sign = self.order.get_sign()
        if isinstance(self.order, PercentOrder):
            self.zipline_order_id = order_api.order_target_percent(
                self.order.stock, sign * self.order.target_percent)
        elif self.order.limit_price:
            self.zipline_order_id = order_api.order(
                self.order.stock, 
                sign * self.order.amount,
                style=zipline_execution.LimitOrder(self.order.limit_price))
        elif is_stop_order and self.order.stop:
            self.zipline_order_id = order_api.order(
                self.order.stock, sign * self.order.amount,
                style=zipline_execution.StopOrder(self.order.stop.get_stop_price()))
        else:
            self.zipline_order_id = order_api.order(
                self.order.stock, sign * self.order.amount)

    def __str__(self):
        return 'PendingOrder(%s), zipline_order_id: %s' % (self.order, self.zipline_order_id)   
    
    def __repr__(self):
        return 'PendingOrder(%s)' % self.order
    
    def __eq__(self, other):
        return self.order == other.order


class PositionManager:
    """Manages positions of one strategy
    
    The position manager is responsible for sending orders to zipline
    and managing stop orders before each session start
      1) cancel any pending orders
      2) adjust stop order price with yesterday's price
      3) send out stop orders
      4) close out positions when day stop order hits
      5) close out positions when target hits.

    Order life cycle:
    Pending order ---filled--> Managed order --stopped--> No position
                                             ---target reached--> No position
                                             ---time stopped--> No position
    Pending order ---not filled--> Cancelled
    """
    pending_orders: dict[Equity, PendingOrder]
    managed_orders: dict[Equity, Order]

    def __init__(self, debug_logger, replay_container:OrderContainer):
        # Pending orders are orders entered in last session
        # Pending order may be filled during last session, in that case
        # it will be moved to managed orders.
        # If pending order is not filled, it will be canceled in the beginning of
        # this session.
        self.pending_orders = {}
        # Managed orders are filled orders, that is they are positions
        # When a pending order is filled, it will be moved from pending
        # orders to managed orders.
        self.managed_orders = {}
        self.order_api = zipline_api
        self.debug_logger = debug_logger
        self.replay_container = replay_container

    def get_day_count(self, asset):
        return self.managed_orders[asset].get_bar_count()
    
    def get_stop_price(self, asset):
        stop = self.managed_orders[asset].stop
        if stop:
            return stop.get_stop_price()
        return -1
    
    def get_target_price(self, asset):
        stop = self.managed_orders[asset].stop
        if stop:
            return stop.get_target_price()
        return -1
    def on_order_filled(self,
                        asset,
                        price:float,
                        amount:int,
                        order):
        today = self.order_api.get_datetime().date()
        if asset in self.pending_orders:
            pending_order = self.pending_orders.pop(asset)
            assert pending_order.zipline_order_id == order.id, \
                f'Mismatched zipline order id: {pending_order} != {order}'
            self.debug_logger.debug_print(
                5, 
                'Pending order filled %s %d' % (asset, amount))
            if pending_order.order.open_close == OpenClose.Close:
                self.managed_orders.pop(asset)
                self.replay_container.add_close_order(pending_order.order, today, price)
            else:
                self.managed_orders[asset] = pending_order.order
                self.replay_container.add_open_order(
                    open_date=today,
                    open_price=price,
                    open_order=pending_order.order)
        else:
            raise UnknownFilledOrderError('Order filled for unknown asset %s: %s' % (asset, order))

    def print_position_status(self):
        self.debug_logger.debug_print(3, '----------Position manager status --------------')
        self.debug_logger.debug_print(3, f'Pending orders: {len(self.pending_orders)}')
        for asset in sorted(self.pending_orders.keys()):
            self.debug_logger.debug_print(3, self.pending_orders[asset])
        self.debug_logger.debug_print(3, f'Managed orders: {len(self.managed_orders)}')
        for asset in sorted(self.managed_orders.keys()):
            self.debug_logger.debug_print(3, self.managed_orders[asset])

    def do_maintenance(self, today:datetime.date, positions:Positions, data:pd.DataFrame):
        """Runs maintenance before each trading session"""
        self.print_position_status()
        for managed_order in self.managed_orders.values():
            managed_order.inc_bar_count()
        self._verify_managed_orders(today, positions)
        self._cancel_pending_orders(today, positions)
        self._adjust_stop_orders(positions, data)
        self._close_out_positions(positions, data)
        self._send_out_stop_orders()

    def _get_expired_assets(self, today:datetime.date, asset_list:list[Equity]):
        today = pd.Timestamp(today)
        return [asset
                for asset in asset_list
                if (asset.auto_close_date and 
                    asset.auto_close_date <= today)]

    def _remove_expired_positions(self, today:datetime.date, positions:Positions):        
        to_move = self._get_expired_assets(today, self.managed_orders.keys())
        for asset in to_move:
            if asset not in positions:
                self.debug_logger.debug_print(
                    3, 
                    'Position %s has passed auto_close_date, removing' % asset)
                self.managed_orders.pop(asset)

    def _verify_managed_orders(self, today:datetime.date, positions:Positions):
        # remove positions has passed auto_close_date
        self._remove_expired_positions(today, positions)
        managed_orders = sorted(self.managed_orders.keys())
        position_assets = sorted(positions.keys())
        if managed_orders != position_assets:
            raise MismatchedManagedOrders(
                f"""[Managed orders] mismatched [positions]:
                {managed_orders}
                {position_assets}
                """)
        
    def _make_and_send_pending_order(self, order:Order, is_stop_order:bool):
        pending_order = PendingOrder(order)
        pending_order.send_order(self.order_api, is_stop_order)
        if order.stock in self.pending_orders:
            raise DuplicatePendingOrderError(
                'Pending order already exists for %s: to add: %s, existing: %s' % (
                    order.stock, order, self.pending_orders[order.stock]))
        self.pending_orders[order.stock] = pending_order
        self.debug_logger.debug_print(
            6,
            f'Sent out order {pending_order.zipline_order_id}: {pending_order.order}'
        )


    def send_orders(self, orders:list[Order]):
        for order in orders:
            self._make_and_send_pending_order(order, is_stop_order=False)

    def _cancel_pending_orders(self, today:datetime.date, positions:Positions):
        all_open_orders = sum(self.order_api.get_open_orders().values(), [])
        cancled_order_ids = set()
        for open_order in all_open_orders:
            self.debug_logger.debug_print(5, 'Cancel pending order %s' % open_order)
            self.order_api.cancel_order(open_order)
            cancled_order_ids.add(open_order.id)
        expired_assets = self._get_expired_assets(today, self.pending_orders.keys())
        # pending orders are from last session. So they either filled 
        # or cancelled by the above cancel orders.
        for pending_order in self.pending_orders.values():
            if pending_order.zipline_order_id in cancled_order_ids:
                continue
            stock = pending_order.order.stock
            if stock in expired_assets:
                self.debug_logger.debug_print(
                    3, 
                    'Position %s has passed auto_close_date, removing' % stock)
                continue
            if stock not in positions and pending_order.zipline_order_id:
                # for some unknonw reason, zipline may return None for zipline order id
                # in that case, the order was not accepted, and will not show up in
                # pending orders
                raise UnhandledOrderException('Unhandled orders: %s' % pending_order)
        
        self.pending_orders = {}  # reset pending orders

    def _adjust_stop_orders(self, positions:Positions, data:pd.DataFrame):
        for asset, managed_order in self.managed_orders.items():
            if not managed_order.stop:
                continue
            position = positions[asset]
            managed_order.stop.do_maintenance(position.cost_basis, data.loc[asset])

    def _close_out_positions(self, positions:Positions, data:pd.DataFrame):
        
        for asset, managed_order in self.managed_orders.items():
            if not managed_order.stop:
                continue
            stop_order_status = managed_order.stop.get_status(data.loc[asset])
            pending_order = None
            if stop_order_status in (StopOrderStatus.INITIAL_STOP, 
                                     StopOrderStatus.TRAILING_STOP):
                self.debug_logger.debug_print(
                    3, 
                    'Closed out position with stop order %s' % managed_order.stock)
                if managed_order.stock in positions:
                    self.debug_logger.debug_print(
                        1, 'Position %s stop triggered but still found in positions %s ' % 
                        (managed_order.stock, positions))
            elif stop_order_status == StopOrderStatus.TARGET_REACHED:
                self.debug_logger.debug_print(
                    3, 
                    'Closed out position with target order %s' % managed_order.stock)
                self._make_and_send_pending_order(
                    managed_order.make_opposite_order(keep_stop=False, keep_limit=False),
                    is_stop_order=False)
            elif stop_order_status == StopOrderStatus.TIME_STOP:
                self.debug_logger.debug_print(
                    3,
                    'Closed out position with time stop order %s' % managed_order.stock)
                self._make_and_send_pending_order(
                    managed_order.make_opposite_order(keep_stop=False, keep_limit=False),
                    is_stop_order=False)
            else:
                self.debug_logger.debug_print(
                    5, 'Position not closed out %s' % managed_order.stock)

    def _send_out_stop_orders(self):
        for asset, managed_order in self.managed_orders.items():
            if not managed_order.stop:
                continue
            # there might already a closing order pending
            if asset in self.pending_orders:
                continue
            self._make_and_send_pending_order(
                managed_order.make_opposite_order(keep_stop=True, keep_limit=False),
                is_stop_order=True)
    
