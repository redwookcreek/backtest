import datetime
import unittest
from unittest.mock import Mock, call
import pandas as pd

from zipbird.basic.order import ShareOrder
from zipbird.basic.stop import FixStop, PercentProfitTarget, StopOrder
from zipbird.basic.types import Equity, LongShort, Position
from zipbird.position_manager.position_manager import DuplicatePendingOrderError, MismatchedManagedOrders, PendingOrder, PositionManager, UnhandledOrderException, UnknownFilledOrderError
from zipline.finance import execution as zipline_execution

class FakeZiplineOrder:
    def __init__(self, id):
        self.id = id

class LimitOrderMatcher:
    def __init__(self, limit_price):
        self.limit_price = limit_price

    def __eq__(self, other):
        return isinstance(other, zipline_execution.LimitOrder) and other.limit_price == self.limit_price

class StopOrderMatcher:
    def __init__(self, stop_price):
        self.stop_price = stop_price

    def __eq__(self, other):
        return isinstance(other, zipline_execution.StopOrder) and other.get_stop_price(True) == self.stop_price

def asset(symbol, auto_close_date=None):
    return Equity(symbol, auto_close_date)

AMZN = asset('AMZN')
MSFT = asset('MSFT')

class TestPositionManager(unittest.TestCase):
    ###########################################################################
    # on_order_filled
    ###########################################################################
    def test_on_order_filled_open_long(self):
        debug_logger = Mock()
        position_manager = PositionManager(debug_logger)
        share_order = ShareOrder.make_open_long(AMZN, 100)
        position_manager.pending_orders = {
            AMZN: PendingOrder(share_order),
        }
        position_manager.pending_orders[AMZN].zipline_order_id = '123'
        position_manager.on_order_filled(AMZN, 100, 100, FakeZiplineOrder('123'))
        self.assertEqual(position_manager.pending_orders, {})
        self.assertEqual(position_manager.managed_orders, 
                         {AMZN: share_order})

    def test_on_order_filled_close(self):
        debug_logger = Mock()
        position_manager = PositionManager(debug_logger)
        open_share_order = ShareOrder.make_open_long(AMZN, 100)
        close_share_order = ShareOrder.make_close_long(AMZN, 100)
        position_manager.pending_orders = {
            AMZN: PendingOrder(close_share_order),
        }
        position_manager.pending_orders[AMZN].zipline_order_id = '123'
        position_manager.managed_orders = {
            AMZN: open_share_order,
        }
        position_manager.on_order_filled(AMZN, -100, 100, FakeZiplineOrder('123'))
        self.assertEqual(position_manager.pending_orders, {})
        self.assertEqual(position_manager.managed_orders, {})

    def test_on_order_filled_unknown(self):
        debug_logger = Mock()
        position_manager = PositionManager(debug_logger)
        with self.assertRaises(UnknownFilledOrderError):
            position_manager.on_order_filled(AMZN, -100, 100, 'market')

    ###########################################################################
    # verify_managed_orders
    ###########################################################################
    def test_verify_managed_orders_mismatched_managed_orders(self):
        debug_logger = Mock()
        position_manager = PositionManager(debug_logger)
        position_manager.managed_orders = {
            AMZN: ShareOrder.make_open_long(AMZN, 100),
        }
        positions = {
            MSFT: Position(MSFT, 100, 100),
        }
        today = datetime.date(2024, 1, 1)
        with self.assertRaises(MismatchedManagedOrders):
            position_manager._verify_managed_orders(today, positions)

    def test_verify_managed_orders_matched(self):
        debug_logger = Mock()
        position_manager = PositionManager(debug_logger)
        position_manager.managed_orders = {
            AMZN: ShareOrder.make_open_long(AMZN, 100),
        }
        positions = {
            AMZN: Position(AMZN, 100, 100),
        }
        today = datetime.date(2024, 1, 1)
        position_manager._verify_managed_orders(today, positions)

    def test_verify_managed_orders_matched_with_auto_close_date(self):
        debug_logger = Mock()
        position_manager = PositionManager(debug_logger)
        amzn = asset('AMZN',
                     auto_close_date=pd.Timestamp(datetime.date(2024, 1, 2) ))
        msft = asset('MSFT',
                     auto_close_date=pd.Timestamp(datetime.date(2024, 1, 1) ))
        position_manager.managed_orders = {
            amzn: ShareOrder.make_open_long(amzn, 100),
            msft: ShareOrder.make_open_long(msft, 100),
        }
        positions = {}
        today = datetime.date(2024, 1, 2)
        position_manager._verify_managed_orders(today, positions)
        self.assertEqual(position_manager.managed_orders, {})

    ###########################################################################
    # send_orders
    ###########################################################################
    def test_send_orders(self):
        debug_logger = Mock()
        position_manager = PositionManager(debug_logger)
        order_api = Mock()
        position_manager.order_api = order_api
        position_manager.send_orders([
            ShareOrder.make_open_long(AMZN, 100),
            ShareOrder.make_open_long(MSFT, 100, limit_price=100),
        ])
        order_api.order.assert_has_calls(
            [
                call(AMZN, 100),
                call(asset('MSFT'), 100, style=LimitOrderMatcher(100)),
            ],
            any_order=True)
        self.assertEqual(position_manager.pending_orders, {
            AMZN: PendingOrder(ShareOrder.make_open_long(AMZN, 100)),
            MSFT: PendingOrder(ShareOrder.make_open_long(MSFT, 100, limit_price=100)),
        })

    def test_send_orders_duplicate(self):
        debug_logger = Mock()
        position_manager = PositionManager(debug_logger)
        order_api = Mock()
        position_manager.order_api = order_api
        with self.assertRaises(DuplicatePendingOrderError):
            position_manager.send_orders([
                ShareOrder.make_open_long(AMZN, 100),
                ShareOrder.make_open_long(AMZN, 100),
            ])

    ###########################################################################
    # cancel_pending_orders
    ###########################################################################
    def test_cancel_pending_orders_unfilled(self):
        debug_logger = Mock()
        position_manager = PositionManager(debug_logger)
        order_api = Mock()
        position_manager.order_api = order_api
        open_order = Mock(id='123')
        order_api.get_open_orders.return_value = {AMZN: [open_order]}
        amzn_order = PendingOrder(ShareOrder.make_open_long(AMZN, 100))
        amzn_order.zipline_order_id = '123'
        position_manager.pending_orders = {
            AMZN: amzn_order,
        }
        positions = {
            asset('MSFT'): Position(asset('MSFT'), 100, 100),
        }
        position_manager._cancel_pending_orders(datetime.date(2024, 1, 1), positions)
        self.assertEqual(position_manager.pending_orders, {})
        order_api.cancel_order.assert_called_once_with(open_order)

    def test_cancel_pending_orders_expired(self):
        debug_logger = Mock()
        position_manager = PositionManager(debug_logger)
        order_api = Mock()
        position_manager.order_api = order_api
        order_api.get_open_orders.return_value = {AMZN: []}

        mfst = asset('MSFT', auto_close_date=pd.Timestamp(datetime.date(2024, 1, 1)))
        amzn = asset('AMZN', auto_close_date=pd.Timestamp(datetime.date(2024, 1, 2)))
        amzn_order = PendingOrder(ShareOrder.make_open_long(amzn, 100))
        amzn_order.zipline_order_id = '123'
        mfst_order = PendingOrder(ShareOrder.make_open_long(mfst, 100))
        mfst_order.zipline_order_id = '456'

        position_manager.pending_orders = {amzn: amzn_order, mfst: mfst_order}
        position_manager._cancel_pending_orders(datetime.date(2024, 1, 2), {})
        self.assertEqual(position_manager.pending_orders, {})
        order_api.cancel_order.assert_not_called()

    def test_cancel_pending_orders_unknown_order(self):
        debug_logger = Mock()
        position_manager = PositionManager(debug_logger)
        order_api = Mock()
        position_manager.order_api = order_api
        order_api.get_open_orders.return_value = {}
        amzn_order = PendingOrder(ShareOrder.make_open_long(AMZN, 100))
        amzn_order.zipline_order_id = '123'
        position_manager.pending_orders = {
            AMZN: amzn_order,
        }
        with self.assertRaises(UnhandledOrderException):
            position_manager._cancel_pending_orders(datetime.date(2024, 1, 1), {})

    ###########################################################################
    # _adjust_stop_orders
    ###########################################################################
    def test_adjust_stop_orders(self):
        debug_logger = Mock()
        position_manager = PositionManager(debug_logger)
        position_manager.managed_orders = {
            AMZN: ShareOrder.make_open_long(AMZN, 100),
        }
        position_manager.managed_orders[AMZN].add_stop(
            StopOrder(initial_stop=FixStop(LongShort.Long, 10)))
        position_manager.managed_orders[AMZN].stop.do_maintenance(open_price=120, data=None)
        self.assertEqual(position_manager.managed_orders[AMZN].stop.get_stop_price(), 110)

    ###########################################################################
    # _close_out_positions
    ###########################################################################
    def test_close_out_positions_trigger_stop(self):
        debug_logger = Mock()
        position_manager = PositionManager(debug_logger)
        order_api = Mock()
        position_manager.order_api = order_api
        position_manager.managed_orders = {
            AMZN: ShareOrder.make_open_long(AMZN, 100),
        }
        position_manager.managed_orders[AMZN].add_stop(
            StopOrder(initial_stop=FixStop(LongShort.Long, 10)))
        position_manager.managed_orders[AMZN].stop.do_maintenance(open_price=120, data=None)

        position_manager._close_out_positions(
            {AMZN: Position(AMZN, 100, 100)}, 
            data=pd.DataFrame({'close': {AMZN: 100}})
        )
        # currently, we do nothing for stop triggers, we assume they will be
        # closed out by the stop order
        order_api.order.assert_not_called()

    def test_close_out_positions_trigger_target_reached(self):
        debug_logger = Mock()
        position_manager = PositionManager(debug_logger)
        order_api = Mock()
        position_manager.order_api = order_api
        position_manager.managed_orders = {
            AMZN: ShareOrder.make_open_long(AMZN, 100),
        }
        position_manager.managed_orders[AMZN].add_stop(
            StopOrder(profit_target=PercentProfitTarget(LongShort.Long, 0.1)))
        position_manager.managed_orders[AMZN].stop.do_maintenance(open_price=120, data=None)
        position_manager._close_out_positions(
            {AMZN: Position(AMZN, 100, 100)}, 
            data=pd.DataFrame({'close': {AMZN: 140}})
        )
        order_api.order.assert_called_once_with(AMZN, -100)
        
    def test_close_out_positions_trigger_target_not_reached(self):
        debug_logger = Mock()
        position_manager = PositionManager(debug_logger)
        order_api = Mock()
        position_manager.order_api = order_api
        position_manager.managed_orders = {
            AMZN: ShareOrder.make_open_long(AMZN, 100),
        }
        position_manager.managed_orders[AMZN].add_stop(
            StopOrder(profit_target=PercentProfitTarget(LongShort.Long, 0.1)))
        position_manager.managed_orders[AMZN].stop.do_maintenance(open_price=120, data=None)
        position_manager._close_out_positions(
            {AMZN: Position(AMZN, 100, 100)}, 
            data=pd.DataFrame({'close': {AMZN: 110}})
        )
        order_api.order.assert_not_called()

    def test_close_out_positions_trigger_time_stop(self):
        debug_logger = Mock()
        position_manager = PositionManager(debug_logger)
        order_api = Mock()
        position_manager.order_api = order_api
        position_manager.managed_orders = {
            AMZN: ShareOrder.make_open_long(AMZN, 100),
        }
        position_manager.managed_orders[AMZN].add_stop(
            StopOrder(time_stop=2))
        
        for day in range(1, 3):
            position_manager.managed_orders[AMZN].stop.do_maintenance(open_price=120, data=None)
            position_manager._close_out_positions(
                {AMZN: Position(AMZN, 100, 100)}, 
                data=pd.DataFrame({'close': {AMZN: 110}})
            )
            if day == 2:
                order_api.order.assert_called_once_with(AMZN, -100)
            else:
                order_api.order.assert_not_called()

    ###########################################################################
    # send_out_stop_orders
    ###########################################################################
    def test_send_out_stop_orders_long(self):
        debug_logger = Mock()
        position_manager = PositionManager(debug_logger)
        order_api = Mock()
        position_manager.order_api = order_api
        position_manager.managed_orders = {
            AMZN: ShareOrder.make_open_long(AMZN, 100),
            MSFT: ShareOrder.make_open_long(MSFT, 100),
        }
        position_manager.managed_orders[AMZN].add_stop(
            StopOrder(initial_stop=FixStop(LongShort.Long, 10)))
        position_manager.managed_orders[AMZN].stop.do_maintenance(open_price=120, data=None)
        position_manager._send_out_stop_orders()
        order_api.order.assert_called_once_with(AMZN, -100, style=StopOrderMatcher(110))

    def test_send_out_stop_orders_short(self):
        debug_logger = Mock()
        position_manager = PositionManager(debug_logger)
        order_api = Mock()
        position_manager.order_api = order_api
        position_manager.managed_orders = {
            AMZN: ShareOrder.make_open_short(AMZN, 100),
            MSFT: ShareOrder.make_open_short(MSFT, 100),
        }
        position_manager.managed_orders[AMZN].add_stop(
            StopOrder(initial_stop=FixStop(LongShort.Short, 10)))
        position_manager.managed_orders[MSFT].add_stop(
            StopOrder(initial_stop=FixStop(LongShort.Short, 20)))
        position_manager.managed_orders[AMZN].stop.do_maintenance(open_price=120, data=None)
        position_manager.managed_orders[MSFT].stop.do_maintenance(open_price=90, data=None)
        position_manager._send_out_stop_orders()

        order_api.order.assert_has_calls(
            [
                call(AMZN, 100, style=StopOrderMatcher(130)),
                call(MSFT, 100, style=StopOrderMatcher(110)),
            ],
            any_order=True)
