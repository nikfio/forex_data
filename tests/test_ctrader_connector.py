# -*- coding: utf-8 -*-
"""
Created on Mon Jun 29 23:15:00 2026

@author: Antigravity
"""

import os
import sys
import struct
import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta, timezone
import polars as pl
from pathlib import Path

from forex_data import cTraderDataConnector
from forex_data.data_management.common import (
    DEFAULT_PATHS,
    COLUMN_NAME,
    POLARS_DTYPE_DICT
)

# Protobuf message imports
from ctrader_open_api.messages.OpenApiCommonMessages_pb2 import ProtoMessage
from ctrader_open_api.messages.OpenApiMessages_pb2 import (
    ProtoOAApplicationAuthRes,
    ProtoOAAccountAuthRes,
    ProtoOASymbolsListRes,
    ProtoOAGetTickDataRes
)

def make_mock_packet(msg) -> bytes:
    proto_msg = ProtoMessage()
    proto_msg.payloadType = msg.payloadType
    proto_msg.payload = msg.SerializeToString()
    
    serialized = proto_msg.SerializeToString()
    length_header = struct.pack(">I", len(serialized))
    return length_header + serialized

class MockSSLSocket:
    def __init__(self):
        self.sent_data = b""
        self.recv_queue = []

    def connect(self, address):
        pass

    def sendall(self, data):
        self.sent_data += data

    def recv(self, bufsize):
        if not self.recv_queue:
            return b""
        current_chunk = self.recv_queue[0]
        if len(current_chunk) <= bufsize:
            self.recv_queue.pop(0)
            return current_chunk
        else:
            ret = current_chunk[:bufsize]
            self.recv_queue[0] = current_chunk[bufsize:]
            return ret

    def close(self):
        pass

class TestcTraderDataConnector(unittest.TestCase):
    """
    Unit tests for cTraderDataConnector.
    """

    @patch("forex_data.data_management.remoteconnector.ssl.create_default_context")
    @patch("forex_data.data_management.remoteconnector.socket.socket")
    def setUp(self, mock_socket, mock_create_default_context):
        # Mock connection and login responses
        mock_context = MagicMock()
        mock_create_default_context.return_value = mock_context
        self.mock_ssl_sock = MockSSLSocket()
        mock_context.wrap_socket.return_value = self.mock_ssl_sock

        app_res = ProtoOAApplicationAuthRes()
        acc_res = ProtoOAAccountAuthRes()
        acc_res.ctidTraderAccountId = 123456

        sym_res = ProtoOASymbolsListRes()
        sym_res.ctidTraderAccountId = 123456
        s1 = sym_res.symbol.add()
        s1.symbolId = 42
        s1.symbolName = "EURUSD"

        self.mock_ssl_sock.recv_queue.extend([
            make_mock_packet(app_res),
            make_mock_packet(acc_res),
            make_mock_packet(sym_res)
        ])

        self.connector = cTraderDataConnector(
            client_id="test-client-id",
            client_secret="test-client-secret",
            access_token="test-access-token",
            broker_account_id="123456",
            data_path=Path.home() / ".test_database"
        )

    def tearDown(self):
        self.connector.close()
        self.connector.clear_temporary_folder()

    def test_connect_and_symbols_mapping(self):
        self.assertEqual(self.connector._symbol_name_to_id.get("EURUSD"), 42)
        self.assertIn("EURUSD", self.connector.get_available_tickers())

    @patch("forex_data.data_management.remoteconnector.ssl.create_default_context")
    @patch("forex_data.data_management.remoteconnector.socket.socket")
    def test_check_connection(self, mock_socket, mock_create_default_context):
        # Close setup socket
        self.connector.close()

        mock_context = MagicMock()
        mock_create_default_context.return_value = mock_context
        new_ssl_sock = MockSSLSocket()
        mock_context.wrap_socket.return_value = new_ssl_sock

        app_res = ProtoOAApplicationAuthRes()
        acc_res = ProtoOAAccountAuthRes()
        acc_res.ctidTraderAccountId = 123456
        sym_res = ProtoOASymbolsListRes()
        sym_res.ctidTraderAccountId = 123456
        s1 = sym_res.symbol.add()
        s1.symbolId = 42
        s1.symbolName = "EURUSD"

        new_ssl_sock.recv_queue.extend([
            make_mock_packet(app_res),
            make_mock_packet(acc_res),
            make_mock_packet(sym_res)
        ])

        connected = self.connector.check_connection()
        self.assertTrue(connected)

    def test_get_data_tick_reconstruction(self):
        # Prepare tick responses for BID and ASK
        # cTrader returns ticks newest-first.
        # We'll mock a single request for BID and a single request for ASK.

        # BID Ticks:
        # 1. Newest tick: timestamp = 1717000000000, tick = 108120 (1.0812)
        # 2. Older tick: timestamp = 5000 (offset), tick = 108110 (1.0811)
        bid_res = ProtoOAGetTickDataRes()
        bid_res.ctidTraderAccountId = 123456
        bid_res.hasMore = False
        t_bid1 = bid_res.tickData.add()
        t_bid1.timestamp = 1717000000000
        t_bid1.tick = 108120
        t_bid2 = bid_res.tickData.add()
        t_bid2.timestamp = 5000
        t_bid2.tick = 108110

        # ASK Ticks:
        # 1. Newest tick: timestamp = 1717000000000, tick = 108140 (1.0814)
        # 2. Older tick: timestamp = 3000 (offset), tick = 108130 (1.0813)
        ask_res = ProtoOAGetTickDataRes()
        ask_res.ctidTraderAccountId = 123456
        ask_res.hasMore = False
        t_ask1 = ask_res.tickData.add()
        t_ask1.timestamp = 1717000000000
        t_ask1.tick = 108140
        t_ask2 = ask_res.tickData.add()
        t_ask2.timestamp = 3000
        t_ask2.tick = 108130

        self.mock_ssl_sock.recv_queue.extend([
            make_mock_packet(bid_res),
            make_mock_packet(ask_res)
        ])

        # Fetch data
        lazy_df = self.connector.get_data(
            symbol="EURUSD",
            timeframe="TICK",
            start_date="2024-05-29 00:00:00",
            end_date="2024-05-30 00:00:00"
        )
        
        self.assertIsInstance(lazy_df, pl.LazyFrame)
        df = lazy_df.collect()

        # Schema check
        for col, dtype in POLARS_DTYPE_DICT.TIME_TICK_DTYPE.items():
            self.assertIn(col, df.columns)
            self.assertEqual(df.schema[col], dtype)

        # Expected output rows (sorted chronologically):
        # 1. timestamp = 1716999997000 (2024-05-29 16:26:37 UTC)
        #    bid = 1.0811 (forward filled from 1716999995000), ask = 1.0813
        # 2. timestamp = 1717000000000 (2024-05-29 16:26:40 UTC)
        #    bid = 1.0812, ask = 1.0814
        self.assertEqual(df.height, 2)
        
        # Verify chronological order
        self.assertEqual(df["timestamp"][0], datetime.fromtimestamp(1716999997000 / 1000.0, tz=timezone.utc).replace(tzinfo=None))
        self.assertEqual(df["timestamp"][1], datetime.fromtimestamp(1717000000000 / 1000.0, tz=timezone.utc).replace(tzinfo=None))

        # Check values
        self.assertAlmostEqual(df["bid"][0], 1.0811)
        self.assertAlmostEqual(df["ask"][0], 1.0813)
        self.assertAlmostEqual(df["vwmp"][0], 1.0812) # (1.0811 + 1.0813) / 2.0
        self.assertEqual(df["ask_volume"][0], 0.0)
        self.assertEqual(df["bid_volume"][0], 0.0)

        self.assertAlmostEqual(df["bid"][1], 1.0812)
        self.assertAlmostEqual(df["ask"][1], 1.0814)
        self.assertAlmostEqual(df["vwmp"][1], 1.0813) # (1.0812 + 1.0814) / 2.0

    def test_get_data_with_pagination(self):
        # Mock pagination (hasMore = True on first chunk)
        # We will mock 2 chunks for BID and 1 chunk for ASK.
        
        # BID Chunk 1 (newest first, hasMore = True):
        # T1: timestamp = 1717000000000, tick = 108120
        # T2: timestamp = 5000, tick = 108110 (this is at 1716999995000)
        bid_res1 = ProtoOAGetTickDataRes()
        bid_res1.ctidTraderAccountId = 123456
        bid_res1.hasMore = True
        t_b1 = bid_res1.tickData.add()
        t_b1.timestamp = 1717000000000
        t_b1.tick = 108120
        t_b2 = bid_res1.tickData.add()
        t_b2.timestamp = 5000
        t_b2.tick = 108110

        # BID Chunk 2 (starts from oldest_ts - 1ms = 1716999994999):
        # T3: timestamp = 1716999990000, tick = 108100
        bid_res2 = ProtoOAGetTickDataRes()
        bid_res2.ctidTraderAccountId = 123456
        bid_res2.hasMore = False
        t_b3 = bid_res2.tickData.add()
        t_b3.timestamp = 1716999990000
        t_b3.tick = 108100

        # ASK Chunk (hasMore = False):
        # T1: timestamp = 1717000000000, tick = 108140
        # T2: timestamp = 10000, tick = 108130 (this is at 1716999990000)
        ask_res = ProtoOAGetTickDataRes()
        ask_res.ctidTraderAccountId = 123456
        ask_res.hasMore = False
        t_a1 = ask_res.tickData.add()
        t_a1.timestamp = 1717000000000
        t_a1.tick = 108140
        t_a2 = ask_res.tickData.add()
        t_a2.timestamp = 10000
        t_a2.tick = 108130

        self.mock_ssl_sock.recv_queue.extend([
            make_mock_packet(bid_res1),
            make_mock_packet(bid_res2),
            make_mock_packet(ask_res)
        ])

        lazy_df = self.connector.get_data(
            symbol="EURUSD",
            timeframe="TICK",
            start_date="2024-05-29 00:00:00",
            end_date="2024-05-30 00:00:00"
        )
        df = lazy_df.collect()

        # Timestamps generated:
        # BID: 1716999990000 (1.0810), 1716999995000 (1.0811), 1717000000000 (1.0812)
        # ASK: 1716999990000 (1.0813), 1717000000000 (1.0814)
        # Merged and forward filled:
        # - 1716999990000: bid = 1.0810, ask = 1.0813
        # - 1716999995000: bid = 1.0811, ask = ffilled to 1.0813
        # - 1717000000000: bid = 1.0812, ask = 1.0814
        self.assertEqual(df.height, 3)
        self.assertEqual(df["timestamp"][0], datetime.fromtimestamp(1716999990000 / 1000.0, tz=timezone.utc).replace(tzinfo=None))
        self.assertEqual(df["timestamp"][1], datetime.fromtimestamp(1716999995000 / 1000.0, tz=timezone.utc).replace(tzinfo=None))
        self.assertEqual(df["timestamp"][2], datetime.fromtimestamp(1717000000000 / 1000.0, tz=timezone.utc).replace(tzinfo=None))

        self.assertAlmostEqual(df["bid"][0], 1.0810)
        self.assertAlmostEqual(df["ask"][0], 1.0813)
        self.assertAlmostEqual(df["bid"][1], 1.0811)
        self.assertAlmostEqual(df["ask"][1], 1.0813)
        self.assertAlmostEqual(df["bid"][2], 1.0812)
        self.assertAlmostEqual(df["ask"][2], 1.0814)

def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(TestcTraderDataConnector)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)

if __name__ == "__main__":
    main()
