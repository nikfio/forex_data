# -*- coding: utf-8 -*-
"""
Tests for CTraderConnector / cTraderDataConnector.
Verifies connection, auth handshake, symbol resolution, tick reconstruction,
trendbar decoding, schema adherence, and 50/50 volume split calculation.
"""

import struct
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
from ctrader_open_api.messages.OpenApiCommonMessages_pb2 import ProtoMessage
from ctrader_open_api.messages.OpenApiMessages_pb2 import (
    ProtoOAAccountAuthRes,
    ProtoOAApplicationAuthRes,
    ProtoOAGetTickDataRes,
    ProtoOAGetTrendbarsRes,
    ProtoOASymbolsListRes,
)
from ctrader_open_api.messages.OpenApiModelMessages_pb2 import ProtoOATrendbarPeriod

from forex_data import (
    POLARS_DTYPE_DICT,
    CTraderConnector,
    cTraderDataConnector,
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


class TestCTraderConnector(unittest.TestCase):
    """
    Unit tests for CTraderConnector.
    """

    @patch("forex_data.data_management.remoteconnector.ssl.create_default_context")
    @patch("forex_data.data_management.remoteconnector.socket.socket")
    def setUp(self, mock_socket, mock_create_default_context):
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
            make_mock_packet(sym_res),
        ])

        self.connector = CTraderConnector(
            client_id="test-client-id",
            client_secret="test-client-secret",
            access_token="test-access-token",
            broker_account_id="123456",
            data_path=Path.home() / ".test_database",
        )

    def tearDown(self):
        self.connector.close()
        self.connector.clear_temporary_folder()

    def test_alias(self):
        self.assertIs(cTraderDataConnector, CTraderConnector)

    def test_connect_and_symbols_mapping(self):
        self.assertEqual(self.connector._symbol_name_to_id.get("EURUSD"), 42)
        self.assertIn("EURUSD", self.connector.get_available_tickers())

    def test_trendbars_and_volume_split(self):
        """
        Verify that trendbars are correctly decoded and that ask_volume and
        bid_volume are each exactly 50% of the total tick volume.
        """
        tb_res = ProtoOAGetTrendbarsRes()
        tb_res.ctidTraderAccountId = 123456
        tb_res.period = ProtoOATrendbarPeriod.H1
        tb_res.timestamp = 1705276800000

        # Bar 1: volume=1000, low=108000, deltaOpen=50, deltaHigh=100, deltaClose=80
        b1 = tb_res.trendbar.add()
        b1.utcTimestampInMinutes = 28421280  # valid UTC timestamp in minutes
        b1.volume = 1000
        b1.low = 108000
        b1.deltaOpen = 50
        b1.deltaHigh = 100
        b1.deltaClose = 80

        # Bar 2: volume=2400, low=108050, deltaOpen=30, deltaHigh=70, deltaClose=40
        b2 = tb_res.trendbar.add()
        b2.utcTimestampInMinutes = 28421340
        b2.volume = 2400
        b2.low = 108050
        b2.deltaOpen = 30
        b2.deltaHigh = 70
        b2.deltaClose = 40

        self.mock_ssl_sock.recv_queue.append(make_mock_packet(tb_res))

        lf = self.connector.get_data(
            symbol="EURUSD",
            timeframe="1h",
            start_date="2024-01-15 00:00:00",
            end_date="2024-01-15 02:00:00",
        )
        self.assertIsInstance(lf, pl.LazyFrame)
        df = lf.collect()

        self.assertEqual(len(df), 2)

        # Check schema
        for col, dtype in POLARS_DTYPE_DICT.TIME_TF_DTYPE.items():
            self.assertIn(col, df.columns)
            self.assertEqual(df.schema[col], dtype)

        # Check prices for Bar 1 (scale 100,000 for EURUSD)
        self.assertAlmostEqual(df["low"][0], 1.08000, places=5)
        self.assertAlmostEqual(df["open"][0], 1.08050, places=5)
        self.assertAlmostEqual(df["high"][0], 1.08100, places=5)
        self.assertAlmostEqual(df["close"][0], 1.08080, places=5)

        # 50/50 Volume Split Verification:
        # Bar 1: total volume = 1000 -> ask_volume = 500, bid_volume = 500
        self.assertAlmostEqual(df["ask_volume"][0], 500.0)
        self.assertAlmostEqual(df["bid_volume"][0], 500.0)
        self.assertAlmostEqual(df["ask_volume"][0] + df["bid_volume"][0], 1000.0)

        # Bar 2: total volume = 2400 -> ask_volume = 1200, bid_volume = 1200
        self.assertAlmostEqual(df["ask_volume"][1], 1200.0)
        self.assertAlmostEqual(df["bid_volume"][1], 1200.0)
        self.assertAlmostEqual(df["ask_volume"][1] + df["bid_volume"][1], 2400.0)

    def test_trendbars_4h_reframing(self):
        """
        Verify that 4h timeframe queries request 1h bars and reframe them,
        preserving the 50/50 volume split aggregated over the 4-hour window.
        """
        tb_res = ProtoOAGetTrendbarsRes()
        tb_res.ctidTraderAccountId = 123456
        tb_res.period = ProtoOATrendbarPeriod.H1
        tb_res.timestamp = 1705276800000

        # Provide 4 consecutive 1-hour bars starting at 00:00 UTC
        # (1705276800 -> 28421280 minutes), each with 500 tick volume (ask=250, bid=250)
        base_min = 28421280
        for i in range(4):
            b = tb_res.trendbar.add()
            b.utcTimestampInMinutes = base_min + (i * 60)
            b.volume = 500
            b.low = 108000 + (i * 10)
            b.deltaOpen = 10
            b.deltaHigh = 20
            b.deltaClose = 15

        self.mock_ssl_sock.recv_queue.append(make_mock_packet(tb_res))

        lf = self.connector.get_data(
            symbol="EURUSD",
            timeframe="4h",
            start_date="2024-01-15 00:00:00",
            end_date="2024-01-15 04:00:00",
        )
        df = lf.collect()

        self.assertEqual(len(df), 1)
        # Total volume across 4 hours = 4 * 500 = 2000
        # Aggregated 50/50 volume split: ask_volume = 1000, bid_volume = 1000
        self.assertAlmostEqual(df["ask_volume"][0], 1000.0)
        self.assertAlmostEqual(df["bid_volume"][0], 1000.0)

    def test_get_tick_data(self):
        bid_res = ProtoOAGetTickDataRes()
        bid_res.ctidTraderAccountId = 123456
        bid_res.hasMore = False
        t_b1 = bid_res.tickData.add()
        t_b1.timestamp = 1717000000000
        t_b1.tick = 108120
        t_b2 = bid_res.tickData.add()
        t_b2.timestamp = 5000
        t_b2.tick = 108110

        ask_res = ProtoOAGetTickDataRes()
        ask_res.ctidTraderAccountId = 123456
        ask_res.hasMore = False
        t_a1 = ask_res.tickData.add()
        t_a1.timestamp = 1717000000000
        t_a1.tick = 108140
        t_a2 = ask_res.tickData.add()
        t_a2.timestamp = 3000
        t_a2.tick = 108130

        self.mock_ssl_sock.recv_queue.extend([
            make_mock_packet(bid_res),
            make_mock_packet(ask_res),
        ])

        lazy_df = self.connector.get_data(
            symbol="EURUSD",
            timeframe="TICK",
            start_date="2024-05-29 00:00:00",
            end_date="2024-05-30 00:00:00",
        )
        self.assertIsInstance(lazy_df, pl.LazyFrame)
        df = lazy_df.collect()

        for col, dtype in POLARS_DTYPE_DICT.TIME_TICK_DTYPE.items():
            self.assertIn(col, df.columns)
            self.assertEqual(df.schema[col], dtype)

        self.assertEqual(df.height, 2)
        self.assertAlmostEqual(df["bid"][0], 1.0811)
        self.assertAlmostEqual(df["ask"][0], 1.0813)
        self.assertAlmostEqual(df["bid"][1], 1.0812)
        self.assertAlmostEqual(df["ask"][1], 1.0814)
