# -*- coding: utf-8 -*-
import unittest
from forex_data.data_management.remoteconnector import (
    CTraderConnector,
    TwelveDataConnector,
    TiingoConnector,
)


class TestConnectorSecretsAndTokens(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.ctrader_conn = CTraderConnector(
            client_id="test_client_id",
            client_secret="SUPER_SECRET_CLIENT_SECRET",
            access_token="SUPER_SECRET_ACCESS_TOKEN",
            refresh_token="SUPER_SECRET_REFRESH_TOKEN",
            broker_account_id="123456",
        )
        cls.twelve_conn = TwelveDataConnector(
            api_key="SUPER_SECRET_TWELVE_KEY",
            plan="free",
        )
        cls.tiingo_conn = TiingoConnector(
            api_key="SUPER_SECRET_TIINGO_KEY",
            plan="free",
        )

    @classmethod
    def tearDownClass(cls):
        pass

    def test_ctrader_repr_redaction(self):
        repr_str = repr(self.ctrader_conn)
        self.assertNotIn("SUPER_SECRET_CLIENT_SECRET", repr_str)
        self.assertNotIn("SUPER_SECRET_ACCESS_TOKEN", repr_str)
        self.assertNotIn("SUPER_SECRET_REFRESH_TOKEN", repr_str)
        self.assertIn("test_client_id", repr_str)
        self.assertIn("123456", repr_str)

    def test_twelvedata_repr_redaction(self):
        repr_str = repr(self.twelve_conn)
        self.assertNotIn("SUPER_SECRET_TWELVE_KEY", repr_str)
        self.assertIn("free", repr_str)

    def test_tiingo_repr_redaction(self):
        repr_str = repr(self.tiingo_conn)
        self.assertNotIn("SUPER_SECRET_TIINGO_KEY", repr_str)
        self.assertIn("free", repr_str)

    def test_ctrader_update_tokens(self):
        conn = CTraderConnector(
            client_id="test_client_id",
            client_secret="test_secret",
            access_token="old_access_token",
            refresh_token="old_refresh_token",
            broker_account_id="123456",
        )
        self.assertEqual(conn.access_token, "old_access_token")
        self.assertEqual(conn.refresh_token, "old_refresh_token")

        conn.update_tokens("new_access_token", "new_refresh_token")
        self.assertEqual(conn.access_token, "new_access_token")
        self.assertEqual(conn.refresh_token, "new_refresh_token")

        conn.update_tokens("newest_access_token")
        self.assertEqual(conn.access_token, "newest_access_token")
        self.assertEqual(conn.refresh_token, "new_refresh_token")

    def test_ctrader_is_token_expired_error(self):
        self.assertTrue(
            CTraderConnector._is_token_expired_error(
                ValueError("CH_ACCESS_TOKEN_INVALID: Token expired")
            )
        )
        self.assertTrue(
            CTraderConnector._is_token_expired_error(
                Exception("Unauthorized access")
            )
        )
        self.assertFalse(
            CTraderConnector._is_token_expired_error(
                ValueError("Network timeout")
            )
        )


if __name__ == "__main__":
    unittest.main()
