from collections.abc import Generator
from typing import Optional

import httpx

from src.common.client import retry_request
from src.indexers.kalshi.models import Market, Trade

KALSHI_API_HOST = "https://api.elections.kalshi.com/trade-api/v2"
KALSHI_DEMO_HOST = "https://demo-api.kalshi.co/trade-api/v2"


class KalshiClient:
    def __init__(self, host: str = KALSHI_API_HOST):
        self.host = host
        self.client = httpx.Client(base_url=host, timeout=30.0)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.client.close()

    def close(self):
        self.client.close()

    @retry_request()
    def _get(self, path: str, params: Optional[dict] = None) -> dict:
        """Make a GET request with retry/backoff."""
        response = self.client.get(path, params=params)
        response.raise_for_status()
        return response.json()

    def get_market(self, ticker: str) -> Market:
        data = self._get(f"/markets/{ticker}")
        return Market.from_dict(data["market"])

    def get_market_trades(
        self,
        ticker: str,
        limit: int = 1000,
        verbose: bool = True,
        min_ts: Optional[int] = None,
        max_ts: Optional[int] = None,
    ) -> list[Trade]:
        all_trades = []
        cursor = None

        while True:
            params = {"ticker": ticker, "limit": limit}
            if cursor:
                params["cursor"] = cursor
            if min_ts is not None:
                params["min_ts"] = min_ts
            if max_ts is not None:
                params["max_ts"] = max_ts

            data = self._get("/markets/trades", params=params)

            trades = [Trade.from_dict(t) for t in data.get("trades", [])]
            if trades:
                all_trades.extend(trades)
                if verbose:
                    print(f"Fetched {len(trades)} trades (total: {len(all_trades)})")

            cursor = data.get("cursor")
            if not cursor:
                break

        return all_trades

    def list_markets(self, limit: int = 20, **kwargs) -> list[Market]:
        params = {"limit": limit, **kwargs}
        data = self._get("/markets", params=params)
        return [Market.from_dict(m) for m in data.get("markets", [])]

    def list_all_markets(self, limit: int = 200) -> list[Market]:
        all_markets = []
        cursor = None

        while True:
            params = {"limit": limit}
            if cursor:
                params["cursor"] = cursor

            data = self._get("/markets", params=params)

            markets = [Market.from_dict(m) for m in data.get("markets", [])]
            if markets:
                all_markets.extend(markets)
                print(f"Fetched {len(markets)} markets (total: {len(all_markets)})")

            cursor = data.get("cursor")
            if not cursor:
                break

        return all_markets

    def iter_markets(
        self,
        limit: int = 200,
        cursor: Optional[str] = None,
        min_close_ts: Optional[int] = None,
        max_close_ts: Optional[int] = None,
    ) -> Generator[tuple[list[Market], Optional[str]], None, None]:
        while True:
            params = {"limit": limit}
            if cursor:
                params["cursor"] = cursor
            if min_close_ts is not None:
                params["min_close_ts"] = min_close_ts
            if max_close_ts is not None:
                params["max_close_ts"] = max_close_ts

            data = self._get("/markets", params=params)

            markets = [Market.from_dict(m) for m in data.get("markets", [])]
            cursor = data.get("cursor")

            yield markets, cursor

            if not cursor:
                break

    def get_recent_trades(self, limit: int = 100) -> list[Trade]:
        data = self._get("/markets/trades", params={"limit": limit})
        return [Trade.from_dict(t) for t in data.get("trades", [])]


class KalshiTradingClient(KalshiClient):
    """Extends ``KalshiClient`` with authenticated portfolio and order management.

    Authentication uses an API key ID + RSA private key (PEM format) as
    described in the Kalshi v2 API documentation.  Set credentials via
    environment variables (loaded from ``.env``) or pass them directly:

    .. code-block:: bash

        KALSHI_API_KEY_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
        KALSHI_PRIVATE_KEY_PATH=/path/to/kalshi-private-key.pem
        # or inline PEM:
        KALSHI_PRIVATE_KEY=-----BEGIN RSA PRIVATE KEY-----\\n...

    Parameters
    ----------
    api_key_id:
        Kalshi API key ID (UUID).  Defaults to the ``KALSHI_API_KEY_ID``
        environment variable.
    private_key_pem:
        PEM-encoded RSA private key string.  Defaults to the value of the
        ``KALSHI_PRIVATE_KEY`` environment variable, or the contents of the
        file pointed to by ``KALSHI_PRIVATE_KEY_PATH``.
    host:
        API host.  Use ``KALSHI_DEMO_HOST`` for the paper-trading demo
        environment.
    """

    def __init__(
        self,
        api_key_id: Optional[str] = None,
        private_key_pem: Optional[str] = None,
        host: str = KALSHI_API_HOST,
    ):
        import os

        self._api_key_id = api_key_id or os.environ.get("KALSHI_API_KEY_ID", "")
        if private_key_pem is None:
            private_key_pem = os.environ.get("KALSHI_PRIVATE_KEY") or self._load_key_from_path(
                os.environ.get("KALSHI_PRIVATE_KEY_PATH", "")
            )
        self._private_key_pem = private_key_pem or ""

        super().__init__(host=host)

    # ------------------------------------------------------------------
    # Auth helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _load_key_from_path(path: str) -> str:
        if not path:
            return ""
        try:
            from pathlib import Path

            return Path(path).read_text()
        except OSError:
            return ""

    def _auth_headers(self, method: str, path: str) -> dict[str, str]:
        """Return the ``Authorization`` header for a signed Kalshi API request."""
        import base64
        import time

        if not self._api_key_id or not self._private_key_pem:
            return {}

        try:
            from cryptography.hazmat.primitives import hashes, serialization
            from cryptography.hazmat.primitives.asymmetric import padding

            ts_ms = str(int(time.time() * 1000))
            msg = ts_ms + method.upper() + path
            private_key = serialization.load_pem_private_key(
                self._private_key_pem.encode(),
                password=None,
            )
            signature = private_key.sign(msg.encode(), padding.PKCS1v15(), hashes.SHA256())
            sig_b64 = base64.b64encode(signature).decode()
            return {
                "KALSHI-ACCESS-KEY": self._api_key_id,
                "KALSHI-ACCESS-TIMESTAMP": ts_ms,
                "KALSHI-ACCESS-SIGNATURE": sig_b64,
            }
        except Exception:
            # Credentials unavailable or malformed; return empty (public access only)
            return {}

    @retry_request()
    def _authed_get(self, path: str, params: Optional[dict] = None) -> dict:
        headers = self._auth_headers("GET", path)
        response = self.client.get(path, params=params, headers=headers)
        response.raise_for_status()
        return response.json()

    @retry_request()
    def _authed_post(self, path: str, body: dict) -> dict:
        headers = self._auth_headers("POST", path)
        response = self.client.post(path, json=body, headers=headers)
        response.raise_for_status()
        return response.json()

    @retry_request()
    def _authed_delete(self, path: str) -> dict:
        headers = self._auth_headers("DELETE", path)
        response = self.client.delete(path, headers=headers)
        response.raise_for_status()
        return response.json()

    # ------------------------------------------------------------------
    # Portfolio endpoints
    # ------------------------------------------------------------------

    def get_balance(self) -> int:
        """Return available balance in cents."""
        data = self._authed_get("/portfolio/balance")
        return data.get("balance", 0)

    def get_positions(self, limit: int = 100) -> list[dict]:
        """Return a list of current positions as raw dicts."""
        data = self._authed_get("/portfolio/positions", params={"limit": limit})
        return data.get("market_positions", [])

    def get_fills(self, limit: int = 100) -> list[dict]:
        """Return recent fill events as raw dicts."""
        data = self._authed_get("/portfolio/fills", params={"limit": limit})
        return data.get("fills", [])

    # ------------------------------------------------------------------
    # Order management
    # ------------------------------------------------------------------

    def create_order(
        self,
        ticker: str,
        side: str,
        action: str,
        contracts: int,
        limit_price: int,
        client_order_id: Optional[str] = None,
    ) -> dict:
        """Place a limit order.

        Parameters
        ----------
        ticker:
            Market ticker.
        side:
            ``'yes'`` or ``'no'``.
        action:
            ``'buy'`` or ``'sell'``.
        contracts:
            Number of contracts.
        limit_price:
            Limit price in cents (1-99).
        client_order_id:
            Optional idempotency key.

        Returns
        -------
        dict
            Raw API response containing at minimum ``order_id`` and ``status``.
        """
        body: dict = {
            "ticker": ticker,
            "side": side,
            "action": action,
            "count": contracts,
            "type": "limit",
            "yes_price": limit_price if side == "yes" else 100 - limit_price,
        }
        if client_order_id:
            body["client_order_id"] = client_order_id
        data = self._authed_post("/portfolio/orders", body)
        order = data.get("order", data)
        return {
            "order_id": order.get("order_id", ""),
            "status": order.get("status", "resting"),
        }

    def cancel_order(self, order_id: str) -> dict:
        """Cancel a resting order by ID."""
        return self._authed_delete(f"/portfolio/orders/{order_id}")

    def get_order(self, order_id: str) -> dict:
        """Fetch the current state of an order by ID."""
        return self._authed_get(f"/portfolio/orders/{order_id}")
