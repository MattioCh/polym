"""Authenticated Kalshi trading client.

Extends the read-only :class:`~src.indexers.kalshi.client.KalshiClient` with
order submission, balance inquiry, and position retrieval.

Authentication uses a Kalshi API key passed as the ``Authorization`` header.
Set ``KALSHI_API_KEY`` in your ``.env`` file (see ``.env.example``).

Example usage::

    import os
    from src.trading.kalshi.client import KalshiTradingClient
    from src.trading.models import Order

    with KalshiTradingClient(api_key=os.environ["KALSHI_API_KEY"]) as client:
        balance = client.get_balance()
        print(f"Available balance: {balance / 100:.2f} USD")

        order = Order(
            ticker="INXD-24DEC31-B6000",
            side="yes",
            action="buy",
            quantity=5,
            price=42,
            rationale="mean_reversion entry",
        )
        submitted = client.submit_order(order)
        print(f"Order submitted: {submitted.order_id}")
"""

from __future__ import annotations

from datetime import datetime, timezone

from src.indexers.kalshi.client import KALSHI_API_HOST, KalshiClient
from src.trading.models import Fill, Order, Position


class KalshiTradingClient(KalshiClient):
    """Kalshi client with authenticated trading capabilities.

    Args:
        api_key: Kalshi API key for the ``Authorization`` header.
        host: Base API URL (defaults to the production Kalshi API).
    """

    def __init__(self, api_key: str, host: str = KALSHI_API_HOST) -> None:
        super().__init__(host=host)
        # Override the httpx client to include auth headers
        import httpx

        self.client = httpx.Client(
            base_url=host,
            timeout=30.0,
            headers={"Authorization": f"Bearer {api_key}"},
        )

    # ------------------------------------------------------------------
    # Account endpoints
    # ------------------------------------------------------------------

    def get_balance(self) -> int:
        """Fetch available account balance.

        Returns:
            Available balance in **cents**.
        """
        data = self._get("/portfolio/balance")
        # Kalshi returns balance in cents as an integer
        return int(data.get("balance", 0))

    # ------------------------------------------------------------------
    # Position endpoints
    # ------------------------------------------------------------------

    def get_positions(self, limit: int = 100) -> list[Position]:
        """Fetch all open positions.

        Args:
            limit: Maximum number of positions to return per page.

        Returns:
            List of open :class:`~src.trading.models.Position` objects.
        """
        positions: list[Position] = []
        cursor: str | None = None

        while True:
            params: dict = {"limit": limit, "settlement_status": "unsettled"}
            if cursor:
                params["cursor"] = cursor

            data = self._get("/portfolio/positions", params=params)
            raw_positions = data.get("market_positions", [])

            for p in raw_positions:
                # Kalshi positions: net_position > 0 means long Yes,
                # negative means long No.  Normalise to side + unsigned qty.
                net = p.get("position", 0)
                if net == 0:
                    continue
                side = "yes" if net > 0 else "no"
                quantity = abs(net)
                avg_price = p.get("market_exposure", 0)
                # avg entry price = total cost / contracts
                entry_price_cents = abs(round(avg_price / quantity)) if quantity else 0

                positions.append(
                    Position(
                        ticker=p.get("ticker", ""),
                        title=p.get("title", ""),
                        side=side,
                        quantity=quantity,
                        avg_entry_price=entry_price_cents,
                        entry_time=datetime.now(timezone.utc),
                    )
                )

            cursor = data.get("cursor")
            if not cursor:
                break

        return positions

    # ------------------------------------------------------------------
    # Order endpoints
    # ------------------------------------------------------------------

    def submit_order(self, order: Order) -> Order:
        """Submit a limit order to Kalshi.

        The order is submitted as a limit-GTC order.  On success the
        ``order_id`` and ``submitted_at`` fields on the returned object are
        populated.

        Args:
            order: The order to submit.

        Returns:
            A copy of the order with ``order_id`` and ``submitted_at`` set.

        Raises:
            httpx.HTTPStatusError: If the API rejects the order.
        """

        body = {
            "ticker": order.ticker,
            "action": order.action,
            "type": "limit",
            "side": order.side,
            "count": order.quantity,
            "yes_price": order.price if order.side == "yes" else 100 - order.price,
            "no_price": order.price if order.side == "no" else 100 - order.price,
        }

        response = self.client.post("/portfolio/orders", json=body)
        response.raise_for_status()
        data = response.json()

        raw = data.get("order", {})
        submitted = Order(
            ticker=order.ticker,
            side=order.side,
            action=order.action,
            quantity=order.quantity,
            price=order.price,
            order_id=raw.get("order_id"),
            submitted_at=datetime.now(timezone.utc),
            rationale=order.rationale,
        )
        return submitted

    def cancel_order(self, order_id: str) -> bool:
        """Cancel an open order.

        Args:
            order_id: The order ID returned by :meth:`submit_order`.

        Returns:
            ``True`` if cancelled successfully, ``False`` otherwise.
        """
        try:
            response = self.client.delete(f"/portfolio/orders/{order_id}")
            response.raise_for_status()
            return True
        except Exception as exc:  # noqa: BLE001
            import sys

            print(f"[cancel_order] Failed to cancel {order_id}: {exc}", file=sys.stderr)
            return False

    def get_fills(self, limit: int = 100, min_ts: int | None = None) -> list[Fill]:
        """Fetch recent order fills.

        Args:
            limit: Maximum fills to return per page (default 100).
            min_ts: Unix timestamp filter — only fills after this time.

        Returns:
            List of :class:`~src.trading.models.Fill` objects, newest first.
        """
        from src.indexers.kalshi.models import parse_datetime

        fills: list[Fill] = []
        cursor: str | None = None

        while True:
            params: dict = {"limit": limit}
            if cursor:
                params["cursor"] = cursor
            if min_ts is not None:
                params["min_ts"] = min_ts

            data = self._get("/portfolio/fills", params=params)
            raw_fills = data.get("fills", [])

            for f in raw_fills:
                fills.append(
                    Fill(
                        fill_id=f.get("fill_id", ""),
                        order_id=f.get("order_id", ""),
                        ticker=f.get("ticker", ""),
                        side=f.get("side", "yes"),
                        action=f.get("action", "buy"),
                        quantity=f.get("count", 0),
                        price=f.get("yes_price", 0) if f.get("side") == "yes" else f.get("no_price", 0),
                        timestamp=parse_datetime(f["created_time"]) if f.get("created_time") else datetime.now(timezone.utc),
                    )
                )

            cursor = data.get("cursor")
            if not cursor:
                break

        return fills
