"""Trading infrastructure for Kalshi and Polymarket.

Modules
-------
models      Core data models: Position, Order, Fill, MarketSnapshot, TakerSignal.
feed        MarketFeed – polls the Kalshi REST API and emits live price snapshots.
recorder    FeedRecorder – persists snapshots as Parquet for orderbook reconstruction.
signals     MeanReversionSignal – produces taker-limit prices when the market
            deviates from its rolling mean.
executor    PreTradeChecker + OrderExecutor – validates balance/price then submits
            orders via the Kalshi API (paper-trading mode is the default).
notifications TradeNotifier – prints/logs trade announcements on every fill event.
positions   PositionManager – tracks open positions and computes unrealised PnL.
portfolio   PortfolioReporter – portfolio-level summaries, historical PnL curve,
            market concentration, and trades-by-hour-to-close breakdown.
"""
