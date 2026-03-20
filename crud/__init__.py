"""crud package — re-exports all public symbols for backwards compatibility.

Importing from ``crud`` directly continues to work as before:

    import crud
    crud.get_transfers(db, spielzeit)
    crud.get_player_points_df(db, spielzeit)
    # ...
"""

from .base import (  # noqa: F401
    SEASON_DATE_RANGES,
    DEFAULT_DATE_RANGE,
    get_date_range,
)

from .transfers import (  # noqa: F401
    get_transfers,
    count_second_bids,
    count_transfers_buys,
)

from .players import (  # noqa: F401
    get_player_market_value,
    get_current_market_values_bulk,
    get_player_market_values_df,
    get_player_current_market_values_df,
    get_player_points_df,
    get_player_points,
    get_player_points_between_dates,
    get_player_points_with_market_value_df,
)

from .portfolio import (  # noqa: F401
    get_portfolio_timeline,
    get_portfolio_current_value_timeline,
    get_or_calculate_portfolio_timeline,
    update_portfolio_cache,
    calculate_portfolio_timeline_from_date,
    calculate_portfolio_timeline_optimized,
    get_portfolio_market_value_fast,
    get_or_calculate_market_value_timeline,
    calculate_market_value_timeline_optimized,
    clear_portfolio_cache,
    get_cache_status,
)
