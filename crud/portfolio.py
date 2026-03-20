"""Portfolio timeline and cache-related CRUD operations."""

from pymongo.mongo_client import MongoClient
import pandas as pd
import logging

from .base import get_date_range


def get_portfolio_timeline(db: MongoClient, user_name: str, spielzeit: str = "2024/2025") -> pd.DataFrame:
    """Get portfolio value timeline for a specific user in a specific season"""

    date_from, date_to = get_date_range(spielzeit)

    # Starting budget
    STARTING_BUDGET = 40_000_000  # 40 million euros

    # Get all transfers for the user in this season
    transfers_collection = db["Transfers"]
    user_transfers = list(transfers_collection.find({
        "member_name": user_name,
        "buy.date": {"$gte": date_from, "$lte": date_to}
    }, {"_id": 0}))

    # Initialize timeline with starting budget
    timeline_data = []
    portfolio_players = {}  # Track current portfolio: {player_id: buy_info}
    available_cash = STARTING_BUDGET

    # Add starting point
    timeline_data.append({
        'Datum': pd.to_datetime(date_from).date(),
        'Portfolio_Wert_Kaufpreis': 0,
        'Verfuegbares_Cash': available_cash,
        'Gesamtwert': available_cash,
        'Anzahl_Spieler': 0,
        'Event_Type': 'start',
        'Event_Player': 'Season Start',
        'Event_Price': 0
    })

    if not user_transfers:
        return pd.DataFrame(timeline_data)

    # Get players collection for market value lookups
    players_collection = db["Players"]

    # Create all events
    all_events = []

    for transfer in user_transfers:
        # Add buy event
        all_events.append({
            'date': transfer['buy']['date'],
            'type': 'buy',
            'player_id': transfer['player_id'],
            'player_name': transfer['player_name'],
            'price': transfer['buy']['price']
        })

        # Add sell event if exists
        if transfer.get('sell'):
            all_events.append({
                'date': transfer['sell']['date'],
                'type': 'sell',
                'player_id': transfer['player_id'],
                'player_name': transfer['player_name'],
                'price': transfer['sell']['price']
            })

    # Sort all events by date
    all_events.sort(key=lambda x: x['date'])

    # Process events and calculate portfolio value at each point
    for event in all_events:
        event_date = event['date']
        # Convert to date object for comparison (removes time/timezone issues)
        if isinstance(event_date, str):
            event_date_obj = pd.to_datetime(event_date).date()
        else:
            event_date_obj = pd.to_datetime(event_date).date()

        if event['type'] == 'buy':
            available_cash -= event['price']
            portfolio_players[event['player_id']] = {
                'name': event['player_name'],
                'buy_price': event['price'],
                'buy_date': event_date
            }
        elif event['type'] == 'sell':
            if event['player_id'] in portfolio_players:
                available_cash += event['price']
                del portfolio_players[event['player_id']]

        # Calculate current portfolio values
        total_investment = sum(player['buy_price'] for player in portfolio_players.values())

        # Calculate current market value of all players in portfolio
        current_market_value = 0
        for player_id in portfolio_players.keys():
            player = players_collection.find_one({"id": int(player_id)})
            if player and 'price_history' in player:
                # Find market value closest to event_date
                price_history = player['price_history']

                # Filter price history up to event_date (using dates only)
                valid_prices = []
                for entry in price_history:
                    try:
                        # Convert timestamp to date for comparison
                        if isinstance(entry['timestamp'], str):
                            entry_date = pd.to_datetime(entry['timestamp']).date()
                        else:
                            entry_date = pd.to_datetime(entry['timestamp']).date()

                        # Simple date comparison (no timezone issues)
                        if entry_date <= event_date_obj:
                            valid_prices.append(entry)
                    except (ValueError, TypeError, KeyError):
                        # Skip entries with invalid timestamps
                        continue

                if valid_prices:
                    # Get the most recent price before or on event_date
                    # Sort by timestamp to get the latest one
                    latest_price = max(valid_prices, key=lambda x: x['timestamp'])
                    current_market_value += latest_price['quotedPrice']
                else:
                    # Fallback to buy price if no market data available
                    current_market_value += portfolio_players[player_id]['buy_price']
            else:
                # Fallback to buy price if player not found
                current_market_value += portfolio_players[player_id]['buy_price']

        # Total value = available cash + current market value of players
        total_value = available_cash + current_market_value

        timeline_data.append({
            'Datum': event_date_obj,
            'Portfolio_Wert_Kaufpreis': total_investment,
            'Portfolio_Wert_Aktuell': current_market_value,
            'Verfuegbares_Cash': available_cash,
            'Gesamtwert': total_value,
            'Anzahl_Spieler': len(portfolio_players),
            'Event_Type': event['type'],
            'Event_Player': event['player_name'],
            'Event_Price': event['price']
        })

    return pd.DataFrame(timeline_data)


def get_portfolio_current_value_timeline(db: MongoClient, user_name: str, spielzeit: str = "2024/2025") -> pd.DataFrame:
    """Get portfolio current market value timeline (sample at weekly intervals)"""

    # Get current team
    date_from, date_to = get_date_range(spielzeit)
    transfers_collection = db["Transfers"]

    # Get current players (bought but not sold)
    current_players_transfers = list(transfers_collection.find({
        "member_name": user_name,
        "buy.date": {"$gte": date_from, "$lte": date_to},
        "sell": {"$exists": False}
    }, {"_id": 0}))

    if not current_players_transfers:
        return pd.DataFrame()

    # Get player IDs
    player_ids = [transfer['player_id'] for transfer in current_players_transfers]

    # Sample dates (weekly from season start to now)
    start_date = pd.to_datetime(date_from).date()
    end_date = min(pd.to_datetime(date_to).date(), pd.to_datetime('today').date())

    # Create weekly date range
    date_range = pd.date_range(start=start_date, end=end_date, freq='W')

    timeline_data = []
    players_collection = db["Players"]

    for sample_datetime in date_range:
        sample_date = sample_datetime.date()  # Convert to date for comparison
        total_market_value = 0
        valid_players = 0

        for player_id in player_ids:
            player = players_collection.find_one({"id": int(player_id)})
            if player and 'price_history' in player:
                # Find market value closest to sample_date
                price_history = player['price_history']

                # Filter price history up to sample_date (using dates only)
                valid_prices = []
                for entry in price_history:
                    try:
                        # Convert timestamp to date for comparison
                        if isinstance(entry['timestamp'], str):
                            entry_date = pd.to_datetime(entry['timestamp']).date()
                        else:
                            entry_date = pd.to_datetime(entry['timestamp']).date()

                        # Simple date comparison
                        if entry_date <= sample_date:
                            valid_prices.append(entry)
                    except (ValueError, TypeError, KeyError):
                        continue

                if valid_prices:
                    # Get the most recent price before or on sample_date
                    latest_price = max(valid_prices, key=lambda x: x['timestamp'])
                    total_market_value += latest_price['quotedPrice']
                    valid_players += 1

        if valid_players > 0:
            timeline_data.append({
                'Datum': sample_date,
                'Marktwert_Gesamt': total_market_value,
                'Anzahl_Spieler': valid_players
            })

    return pd.DataFrame(timeline_data)


def get_or_calculate_portfolio_timeline(db: MongoClient, user_name: str, spielzeit: str = "2024/2025") -> pd.DataFrame:
    """Get portfolio timeline from cache and calculate missing dates"""

    try:
        cache_collection = db["PortfolioCache"]
        cache_key = f"{user_name}_{spielzeit}"
        date_from, date_to = get_date_range(spielzeit)
        today = pd.to_datetime('today').date()

        # Try to get from cache first
        cached_result = cache_collection.find_one({"cache_key": cache_key})

        if cached_result:
            # Get cached timeline data
            cached_timeline_data = cached_result["timeline_data"]
            cached_df = pd.DataFrame(cached_timeline_data)

            if not cached_df.empty and 'Datum' in cached_df.columns:
                cached_df['Datum'] = pd.to_datetime(cached_df['Datum']).dt.date

                # Find the last cached date
                last_cached_date = cached_df['Datum'].max()

                print(f"Found cache for {user_name} up to {last_cached_date}")

                # Determine what needs to be calculated
                needs_calculation = False
                calculation_start_date = None

                # Check if we need to calculate missing dates
                if last_cached_date < today:
                    needs_calculation = True
                    calculation_start_date = last_cached_date
                    print(f"Need to calculate from {last_cached_date} to {today}")

                # Always recalculate today to get latest market values and any new transfers
                elif last_cached_date == today:
                    needs_calculation = True
                    calculation_start_date = last_cached_date
                    print(f"Recalculating today ({today}) for latest market values")

                if needs_calculation:
                    # Calculate the missing portion
                    missing_df = calculate_portfolio_timeline_from_date(
                        db, user_name, spielzeit, calculation_start_date
                    )

                    if not missing_df.empty:
                        # Combine cached data (excluding overlap) with new data
                        if calculation_start_date == last_cached_date:
                            # Remove the last cached date if we're recalculating it
                            cached_df = cached_df[cached_df['Datum'] < last_cached_date]

                        combined_df = pd.concat([cached_df, missing_df], ignore_index=True)
                        combined_df = combined_df.sort_values('Datum').reset_index(drop=True)

                        # Update cache with combined data
                        update_portfolio_cache(db, cache_key, user_name, spielzeit, combined_df)

                        print(f"Updated cache with {len(missing_df)} new entries")
                        return combined_df
                    else:
                        print("No new data calculated, returning cached data")
                        return cached_df
                else:
                    print("Cache is up to date")
                    return cached_df

        # Calculate from scratch if no cache exists
        print(f"No cache found for {user_name}, calculating from scratch")
        timeline_df = calculate_portfolio_timeline_optimized(db, user_name, spielzeit)

        # Save to cache
        if not timeline_df.empty:
            update_portfolio_cache(db, cache_key, user_name, spielzeit, timeline_df)

        return timeline_df

    except Exception as e:
        print(f"Cache error: {e}")
        # Fallback to direct calculation without caching
        return calculate_portfolio_timeline_optimized(db, user_name, spielzeit)


def update_portfolio_cache(db: MongoClient, cache_key: str, user_name: str, spielzeit: str, timeline_df: pd.DataFrame):
    """Update the portfolio cache with new timeline data"""
    cache_collection = db["PortfolioCache"]

    # Convert dates to strings for BSON compatibility
    cache_data = timeline_df.copy()
    if 'Datum' in cache_data.columns:
        cache_data['Datum'] = cache_data['Datum'].astype(str)

    cache_doc = {
        "cache_key": cache_key,
        "user_name": user_name,
        "spielzeit": spielzeit,
        "timeline_data": cache_data.to_dict('records'),
        "calculated_at": pd.Timestamp.now().isoformat()
    }

    # Upsert to cache (creates collection if it doesn't exist)
    cache_collection.replace_one(
        {"cache_key": cache_key},
        cache_doc,
        upsert=True
    )


def calculate_portfolio_timeline_from_date(db: MongoClient, user_name: str, spielzeit: str, from_date) -> pd.DataFrame:
    """Calculate portfolio timeline starting from a specific date to today"""

    date_from, date_to = get_date_range(spielzeit)
    today = pd.to_datetime('today').date()

    # Convert from_date to proper format
    if hasattr(from_date, 'strftime'):
        from_date_obj = from_date
    else:
        from_date_obj = pd.to_datetime(from_date).date()

    print(f"Calculating portfolio timeline from {from_date_obj} to {today}")

    # Get all transfers for this user in the season
    transfers_collection = db["Transfers"]
    all_transfers = list(transfers_collection.find({
        "member_name": user_name,
        "buy.date": {"$gte": date_from, "$lte": date_to}
    }, {"_id": 0}))

    if not all_transfers:
        return pd.DataFrame()

    # Build complete event timeline up to today
    all_events = []
    for transfer in all_transfers:
        # Buy event
        buy_date = pd.to_datetime(transfer['buy']['date']).date()
        all_events.append({
            'date': buy_date,
            'date_str': transfer['buy']['date'],
            'type': 'buy',
            'player_id': transfer['player_id'],
            'player_name': transfer['player_name'],
            'price': transfer['buy']['price']
        })

        # Sell event if exists
        if transfer.get('sell'):
            sell_date = pd.to_datetime(transfer['sell']['date']).date()
            all_events.append({
                'date': sell_date,
                'date_str': transfer['sell']['date'],
                'type': 'sell',
                'player_id': transfer['player_id'],
                'player_name': transfer['player_name'],
                'price': transfer['sell']['price']
            })

    # Sort all events by date
    all_events.sort(key=lambda x: x['date'])

    # Filter events to only those from from_date onwards
    relevant_events = [event for event in all_events if event['date'] >= from_date_obj]

    if not relevant_events:
        return pd.DataFrame()

    # Get all unique player IDs for price lookup
    all_player_ids = set()
    for transfer in all_transfers:
        all_player_ids.add(transfer['player_id'])

    # Bulk fetch price history for all players
    players_collection = db["Players"]
    players_data = {}

    for player_doc in players_collection.find(
        {"id": {"$in": list(map(int, all_player_ids))}},
        {"id": 1, "price_history": 1}
    ):
        player_id = str(player_doc["id"])
        price_history = []
        for entry in player_doc.get("price_history", []):
            try:
                if isinstance(entry['timestamp'], str):
                    entry_date = pd.to_datetime(entry['timestamp']).date()
                else:
                    entry_date = pd.to_datetime(entry['timestamp']).date()

                price_history.append({
                    'date': entry_date,
                    'price': entry['quotedPrice']
                })
            except (ValueError, TypeError, KeyError):
                continue

        price_history.sort(key=lambda x: x['date'])
        players_data[player_id] = price_history

    # Rebuild portfolio state up to from_date
    portfolio_players = {}
    available_cash = 40_000_000  # Starting budget

    # Process all events up to (but not including) from_date to rebuild state
    for event in all_events:
        if event['date'] < from_date_obj:
            if event['type'] == 'buy':
                available_cash -= event['price']
                portfolio_players[event['player_id']] = {
                    'name': event['player_name'],
                    'buy_price': event['price'],
                    'buy_date': event['date_str']
                }
            elif event['type'] == 'sell':
                if event['player_id'] in portfolio_players:
                    available_cash += event['price']
                    del portfolio_players[event['player_id']]

    # Now create daily timeline from from_date onwards
    timeline_data = []
    end_date = min(today, pd.to_datetime(date_to).date())

    # Create a dictionary of events by date for easy lookup
    events_by_date = {}
    for event in relevant_events:
        event_date = event['date']
        if event_date not in events_by_date:
            events_by_date[event_date] = []
        events_by_date[event_date].append(event)

    # Process each day from from_date to today
    current_date = from_date_obj
    while current_date <= end_date:
        # Process any events that happened on this date
        last_event_type = None
        last_event_player = None
        last_event_price = 0

        if current_date in events_by_date:
            for event in events_by_date[current_date]:
                # Update portfolio based on event
                if event['type'] == 'buy':
                    available_cash -= event['price']
                    portfolio_players[event['player_id']] = {
                        'name': event['player_name'],
                        'buy_price': event['price'],
                        'buy_date': event['date_str']
                    }
                    last_event_type = 'buy'
                    last_event_player = event['player_name']
                    last_event_price = event['price']
                elif event['type'] == 'sell':
                    if event['player_id'] in portfolio_players:
                        available_cash += event['price']
                        del portfolio_players[event['player_id']]
                        last_event_type = 'sell'
                        last_event_player = event['player_name']
                        last_event_price = event['price']

        # Calculate daily portfolio values (regardless of whether there were events)
        total_investment = sum(player['buy_price'] for player in portfolio_players.values())
        current_market_value = get_portfolio_market_value_fast(players_data, portfolio_players, current_date)
        total_value = available_cash + current_market_value

        # Determine event type for this day
        if last_event_type:
            event_type = last_event_type
            event_player = last_event_player
            event_price = last_event_price
        else:
            event_type = 'daily_update'
            event_player = 'Market Update'
            event_price = 0

        timeline_data.append({
            'Datum': current_date,
            'Portfolio_Wert_Kaufpreis': total_investment,
            'Portfolio_Wert_Aktuell': current_market_value,
            'Verfuegbares_Cash': available_cash,
            'Gesamtwert': total_value,
            'Anzahl_Spieler': len(portfolio_players),
            'Event_Type': event_type,
            'Event_Player': event_player,
            'Event_Price': event_price
        })

        # Move to next day
        current_date += pd.Timedelta(days=1).to_pytimedelta()

    return pd.DataFrame(timeline_data)


def calculate_portfolio_timeline_optimized(db: MongoClient, user_name: str, spielzeit: str = "2024/2025") -> pd.DataFrame:
    """Optimized portfolio timeline calculation with bulk operations"""

    date_from, date_to = get_date_range(spielzeit)
    STARTING_BUDGET = 40_000_000

    # Get all transfers for the user in this season
    transfers_collection = db["Transfers"]
    user_transfers = list(transfers_collection.find({
        "member_name": user_name,
        "buy.date": {"$gte": date_from, "$lte": date_to}
    }, {"_id": 0}))

    if not user_transfers:
        # Return just starting point
        return pd.DataFrame([{
            'Datum': pd.to_datetime(date_from).date(),
            'Portfolio_Wert_Kaufpreis': 0,
            'Portfolio_Wert_Aktuell': 0,
            'Verfuegbares_Cash': STARTING_BUDGET,
            'Gesamtwert': STARTING_BUDGET,
            'Anzahl_Spieler': 0,
            'Event_Type': 'start',
            'Event_Player': 'Season Start',
            'Event_Price': 0
        }])

    # Get all unique player IDs that this user ever owned
    all_player_ids = set()
    for transfer in user_transfers:
        all_player_ids.add(transfer['player_id'])

    # Bulk fetch ALL price history for ALL players at once
    players_collection = db["Players"]
    players_data = {}

    logging.debug(f"Loading price data for {len(all_player_ids)} unique players")
    logging.debug(f"Player IDs to load: {sorted(all_player_ids)}")

    for player_doc in players_collection.find(
        {"id": {"$in": list(map(int, all_player_ids))}},
        {"id": 1, "price_history": 1}
    ):
        player_id = str(player_doc["id"])
        logging.debug(f"Loading price data for player ID: {player_id}")
        # Pre-process price history into date-sorted format
        price_history = []
        for entry in player_doc.get("price_history", []):
            try:
                if isinstance(entry['timestamp'], str):
                    entry_date = pd.to_datetime(entry['timestamp']).date()
                else:
                    entry_date = pd.to_datetime(entry['timestamp']).date()

                price_history.append({
                    'date': entry_date,
                    'price': entry['quotedPrice']
                })
            except (ValueError, TypeError, KeyError):
                continue

        # Sort by date for efficient lookup
        price_history.sort(key=lambda x: x['date'])
        players_data[player_id] = price_history

        logging.debug(f"Player {player_id}: {len(price_history)} price entries")
        if price_history:
            logging.debug(f"  Date range: {price_history[0]['date']} to {price_history[-1]['date']}")

    logging.debug(f"Loaded price data for {len(players_data)} players")
    logging.debug(f"Players loaded: {sorted(players_data.keys())}")

    # Create all events
    all_events = []
    for transfer in user_transfers:
        all_events.append({
            'date': transfer['buy']['date'],
            'type': 'buy',
            'player_id': transfer['player_id'],
            'player_name': transfer['player_name'],
            'price': transfer['buy']['price']
        })

        if transfer.get('sell'):
            all_events.append({
                'date': transfer['sell']['date'],
                'type': 'sell',
                'player_id': transfer['player_id'],
                'player_name': transfer['player_name'],
                'price': transfer['sell']['price']
            })

    # Sort all events by date
    all_events.sort(key=lambda x: x['date'])

    # Initialize timeline
    timeline_data = []
    portfolio_players = {}
    available_cash = STARTING_BUDGET

    # Add starting point
    timeline_data.append({
        'Datum': pd.to_datetime(date_from).date(),
        'Portfolio_Wert_Kaufpreis': 0,
        'Portfolio_Wert_Aktuell': 0,
        'Verfuegbares_Cash': available_cash,
        'Gesamtwert': available_cash,
        'Anzahl_Spieler': 0,
        'Event_Type': 'start',
        'Event_Player': 'Season Start',
        'Event_Price': 0
    })

    # Generate daily timeline from season start to today
    start_date = pd.to_datetime(date_from).date()
    end_date = min(pd.to_datetime('today').date(), pd.to_datetime(date_to).date())

    # Create a dictionary of events by date for easy lookup
    events_by_date = {}
    for event in all_events:
        event_date = event['date']
        if isinstance(event_date, str):
            event_date_obj = pd.to_datetime(event_date).date()
        else:
            event_date_obj = pd.to_datetime(event_date).date()

        if event_date_obj not in events_by_date:
            events_by_date[event_date_obj] = []
        events_by_date[event_date_obj].append(event)

    # Process each day from start to end
    current_date = start_date
    while current_date <= end_date:
        # Process any events that happened on this date
        last_event_type = None
        last_event_player = None
        last_event_price = 0

        if current_date in events_by_date:
            for event in events_by_date[current_date]:
                # Update portfolio based on event
                if event['type'] == 'buy':
                    available_cash -= event['price']
                    portfolio_players[event['player_id']] = {
                        'name': event['player_name'],
                        'buy_price': event['price'],
                        'buy_date': event['date']
                    }
                    last_event_type = 'buy'
                    last_event_player = event['player_name']
                    last_event_price = event['price']
                elif event['type'] == 'sell':
                    if event['player_id'] in portfolio_players:
                        available_cash += event['price']
                        del portfolio_players[event['player_id']]
                        last_event_type = 'sell'
                        last_event_player = event['player_name']
                        last_event_price = event['price']

        # Calculate daily portfolio values (regardless of whether there were events)
        total_investment = sum(player['buy_price'] for player in portfolio_players.values())
        current_market_value = get_portfolio_market_value_fast(players_data, portfolio_players, current_date)
        total_value = available_cash + current_market_value

        # Determine event type for this day
        if current_date == start_date:
            event_type = 'start'
            event_player = 'Season Start'
            event_price = 0
        elif last_event_type:
            event_type = last_event_type
            event_player = last_event_player
            event_price = last_event_price
        else:
            event_type = 'daily_update'
            event_player = 'Market Update'
            event_price = 0

        timeline_data.append({
            'Datum': current_date,
            'Portfolio_Wert_Kaufpreis': total_investment,
            'Portfolio_Wert_Aktuell': current_market_value,
            'Verfuegbares_Cash': available_cash,
            'Gesamtwert': total_value,
            'Anzahl_Spieler': len(portfolio_players),
            'Event_Type': event_type,
            'Event_Player': event_player,
            'Event_Price': event_price
        })

        # Move to next day
        current_date += pd.Timedelta(days=1).to_pytimedelta()

    return pd.DataFrame(timeline_data)


def get_portfolio_market_value_fast(players_data, portfolio_players, target_date):
    """Fast market value calculation using pre-loaded price data"""
    total_market_value = 0

    logging.debug(f"=== MARKET VALUE LOOKUP DEBUG for {target_date} ===")
    logging.debug(f"Portfolio contains {len(portfolio_players)} players")

    for player_id in portfolio_players.keys():
        player_name = portfolio_players[player_id]['name']
        buy_price = portfolio_players[player_id]['buy_price']

        logging.debug(f"Player: {player_name} (ID: {player_id})")
        logging.debug(f"  Buy price: {buy_price}")
        logging.debug(f"  player_id type: {type(player_id)}, value: {repr(player_id)}")
        logging.debug(f"  Available player keys: {list(players_data.keys())}")

        # Convert player_id to string for lookup (price data keys are strings)
        player_id_str = str(player_id)
        if player_id_str in players_data:
            price_history = players_data[player_id_str]
            logging.debug(f"  Price history entries: {len(price_history)}")

            if price_history:
                # Show first and last entries for context
                logging.debug(f"  First entry: {price_history[0]['date']} -> {price_history[0]['price']}")
                logging.debug(f"  Last entry: {price_history[-1]['date']} -> {price_history[-1]['price']}")

                # Check data types for comparison
                logging.debug(f"  Target date type: {type(target_date)}, value: {target_date}")
                logging.debug(f"  First price date type: {type(price_history[0]['date'])}, value: {price_history[0]['date']}")

                # Binary search or linear search for the right price
                # Since it's sorted by date, we can use bisect for O(log n) lookup
                valid_price = None
                found_entry = None

                for i, price_entry in enumerate(reversed(price_history)):  # Start from most recent
                    logging.debug(f"  Checking entry {len(price_history)-i-1}: {price_entry['date']} <= {target_date}? {price_entry['date'] <= target_date}")
                    if price_entry['date'] <= target_date:
                        valid_price = price_entry['price']
                        found_entry = price_entry
                        break

                if valid_price is not None and found_entry is not None:
                    logging.debug(f"  Found market value: {valid_price} from {found_entry['date']}")
                    total_market_value += valid_price
                else:
                    logging.warning(f"  No market value found for {target_date}, using buy price: {buy_price}")
                    total_market_value += buy_price
            else:
                logging.warning(f"  Empty price history, using buy price: {buy_price}")
                total_market_value += buy_price
        else:
            logging.warning(f"  Player not found in price data, using buy price: {buy_price}")
            total_market_value += buy_price

    logging.debug(f"Total market value: {total_market_value}")
    logging.debug(f"=== END MARKET VALUE LOOKUP DEBUG ===")

    return total_market_value


def get_or_calculate_market_value_timeline(db: MongoClient, user_name: str, spielzeit: str = "2024/2025") -> pd.DataFrame:
    """Get market value timeline from cache or calculate and cache if not exists"""

    try:
        cache_collection = db["MarketValueCache"]
        cache_key = f"{user_name}_{spielzeit}_market"

        # Try to get from cache first
        cached_result = cache_collection.find_one({"cache_key": cache_key})

        if cached_result:
            timeline_data = cached_result["timeline_data"]
            df = pd.DataFrame(timeline_data)
            # Convert date strings back to date objects
            if not df.empty and 'Datum' in df.columns:
                df['Datum'] = pd.to_datetime(df['Datum']).dt.date
            return df

        # Calculate if not in cache
        timeline_df = calculate_market_value_timeline_optimized(db, user_name, spielzeit)

        # Save to cache
        if not timeline_df.empty:
            # Convert dates to strings for BSON compatibility
            cache_data = timeline_df.copy()
            if 'Datum' in cache_data.columns:
                cache_data['Datum'] = cache_data['Datum'].astype(str)

            cache_doc = {
                "cache_key": cache_key,
                "user_name": user_name,
                "spielzeit": spielzeit,
                "timeline_data": cache_data.to_dict('records'),
                "calculated_at": pd.Timestamp.now().isoformat()
            }

            cache_collection.replace_one(
                {"cache_key": cache_key},
                cache_doc,
                upsert=True
            )

        return timeline_df

    except Exception as e:
        print(f"Cache error: {e}")
        # Fallback to direct calculation without caching
        return calculate_market_value_timeline_optimized(db, user_name, spielzeit)


def calculate_market_value_timeline_optimized(db: MongoClient, user_name: str, spielzeit: str = "2024/2025") -> pd.DataFrame:
    """Optimized market value timeline calculation"""

    date_from, date_to = get_date_range(spielzeit)
    transfers_collection = db["Transfers"]

    # Get current players (bought but not sold)
    current_players_transfers = list(transfers_collection.find({
        "member_name": user_name,
        "buy.date": {"$gte": date_from, "$lte": date_to},
        "sell": {"$exists": False}
    }, {"_id": 0}))

    if not current_players_transfers:
        return pd.DataFrame()

    # Get player IDs
    player_ids = [transfer['player_id'] for transfer in current_players_transfers]

    # Bulk fetch price history for all current players
    players_collection = db["Players"]
    players_price_data = {}

    for player_doc in players_collection.find(
        {"id": {"$in": list(map(int, player_ids))}},
        {"id": 1, "price_history": 1}
    ):
        player_id = str(player_doc["id"])
        price_history = []
        for entry in player_doc.get("price_history", []):
            try:
                if isinstance(entry['timestamp'], str):
                    entry_date = pd.to_datetime(entry['timestamp']).date()
                else:
                    entry_date = pd.to_datetime(entry['timestamp']).date()

                price_history.append({
                    'date': entry_date,
                    'price': entry['quotedPrice']
                })
            except (ValueError, TypeError, KeyError):
                continue

        price_history.sort(key=lambda x: x['date'])
        players_price_data[player_id] = price_history

    # Sample dates (weekly)
    start_date = pd.to_datetime(date_from).date()
    end_date = min(pd.to_datetime(date_to).date(), pd.to_datetime('today').date())
    date_range = pd.date_range(start=start_date, end=end_date, freq='W')

    timeline_data = []

    for sample_datetime in date_range:
        sample_date = sample_datetime.date()
        total_market_value = 0
        valid_players = 0

        for player_id in player_ids:
            if player_id in players_price_data:
                price_history = players_price_data[player_id]

                # Find the most recent price before or on sample_date
                valid_price = None
                for price_entry in reversed(price_history):
                    if price_entry['date'] <= sample_date:
                        valid_price = price_entry['price']
                        break

                if valid_price is not None:
                    total_market_value += valid_price
                    valid_players += 1

        if valid_players > 0:
            timeline_data.append({
                'Datum': sample_date,
                'Marktwert_Gesamt': total_market_value,
                'Anzahl_Spieler': valid_players
            })

    return pd.DataFrame(timeline_data)


def clear_portfolio_cache(db: MongoClient, user_name: str | None = None, spielzeit: str | None = None):
    """Clear portfolio cache for specific user/season or all"""
    cache_collection = db["PortfolioCache"]
    market_cache_collection = db["MarketValueCache"]

    query = {}
    if user_name:
        query["user_name"] = user_name
    if spielzeit:
        query["spielzeit"] = spielzeit

    deleted_portfolio = cache_collection.delete_many(query).deleted_count
    deleted_market = market_cache_collection.delete_many(query).deleted_count

    return deleted_portfolio + deleted_market


def get_cache_status(db: MongoClient) -> pd.DataFrame:
    """Get status of all cached portfolio calculations"""

    cache_collection = db["PortfolioCache"]
    market_cache_collection = db["MarketValueCache"]

    cache_data = []

    # Portfolio cache
    for doc in cache_collection.find({}, {"cache_key": 1, "user_name": 1, "spielzeit": 1, "calculated_at": 1}):
        cache_data.append({
            "Type": "Portfolio",
            "User": doc.get("user_name"),
            "Season": doc.get("spielzeit"),
            "Cache Key": doc.get("cache_key"),
            "Calculated At": doc.get("calculated_at")
        })

    # Market value cache
    for doc in market_cache_collection.find({}, {"cache_key": 1, "user_name": 1, "spielzeit": 1, "calculated_at": 1}):
        cache_data.append({
            "Type": "Market Value",
            "User": doc.get("user_name"),
            "Season": doc.get("spielzeit"),
            "Cache Key": doc.get("cache_key"),
            "Calculated At": doc.get("calculated_at")
        })

    # Always return a DataFrame, even if cache_data is empty
    return pd.DataFrame(cache_data)
