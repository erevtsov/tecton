from enum import Enum


class StatType(Enum):
    opening_price = 1  # The price and quantity of the first trade of an instrument.
    indicative_opening_price = 2  # The probable price of the first trade of an instrument published during pre-open.
    settlement_price = 3  # The settlement price of an instrument. Flags will indicate whether the price is final or preliminary and actual or theoretical.
    trading_session_low_price = 4  # The lowest trade price of an instrument during the trading session.
    trading_session_high_price = 5  # The highest trade price of an instrument during the trading session.
    cleared_volume = 6  # The number of contracts cleared for an instrument on the previous trading date.
    lowest_offer = 7  # The lowest offer price for an instrument during the trading session.
    highest_bid = 8  # The highest bid price for an instrument during the trading session.
    open_interest = 9  # The current number of outstanding contracts of an instrument.
    fixing_price = 10  # The volume-weighted average price (VWAP) for a fixing period.
