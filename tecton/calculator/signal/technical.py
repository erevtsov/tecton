import numpy as np
import talib as ta


def ma_crossover(close: np.ndarray, fast_period: int = 10, slow_period: int = 20) -> np.ndarray:
    """
    Calculate Moving Average Crossover signal.

    The MA crossover is a trend-following indicator that generates signals when two moving
    averages cross each other:
    - Bullish Signal (1): When the faster MA crosses above the slower MA, indicating
      potential upward momentum and a buy signal
    - Bearish Signal (-1): When the faster MA crosses below the slower MA, indicating
      potential downward momentum and a sell signal
    - No Signal (0): When no crossover occurs

    Interpretation:
    - The signal strength depends on the time periods chosen - longer periods generate
      fewer but potentially more reliable signals
    - False signals are common in choppy/sideways markets
    - Best used in conjunction with other indicators and in trending markets

    Args:
        close: Array of closing prices
        fast_period: Period for the faster moving average (default: 10)
        slow_period: Period for the slower moving average (default: 20)

    Returns:
        np.ndarray: Array of signals where:
            1 = bullish crossover
            -1 = bearish crossover
            0 = no signal
    """
    fast_ma = ta.SMA(close, timeperiod=fast_period)
    slow_ma = ta.SMA(close, timeperiod=slow_period)

    # Calculate crossover signals
    signal = np.zeros_like(close)
    signal[1:] = np.where(
        (fast_ma[1:] > slow_ma[1:]) & (fast_ma[:-1] <= slow_ma[:-1]),
        1,  # Bullish crossover
        np.where((fast_ma[1:] < slow_ma[1:]) & (fast_ma[:-1] >= slow_ma[:-1]), -1, 0),  # Bearish crossover
    )
    return signal


def macd(
    close: np.ndarray,
    fast_period: int = 12,
    slow_period: int = 26,
    signal_period: int = 9,
) -> np.ndarray:
    """
    Calculate MACD (Moving Average Convergence Divergence) crossover signals.

    MACD is a trend-following momentum indicator that generates signals when the MACD
    line crosses the signal line:
    - Bullish Signal (1): When MACD crosses above signal line
    - Bearish Signal (-1): When MACD crosses below signal line
    - No Signal (0): When no crossover occurs

    Args:
        close: Array of closing prices
        fast_period: Period for fast EMA (default: 12)
        slow_period: Period for slow EMA (default: 26)
        signal_period: Period for signal line EMA (default: 9)

    Returns:
        np.ndarray: Array of signals where:
            1 = bullish crossover
            -1 = bearish crossover
            0 = no signal
    """
    macd_line, signal_line, _ = ta.MACD(
        close, fastperiod=fast_period, slowperiod=slow_period, signalperiod=signal_period
    )

    # Calculate crossover signals
    signal = np.zeros_like(close)
    signal[1:] = np.where(
        (macd_line[1:] > signal_line[1:]) & (macd_line[:-1] <= signal_line[:-1]),
        1,  # Bullish crossover
        np.where(
            (macd_line[1:] < signal_line[1:]) & (macd_line[:-1] >= signal_line[:-1]),
            -1,  # Bearish crossover
            0,
        ),
    )
    return signal


def donchian_channels(high: np.ndarray, low: np.ndarray, period: int = 20) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Calculate Donchian Channels.

    Donchian Channels are volatility-based bands consisting of three lines:
    - Upper: Highest high over the specified period
    - Middle: Average of upper and lower bands
    - Lower: Lowest low over the specified period

    Interpretation:
    1. Trend Direction:
       - Uptrend: Price consistently near upper band
       - Downtrend: Price consistently near lower band
       - Sideways: Price oscillating around middle band

    2. Breakouts:
       - Bullish: Price breaks above upper band
       - Bearish: Price breaks below lower band

    3. Channel Width:
       - Widening channels suggest increasing volatility
       - Narrowing channels suggest decreasing volatility

    4. Trading Signals:
       - Traditional: Buy at upper band, sell at lower band
       - Breakout: Enter when price moves outside the bands
       - Mean Reversion: Enter when price reaches extremes

    Args:
        high: Array of high prices
        low: Array of low prices
        period: Lookback period (default: 20)

    Returns:
        Tuple containing:
        - upper: Upper band (highest high)
        - middle: Middle band (average of upper and lower)
        - lower: Lower band (lowest low)
    """
    upper = np.array([np.nan] * len(high))
    lower = np.array([np.nan] * len(low))

    for i in range(period - 1, len(high)):
        upper[i] = np.nanmax(high[i - period + 1 : i + 1])
        lower[i] = np.nanmin(low[i - period + 1 : i + 1])

    middle = (upper + lower) / 2
    return upper, middle, lower


def adx(
    high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int = 14
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Calculate Average Directional Index (ADX).

    ADX measures trend strength regardless of direction, while +DI and -DI
    indicate trend direction:
    - ADX: Trend strength (0-100)
    - +DI: Positive directional movement
    - -DI: Negative directional movement

    Interpretation:
    1. ADX Values:
       - 0-25: Weak trend / No trend
       - 25-50: Strong trend
       - 50-75: Very strong trend
       - 75-100: Extremely strong trend

    2. Directional Movement:
       - When +DI crosses above -DI: Potential bullish signal
       - When -DI crosses above +DI: Potential bearish signal

    3. Trend Confirmation:
       - Rising ADX: Trend is strengthening
       - Falling ADX: Trend is weakening
       - Flat ADX: Trend strength stable

    4. Best Practices:
       - ADX > 25: Trend-following strategies more effective
       - ADX < 25: Range-trading strategies more effective
       - Use with price action and other indicators for confirmation

    Args:
        high: Array of high prices
        low: Array of low prices
        close: Array of closing prices
        period: Calculation period (default: 14)

    Returns:
        Tuple containing:
        - adx: Average Directional Index
        - plus_di: Positive Directional Indicator
        - minus_di: Negative Directional Indicator
    """
    if len(high) < period + 1:
        return (np.array([np.nan] * len(high)), np.array([np.nan] * len(high)), np.array([np.nan] * len(high)))

    adx = ta.ADX(high, low, close, timeperiod=period)
    plus_di = ta.PLUS_DI(high, low, close, timeperiod=period)
    minus_di = ta.MINUS_DI(high, low, close, timeperiod=period)

    # Replace None/inf values with NaN
    adx = np.nan_to_num(adx, nan=np.nan, posinf=np.nan, neginf=np.nan)
    plus_di = np.nan_to_num(plus_di, nan=np.nan, posinf=np.nan, neginf=np.nan)
    minus_di = np.nan_to_num(minus_di, nan=np.nan, posinf=np.nan, neginf=np.nan)

    return adx, plus_di, minus_di
