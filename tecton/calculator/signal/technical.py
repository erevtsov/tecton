import numpy as np
import talib as ta


def ma_crossover(close: np.ndarray, fast_period: int = 10, slow_period: int = 20) -> np.ndarray:
    """
    Calculate Moving Average position signal.

    Generates signals based on the relative position of two moving averages:
    - Bullish Signal (1): When faster MA is above slower MA, indicating
      potential upward momentum
    - Bearish Signal (-1): When faster MA is below slower MA, indicating
      potential downward momentum
    - No Signal (0): When MAs are equal (rare) or during initialization

    Args:
        close: Array of closing prices
        fast_period: Period for the faster moving average (default: 10)
        slow_period: Period for the slower moving average (default: 20)

    Returns:
        np.ndarray: Array of signals where:
            1 = faster MA above slower MA
            -1 = faster MA below slower MA
            0 = equal or initialization period
    """
    fast_ma = ta.SMA(close, timeperiod=fast_period)
    slow_ma = ta.SMA(close, timeperiod=slow_period)

    # Calculate position-based signals
    signal = np.zeros_like(close)
    signal[fast_period:] = np.where(
        fast_ma[fast_period:] > slow_ma[fast_period:],
        1,  # Bullish position
        np.where(
            fast_ma[fast_period:] < slow_ma[fast_period:],
            -1,  # Bearish position
            0,  # Equal (rare)
        ),
    )
    return signal


def macd(
    close: np.ndarray,
    fast_period: int = 12,
    slow_period: int = 26,
    signal_period: int = 9,
) -> np.ndarray:
    """
    Calculate MACD (Moving Average Convergence Divergence) position signals.

    MACD is a trend-following momentum indicator that generates signals based on
    the relative position of MACD line vs signal line:
    - Bullish Signal (1): When MACD is above signal line
    - Bearish Signal (-1): When MACD is below signal line
    - No Signal (0): When lines are equal or during initialization

    Args:
        close: Array of closing prices
        fast_period: Period for fast EMA (default: 12)
        slow_period: Period for slow EMA (default: 26)
        signal_period: Period for signal line EMA (default: 9)

    Returns:
        np.ndarray: Array of signals where:
            1 = MACD above signal line
            -1 = MACD below signal line
            0 = equal or initialization period
    """
    macd_line, signal_line, _ = ta.MACD(
        close, fastperiod=fast_period, slowperiod=slow_period, signalperiod=signal_period
    )

    # Calculate position-based signals
    signal = np.zeros_like(close)

    # Start signals after initialization period
    start_idx = max(fast_period, slow_period, signal_period)
    signal[start_idx:] = np.where(
        macd_line[start_idx:] > signal_line[start_idx:],
        1,  # Bullish position
        np.where(
            macd_line[start_idx:] < signal_line[start_idx:],
            -1,  # Bearish position
            0,  # Equal
        ),
    )
    return signal


def donchian_channels(high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int = 20) -> np.ndarray:
    """
    Calculate Donchian Channel position signals using vectorized operations.

    Donchian Channels are volatility-based bands that generate signals based on
    price position relative to the channel:
    - Bullish Signal (1): When price is above previous period's high
    - Bearish Signal (-1): When price is below previous period's low
    - No Signal (0): When price is within the channel

    Args:
        high: Array of high prices
        low: Array of low prices
        close: Array of closing prices
        period: Lookback period (default: 20)

    Returns:
        np.ndarray: Array of signals where:
            1 = price above channel
            -1 = price below channel
            0 = price within channel or initialization
    """
    # Initialize signal array
    signal = np.zeros_like(close)

    # Skip first 'period' elements as we can't calculate channels yet
    for i in range(period, len(close)):
        # Calculate channels from previous period's data
        lookback_high = np.max(high[i - period : i])  # Upper channel
        lookback_low = np.min(low[i - period : i])  # Lower channel

        # Generate signals based on current close vs previous channels
        if close[i] > lookback_high:
            signal[i] = 1  # Bullish position
        elif close[i] < lookback_low:
            signal[i] = -1  # Bearish position
        # else: signal remains 0 (within channel)

    return signal


def adx(high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int = 14, threshold: float = 25.0) -> np.ndarray:
    """
    Calculate ADX-based signal weight.

    Provides a dynamic weight (0-1) based on trend strength:
    - Strong trends (ADX > threshold): Higher weights
    - Weak trends (ADX < threshold): Lower weights

    The weight is calculated using a sigmoid-like normalization
    that considers both ADX value and the standard threshold of 25.

    Args:
        high: Array of high prices
        low: Array of low prices
        close: Array of closing prices
        period: ADX calculation period (default: 14)
        threshold: ADX threshold for trend strength (default: 25.0)

    Returns:
        np.ndarray: Array of signal weights between 0 and 1, where:
            0.0-0.3: Very weak trend
            0.3-0.5: Weak trend
            0.5-0.7: Moderate trend
            0.7-1.0: Strong trend
    """
    if len(high) < period + 1:
        return np.array([np.nan] * len(high))

    # Calculate ADX
    adx = ta.ADX(high, low, close, timeperiod=period)

    # Replace None/inf values with NaN
    adx = np.nan_to_num(adx, nan=np.nan, posinf=np.nan, neginf=np.nan)

    # Calculate normalized weights using a modified sigmoid function
    # This provides a smooth transition around the threshold
    weights = 1 / (1 + np.exp(-(adx - threshold) / 10))

    return weights
