import numpy as np
import pytest
import talib as ta

from tecton.calculator.signal.technical import adx, donchian_channels, ma_crossover, macd


@pytest.fixture
def sample_data():
    # Create sample price data
    close = np.array([10, 11, 12, 11, 10, 9, 8, 7, 8, 9, 10, 11, 12, 13, 14], dtype=float)
    high = close + 1
    low = close - 1
    return high, low, close


def test_ma_crossover(sample_data):
    _, _, close = sample_data
    # Test data: trend up, then down
    close = np.array([10, 11, 12, 13, 14, 13, 12, 11, 10, 9], dtype=float)
    signals = ma_crossover(close, fast_period=3, slow_period=5)

    # Verify signal array shape
    assert len(signals) == len(close)

    # Verify signals are -1, 0, or 1
    assert set(np.unique(signals)).issubset({-1, 0, 1})

    # First fast_period elements should be 0 due to initialization
    assert np.all(signals[:3] == 0)

    # During uptrend, fast MA should be above slow MA (bullish)
    uptrend_idx = 4  # After initialization period
    assert signals[uptrend_idx] == 1

    # During downtrend, fast MA should be below slow MA (bearish)
    downtrend_idx = 8  # After price peaks and declines
    assert signals[downtrend_idx] == -1

    # Calculate actual MAs to verify signals match MA positions
    fast_ma = ta.SMA(close, timeperiod=3)
    slow_ma = ta.SMA(close, timeperiod=5)

    # Verify signals match MA positions after initialization
    valid_idx = ~np.isnan(fast_ma) & ~np.isnan(slow_ma)
    expected_signals = np.where(fast_ma > slow_ma, 1, np.where(fast_ma < slow_ma, -1, 0))
    np.testing.assert_array_equal(signals[valid_idx], expected_signals[valid_idx])


def test_macd():
    # Test data: price trending up, then down
    close = np.array([10.0, 11, 12, 13, 14, 15, 14, 13, 12, 11])

    signal = macd(close, fast_period=2, slow_period=4, signal_period=2)

    # Verify signal array properties
    assert isinstance(signal, np.ndarray)
    assert len(signal) == len(close)
    assert set(np.unique(signal)).issubset({-1, 0, 1})

    # First elements should be 0 during initialization
    start_idx = max(2, 4, 2)  # max of fast, slow, signal periods
    assert np.all(signal[:start_idx] == 0)

    # Calculate actual MACD values to verify signals
    macd_line, signal_line, _ = ta.MACD(close, fastperiod=2, slowperiod=4, signalperiod=2)

    # Verify signals match MACD line position vs signal line
    valid_idx = ~np.isnan(macd_line) & ~np.isnan(signal_line)
    expected_signals = np.where(macd_line > signal_line, 1, np.where(macd_line < signal_line, -1, 0))
    np.testing.assert_array_equal(signal[valid_idx], expected_signals[valid_idx])


def test_donchian_channels():
    # Test data with clear position patterns
    period = 3
    # Index:    0   1   2   3   4   5   6   7   8   9
    close = np.array([10, 11, 12, 13, 15, 11, 10, 9, 7, 8])
    high = np.array([11, 12, 13, 14, 16, 12, 11, 10, 8, 9])
    low = np.array([9, 10, 11, 12, 14, 10, 9, 8, 6, 7])

    signal = donchian_channels(high, low, close, period=period)

    # Verify signal array properties
    assert isinstance(signal, np.ndarray)
    assert len(signal) == len(close)
    assert set(np.unique(signal)).issubset({-1, 0, 1})

    # First period elements should be 0 as we can't calculate channels yet
    assert np.all(signal[:period] == 0)

    # Verify bullish position (above channel)
    # At index 4, close should be above previous high (positions 1-3)
    prev_high = max(high[1:4])  # Should be 13
    assert close[4] > prev_high  # 15 > 13
    assert signal[4] == 1  # Should show bullish position

    # Verify bearish position (below channel)
    # At index 8, close should be below previous low (positions 5-7)
    prev_low = min(low[5:8])  # Should be 8
    assert close[8] < prev_low  # 7 < 8
    assert signal[8] == -1  # Should show bearish position

    # Verify neutral position (within channel)
    # At index 6, close should be within previous channel (positions 3-5)
    prev_high = max(high[3:6])  # Should be 16
    prev_low = min(low[3:6])  # Should be 10
    assert prev_low <= close[6] <= prev_high  # 10 within [10, 16]
    assert signal[6] == 0  # Should show neutral position


def test_adx(sample_data):
    # Create test data with known trend patterns
    close = np.array(
        [
            10.0,
            11,
            12,
            13,
            14,
            15,
            16,
            17,
            18,
            19,  # Strong uptrend
            19,
            18,
            17,
            16,
            15,
            14,
            13,
            12,
            11,
            10,
        ]
    )  # Strong downtrend
    high = close + 0.5
    low = close - 0.5

    period = 14
    threshold = 25.0
    weights = adx(high, low, close, period=period, threshold=threshold)

    # Verify shape and initialization
    assert len(weights) == len(close)
    assert np.all(np.isnan(weights[:period]))

    # Verify weight ranges
    valid_idx = ~np.isnan(weights)
    assert np.all((weights[valid_idx] >= 0) & (weights[valid_idx] <= 1))

    # Get actual ADX values for comparison
    adx_values = ta.ADX(high, low, close, timeperiod=period)
    valid_idx = ~np.isnan(adx_values)

    # Strong trend periods should have higher weights
    strong_trend_idx = adx_values > threshold
    weak_trend_idx = adx_values <= threshold

    # Check weight distributions
    strong_weights = weights[strong_trend_idx & valid_idx]
    weak_weights = weights[weak_trend_idx & valid_idx]

    if len(strong_weights) > 0 and len(weak_weights) > 0:
        assert np.mean(strong_weights) > 0.5  # Strong trends should have higher weights
        assert np.mean(weak_weights) < 0.5  # Weak trends should have lower weights

    # Test threshold sensitivity
    weights_strict = adx(high, low, close, period=period, threshold=30)
    weights_loose = adx(high, low, close, period=period, threshold=20)

    valid_idx = ~np.isnan(weights_strict) & ~np.isnan(weights) & ~np.isnan(weights_loose)
    if np.any(valid_idx):
        assert np.mean(weights_strict[valid_idx]) <= np.mean(weights[valid_idx])
        assert np.mean(weights_loose[valid_idx]) >= np.mean(weights[valid_idx])


def test_edge_cases():
    # Test with minimal data
    min_data = np.array([1.0, 2.0, 3.0])

    # Should not raise errors
    ma_crossover(min_data)
    macd(min_data)
    donchian_channels(min_data, min_data, min_data)
    adx(min_data, min_data, min_data)

    # Test with NaN values
    nan_data = np.array([1.0, np.nan, 3.0])

    # Should handle NaN values without errors
    ma_crossover(nan_data)
    macd(nan_data)
    donchian_channels(nan_data, nan_data, nan_data)
    adx(nan_data, nan_data, nan_data)
