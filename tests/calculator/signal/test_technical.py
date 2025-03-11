import numpy as np
import pytest

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
    signals = ma_crossover(close, fast_period=3, slow_period=5)

    # Verify signal array shape
    assert len(signals) == len(close)

    # Verify signals are -1, 0, or 1
    assert set(np.unique(signals)).issubset({-1, 0, 1})

    # First signal should be 0 due to initialization
    assert signals[0] == 0


def test_macd():
    # Test data: price trending up, then down
    close = np.array([10.0, 11, 12, 13, 14, 15, 14, 13, 12, 11])

    signal = macd(close, fast_period=2, slow_period=4, signal_period=2)

    # Verify signal array properties
    assert isinstance(signal, np.ndarray)
    assert len(signal) == len(close)
    assert set(np.unique(signal)).issubset({-1, 0, 1})  # Signal should only contain -1, 0, 1

    # First element should be 0 as we can't calculate signal for it
    assert signal[0] == 0


def test_donchian_channels():
    # Test data with clear breakout patterns
    period = 3
    # Construct test data with clear breakout patterns
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

    # At index 4, check if price breaks above the channel (positions 1-3)
    prev_high = max(high[1:4])  # Should be 13 from positions 1-3
    assert close[4] > prev_high  # 15 > 13
    assert signal[4] == 1  # Should be bullish breakout

    # At index 8, check if price breaks below the channel (positions 5-7)
    prev_low = min(low[5:8])  # Should be 8 from positions 5-7
    assert close[8] < prev_low  # 7 < 8
    assert signal[8] == -1  # Should be bearish breakout


def test_adx(sample_data):
    high, low, close = sample_data
    period = 14
    weights = adx(high, low, close, period=period)

    # Verify shape
    assert len(weights) == len(close)

    # First period values should be NaN due to initialization
    assert np.all(np.isnan(weights[:period]))

    # Verify weight ranges for non-NaN values
    valid_idx = ~np.isnan(weights)
    assert np.all((weights[valid_idx] >= 0) & (weights[valid_idx] <= 1))

    # Test with threshold adjustment
    weights_strict = adx(high, low, close, period=period, threshold=30)
    weights_loose = adx(high, low, close, period=period, threshold=20)

    # Compare means only for valid (non-NaN) values
    valid_idx = ~np.isnan(weights_strict) & ~np.isnan(weights) & ~np.isnan(weights_loose)
    if np.any(valid_idx):  # Only compare if we have valid data
        strict_mean = np.mean(weights_strict[valid_idx])
        base_mean = np.mean(weights[valid_idx])
        loose_mean = np.mean(weights_loose[valid_idx])

        # Allow for small numerical differences
        assert strict_mean <= base_mean + 1e-10
        assert loose_mean >= base_mean - 1e-10


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
