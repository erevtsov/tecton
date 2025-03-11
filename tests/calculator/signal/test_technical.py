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


def test_donchian_channels(sample_data):
    high, low, _ = sample_data
    period = 3
    upper, middle, lower = donchian_channels(high, low, period=period)

    # Verify shapes
    assert len(upper) == len(high)
    assert len(middle) == len(high)
    assert len(lower) == len(high)

    # First period-1 values should be NaN
    assert np.all(np.isnan(upper[: period - 1]))
    assert np.all(np.isnan(lower[: period - 1]))
    assert np.all(np.isnan(middle[: period - 1]))

    # Verify middle is average of upper and lower for non-NaN values
    valid_idx = ~np.isnan(middle)
    np.testing.assert_array_almost_equal(middle[valid_idx], (upper[valid_idx] + lower[valid_idx]) / 2)

    # Verify upper >= lower for non-NaN values
    assert np.all(upper[valid_idx] >= lower[valid_idx])


def test_adx(sample_data):
    high, low, close = sample_data
    period = 3
    score, plus_di, minus_di = adx(high, low, close, period=period)

    # Verify shapes
    assert len(score) == len(close)
    assert len(plus_di) == len(close)
    assert len(minus_di) == len(close)

    # First period values should be NaN due to initialization
    assert np.all(np.isnan(score[:period]))
    assert np.all(np.isnan(plus_di[:period]))
    assert np.all(np.isnan(minus_di[:period]))

    # Verify ADX and DI ranges for non-NaN values
    valid_idx = ~np.isnan(score)
    assert np.all((score[valid_idx] >= 0) & (score[valid_idx] <= 100))
    assert np.all((plus_di[valid_idx] >= 0) & (plus_di[valid_idx] <= 100))
    assert np.all((minus_di[valid_idx] >= 0) & (minus_di[valid_idx] <= 100))


def test_edge_cases():
    # Test with minimal data
    min_data = np.array([1.0, 2.0, 3.0])

    # Should not raise errors
    ma_crossover(min_data)
    macd(min_data)
    donchian_channels(min_data, min_data)
    adx(min_data, min_data, min_data)

    # Test with NaN values
    nan_data = np.array([1.0, np.nan, 3.0])

    # Should handle NaN values without errors
    ma_crossover(nan_data)
    macd(nan_data)
    donchian_channels(nan_data, nan_data)
    adx(nan_data, nan_data, nan_data)
