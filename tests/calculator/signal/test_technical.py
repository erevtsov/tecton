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
    close = np.array([10, 11, 12, 13, 15, 11, 10, 9, 8, 7])

    signal = donchian_channels(close, period=period)

    # Verify signal array properties
    assert isinstance(signal, np.ndarray)
    assert len(signal) == len(close)
    assert set(np.unique(signal)).issubset({-1, 0, 1})

    # First period elements should be 0 as we can't calculate channels yet
    assert np.all(signal[:period] == 0)

    # Verify specific breakouts
    assert signal[4] == 1  # Price breaks above previous 3-period high
    assert signal[8] == -1  # Price breaks below previous 3-period low


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
    donchian_channels(min_data)  # Remove second parameter
    adx(min_data, min_data, min_data)

    # Test with NaN values
    nan_data = np.array([1.0, np.nan, 3.0])

    # Should handle NaN values without errors
    ma_crossover(nan_data)
    macd(nan_data)
    donchian_channels(nan_data)  # Remove second parameter
    adx(nan_data, nan_data, nan_data)
