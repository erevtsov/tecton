import pytest
import yaml

from tecton.dal.instrument.futures.market import Market, Markets


@pytest.fixture
def sample_config():
    return {
        'ES': {'name': 'E-mini S&P 500', 'asset_class': 'Equity', 'sector': 'Developed', 'sub_sector': 'US'},
        'GC': {'name': 'Gold', 'asset_class': 'Commodity', 'sector': 'Metals', 'sub_sector': 'Precious'},
        '6E': {'name': 'Euro FX', 'asset_class': 'FX', 'sector': 'Developed', 'sub_sector': 'EUR'},
    }


@pytest.fixture
def sample_markets(sample_config, tmp_path):
    # Create temporary config file
    config_path = tmp_path / 'config.yaml'
    with open(config_path, 'w') as f:
        yaml.dump(sample_config, f)

    return Markets.from_config(config_path=config_path)


def test_market_creation():
    market = Market(root='ES', name='E-mini S&P 500', asset_class='Equity', sector='Developed')
    assert market.root == 'ES'
    assert market.name == 'E-mini S&P 500'
    assert market.asset_class == 'Equity'
    assert market.sector == 'Developed'


def test_market_immutability():
    market = Market(root='ES', name='E-mini S&P 500', asset_class='Equity', sector='Developed')
    with pytest.raises(AttributeError):
        market.root = 'GC'


def test_markets_load_from_config(sample_markets):
    assert len(sample_markets) == 3
    assert 'ES' in sample_markets
    assert 'GC' in sample_markets
    assert '6E' in sample_markets


def test_markets_load_specific_symbols(sample_config, tmp_path):
    config_path = tmp_path / 'config.yaml'
    with open(config_path, 'w') as f:
        yaml.dump(sample_config, f)

    Market.__module__ = str(tmp_path)
    markets = Markets.from_config(roots=['ES', 'GC'])
    assert len(markets) == 2
    assert 'ES' in markets
    assert 'GC' in markets
    assert '6E' not in markets


def test_markets_filter_by_asset_class(sample_markets):
    equity_markets = sample_markets.filter(asset_class='Equity')
    assert len(equity_markets) == 1
    assert 'ES' in equity_markets
    assert 'GC' not in equity_markets


def test_markets_filter_by_sector(sample_markets):
    developed_markets = sample_markets.filter(sector='Developed')
    assert len(developed_markets) == 2
    assert 'ES' in developed_markets
    assert '6E' in developed_markets
    assert 'GC' not in developed_markets


def test_markets_filter_combined(sample_markets):
    equity_developed = sample_markets.filter(asset_class='Equity', sector='Developed')
    assert len(equity_developed) == 1
    assert 'ES' in equity_developed
    assert '6E' not in equity_developed
    assert 'GC' not in equity_developed


def test_markets_asset_classes(sample_markets):
    assert sample_markets.asset_classes == {'Equity', 'Commodity', 'FX'}


def test_markets_sectors(sample_markets):
    assert sample_markets.sectors == {'Developed', 'Metals'}


def test_markets_iteration(sample_markets):
    symbols = [market.symbol for market in sample_markets]
    assert len(symbols) == 3
    assert 'ES' in symbols
    assert 'GC' in symbols
    assert '6E' in symbols


def test_empty_markets():
    markets = Markets()
    assert len(markets) == 0
    assert markets.asset_classes == set()
    assert markets.sectors == set()


def test_markets_no_recursion(sample_markets):
    """Verify that iterating over markets doesn't cause recursion"""
    # Should complete without RecursionError
    list(sample_markets)
    list(sample_markets.sectors)
    list(sample_markets.asset_classes)
