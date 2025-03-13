from collections import UserDict
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import yaml


@dataclass(frozen=True)
class Market:
    """
    Represents a futures market with its associated metadata.
    Immutable to allow use as dictionary key.
    """

    root: str
    name: str
    asset_class: str
    sector: str
    sub_sector: str


class Markets(UserDict):
    """
    Collection of Market instances with convenient filtering and lookup methods.
    """

    def __init__(self, markets: dict[str, Market] | None = None):
        # Initialize with empty dict if None provided
        super().__init__(markets or {})

    @classmethod
    def from_config(cls, roots: list[str] | None = None, config_path: Path | None = None) -> 'Markets':
        """
        Load markets from config file

        Args:
            roots: Optional list of market symbols to load
            config_path: Optional path to config file. If None, uses default
        """
        if config_path is None:
            config_path = Path(__file__).parent / 'config.yaml'
        with open(config_path) as f:
            config = yaml.load(f.read(), Loader=yaml.FullLoader)
        markets = {}
        for root, data in config.items():
            if roots is None or root in roots:
                markets[root] = Market(
                    root=root,
                    name=data['name'],
                    asset_class=data['asset_class'],
                    sector=data['sector'],
                    sub_sector=data.get('sub_sector', ''),
                )
        return cls(markets)

    @property
    def asset_classes(self) -> set[str]:
        """Get unique asset classes in collection"""
        return {market.asset_class for market in self.data.values()}

    @property
    def sectors(self) -> set[str]:
        """Get unique sectors in collection"""
        return {market.sector for market in self.data.values()}

    def filter(self, asset_class: str | None = None, sector: str | None = None) -> 'Markets':
        """
        Filter markets by asset class and/or sector

        Args:
            asset_class: Optional asset class to filter by
            sector: Optional sector to filter by

        Returns:
            New Markets instance containing only matching markets
        """
        filtered = {}
        for root, market in self.data.items():
            if asset_class and market.asset_class != asset_class:
                continue
            if sector and market.sector != sector:
                continue
            filtered[root] = market
        return Markets(filtered)

    def to_table(self) -> pd.DataFrame:
        return pd.DataFrame.from_dict(self.data)

    def __iter__(self) -> Iterator[Market]:
        """Iterate over Market instances rather than symbols."""
        return iter(self.data.values())


# Example usage:
if __name__ == '__main__':
    # Load all markets
    markets = Markets.from_config()

    # Get single market
    es = markets['ES']

    # Filter markets
    equity_markets = markets.filter(asset_class='Equity')
    us_markets = markets.filter(sector='US')
    us_equity = markets.filter(asset_class='Equity', sector='US')

    # Iterate through markets
    for market in markets:
        print(f'{market.root}: {market.name}')

    # Get unique categories
    print(f'Asset Classes: {markets.asset_classes}')
    print(f'Sectors: {markets.sectors}')
