from tecton.ingestion.apitools.alpha_vantage import etf_profile


def test_etf_profile():
    symbol = 'SPY'
    response = etf_profile(symbol)
    print('ETF profile response:', response.head())
