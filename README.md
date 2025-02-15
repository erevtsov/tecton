# tecton

### Notes on env setup:

#### Install uv

`curl -LsSf https://astral.sh/uv/install.sh | sh`

#### sync dependencies
`uv sync`

#### .bashrc
```
source ~/.local/bin/env
source .venv/bin/activate
```

### Target Data Calls

```
from tecton.dal.loader import Scout

Scout.fetch(Scout.tables.universe, start, end, elements=[])

```

### TODOs

#### APIs to set up
- alpha vantage
- yahoo finance
- polygon
- tiingo
- financial modeling prep

#### Features
- lookback capability


