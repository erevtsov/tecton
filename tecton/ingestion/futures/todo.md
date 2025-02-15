## TODOs on Futures Data Ingestion

### Necessary Utils
<ol>
    <li>Processor for descriptive data</li>
    <li>Processor for statistical data</li>
    <ol>
        <li>decision tree if multiple data points per day</li>
    </ol>
</ol>

### Process Historical Data
- assume 

### Process Live Data
- check if file for said month exists? if not, query API?

#### Misc Helpers/Functionality
- i need to store the futures universe somewhere

#### Misc things to keep in mind
- the quoted currency can change through time




* build processor for definition and statistics datasets
* build pipeline for processing historical data
    - what's the storage pattern?
        - monthly, for all assets
        - how to handle expanding universe?
    - how to handle missing data?
* build pipeline for updating live data



* Determine full investment universe
    - download last month of complete CME data
    - pick futures >200m ADV