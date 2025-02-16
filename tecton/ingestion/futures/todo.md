## TODOs on Futures Data Ingestion

- set up API access 
- implement asset for daily updates (that are still stored to monthly files)
    - maybe save the daily data in addition to appending to monthly files
- implement continuous ticker concept
    - make sure to account for contango
- add monthly file parsing to Mantle select

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