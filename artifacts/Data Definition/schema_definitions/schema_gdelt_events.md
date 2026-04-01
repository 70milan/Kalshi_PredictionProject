# Schema: GDELT Events

**Script:** `gdelt_events_ingest.py`  
**Output:** `data/bronze/gdelt/gdelt_events/[latest.parquet / gdelt_XX.parquet]`

| Field | Type | Description |
| :--- | :--- | :--- |
| `GLOBALEVENTID` | BIGINT | Unique ID for the event. |
| `SQLDATE` | STRING | Date the event took place (YYYYMMDD). |
| `Actor1Name` | STRING | Entity name of the primary actor. |
| `Actor2Name` | STRING | Entity name of the secondary actor (at whom the action is directed). |
| `EventCode` | STRING | CAMEO action code (e.g. `010` for Make Public Statement). |
| `EventBaseCode` | STRING | Three-digit root version of the EventCode. |
| `EventRootCode` | STRING | Two-digit root version of the EventCode (the highest level of action). |
| `GoldsteinScale` | DECIMAL | Theoretical impact an event will have on state stability (-10.0 to +10.0). |
| `NumMentions` | INT | Number of times this event was mentioned across all source documents. |
| `NumSources` | INT | Number of distinct news sources reporting this event. |
| `NumArticles` | INT | Number of distinct source articles mentioning this event. |
| `AvgTone` | DECIMAL | Average sentiment/tone of all articles mentioning the event (-100 to +100). |
| `Actor1Geo_CountryCode` | STRING | FIPS country code of Actor1's location. |
| `Actor2Geo_CountryCode` | STRING | FIPS country code of Actor2's location. |
| `ActionGeo_CountryCode` | STRING | FIPS country code where the action actually took place. |
| `ActionGeo_FullName` | STRING | Human-readable name of the location where the action took place. |
| `SOURCEURL` | STRING | URL of the first news document that reported this event. |
| `ingested_at` | STRING | UTC ISO timestamp showing when this record entered PredictIQ. |
