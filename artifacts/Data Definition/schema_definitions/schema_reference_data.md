# Schema: Reference Data Tables

These files are used by the **Silver Layer ETL** and **Scoring Engine** to translate raw GDELT codes/IDs into human-readable text.

| Table Name | Source | Use Case |
| :--- | :--- | :--- |
| `cameo_eventcodes.csv` | GDELT Ref | Translates `EventRootCode` into Action Labels (e.g., "Protest"). |
| `cameo_actortypes.csv` | GDELT Ref | Translates `ActorType` codes (e.g., `GOV`, `MIL`). |
| `fips_countries.csv` | GDELT Ref | Translates 2-digit FIPS codes into Country Names. |
| `cameo_quadclass.csv` | GDELT Ref | Maps events to 4 broad categories (Verbal/Material Cooperation/Conflict). |

---

### `cameo_eventcodes.csv`
| Field | Type | Description |
| :--- | :--- | :--- |
| `code` | STRING | 2-digit CAMEO root code (e.g. `14`). |
| `label` | STRING | The name of the action (e.g. "Protest"). |

### `fips_countries.csv`
| Field | Type | Description |
| :--- | :--- | :--- |
| `code` | STRING | 2-character FIPS country code (e.g. `US`, `CH`). |
| `country` | STRING | The full name of the nation. |

### `cameo_actortypes.csv`
| Field | Type | Description |
| :--- | :--- | :--- |
| `code` | STRING | 3-character type code (e.g. `GOV` for Government). |
| `type` | STRING | Description of the actor role. |
