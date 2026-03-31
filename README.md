# EIA Energy Asset Downloader

This Streamlit app lets a user:

1. choose a U.S. state,
2. choose a single energy infrastructure layer,
3. generate the backend selection manifest as JSON, and
4. download the state-clipped asset layer as GeoJSON.

## Files

- `app.py` — Streamlit UI plus backend query logic.
- `asset_registry.json` — external registry of ArcGIS layer endpoints.
- `requirements.txt` — minimal Python dependencies.

## Run

```bash
pip install -r requirements.txt
streamlit run app.py
```

## How it works

The backend does three things:

1. queries the state boundary layer for the chosen state polygon,
2. builds an ArcGIS `query` payload for the chosen asset layer, and
3. paginates through the result set and emits GeoJSON.

The `asset_registry.json` file is the extension point. To add another dataset, add another object under `assets` with:

- `label`
- `category`
- `layer_url`
- `page_size`
- `default_where`
- `supports_geojson`
- `description`

## Caveat

This app depends on the upstream ArcGIS `FeatureServer` layers remaining public and queryable.
