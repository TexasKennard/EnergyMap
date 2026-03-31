# EIA Energy Asset Downloader

This revised Streamlit app addresses the main causes of local UI lockups in the first version.

## What changed

1. The selection manifest is compact and no longer embeds the full state polygon.
2. The backend uses POST for ArcGIS queries that include geometry, which avoids oversized URLs.
3. The download output is compressed as `.geojson.gz` instead of rendering or storing a large pretty-printed GeoJSON string.
4. Large in-memory download state is cleared whenever the user changes the state, asset, or SQL filter.

## Run

```bash
pip install -r requirements.txt
streamlit run app.py
```

## Common cause of the original freeze

The first version rendered a large manifest on every rerun and included both the full polygon geometry and a massive URL-encoded preview URL. Large states such as Texas and Michigan could therefore make the app appear unresponsive even before the full asset download finished.
