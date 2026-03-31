from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import requests
import streamlit as st

APP_DIR = Path(__file__).resolve().parent
REGISTRY_PATH = APP_DIR / "asset_registry.json"
REQUEST_TIMEOUT = 120
USER_AGENT = "eia-energy-asset-downloader/0.1"

STATES = [
    ("Alabama", "AL"),
    ("Alaska", "AK"),
    ("Arizona", "AZ"),
    ("Arkansas", "AR"),
    ("California", "CA"),
    ("Colorado", "CO"),
    ("Connecticut", "CT"),
    ("Delaware", "DE"),
    ("District of Columbia", "DC"),
    ("Florida", "FL"),
    ("Georgia", "GA"),
    ("Hawaii", "HI"),
    ("Idaho", "ID"),
    ("Illinois", "IL"),
    ("Indiana", "IN"),
    ("Iowa", "IA"),
    ("Kansas", "KS"),
    ("Kentucky", "KY"),
    ("Louisiana", "LA"),
    ("Maine", "ME"),
    ("Maryland", "MD"),
    ("Massachusetts", "MA"),
    ("Michigan", "MI"),
    ("Minnesota", "MN"),
    ("Mississippi", "MS"),
    ("Missouri", "MO"),
    ("Montana", "MT"),
    ("Nebraska", "NE"),
    ("Nevada", "NV"),
    ("New Hampshire", "NH"),
    ("New Jersey", "NJ"),
    ("New Mexico", "NM"),
    ("New York", "NY"),
    ("North Carolina", "NC"),
    ("North Dakota", "ND"),
    ("Ohio", "OH"),
    ("Oklahoma", "OK"),
    ("Oregon", "OR"),
    ("Pennsylvania", "PA"),
    ("Rhode Island", "RI"),
    ("South Carolina", "SC"),
    ("South Dakota", "SD"),
    ("Tennessee", "TN"),
    ("Texas", "TX"),
    ("Utah", "UT"),
    ("Vermont", "VT"),
    ("Virginia", "VA"),
    ("Washington", "WA"),
    ("West Virginia", "WV"),
    ("Wisconsin", "WI"),
    ("Wyoming", "WY"),
]
STATE_NAMES = [name for name, _ in STATES]
STATE_ABBR = {name: abbr for name, abbr in STATES}


class BackendError(RuntimeError):
    """Raised when an upstream ArcGIS query fails."""


def _request_json(url: str, params: dict[str, Any]) -> dict[str, Any]:
    headers = {"User-Agent": USER_AGENT}
    response = requests.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    payload = response.json()

    if isinstance(payload, dict) and "error" in payload:
        message = payload["error"].get("message", "Unknown ArcGIS error")
        details = payload["error"].get("details") or []
        detail_text = " | ".join(str(item) for item in details)
        if detail_text:
            message = f"{message}: {detail_text}"
        raise BackendError(message)

    return payload


@st.cache_data(show_spinner=False)
def load_registry() -> dict[str, Any]:
    return json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))


@st.cache_data(show_spinner=False)
def fetch_state_geometry(states_layer_url: str, state_name: str) -> tuple[dict[str, Any], dict[str, Any]]:
    query_url = f"{states_layer_url.rstrip('/')}/query"
    escaped_name = state_name.replace("'", "''")
    params = {
        "where": f"NAME = '{escaped_name}'",
        "outFields": "NAME,STUSAB,GEOID",
        "returnGeometry": "true",
        "returnCentroid": "false",
        "f": "json",
        "outSR": 4326,
    }
    payload = _request_json(query_url, params)
    features = payload.get("features", [])
    if not features:
        raise BackendError(f"No state geometry returned for {state_name}.")

    feature = features[0]
    geometry = feature.get("geometry")
    attributes = feature.get("attributes", {})
    if not geometry:
        raise BackendError(f"State geometry was empty for {state_name}.")
    return geometry, attributes


def build_manifest(
    *,
    state_name: str,
    asset_key: str,
    custom_where: str,
    registry: dict[str, Any],
) -> dict[str, Any]:
    asset = registry["assets"][asset_key]
    state_layer = registry["state_boundaries"]
    geometry, state_attributes = fetch_state_geometry(state_layer["layer_url"], state_name)

    query_params: dict[str, Any] = {
        "where": custom_where or asset.get("default_where", "1=1"),
        "geometry": geometry,
        "geometryType": "esriGeometryPolygon",
        "inSR": 4326,
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": "*",
        "returnGeometry": "true",
        "outSR": 4326,
        "f": "geojson" if asset.get("supports_geojson", True) else "json",
        "resultOffset": 0,
        "resultRecordCount": asset.get("page_size", 1000),
    }

    query_url = f"{asset['layer_url'].rstrip('/')}/query"

    request_preview_params = query_params.copy()
    request_preview_params["geometry"] = json.dumps(geometry, separators=(",", ":"))
    request_preview_url = f"{query_url}?{urlencode(request_preview_params)}"

    return {
        "selection": {
            "state_name": state_name,
            "state_abbr": state_attributes.get("STUSAB", STATE_ABBR.get(state_name, "")),
            "asset_key": asset_key,
            "asset_label": asset["label"],
            "asset_category": asset.get("category", ""),
        },
        "state_lookup": {
            "layer_url": state_layer["layer_url"],
            "where": f"NAME = '{state_name.replace("'", "''")}'",
            "attributes": state_attributes,
        },
        "asset_query": {
            "layer_url": asset["layer_url"],
            "query_url": query_url,
            "params": query_params,
            "request_preview_url": request_preview_url,
            "page_size": asset.get("page_size", 1000),
            "supports_geojson": asset.get("supports_geojson", True),
        },
        "asset_metadata": {
            "description": asset.get("description", ""),
            "default_where": asset.get("default_where", "1=1"),
        },
    }


def _serialized_query_params(params: dict[str, Any]) -> dict[str, Any]:
    serialized = params.copy()
    geometry = serialized.get("geometry")
    if isinstance(geometry, dict):
        serialized["geometry"] = json.dumps(geometry, separators=(",", ":"))
    return serialized


def estimate_count(manifest: dict[str, Any]) -> int:
    query = manifest["asset_query"]
    params = query["params"].copy()
    params.update({
        "returnCountOnly": "true",
        "returnGeometry": "false",
        "f": "json",
    })
    params.pop("resultOffset", None)
    params.pop("resultRecordCount", None)
    payload = _request_json(query["query_url"], _serialized_query_params(params))
    count = payload.get("count")
    if count is None:
        raise BackendError("Count query succeeded but no count was returned.")
    return int(count)


def fetch_geojson(manifest: dict[str, Any]) -> dict[str, Any]:
    query = manifest["asset_query"]
    page_size = int(query.get("page_size", 1000))
    offset = 0
    combined: dict[str, Any] | None = None
    total_features = 0

    while True:
        params = query["params"].copy()
        params["resultOffset"] = offset
        params["resultRecordCount"] = page_size
        params["f"] = "geojson"

        payload = _request_json(query["query_url"], _serialized_query_params(params))
        features = payload.get("features", [])
        if combined is None:
            combined = payload
            combined["features"] = []
        combined["features"].extend(features)
        total_features += len(features)

        if not features or len(features) < page_size:
            break
        offset += page_size

    if combined is None:
        combined = {"type": "FeatureCollection", "features": []}

    combined.setdefault(
        "metadata",
        {
            "state": manifest["selection"]["state_name"],
            "asset": manifest["selection"]["asset_label"],
            "record_count": total_features,
        },
    )
    return combined


def make_filename(state_name: str, asset_key: str, suffix: str) -> str:
    safe_state = state_name.lower().replace(" ", "_")
    return f"{safe_state}_{asset_key}.{suffix}"


def main() -> None:
    st.set_page_config(page_title="EIA Energy Asset Downloader", layout="wide")
    registry = load_registry()

    st.title(registry.get("app_name", "EIA Energy Asset Downloader"))
    st.write(
        "Select a state and a single infrastructure layer. The app builds the ArcGIS query manifest, "
        "clips the selected layer to the state polygon on the server side, and prepares GeoJSON for download."
    )

    asset_keys = list(registry["assets"].keys())
    default_state_index = STATE_NAMES.index("Texas")
    default_asset_index = asset_keys.index("power_plants")

    left, right = st.columns([1, 1])
    with left:
        state_name = st.selectbox("State", STATE_NAMES, index=default_state_index)
        asset_key = st.selectbox(
            "Energy asset",
            asset_keys,
            index=default_asset_index,
            format_func=lambda key: registry["assets"][key]["label"],
        )
    with right:
        asset = registry["assets"][asset_key]
        custom_where = st.text_input(
            "Optional SQL filter",
            value=asset.get("default_where", "1=1"),
            help="ArcGIS standardized SQL. Leave as 1=1 to request the full layer for the selected state.",
        )
        st.caption(asset.get("description", ""))

    try:
        manifest = build_manifest(
            state_name=state_name,
            asset_key=asset_key,
            custom_where=custom_where,
            registry=registry,
        )
    except Exception as exc:  # noqa: BLE001
        st.error(f"Failed to build the selection manifest: {exc}")
        return

    manifest_json = json.dumps(manifest, indent=2)
    st.subheader("Selection manifest")
    st.code(manifest_json, language="json")
    st.download_button(
        label="Download selection manifest (.json)",
        data=manifest_json,
        file_name=make_filename(state_name, asset_key, "json"),
        mime="application/json",
    )

    col_count, col_fetch = st.columns([1, 1])
    with col_count:
        if st.button("Estimate matching record count"):
            try:
                with st.spinner("Estimating record count..."):
                    count = estimate_count(manifest)
                st.success(f"Estimated records: {count:,}")
            except Exception as exc:  # noqa: BLE001
                st.error(f"Count query failed: {exc}")

    with col_fetch:
        if st.button("Fetch data and prepare GeoJSON"):
            try:
                with st.spinner("Downloading paginated GeoJSON..."):
                    geojson = fetch_geojson(manifest)
                st.session_state["geojson_text"] = json.dumps(geojson, indent=2)
                st.session_state["geojson_filename"] = make_filename(state_name, asset_key, "geojson")
                st.success(
                    f"Prepared {len(geojson.get('features', [])):,} features for download."
                )
            except Exception as exc:  # noqa: BLE001
                st.error(f"GeoJSON download failed: {exc}")

    geojson_text = st.session_state.get("geojson_text")
    geojson_filename = st.session_state.get("geojson_filename")
    if geojson_text and geojson_filename:
        st.subheader("GeoJSON download")
        st.download_button(
            label="Download clipped asset data (.geojson)",
            data=geojson_text,
            file_name=geojson_filename,
            mime="application/geo+json",
        )

    with st.expander("Notes"):
        st.markdown(
            "- The asset registry is externalized in `asset_registry.json`, so you can add more layers without changing the UI.\n"
            "- The backend uses the EIA-hosted state boundary layer to obtain polygon geometry, then sends that geometry to the selected asset layer using `esriSpatialRelIntersects`.\n"
            "- For large layers, the backend paginates with `resultOffset` and `resultRecordCount`.\n"
            "- The app assumes the selected layers continue to expose public `Query` operations."
        )


if __name__ == "__main__":
    main()
