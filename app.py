from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import Any

import requests
import streamlit as st

APP_DIR = Path(__file__).resolve().parent
REGISTRY_PATH = APP_DIR / "asset_registry.json"
REQUEST_TIMEOUT = 120
USER_AGENT = "eia-energy-asset-downloader/0.2"
POST_THRESHOLD_CHARS = 1800
LARGE_DOWNLOAD_THRESHOLD = 25000

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


def _normalize_params(params: dict[str, Any]) -> dict[str, Any]:
    normalized = params.copy()
    geometry = normalized.get("geometry")
    if isinstance(geometry, dict):
        normalized["geometry"] = json.dumps(geometry, separators=(",", ":"))
    return normalized


def _request_json(url: str, params: dict[str, Any]) -> dict[str, Any]:
    headers = {"User-Agent": USER_AGENT}
    normalized = _normalize_params(params)
    serialized_length = sum(len(str(k)) + len(str(v)) for k, v in normalized.items())
    use_post = "geometry" in normalized or serialized_length > POST_THRESHOLD_CHARS

    if use_post:
        response = requests.post(url, data=normalized, headers=headers, timeout=REQUEST_TIMEOUT)
    else:
        response = requests.get(url, params=normalized, headers=headers, timeout=REQUEST_TIMEOUT)

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


def build_selection_manifest(
    *,
    state_name: str,
    asset_key: str,
    custom_where: str,
    registry: dict[str, Any],
) -> dict[str, Any]:
    asset = registry["assets"][asset_key]
    state_layer = registry["state_boundaries"]
    state_abbr = STATE_ABBR.get(state_name, "")

    return {
        "selection": {
            "state_name": state_name,
            "state_abbr": state_abbr,
            "asset_key": asset_key,
            "asset_label": asset["label"],
            "asset_category": asset.get("category", ""),
        },
        "state_lookup": {
            "layer_url": state_layer["layer_url"],
            "where": f"NAME = '{state_name.replace("'", "''")}'",
            "fields": {
                "state_name_field": state_layer.get("state_name_field", "NAME"),
                "state_abbr_field": state_layer.get("state_abbr_field", "STUSAB"),
            },
        },
        "asset_query": {
            "layer_url": asset["layer_url"],
            "query_url": f"{asset['layer_url'].rstrip('/')}/query",
            "where": custom_where or asset.get("default_where", "1=1"),
            "geometry_source": "fetch state polygon at runtime from state_lookup",
            "geometry_type": "esriGeometryPolygon",
            "spatial_relation": "esriSpatialRelIntersects",
            "out_fields": "*",
            "return_geometry": True,
            "out_sr": 4326,
            "result_format": "geojson" if asset.get("supports_geojson", True) else "json",
            "page_size": asset.get("page_size", 1000),
            "request_method": "POST preferred when geometry is present",
        },
        "asset_metadata": {
            "description": asset.get("description", ""),
            "default_where": asset.get("default_where", "1=1"),
        },
    }


def build_runtime_query(manifest: dict[str, Any], registry: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    state_name = manifest["selection"]["state_name"]
    state_layer_url = manifest["state_lookup"]["layer_url"]
    geometry, state_attributes = fetch_state_geometry(state_layer_url, state_name)
    asset_key = manifest["selection"]["asset_key"]
    asset = registry["assets"][asset_key]

    params: dict[str, Any] = {
        "where": manifest["asset_query"]["where"],
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
    return params, state_attributes


def estimate_count(manifest: dict[str, Any], registry: dict[str, Any]) -> int:
    params, _ = build_runtime_query(manifest, registry)
    params.update({
        "returnCountOnly": "true",
        "returnGeometry": "false",
        "f": "json",
    })
    params.pop("resultOffset", None)
    params.pop("resultRecordCount", None)

    query_url = manifest["asset_query"]["query_url"]
    payload = _request_json(query_url, params)
    count = payload.get("count")
    if count is None:
        raise BackendError("Count query succeeded but no count was returned.")
    return int(count)


def fetch_geojson(manifest: dict[str, Any], registry: dict[str, Any]) -> dict[str, Any]:
    params, state_attributes = build_runtime_query(manifest, registry)
    query_url = manifest["asset_query"]["query_url"]
    page_size = int(manifest["asset_query"].get("page_size", 1000))

    offset = 0
    combined: dict[str, Any] | None = None
    total_features = 0

    while True:
        page_params = params.copy()
        page_params["resultOffset"] = offset
        page_params["resultRecordCount"] = page_size
        page_params["f"] = "geojson"

        payload = _request_json(query_url, page_params)
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

    combined["metadata"] = {
        "state": manifest["selection"]["state_name"],
        "state_abbr": state_attributes.get("STUSAB", manifest["selection"]["state_abbr"]),
        "asset": manifest["selection"]["asset_label"],
        "asset_key": manifest["selection"]["asset_key"],
        "record_count": total_features,
    }
    return combined


def make_filename(state_name: str, asset_key: str, suffix: str) -> str:
    safe_state = state_name.lower().replace(" ", "_")
    return f"{safe_state}_{asset_key}.{suffix}"


def reset_download_state() -> None:
    st.session_state.pop("geojson_gz_bytes", None)
    st.session_state.pop("geojson_gz_filename", None)
    st.session_state.pop("last_selection_key", None)
    st.session_state.pop("estimated_count", None)


def main() -> None:
    st.set_page_config(page_title="EIA Energy Asset Downloader", layout="wide")
    registry = load_registry()

    st.title(registry.get("app_name", "EIA Energy Asset Downloader"))
    st.write(
        "Select a state and a single infrastructure layer. The app builds a compact JSON selection manifest, "
        "then fetches the state polygon at runtime and queries the selected ArcGIS layer using a POST request when needed."
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

    selection_key = f"{state_name}|{asset_key}|{custom_where}"
    if st.session_state.get("last_selection_key") != selection_key:
        reset_download_state()
        st.session_state["last_selection_key"] = selection_key

    manifest = build_selection_manifest(
        state_name=state_name,
        asset_key=asset_key,
        custom_where=custom_where,
        registry=registry,
    )

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
                    count = estimate_count(manifest, registry)
                st.session_state["estimated_count"] = count
                st.success(f"Estimated records: {count:,}")
            except Exception as exc:  # noqa: BLE001
                st.error(f"Count query failed: {exc}")

    estimated_count = st.session_state.get("estimated_count")
    if isinstance(estimated_count, int) and estimated_count > LARGE_DOWNLOAD_THRESHOLD:
        st.warning(
            f"This selection is large ({estimated_count:,} records). A local download can still be slow or memory-intensive. "
            "Add a SQL filter if you want a smaller extract."
        )

    with col_fetch:
        if st.button("Fetch data and prepare compressed GeoJSON"):
            try:
                with st.spinner("Downloading paginated GeoJSON..."):
                    geojson = fetch_geojson(manifest, registry)
                geojson_bytes = json.dumps(geojson, separators=(",", ":")).encode("utf-8")
                gz_bytes = gzip.compress(geojson_bytes)
                st.session_state["geojson_gz_bytes"] = gz_bytes
                st.session_state["geojson_gz_filename"] = make_filename(state_name, asset_key, "geojson.gz")
                st.success(
                    f"Prepared {len(geojson.get('features', [])):,} features for download. "
                    f"Compressed size: {len(gz_bytes) / (1024 * 1024):.2f} MB"
                )
            except Exception as exc:  # noqa: BLE001
                st.error(f"GeoJSON download failed: {exc}")

    geojson_gz_bytes = st.session_state.get("geojson_gz_bytes")
    geojson_gz_filename = st.session_state.get("geojson_gz_filename")
    if geojson_gz_bytes and geojson_gz_filename:
        st.subheader("Compressed GeoJSON download")
        st.download_button(
            label="Download clipped asset data (.geojson.gz)",
            data=geojson_gz_bytes,
            file_name=geojson_gz_filename,
            mime="application/gzip",
        )

    with st.expander("Notes"):
        st.markdown(
            "- The asset registry is externalized in `asset_registry.json`, so you can add more layers without changing the UI.\n"
            "- The JSON manifest is intentionally compact and does not embed the full state polygon.\n"
            "- The backend fetches the state polygon at runtime and uses `esriSpatialRelIntersects` against the selected asset layer.\n"
            "- ArcGIS requests that include geometry are sent with POST rather than GET to avoid oversized URLs.\n"
            "- Download output is compressed as `.geojson.gz` to reduce browser and process memory pressure."
        )


if __name__ == "__main__":
    main()
