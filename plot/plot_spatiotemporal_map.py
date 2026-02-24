import json
from pathlib import Path

def _decode_cell_to_lat_lon(cell: str):
    """
    Decode cell id to representative lat/lon.
    Supports:
    1) grid_<lat>_<lon> fallback format
    2) h3 cell index (if h3 package is installed)
    """
    if cell.startswith("grid_"):
        parts = cell.split("_")
        if len(parts) >= 3:
            return float(parts[1]), float(parts[2])

    try:
        import h3  # type: ignore

        lat, lon = h3.h3_to_geo(cell)
        return float(lat), float(lon)
    except Exception as exc:
        raise ValueError(f"Unable to decode HAT cell '{cell}'") from exc


def _speed_to_color(speed_value: float) -> str:
    """
    Color rule requested by user:
    - 0..40 : green intensity decreases as value approaches 40
    - 40..100 : red intensity increases as value approaches 100
    """
    v = max(0.0, min(100.0, float(speed_value)))
    if v <= 40.0:
        g = int(round((1.0 - (v / 40.0)) * 255))
        return f"rgb(0,{g},0)"
    r = int(round(((v - 40.0) / 60.0) * 255))
    return f"rgb({r},0,0)"


def plot_spatiotemporal_map(
    output_json_path: str = "output/results/spatio_temporal_mean_output.json",
    value_field: str = "dp_mean",
    output_html_path: str = "output/spatio_temporal_hat_map.html",
    h3_resolution: int = 8,
):
    output_json = Path(output_json_path)
    if not output_json.exists():
        raise FileNotFoundError(f"Output JSON not found: {output_json_path}")

    payload = json.loads(output_json.read_text())
    result = payload.get("result", {})
    hat_means = result.get("hat_means", [])
    hex_cells = result.get("hex_cells", [])
    timeslots = result.get("timeslots", [])
    if not hat_means:
        raise ValueError("No hat_means found in output JSON")

    # New structured format: precomputed hex_cells + per-(timeslot, cell) values.
    if hex_cells and timeslots:
        records = []
        for row in hat_means:
            v = float(row.get(value_field, row.get("dp_mean", 0.0)))
            records.append(
                {
                    "timeslot": str(row.get("timeslot", "")),
                    "cell_id": str(row.get("cell_id", "")),
                    "true_mean": float(row.get("true_mean", 0.0)),
                    "dp_mean": float(row.get("dp_mean", 0.0)),
                    "count": int(row.get("count", 0)),
                    "plot_value": v,
                    "color": _speed_to_color(v),
                }
            )

        payload_obj = {
            "hex_cells": hex_cells,
            "timeslots": sorted([str(t) for t in timeslots]),
            "records": records,
        }
        payload_js = json.dumps(payload_obj)

        output_html = Path(output_html_path)
        output_html.parent.mkdir(parents=True, exist_ok=True)
        html = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Spatio-Temporal HAT Map ({value_field})</title>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
  <style>
    html, body, #map {{ height: 100%; margin: 0; }}
    .panel {{
      position: absolute;
      top: 10px;
      left: 10px;
      z-index: 1000;
      background: white;
      border: 1px solid #bbb;
      border-radius: 6px;
      padding: 8px 10px;
      font-family: Arial, sans-serif;
      font-size: 12px;
      width: 360px;
    }}
    .legend {{
      background: white;
      padding: 8px 10px;
      line-height: 1.3;
      font-family: Arial, sans-serif;
      font-size: 12px;
      border: 1px solid #bbb;
    }}
    #slider {{ width: 100%; }}
  </style>
</head>
<body>
  <div id="map"></div>
  <div class="panel">
    <div><b>Timeslot</b>: <span id="slotLabel"></span></div>
    <input id="slider" type="range" min="0" max="0" value="0" step="1"/>
  </div>
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  <script>
    const payload = {payload_js};
    const map = L.map('map');
    L.tileLayer('https://tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
      maxZoom: 19,
      attribution: '&copy; OpenStreetMap'
    }}).addTo(map);

    const bounds = [];
    payload.hex_cells.forEach(c => {{
      bounds.push([c.center_lat, c.center_lon]);
    }});
    if (bounds.length > 0) map.fitBounds(bounds, {{padding: [20,20]}});
    else map.setView([12.97, 77.59], 11);

    const bySlot = {{}};
    payload.records.forEach(r => {{
      if (!bySlot[r.timeslot]) bySlot[r.timeslot] = {{}};
      bySlot[r.timeslot][r.cell_id] = r;
    }});

    const layerGroup = L.layerGroup().addTo(map);
    const slider = document.getElementById('slider');
    const slotLabel = document.getElementById('slotLabel');
    const slots = payload.timeslots;
    slider.max = Math.max(0, slots.length - 1);

    function renderSlot(slot) {{
      layerGroup.clearLayers();
      const slotMap = bySlot[slot] || {{}};
      payload.hex_cells.forEach(cell => {{
        const rec = slotMap[cell.cell_id];
        const color = rec ? rec.color : 'rgb(220,220,220)';
        const polygon = L.polygon(cell.boundary, {{
          color: color,
          fillColor: color,
          fillOpacity: rec ? 0.6 : 0.15,
          weight: 1.2
        }});
        const popup = rec
          ? `<b>${{slot}} / ${{cell.cell_id}}</b><br/>` +
            `{value_field}: ${{rec.plot_value.toFixed(2)}}<br/>` +
            `true_mean: ${{rec.true_mean.toFixed(2)}}<br/>` +
            `dp_mean: ${{rec.dp_mean.toFixed(2)}}<br/>` +
            `count: ${{rec.count}}`
          : `<b>${{slot}} / ${{cell.cell_id}}</b><br/>No data`;
        polygon.bindPopup(popup);
        polygon.addTo(layerGroup);
      }});
      slotLabel.textContent = slot;
    }}

    slider.addEventListener('input', e => {{
      const idx = parseInt(e.target.value, 10);
      const slot = slots[idx] || '';
      renderSlot(slot);
    }});

    if (slots.length > 0) renderSlot(slots[0]);

    const legend = L.control({{position: 'bottomright'}});
    legend.onAdd = function() {{
      const div = L.DomUtil.create('div', 'legend');
      div.innerHTML = `
        <b>Speed Color Rule (Hexagons)</b><br/>
        0-40: Green intensity increases<br/>
        40-100: Red intensity increases
      `;
      return div;
    }};
    legend.addTo(map);
  </script>
</body>
</html>
"""
        output_html.write_text(html)
        return str(output_html)

    # Fallback old format support.
    points = []
    for row in hat_means:
        hat = row["HAT"]
        if " " not in hat:
            continue
        timeslot, cell = hat.split(" ", 1)
        lat, lon = _decode_cell_to_lat_lon(cell)
        v = float(row.get(value_field, row.get("dp_mean", 0.0)))
        points.append(
            {
                "HAT": hat,
                "timeslot": timeslot,
                "cell": cell,
                "lat": lat,
                "lon": lon,
                "true_mean": float(row.get("true_mean", 0.0)),
                "dp_mean": float(row.get("dp_mean", 0.0)),
                "count": int(row.get("count", 0)),
                "color": _speed_to_color(v),
                "plot_value": v,
            }
        )

    if not points:
        raise ValueError("No plottable HAT rows after decoding")

    output_html = Path(output_html_path)
    output_html.parent.mkdir(parents=True, exist_ok=True)

    points_js = json.dumps(points)
    html = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Spatio-Temporal HAT Map ({value_field})</title>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
  <style>
    html, body, #map {{ height: 100%; margin: 0; }}
    .legend {{
      background: white;
      padding: 8px 10px;
      line-height: 1.3;
      font-family: Arial, sans-serif;
      font-size: 12px;
      border: 1px solid #bbb;
    }}
  </style>
</head>
<body>
  <div id="map"></div>
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  <script>
    const points = {points_js};
    const h3Resolution = {int(h3_resolution)};
    const map = L.map('map');
    L.tileLayer('https://tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
      maxZoom: 19,
      attribution: '&copy; OpenStreetMap'
    }}).addTo(map);

    const bounds = [];
    function hexRadiusDeg(resolution) {{
      // Coarse visual approximation so hexes are clearly visible.
      // res=5 ~ 0.0022 deg, res=8 ~ 0.00055 deg
      return Math.max(0.00025, 0.0175 / Math.pow(2, Math.max(0, resolution - 2)));
    }}

    function regularHexBoundary(lat, lon, radiusDeg) {{
      const pts = [];
      for (let i = 0; i < 6; i++) {{
        const ang = (Math.PI / 3) * i + Math.PI / 6;
        const dLat = radiusDeg * Math.sin(ang);
        // longitude scale correction by latitude
        const dLon = (radiusDeg * Math.cos(ang)) / Math.max(0.2, Math.cos(lat * Math.PI / 180));
        pts.push([lat + dLat, lon + dLon]);
      }}
      return pts;
    }}

    points.forEach(p => {{
      const boundary = regularHexBoundary(p.lat, p.lon, hexRadiusDeg(h3Resolution));
      const polygon = L.polygon(boundary, {{
        color: p.color,
        fillColor: p.color,
        fillOpacity: 0.55,
        weight: 1.5
      }});
      polygon.bindPopup(
        `<b>${{p.HAT}}</b><br/>` +
        `cell: ${{p.cell}}<br/>` +
        `timeslot: ${{p.timeslot}}<br/>` +
        `{value_field}: ${{p.plot_value.toFixed(2)}}<br/>` +
        `true_mean: ${{p.true_mean.toFixed(2)}}<br/>` +
        `dp_mean: ${{p.dp_mean.toFixed(2)}}<br/>` +
        `count: ${{p.count}}`
      );
      polygon.addTo(map);
      bounds.push([p.lat, p.lon]);
    }});

    if (bounds.length > 0) {{
      map.fitBounds(bounds, {{padding: [20, 20]}});
    }} else {{
      map.setView([20, 77], 5);
    }}

    const legend = L.control({{position: 'bottomright'}});
    legend.onAdd = function() {{
      const div = L.DomUtil.create('div', 'legend');
      div.innerHTML = `
        <b>Speed Color Rule (Hexagons)</b><br/>
        0-40: Green intensity increases<br/>
        40-100: Red intensity increases
      `;
      return div;
    }};
    legend.addTo(map);
  </script>
</body>
</html>
"""
    output_html.write_text(html)
    return str(output_html)


if __name__ == "__main__":
    out = plot_spatiotemporal_map()
    print(f"Map written to: {out}")
