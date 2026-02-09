import pandas as pd
from tqdm import tqdm
import folium
from folium.plugins import HeatMap
from geopy.geocoders import Nominatim
import time
from geocode_cache import GeoCache

def build_query(row: pd.Series) -> str:
    parts = []
    pc = str(row.get("destination_postal_code") or "").strip()
    city = str(row.get("destination_city") or "").strip()
    state = str(row.get("destination_state") or "").strip()

    if pc and pc.lower() != "nan":
        parts.append(pc)
    if city and city.lower() != "nan":
        parts.append(city)
    elif state and state.lower() != "nan":
        parts.append(state)

    parts.append("Netherlands")
    return ", ".join(parts)

def geocode_points(df: pd.DataFrame, cache_db: str, user_agent: str, throttle_s: float = 1.1) -> pd.DataFrame:
    geolocator = Nominatim(user_agent=user_agent, timeout=10)
    cache = GeoCache(cache_db)

    lats, lons, queries = [], [], []

    for _, row in tqdm(df.iterrows(), total=len(df), desc="Geocoding"):
        q = build_query(row)
        queries.append(q)

        cached = cache.get(q)
        if cached:
            lat, lon = cached
        else:
            loc = geolocator.geocode(q)
            if loc:
                lat, lon = loc.latitude, loc.longitude
                cache.set(q, lat, lon, provider="nominatim")
            else:
                lat, lon = None, None
            time.sleep(throttle_s)

        lats.append(lat)
        lons.append(lon)

    cache.close()
    out = df.copy()
    out["geocode_query"] = queries
    out["lat"] = lats
    out["lon"] = lons
    return out

def make_heatmap(df_points: pd.DataFrame, output_html: str):
    m = folium.Map(location=[52.2, 5.3], zoom_start=7, tiles="CartoDB positron")
    pts = df_points.dropna(subset=["lat", "lon"])
    HeatMap(pts[["lat", "lon"]].values.tolist(), radius=10, blur=14, max_zoom=10).add_to(m)
    m.save(output_html)
    print(f"Saved -> {output_html} (points used: {len(pts)})")

def main():
    df = pd.read_csv("output/nl_delivery_points.csv")
    df_geo = geocode_points(df, cache_db="cache/geocache.sqlite",
                            user_agent="nl-aftership-heatmap/1.0 (g.jansen@vidaxl.com)")
    df_geo.to_csv("output/nl_delivery_points_geocoded.csv", index=False)
    make_heatmap(df_geo, "output/nl_delivery_heatmap.html")

if __name__ == "__main__":
    main()
