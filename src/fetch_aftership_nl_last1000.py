import os
import json
import time
import requests
from datetime import datetime, timedelta, timezone

AFTERSHIP_API_KEY = os.environ["AFTERSHIP_API_KEY"]

def unix(dt: datetime) -> int:
    return int(dt.replace(tzinfo=timezone.utc).timestamp())

def fetch_window(destination="NLD", tag="Delivered", created_at_min=None, created_at_max=None, throttle_s=0.4):
    url = "https://api.aftership.com/v4/trackings"
    headers = {"as-api-key": AFTERSHIP_API_KEY, "Content-Type": "application/json"}

    all_trackings = []
    cursor = None
    page = 0

    while True:
        page += 1
        params = {"destination": destination, "limit": 200}
        if tag:
            params["tag"] = tag
        if created_at_min is not None:
            params["created_at_min"] = created_at_min
        if created_at_max is not None:
            params["created_at_max"] = created_at_max
        if cursor:
            params["cursor"] = cursor

        r = requests.get(url, params=params, headers=headers, timeout=30)
        r.raise_for_status()
        data = (r.json() or {}).get("data") or {}
        trackings = data.get("trackings") or []
        cursor = data.get("cursor")

        print(f"  window page {page}: got {len(trackings)}; cursor={cursor}")

        if not trackings:
            break

        all_trackings.extend(trackings)

        if not cursor:
            break

        time.sleep(throttle_s)

    return all_trackings

def fetch_day_by_day(max_total=1000, max_days_back=60, destination="NLD", tag="Delivered"):
    # Use UTC day boundaries to be consistent
    now = datetime.now(timezone.utc)
    today = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)

    seen = set()
    collected = []

    for day_offset in range(max_days_back):
        day_start = today - timedelta(days=day_offset)
        day_end = day_start + timedelta(days=1)

        print(f"Fetching day window: {day_start.date()} (created_at_min={unix(day_start)} max={unix(day_end)})")

        trackings = fetch_window(
            destination=destination,
            tag=tag,
            created_at_min=unix(day_start),
            created_at_max=unix(day_end),
        )

        # Deduplicate (prefer AfterShip internal id if present)
        for t in trackings:
            key = t.get("id") or t.get("tracking_number") or json.dumps(t, sort_keys=True)
            if key in seen:
                continue
            seen.add(key)
            collected.append(t)

        print(f"  total collected so far: {len(collected)}")

        if len(collected) >= max_total:
            break

        # small pause between day windows
        time.sleep(0.4)

    return collected[:max_total]

if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)

    trackings = fetch_day_by_day(
        max_total=1000,
        max_days_back=90,        # increase if needed
        destination="NLD",
        tag="Delivered",         # set to None to broaden
    )

    payload = {"data": {"trackings": trackings}}
    out_path = "data/aftership_last1000_nl.json"

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"Saved -> {out_path} (trackings: {len(trackings)})")
