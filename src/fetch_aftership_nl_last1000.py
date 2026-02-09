import os
import json
import time
import requests
from datetime import datetime, timedelta, timezone

AFTERSHIP_API_KEY = os.environ["AFTERSHIP_API_KEY"]

def unix_ms(dt: datetime) -> int:
    """Unix epoch milliseconds."""
    return int(dt.timestamp() * 1000)

def dedupe_key(t: dict) -> str:
    slug = t.get("slug") or ""
    tn = t.get("tracking_number") or t.get("title") or ""
    if slug and tn:
        return f"{slug}|{tn}"
    # last resort to avoid None-collisions
    raw = {
        "slug": slug,
        "tracking_number": t.get("tracking_number"),
        "title": t.get("title"),
        "order_id": t.get("order_id"),
        "created_at": t.get("created_at"),
    }
    return json.dumps(raw, sort_keys=True)

def fetch_window(
    destination: str = "NLD",
    tag: str | None = "Delivered",
    created_at_min_ms: int | None = None,
    created_at_max_ms: int | None = None,
    throttle_s: float = 0.4,
) -> list[dict]:
    """
    Fetch a single time window of trackings.
    Uses AfterShip max limit=200. If cursor exists, paginates within the window.
    """
    url = "https://api.aftership.com/v4/trackings"
    headers = {"as-api-key": AFTERSHIP_API_KEY, "Content-Type": "application/json"}

    results: list[dict] = []
    cursor = None
    page = 0

    while True:
        page += 1
        params = {
            "destination": destination,
            "limit": 200,  # AfterShip API max per request
        }
        if tag:
            params["tag"] = tag
        if created_at_min_ms is not None:
            params["created_at_min"] = created_at_min_ms
        if created_at_max_ms is not None:
            params["created_at_max"] = created_at_max_ms
        if cursor:
            params["cursor"] = cursor

        r = requests.get(url, params=params, headers=headers, timeout=30)
        r.raise_for_status()
        payload = r.json() or {}
        data = payload.get("data") or {}

        trackings = data.get("trackings") or []
        cursor = data.get("cursor")

        print(f"  window page {page}: got {len(trackings)}; cursor={cursor}")

        # Verify whether the API is honoring date windows
        if trackings:
            ca_first = trackings[0].get("created_at")
            ca_last = trackings[-1].get("created_at")
            print(f"    created_at range in page: first={ca_first} last={ca_last}")

        if not trackings:
            break

        results.extend(trackings)

        if not cursor:
            break

        time.sleep(throttle_s)

    return results

def fetch_day_by_day(
    max_total: int = 1000,
    max_days_back: int = 120,
    destination: str = "NLD",
    tag: str | None = "Delivered",
) -> list[dict]:
    """
    Fetch day windows: today, yesterday, etc. Dedupes across windows until max_total.
    Uses created_at_min/max in epoch milliseconds.
    """
    now = datetime.now(timezone.utc)
    today_utc = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)

    seen: set[str] = set()
    collected: list[dict] = []

    for day_offset in range(max_days_back):
        day_start = today_utc - timedelta(days=day_offset)
        day_end = day_start + timedelta(days=1)

        min_ms = unix_ms(day_start)
        max_ms = unix_ms(day_end)

        print(f"Fetching day window: {day_start.date()} (created_at_min={min_ms} max={max_ms})")

        trackings = fetch_window(
            destination=destination,
            tag=tag,
            created_at_min_ms=min_ms,
            created_at_max_ms=max_ms,
        )

        added = 0
        for t in trackings:
            k = dedupe_key(t)
            if k in seen:
                continue
            seen.add(k)
            collected.append(t)
            added += 1

        print(f"  added this window: {added}; total collected so far: {len(collected)}")

        if len(collected) >= max_total:
            break

        time.sleep(0.4)

    return collected[:max_total]

if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)

    # Keep tag="Delivered" if you only want delivered shipments.
    # If you want to broaden results (often helps reach 1000), set tag=None.
    trackings = fetch_day_by_day(
        max_total=1000,
        max_days_back=120,
        destination="NLD",
        tag="Delivered",
    )

    payload = {"data": {"trackings": trackings}}
    out_path = "data/aftership_last1000_nl.json"

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"Saved -> {out_path} (trackings: {len(trackings)})")
