import os
import json
import time
import requests
from datetime import datetime, timedelta, timezone

AFTERSHIP_API_KEY = os.environ["AFTERSHIP_API_KEY"]

def unix(dt: datetime) -> int:
    return int(dt.timestamp())

def fetch_window(
    destination: str = "NLD",
    tag: str | None = "Delivered",
    created_at_min: int | None = None,
    created_at_max: int | None = None,
    throttle_s: float = 0.4,
) -> list[dict]:
    """
    Fetch a single time window of trackings. Returns a list of tracking dicts.
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
        if created_at_min is not None:
            params["created_at_min"] = created_at_min
        if created_at_max is not None:
            params["created_at_max"] = created_at_max
        if cursor:
            params["cursor"] = cursor

        r = requests.get(url, params=params, headers=headers, timeout=30)
        r.raise_for_status()
        payload = r.json() or {}

        data = payload.get("data") or {}
        trackings = data.get("trackings") or []
        cursor = data.get("cursor")

        print(f"  window page {page}: got {len(trackings)}; cursor={cursor}")

        if not trackings:
            break

        results.extend(trackings)

        # stop if no next page
        if not cursor:
            break

        time.sleep(throttle_s)

    return results

def dedupe_key(t: dict) -> str:
    """
    Build a stable unique key. Prefer (slug + tracking_number).
    Falls back to (slug + title) or a small JSON fingerprint.
    """
    slug = t.get("slug") or ""
    tn = t.get("tracking_number") or ""
    title = t.get("title") or ""

    if slug and tn:
        return f"{slug}|{tn}"
    if slug and title:
        return f"{slug}|{title}"

    # last resort fingerprint to avoid collapsing to the same "None" key
    # (keep it short-ish and stable)
    raw = {
        "slug": slug,
        "tracking_number": tn,
        "title": title,
        "order_id": t.get("order_id"),
        "shipment_delivery_date": t.get("shipment_delivery_date"),
    }
    return json.dumps(raw, sort_keys=True)

def fetch_day_by_day(
    max_total: int = 1000,
    max_days_back: int = 120,
    destination: str = "NLD",
    tag: str | None = "Delivered",
) -> list[dict]:
    """
    Fetches day windows: today, yesterday, etc. Dedupes across windows until max_total.
    """
    now = datetime.now(timezone.utc)
    today_utc = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)

    seen: set[str] = set()
    collected: list[dict] = []

    for day_offset in range(max_days_back):
        day_start = today_utc - timedelta(days=day_offset)
        day_end = day_start + timedelta(days=1)

        print(f"Fetching day window: {day_start.date()} (created_at_min={unix(day_start)} max={unix(day_end)})")

        trackings = fetch_window(
            destination=destination,
            tag=tag,
            created_at_min=unix(day_start),
            created_at_max=unix(day_end),
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

        # small pause between day windows
        time.sleep(0.4)

    return collected[:max_total]

if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)

    # Keep tag="Delivered" if you only want delivered shipments.
    # If you want to broaden results to reach 1000 faster, set tag=None.
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
