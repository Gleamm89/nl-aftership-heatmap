import os
import json
import time
import requests

AFTERSHIP_API_KEY = os.environ["AFTERSHIP_API_KEY"]

def fetch_nl_trackings(
    max_total: int = 1000,
    tag: str | None = "Delivered",
    throttle_s: float = 0.4,
    created_at_min: int | None = None,  # unix seconds
) -> dict:
    url = "https://api.aftership.com/v4/trackings"
    headers = {"as-api-key": AFTERSHIP_API_KEY, "Content-Type": "application/json"}

    all_trackings = []
    cursor = None
    page = 0

    while len(all_trackings) < max_total:
        page += 1
        params = {
            "destination": "NLD",
            "limit": 200,  # API max per request
        }
        if tag:
            params["tag"] = tag
        if cursor:
            params["cursor"] = cursor
        if created_at_min:
            params["created_at_min"] = created_at_min

        r = requests.get(url, params=params, headers=headers, timeout=30)
        r.raise_for_status()
        payload = r.json()

        data = payload.get("data") or {}
        trackings = data.get("trackings") or []
        cursor = data.get("cursor")

        print(f"Page {page}: got {len(trackings)} trackings; total={len(all_trackings)+len(trackings)}; cursor={cursor}")

        if not trackings:
            break

        all_trackings.extend(trackings)

        if not cursor:
            break

        time.sleep(throttle_s)

    return {"data": {"trackings": all_trackings[:max_total]}}

if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)

    # OPTIONAL: broaden the window by forcing a minimum created_at.
    # Example: last 365 days (if endpoint honors it).
    # Uncomment if needed:
    # import time as _t
    # created_at_min = int(_t.time()) - 365 * 24 * 3600

    created_at_min = None

    payload = fetch_nl_trackings(max_total=1000, tag="Delivered", created_at_min=created_at_min)
    out_path = "data/aftership_last1000_nl.json"

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"Saved -> {out_path} (trackings: {len(payload['data']['trackings'])})")
