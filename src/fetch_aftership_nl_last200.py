import os
import json
import time
import requests

AFTERSHIP_API_KEY = os.environ["AFTERSHIP_API_KEY"]

def fetch_nl_trackings(max_total: int = 1000, tag: str | None = "Delivered", throttle_s: float = 0.4) -> dict:
    """
    Fetch up to max_total trackings for destination NLD using cursor pagination.
    AfterShip max limit per request is 200, so we page until we reach max_total or no cursor.
    Returns a payload shaped like AfterShip v4: {"data": {"trackings":[...], "cursor": ...}}
    """
    url = "https://api.aftership.com/v4/trackings"
    headers = {"as-api-key": AFTERSHIP_API_KEY, "Content-Type": "application/json"}

    all_trackings = []
    cursor = None

    while len(all_trackings) < max_total:
        params = {
            "destination": "NLD",
            "limit": 200,  # API max per request
        }
        if tag:
            params["tag"] = tag
        if cursor:
            params["cursor"] = cursor

        r = requests.get(url, params=params, headers=headers, timeout=30)
        r.raise_for_status()
        payload = r.json()

        data = payload.get("data") or {}
        trackings = data.get("trackings") or []
        cursor = data.get("cursor")

        if not trackings:
            break

        all_trackings.extend(trackings)

        # stop if no next page
        if not cursor:
            break

        # be polite to avoid throttling
        time.sleep(throttle_s)

    # trim to exactly max_total
    all_trackings = all_trackings[:max_total]

    # return in a familiar wrapper so your extractor can still read it
    return {"data": {"trackings": all_trackings}}

if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)

    payload = fetch_nl_trackings(max_total=1000, tag="Delivered")
    out_path = "data/aftership_last1000_nl.json"

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"Saved -> {out_path} (trackings: {len(payload['data']['trackings'])})")
