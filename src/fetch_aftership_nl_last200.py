import os
import json
import requests

AFTERSHIP_API_KEY = os.environ["AFTERSHIP_API_KEY"]

def fetch_last_200_nl(tag: str | None = "Delivered") -> dict:
    url = "https://api.aftership.com/v4/trackings"
    params = {"destination": "NLD", "limit": 15}
    if tag:
        params["tag"] = tag

    r = requests.get(
        url,
        params=params,
        headers={"as-api-key": AFTERSHIP_API_KEY, "Content-Type": "application/json"},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()

if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)
    payload = fetch_last_200_nl(tag="Delivered")  # change to None if you want any status
    with open("data/aftership_last200_nl.json", "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print("Saved -> data/aftership_last200_nl.json")
