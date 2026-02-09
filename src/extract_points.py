import json
import pandas as pd
from datetime import datetime

def parse_dt(s: str):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None

def load_trackings(path: str):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, dict) and "data" in data and isinstance(data["data"], dict) and "trackings" in data["data"]:
        return data["data"]["trackings"]
    if isinstance(data, dict) and "trackings" in data:
        return data["trackings"]
    if isinstance(data, list):
        return data
    return []

def get_delivered_checkpoint(t: dict):
    cps = t.get("checkpoints") or []
    delivered = [cp for cp in cps if cp.get("tag") == "Delivered" or str(cp.get("subtag","")).startswith("Delivered")]
    if not delivered and t.get("tag") == "Delivered" and cps:
        delivered = cps[-1:]
    delivered_sorted = sorted(delivered, key=lambda cp: parse_dt(cp.get("checkpoint_time")) or datetime.min)
    return delivered_sorted[-1] if delivered_sorted else None

def main(input_json: str, output_csv: str):
    trackings = load_trackings(input_json)

    rows = []
    for t in trackings:
        # destination filter safety
        if t.get("destination_country_region") not in (None, "", "NLD"):
            continue

        cp = get_delivered_checkpoint(t)
        delivered_time = (cp.get("checkpoint_time") if cp else None) or t.get("shipment_delivery_date")

        rows.append({
            "order_id": t.get("order_id"),
            "tracking_number": t.get("tracking_number") or t.get("title"),
            "slug": t.get("slug"),
            "delivered_time": delivered_time,
            "destination_raw_location": t.get("destination_raw_location"),
            "destination_postal_code": t.get("destination_postal_code"),
            "destination_city": t.get("destination_city"),
            "destination_state": t.get("destination_state"),
        })

    df = pd.DataFrame(rows)
    df.to_csv(output_csv, index=False)
    print(f"Saved {len(df)} rows -> {output_csv}")

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    args = ap.parse_args()
    main(args.input, args.output)
