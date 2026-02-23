import argparse
import random
from datetime import datetime, timedelta
import csv
from pathlib import Path

MERCHANTS = [
    ("Ncell", "telecom"),
    ("NTC", "telecom"),
    ("Bhatbhateni", "grocery"),
    ("BigMart", "grocery"),
    ("Daraz", "ecommerce"),
    ("SastoDeal", "ecommerce"),
    ("Pathao", "transport"),
    ("InDrive", "transport"),
    ("Apple", "shopping"),
    ("Samsung", "shopping"),
]

CHANNELS = ["mobile", "web", "pos"]
CITIES = ["Kathmandu", "Lalitpur", "Biratnagar", "Pokhara"]

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--out", required=True)
    p.add_argument("--rows", type=int, default=200000)
    p.add_argument("--accounts", type=int, default=5000)
    p.add_argument("--days", type=int, default=45)
    args = p.parse_args()

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    start = datetime(2025, 11, 15, 0, 0, 0)

    with out_path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["transaction_id","account_id","amount","currency","merchant","category","txn_ts","status","channel","city","country"])

        for i in range(args.rows):
            txid = f"tx_{i:07d}"
            account_id = random.randint(1001, 1000 + args.accounts)

            merchant, category = random.choice(MERCHANTS)
            amount = round(random.uniform(20, 20000), 2)

            status = "APPROVED" if random.random() > 0.08 else "DECLINED"
            if random.random() < 0.01:
                amount = 0
            if random.random() < 0.005:
                amount = -abs(amount)

            dt = start + timedelta(
                days=random.randint(0, args.days - 1),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59),
            )
            txn_ts = dt.strftime("%Y-%m-%d %H:%M:%S")

            channel = random.choice(CHANNELS)
            city = random.choice(CITIES)

            w.writerow([txid, account_id, amount, "NPR", merchant, category, txn_ts, status, channel, city, "Nepal"])

    print(f"Wrote {args.rows} rows to {out_path}")

if __name__ == "__main__":
    main()