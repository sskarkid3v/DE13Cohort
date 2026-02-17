import csv, random, os
from datetime import datetime, timedelta, date
from faker import Faker
fake = Faker()

# Tunables for classroom speed
N_BRANCHES = 10
N_CUSTOMERS = 1000
N_ACCOUNTS = 1500
N_TRANSACTIONS = 30000

os.makedirs("data/raw", exist_ok=True)

def daterange(start: date, end: date):
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, max(delta, 1)))

def weighted_choice(options):
    items, weights = zip(*options)
    return random.choices(items, weights=weights, k=1)[0]

# 1) Branches
branches = []
for i in range(1, N_BRANCHES+1):
    city = fake.city()
    state = fake.state_abbr()
    branches.append({
        "branch_id": i,
        "branch_name": f"{city} Branch",
        "city": city,
        "state": state
    })

# 2) Customers
customers = []
for i in range(1, N_CUSTOMERS+1):
    first = fake.first_name()
    last = fake.last_name()
    dob = fake.date_of_birth(minimum_age=18, maximum_age=85)
    customers.append({
        "customer_id": i,
        "first_name": first,
        "last_name": last,
        "dob": dob.isoformat(),
        "email": f"{first.lower()}.{last.lower()}@example.com",
        "phone": fake.msisdn()[:10],
        "address": fake.street_address(),
        "city": fake.city(),
        "state": fake.state_abbr(),
        "postal_code": fake.postcode(),
        "created_at": fake.date_time_between(start_date="-3y", end_date="now").isoformat(sep=' ')
    })

# 3) Accounts
accounts = []
for i in range(1, N_ACCOUNTS+1):
    cust = random.randint(1, N_CUSTOMERS)
    br = random.randint(1, N_BRANCHES)
    acct_type = weighted_choice([("checking", 0.55), ("savings", 0.35), ("credit", 0.10)])
    opened = fake.date_time_between(start_date="-3y", end_date="-1d")
    status = weighted_choice([("active", 0.9), ("frozen", 0.05), ("closed", 0.05)])
    accounts.append({
        "account_id": i,
        "customer_id": cust,
        "branch_id": br,
        "account_type": acct_type,
        "opened_at": opened.isoformat(sep=' '),
        "status": status
    })

# 4) Transactions
txn_types = [("purchase", 0.55), ("withdrawal", 0.2), ("deposit", 0.18), ("fee", 0.04), ("interest", 0.03)]
channels   = [("pos", 0.55), ("online", 0.25), ("atm", 0.15), ("branch", 0.05)]
mccs = ["5411","5812","5912","5541","5732","5999","6011","4789"] # groceries, restaurants, pharmacy, fuel, electronics, misc, atm, transport

transactions = []
start_dt = datetime.now() - timedelta(days=365)
end_dt = datetime.now()
for tid in range(1, N_TRANSACTIONS+1):
    acct = accounts[random.randint(0, N_ACCOUNTS-1)]
    ts = fake.date_time_between(start_date=start_dt, end_date=end_dt)
    ttype = weighted_choice(txn_types)
    channel = weighted_choice(channels)
    mcc = random.choice(mccs)
    merchant = fake.company() if ttype in ("purchase","fee") else ("Employer Inc" if ttype=="deposit" else "Bank Service")
    # amount distribution
    if ttype == "deposit":
        amt = round(random.uniform(50, 3000), 2)
    elif ttype == "interest":
        amt = round(random.uniform(0.1, 15), 2)
    elif ttype == "fee":
        amt = round(random.uniform(1, 25), 2) * -1
    elif ttype == "withdrawal":
        amt = round(random.uniform(20, 500), 2) * -1
    else:  # purchase
        amt = round(random.uniform(5, 800), 2) * -1

    transactions.append({
        "transaction_id": tid,
        "account_id": acct["account_id"],
        "ts": ts.isoformat(sep=' '),
        "amount": amt,
        "txn_type": ttype,
        "merchant": merchant,
        "mcc": mcc,
        "channel": channel
    })

# Write CSVs
with open("data/raw/branches.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=branches[0].keys()); w.writeheader(); w.writerows(branches)

with open("data/raw/customers.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=customers[0].keys()); w.writeheader(); w.writerows(customers)

with open("data/raw/accounts.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=accounts[0].keys()); w.writeheader(); w.writerows(accounts)

with open("data/raw/transactions.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=transactions[0].keys()); w.writeheader(); w.writerows(transactions)

print("Generated CSVs in data/raw/")