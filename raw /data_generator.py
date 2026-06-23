import pandas as pd
import random
from datetime import datetime, timedelta

accounts = ["ACC001", "ACC002", "ACC003", "ACC004"]
currencies = ["ZAR", "USD"]

def generate_data(n=100):

    data = []

    for i in range(n):

        data.append({
            "Account": random.choice(accounts),
            "TradeReference": f"TREF{i}",
            "SettlementDate": (datetime.today() - timedelta(days=random.randint(0,5))).strftime("%Y-%m-%d"),
            "Amount": round(random.uniform(100, 5000), 2),
            "Currency": random.choice(currencies)
        })

    return pd.DataFrame(data)


cash = generate_data(100)
rada = generate_data(90)

cash.to_csv("data/raw/cash.csv", index=False)
rada.to_csv("data/raw/rada.csv", index=False)

print("Sample datasets generated")
