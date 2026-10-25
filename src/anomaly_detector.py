import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.metrics import precision_score, recall_score, f1_score

orders_path = "C:/pipeline-data/processed/orders"
features = ["quantity", "unit_price", "total_amount", "hour", "weekday"]

df = pd.read_parquet(orders_path)
df["hour"] = df["order_time"].dt.hour
df["weekday"] = df["order_time"].dt.day_of_week
print("orders loaded:", len(df))

# ---- part 1: asli data pe model, koi nakli anomaly nahi ----
model = IsolationForest(n_estimators=100, contamination=0.01, random_state=42)
df["flag"] = model.fit_predict(df[features])
df["score"] = model.score_samples(df[features])

flagged = df[df["flag"] == -1]
print("\n== part 1: real data, contamination 0.01 ==")
print("unusual orders flagged:", len(flagged))
print("5 most unusual orders:")
top = flagged.sort_values("score").head(5)
print(top[["order_id", "quantity", "unit_price", "total_amount", "payment_method"]].to_string(index=False))

# ---- part 2: nakli anomalies daal ke evaluation ----
rng = np.random.default_rng(42)
fake_count = 1000


def run_test(name, price_range, qty_range):
    test = df.copy().reset_index(drop=True)
    test["quantity"] = test["quantity"].astype("int64")
    chosen = rng.choice(len(test), size=fake_count, replace=False)

    test["is_fake"] = 0
    test.loc[chosen, "is_fake"] = 1
    test.loc[chosen, "unit_price"] = test.loc[chosen, "unit_price"] * rng.uniform(
        price_range[0], price_range[1], size=fake_count
    )
    if qty_range is not None:
        test.loc[chosen, "quantity"] = rng.integers(qty_range[0], qty_range[1], size=fake_count)
    test.loc[chosen, "total_amount"] = test.loc[chosen, "quantity"] * test.loc[chosen, "unit_price"]

    test_model = IsolationForest(
        n_estimators=100, contamination=fake_count / len(test), random_state=42
    )
    predicted = (test_model.fit_predict(test[features]) == -1).astype(int)

    caught = int(((predicted == 1) & (test["is_fake"] == 1)).sum())
    false_alarms = int(((predicted == 1) & (test["is_fake"] == 0)).sum())

    print("\n==", name, "==")
    print("injected:", fake_count, "| caught:", caught, "| false alarms:", false_alarms)
    print("precision:", round(precision_score(test["is_fake"], predicted), 3))
    print("recall   :", round(recall_score(test["is_fake"], predicted), 3))
    print("f1 score :", round(f1_score(test["is_fake"], predicted), 3))


print("\n== part 2: injected (synthetic) anomalies ==")
run_test("strong: price x10-50, quantity 5-30", (10, 50), (5, 30))
run_test("mild: price x3-6, quantity unchanged", (3, 6), None)