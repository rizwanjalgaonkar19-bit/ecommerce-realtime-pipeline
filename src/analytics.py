import duckdb

data_folder = "C:/pipeline-data/processed/"

con = duckdb.connect()

# parquet folders ko table jaisa bana lete hain
for name, folder in [("orders", "orders"), ("customers", "customers"),
                     ("products", "products"), ("clicks", "clickstream")]:
    con.execute(
        "create view " + name + " as select * from read_parquet('"
        + data_folder + folder + "/*.parquet')"
    )


def run_queries():
    text = open("sql/kpi_queries.sql").read()
    for query in text.split(";"):
        query = query.strip()
        if query == "":
            continue
        title = query.split("\n")[0].replace("--", "").strip()
        print("\n==", title, "==")
        print(con.execute(query).df().to_string(index=False))


if __name__ == "__main__":
    run_queries()