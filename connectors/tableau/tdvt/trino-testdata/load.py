#!/usr/bin/env python3
"""
Load the TDVT TestV1 tables (Calcs, Staples) into a Trino `memory` catalog so
TDVT can run the argus-trino connector against them.

    pip install trino
    python load.py --datadir <sdk>/tests/datasets/TestV1 \
                   --host localhost --port 8080 --user test \
                   --catalog memory --schema tdvt

Reads Calcs.csv and Staples_utf8.csv (the header-less data files), typed per the
DDL in ddl.sql. Empty CSV fields become SQL NULL (TDVT requires empty = null).
Run ddl.sql first (CREATE SCHEMA memory.tdvt; + the two CREATE TABLEs).
"""
import argparse
import csv
import sys

try:
    import trino
except ImportError:
    sys.exit("pip install trino")

# Column name + type, in CSV order. 's'=varchar, 'd'=double, 'i'=int,
# 'b'=boolean, 'D'=date, 'T'=timestamp, 't'=time, 'n'=decimal.
CALCS = [
    ("key", "s"), ("num0", "d"), ("num1", "d"), ("num2", "d"), ("num3", "d"),
    ("num4", "d"), ("str0", "s"), ("str1", "s"), ("str2", "s"), ("str3", "s"),
    ("int0", "i"), ("int1", "i"), ("int2", "i"), ("int3", "i"),
    ("bool0", "b"), ("bool1", "b"), ("bool2", "b"), ("bool3", "b"),
    ("date0", "D"), ("date1", "D"), ("date2", "D"), ("date3", "D"),
    ("time0", "T"), ("time1", "t"), ("datetime0", "T"), ("datetime1", "s"),
    ("zzz", "s"),
]
STAPLES = [
    ("Item Count", "i"), ("Ship Priority", "s"), ("Order Priority", "s"),
    ("Order Status", "s"), ("Order Quantity", "d"), ("Sales Total", "d"),
    ("Discount", "d"), ("Tax Rate", "d"), ("Ship Mode", "s"), ("Fill Time", "d"),
    ("Gross Profit", "d"), ("Price", "n"), ("Ship Handle Cost", "n"),
    ("Employee Name", "s"), ("Employee Dept", "s"), ("Manager Name", "s"),
    ("Employee Yrs Exp", "d"), ("Employee Salary", "n"), ("Customer Name", "s"),
    ("Customer State", "s"), ("Call Center Region", "s"), ("Customer Balance", "d"),
    ("Customer Segment", "s"), ("Prod Type1", "s"), ("Prod Type2", "s"),
    ("Prod Type3", "s"), ("Prod Type4", "s"), ("Product Name", "s"),
    ("Product Container", "s"), ("Ship Promo", "s"), ("Supplier Name", "s"),
    ("Supplier Balance", "d"), ("Supplier Region", "s"), ("Supplier State", "s"),
    ("Order ID", "s"), ("Order Year", "i"), ("Order Month", "i"),
    ("Order Day", "i"), ("Order Date", "T"), ("Order Quarter", "s"),
    ("Product Base Margin", "d"), ("Product ID", "s"), ("Receive Time", "d"),
    ("Received Date", "T"), ("Ship Date", "T"), ("Ship Charge", "n"),
    ("Total Cycle Time", "d"), ("Product In Stock", "s"), ("PID", "i"),
    ("Market Segment", "s"),
]


def lit(val, typ):
    """A CSV field rendered as a typed Trino literal, or NULL if empty."""
    if val is None or val == "":
        return "NULL"
    if typ in ("d", "i", "n"):
        return val                                  # numeric, verbatim
    if typ == "b":
        return "true" if val.strip().lower() in ("1", "true", "t", "yes") else "false"
    esc = val.replace("'", "''")
    if typ == "D":
        return f"DATE '{esc}'"
    if typ == "T":
        return f"TIMESTAMP '{esc}'"
    if typ == "t":
        return f"TIME '{esc}'"
    return f"'{esc}'"                               # varchar


def load(cur, table, cols, csv_path):
    quoted = ", ".join(f'"{c}"' for c, _ in cols)
    with open(csv_path, newline="", encoding="utf-8") as f:
        rows = list(csv.reader(f))
    n, batch = 0, []
    for row in rows:
        if len(row) < len(cols):
            row = row + [""] * (len(cols) - len(row))
        vals = "(" + ", ".join(lit(row[i], cols[i][1]) for i in range(len(cols))) + ")"
        batch.append(vals)
        if len(batch) >= 500:
            cur.execute(f'INSERT INTO "{table}" ({quoted}) VALUES ' + ",".join(batch))
            cur.fetchall(); n += len(batch); batch = []
    if batch:
        cur.execute(f'INSERT INTO "{table}" ({quoted}) VALUES ' + ",".join(batch))
        cur.fetchall(); n += len(batch)
    print(f"  {table}: {n} rows")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--datadir", required=True, help="SDK tests/datasets/TestV1")
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=8080)
    ap.add_argument("--user", default="test")
    ap.add_argument("--catalog", default="memory")
    ap.add_argument("--schema", default="tdvt")
    a = ap.parse_args()

    conn = trino.dbapi.connect(host=a.host, port=a.port, user=a.user,
                               catalog=a.catalog, schema=a.schema)
    cur = conn.cursor()
    print(f"loading into {a.catalog}.{a.schema} (run ddl.sql first)")
    load(cur, "Calcs", CALCS, f"{a.datadir}/Calcs.csv")
    load(cur, "Staples", STAPLES, f"{a.datadir}/Staples_utf8.csv")
    print("done")


if __name__ == "__main__":
    main()
