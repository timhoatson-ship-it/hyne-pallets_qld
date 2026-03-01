#!/usr/bin/env python3
"""
seed_50_orders.py — Hyne Pallets MMS Test Data Seeder
=====================================================
Generates 50 randomised work orders covering diverse test scenarios:
  - All 4 zones (Viking, Handmade, DTL, Crates)
  - All pipeline stages (T, C, R, P, F)
  - Stock runs vs customer orders
  - Orders with/without requested delivery dates
  - Multi-line orders (items across different zones)
  - Varying quantities (small, medium, large)
  - Partial production progress on in-progress orders
  - Orders ready for QA sign-off
  - Orders ready for dispatch

Usage:
  1. Upload this file to your Railway project root (same folder as server.py)
  2. Run:  python seed_50_orders.py
  3. Restart your Railway app (or it picks up on next request)

The script connects to data.db in the same directory and uses the existing
clients/SKUs/zones/stations. If they don't exist, run the app once first
so init_db() creates them.
"""

import sqlite3
import os
import random
from datetime import datetime, timedelta

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data.db")
SEED_ORDER_START = 20001  # WO numbers start here to avoid collisions

# ---------------------------------------------------------------------------
# Additional clients to seed (realistic Brisbane-area pallet customers)
# ---------------------------------------------------------------------------
EXTRA_CLIENTS = [
    ("Visy Packaging", "Karen Mitchell", "karen@visy.com.au", "07 3265 1000"),
    ("Orora Group", "James Foster", "james.foster@orora.com", "07 3868 2200"),
    ("Toll Group", "Megan Schultz", "megan.schultz@toll.com.au", "07 3258 6000"),
    ("Linfox Logistics", "Dave Mercer", "dave.mercer@linfox.com", "07 3722 4100"),
    ("CHR Hansen", "Lisa Chen", "lisa.chen@chr-hansen.com", "07 3715 5500"),
    ("Coopers Brewery", "Mark Sullivan", "mark.sullivan@coopers.com.au", "08 8440 1800"),
    ("Dulux Group", "Steve Barker", "steve.barker@dulux.com.au", "07 3246 7800"),
    ("Boral Limited", "Tony Graves", "tony.graves@boral.com.au", "07 3233 5000"),
    ("CSR Building Products", "Sandra Lee", "sandra.lee@csr.com.au", "07 3864 1200"),
    ("Metcash Trading", "Phil Doyle", "phil.doyle@metcash.com", "07 3722 9800"),
    ("Pepsico Australia", "Angela Torres", "angela.torres@pepsico.com", "07 3902 5000"),
    ("Goodman Fielder", "Nathan Blake", "nathan.blake@goodmanfielder.com", "07 3115 8800"),
    ("Fletcher Building", "Craig O'Brien", "craig.obrien@fbu.com", "07 3624 2100"),
    ("Reece Group", "Deborah Ryan", "deborah.ryan@reece.com.au", "07 3260 4300"),
    ("Cleanaway Waste", "Ian Groves", "ian.groves@cleanaway.com.au", "07 3868 3600"),
    ("Incitec Pivot", "Helen Zhang", "helen.zhang@incitecpivot.com.au", "07 3253 7200"),
]

# ---------------------------------------------------------------------------
# Additional SKUs to seed (realistic Viking / Handmade / DTL / Crate products)
# Drawing numbers from actual Viking rate card ranges
# ---------------------------------------------------------------------------
EXTRA_SKUS = [
    # Viking SKUs — common pallet sizes
    ("VP1165SNC", "1165 x 1165 SNC Branded Pallet", "8015", 3.10, 8.50, 14.95, "VIK"),
    ("VP1100TOLL", "1100 x 1100 Toll Pallet", "7001", 2.85, 7.20, 13.50, "VIK"),
    ("VP1200VISY", "1200 x 1000 Visy Pallet", "6002", 4.20, 9.80, 17.50, "VIK"),
    ("VP1100LINFOX", "1100 x 1100 Linfox Pallet", "7001", 2.85, 7.20, 13.00, "VIK"),
    ("VP1200ORORA", "1200 x 1000 Orora Branded", "6002", 4.20, 9.80, 17.80, "VIK"),
    ("VP1165PEPSICO", "1165 x 1165 Pepsico Pallet", "8015", 3.10, 8.50, 15.20, "VIK"),
    ("VP900BORAL", "900 x 900 Boral Pallet", "6010", 2.50, 6.80, 12.00, "VIK"),
    ("VP1400DULUX", "1400 x 1100 Dulux Heavy Duty", "5015", 6.50, 14.00, 25.50, "VIK"),
    ("VP800METCASH", "800 x 600 Metcash Half Pallet", "6020", 1.90, 4.50, 8.80, "VIK"),
    ("VP1200COOPERS", "1200 x 1200 Coopers Export", "6015", 4.50, 10.20, 18.50, "VIK"),
    ("VP1100GOODMAN", "1100 x 1100 Goodman Pallet", "7001", 2.85, 7.20, 13.20, "VIK"),
    ("VP1200REECE", "1200 x 1000 Reece Pallet", "6002", 4.20, 9.80, 17.60, "VIK"),
    ("VP1165FLETBLD", "1165 x 1165 Fletcher Pallet", "8015", 3.10, 8.50, 15.00, "VIK"),
    ("VP1200CHR", "1200 x 1000 CHR Hansen Pallet", "6002", 4.20, 9.80, 17.90, "VIK"),
    ("VP1100INCITEC", "1100 x 1100 Incitec Pallet", "7001", 2.85, 7.20, 13.40, "VIK"),
    ("VP1200CLEAN", "1200 x 1000 Cleanaway Pallet", "6002", 4.20, 9.80, 17.20, "VIK"),
    ("VP1100CSR", "1100 x 1100 CSR Pallet", "7001", 2.85, 7.20, 13.10, "VIK"),
    # Handmade SKUs
    ("HM1500CST", "1500 x 1200 Oversized Pallet", "3060", 15.00, 22.00, 55.00, "HMP"),
    ("HM1200HD", "1200 x 1200 Heavy Duty Custom", "3055", 14.00, 20.00, 48.00, "HMP"),
    ("HM1800SPEC", "1800 x 1200 Special Pallet", "3070", 22.00, 30.00, 75.00, "HMP"),
    ("HM0900CST", "900 x 900 Handmade Pallet", "3040", 10.00, 15.00, 35.00, "HMP"),
    ("HM1200SURR", "1200 x 1200 Pallet + Surround", "3080", 18.00, 28.00, 62.00, "HMP"),
    # DTL SKUs
    ("DT1500HTR", "1500mm Heat Treated Bearer", "D010", 2.20, 4.50, 8.50, "DTL"),
    ("DT1800GRV", "1800mm Grooved Bearer", "D015", 2.80, 5.20, 9.80, "DTL"),
    ("DT0600BLK", "600mm Block Set", "D020", 0.80, 1.60, 3.50, "DTL"),
    ("DT1200HT", "1200mm HT Dunnage", "D025", 1.60, 3.40, 6.50, "DTL"),
    # Crate SKUs
    ("CR0002EXP", "Export Crate (Standard)", "CR002", 30.00, 55.00, 120.00, "CRT"),
    ("CR0003HD", "Heavy Duty Shipping Crate", "CR003", 45.00, 80.00, 175.00, "CRT"),
    ("CR0004SM", "Small Parts Crate", "CR004", 15.00, 25.00, 55.00, "CRT"),
]

# ---------------------------------------------------------------------------
# 50 Work Order definitions
# ---------------------------------------------------------------------------
# Each tuple: (order_suffix, client_name, status, is_verified, delivery_type,
#   is_stock_run, has_delivery_date, days_offset_delivery, notes, items)
#
# items: list of (sku_code, quantity, produced_qty)
#
# Scenarios covered:
#  - T (unverified): brand new orders, need office verification
#  - T (verified): verified but not yet planned (cut list not issued)
#  - C: cut list issued, ready for production planning
#  - R: scheduled and ready on the floor
#  - P: actively in production with partial counts
#  - F: finished, awaiting QA sign-off / dispatch
#  - Stock runs (no client, goes to inventory)
#  - Multi-zone orders (Viking + Handmade items on same order)
#  - Delivery vs Collection
#  - With/without requested delivery dates
#  - Small (10-50), medium (100-500), large (1000+) quantities
# ---------------------------------------------------------------------------

ORDERS = [
    # === STATUS T — UNVERIFIED (brand new, need office review) ===
    (20001, "Visy Packaging", "T", 0, "delivery", 0, True, 14,
     "New order - awaiting verification",
     [("VP1200VISY", 400, 0), ("DT1200HT", 200, 0)]),

    (20002, "Toll Group", "T", 0, "delivery", 0, False, 0,
     "Phone order from Megan - needs PO confirmation",
     [("VP1100TOLL", 800, 0)]),

    (20003, "Pepsico Australia", "T", 0, "delivery", 0, True, 7,
     "URGENT - short lead time",
     [("VP1165PEPSICO", 600, 0)]),

    (20004, "Boral Limited", "T", 0, "collection", 0, False, 0,
     "Customer collecting - standard lead time",
     [("VP900BORAL", 300, 0), ("DT0600BLK", 500, 0)]),

    (20005, "CSR Building Products", "T", 0, "delivery", 0, True, 21,
     "Monthly recurring order",
     [("VP1100CSR", 1200, 0)]),

    # === STATUS T — VERIFIED (ready for planning, cut list not yet issued) ===
    (20006, "Linfox Logistics", "T", 1, "delivery", 0, True, 10,
     "Verified - schedule for next week",
     [("VP1100LINFOX", 500, 0), ("VP1200ORORA", 200, 0)]),

    (20007, "Dulux Group", "T", 1, "delivery", 0, True, 14,
     "Verified - heavy duty order, Champion machine",
     [("VP1400DULUX", 150, 0)]),

    (20008, "Goodman Fielder", "T", 1, "delivery", 0, False, 0,
     "Verified - standard turnaround",
     [("VP1100GOODMAN", 350, 0)]),

    (20009, "CHR Hansen", "T", 1, "collection", 0, True, 12,
     "Verified - customer collection",
     [("VP1200CHR", 250, 0), ("HM1200HD", 40, 0)]),

    (20010, "Reece Group", "T", 1, "delivery", 0, True, 8,
     "Verified - priority delivery date",
     [("VP1200REECE", 600, 0)]),

    # === STATUS C — CUT LIST ISSUED (scheduled, ready for production floor) ===
    (20011, "Simon National Carriers", "C", 1, "delivery", 0, True, 5,
     "Cut list issued - Turbo 504",
     [("VP1165SNC", 1500, 0)]),

    (20012, "Hisense Australia", "C", 1, "delivery", 0, True, 7,
     "Cut list issued - Turbo 505",
     [("VP1165743HIS", 300, 0)]),

    (20013, "Orora Group", "C", 1, "delivery", 0, True, 10,
     "Cut list issued - large order across 2 machines",
     [("VP1200ORORA", 2000, 0)]),

    (20014, "Fletcher Building", "C", 1, "delivery", 0, False, 0,
     "Cut list issued - standard",
     [("VP1165FLETBLD", 400, 0)]),

    (20015, "Metcash Trading", "C", 1, "delivery", 0, True, 6,
     "Cut list issued - half pallets, fast turnaround",
     [("VP800METCASH", 1000, 0)]),

    (20016, "Incitec Pivot", "C", 1, "delivery", 0, True, 14,
     "Cut list issued - plus handmade component",
     [("VP1100INCITEC", 800, 0), ("HM1500CST", 30, 0)]),

    (20017, "Cleanaway Waste", "C", 1, "collection", 0, True, 9,
     "Cut list issued - collection order",
     [("VP1200CLEAN", 450, 0)]),

    # === STATUS C — HANDMADE ZONE ===
    (20018, "Brisbane Transport Co", "C", 1, "delivery", 0, True, 14,
     "Cut list issued - Handmade zone only",
     [("HM1800SPEC", 25, 0), ("HM1200SURR", 60, 0)]),

    (20019, "Pacific Pallets Pty Ltd", "C", 1, "delivery", 0, True, 10,
     "Cut list issued - Handmade oversized",
     [("HM1500CST", 45, 0)]),

    # === STATUS R — READY FOR PRODUCTION (scheduled, waiting for floor start) ===
    (20020, "Visy Packaging", "R", 1, "delivery", 0, True, 4,
     "Ready for production - Turbo 504, Mon priority",
     [("VP1200VISY", 500, 0)]),

    (20021, "Toll Group", "R", 1, "delivery", 0, True, 3,
     "Ready for production - Turbo 505",
     [("VP1100TOLL", 300, 0)]),

    (20022, "Coopers Brewery", "R", 1, "delivery", 0, True, 5,
     "Ready for production - export pallet, Champion",
     [("VP1200COOPERS", 200, 0)]),

    (20023, "Linfox Logistics", "R", 1, "delivery", 0, True, 6,
     "Ready - Handmade zone, Table 3",
     [("HM0900CST", 40, 0)]),

    (20024, "Dulux Group", "R", 1, "delivery", 0, True, 5,
     "Ready - DTL zone",
     [("DT1500HTR", 300, 0), ("DT1800GRV", 200, 0)]),

    # === STATUS P — IN PRODUCTION (partial counts) ===
    (20025, "Simon National Carriers", "P", 1, "delivery", 0, True, 2,
     "In production on Turbo 504 - 60% done",
     [("VP1165SNC", 2000, 1200)]),

    (20026, "Hisense Australia", "P", 1, "delivery", 0, True, 3,
     "In production on Turbo 505 - 40% done",
     [("VP1165743HIS", 500, 200)]),

    (20027, "Orora Group", "P", 1, "delivery", 0, True, 4,
     "In production - multi-item, staggered progress",
     [("VP1200ORORA", 800, 600), ("DT1200HT", 400, 400)]),

    (20028, "Pepsico Australia", "P", 1, "delivery", 0, True, 1,
     "URGENT - in production, behind schedule",
     [("VP1165PEPSICO", 1000, 350)]),

    (20029, "Goodman Fielder", "P", 1, "delivery", 0, True, 3,
     "In production - Handmade zone, 50% done",
     [("HM1200HD", 80, 40)]),

    (20030, "Boral Limited", "P", 1, "collection", 0, True, 2,
     "In production - nearly complete",
     [("VP900BORAL", 500, 475)]),

    (20031, "CSR Building Products", "P", 1, "delivery", 0, True, 5,
     "In production - large order, early stage",
     [("VP1100CSR", 1500, 200)]),

    (20032, "Reece Group", "P", 1, "delivery", 0, False, 0,
     "In production - Champion machine, steady progress",
     [("VP1200REECE", 400, 280)]),

    # === STATUS F — FINISHED (awaiting QA sign-off / dispatch) ===
    (20033, "Visy Packaging", "F", 1, "delivery", 0, True, 0,
     "Finished - QA sign-off required",
     [("VP1200VISY", 600, 600)]),

    (20034, "Toll Group", "F", 1, "delivery", 0, True, 1,
     "Finished - ready for dispatch scheduling",
     [("VP1100TOLL", 400, 400), ("DT0900DUN", 200, 200)]),

    (20035, "Linfox Logistics", "F", 1, "delivery", 0, True, 0,
     "Finished - overdue, needs immediate dispatch",
     [("VP1100LINFOX", 300, 300)]),

    (20036, "CHR Hansen", "F", 1, "collection", 0, True, 2,
     "Finished - customer notified for collection",
     [("VP1200CHR", 200, 200), ("HM1200SURR", 30, 30)]),

    (20037, "Coopers Brewery", "F", 1, "delivery", 0, True, 1,
     "Finished - export pallets, fumigation complete",
     [("VP1200COOPERS", 500, 500)]),

    (20038, "Metcash Trading", "F", 1, "delivery", 0, False, 0,
     "Finished - half pallets batch",
     [("VP800METCASH", 800, 800)]),

    (20039, "Pacific Pallets Pty Ltd", "F", 1, "delivery", 0, True, -1,
     "Finished - OVERDUE delivery, escalate",
     [("HM1500CST", 50, 50), ("HM0800CST", 100, 100)]),

    # === STOCK RUNS (no client, goes to inventory) ===
    (20040, None, "T", 1, "delivery", 1, False, 0,
     "Stock run - 1165 x 1165 standard (Viking)",
     [("VP1165743SIM", 500, 0)]),

    (20041, None, "C", 1, "delivery", 1, False, 0,
     "Stock run - 1100 x 1100 plain (Viking, cut list issued)",
     [("VP1100PLAIN", 1000, 0)]),

    (20042, None, "P", 1, "delivery", 1, False, 0,
     "Stock run - Euro pallets in production",
     [("VP1200STD", 600, 350)]),

    (20043, None, "F", 1, "delivery", 1, False, 0,
     "Stock run - finished, add to inventory",
     [("VP900CHEP", 400, 400)]),

    (20044, None, "C", 1, "delivery", 1, False, 0,
     "Stock run - Handmade export pallets for inventory",
     [("HM1200EXP", 50, 0)]),

    # === CRATE ORDERS ===
    (20045, "Fletcher Building", "C", 1, "delivery", 0, True, 14,
     "Custom export crate - engineering drawings attached",
     [("CR0002EXP", 10, 0)]),

    (20046, "Incitec Pivot", "P", 1, "delivery", 0, True, 7,
     "Heavy duty crates - in production",
     [("CR0003HD", 5, 3)]),

    (20047, "Cleanaway Waste", "F", 1, "collection", 0, True, 3,
     "Small parts crates - finished, awaiting collection",
     [("CR0004SM", 20, 20)]),

    # === MULTI-ZONE ORDERS (items spanning Viking + Handmade + DTL) ===
    (20048, "Dulux Group", "C", 1, "delivery", 0, True, 12,
     "Mixed order - Viking pallets + Handmade specials + DTL bearers",
     [("VP1400DULUX", 200, 0), ("HM1800SPEC", 15, 0), ("DT1500HTR", 100, 0)]),

    (20049, "Visy Packaging", "P", 1, "delivery", 0, True, 5,
     "Mixed order - Viking done, Handmade in progress",
     [("VP1200VISY", 300, 300), ("HM1200HD", 60, 25), ("DT1200HT", 150, 150)]),

    (20050, "Simon National Carriers", "T", 1, "delivery", 0, True, 18,
     "Large mixed order - all zones, forward planning",
     [("VP1165SNC", 3000, 0), ("HM1500CST", 40, 0), ("DT1800GRV", 200, 0), ("CR0002EXP", 8, 0)]),
]


def main():
    if not os.path.exists(DB_PATH):
        print(f"ERROR: {DB_PATH} not found. Run the app once first so init_db() creates it.")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    c = conn.cursor()

    # ------------------------------------------------------------------
    # 1. Seed extra clients
    # ------------------------------------------------------------------
    for company, contact, email, phone in EXTRA_CLIENTS:
        existing = c.execute("SELECT id FROM clients WHERE company_name=?", [company]).fetchone()
        if not existing:
            c.execute("INSERT INTO clients (company_name, contact_name, email, phone) VALUES (?,?,?,?)",
                      [company, contact, email, phone])
            print(f"  + Client: {company}")
    conn.commit()

    # ------------------------------------------------------------------
    # 2. Seed extra SKUs
    # ------------------------------------------------------------------
    zone_map = {}
    for row in c.execute("SELECT id, code FROM zones").fetchall():
        zone_map[row["code"]] = row["id"]

    for code, name, drawing, labour, material, sell, zone_code in EXTRA_SKUS:
        existing = c.execute("SELECT id FROM skus WHERE code=?", [code]).fetchone()
        if not existing:
            zid = zone_map.get(zone_code)
            c.execute("INSERT INTO skus (code, name, drawing_number, labour_cost, material_cost, sell_price, zone_id) VALUES (?,?,?,?,?,?,?)",
                      [code, name, drawing, labour, material, sell, zid])
            print(f"  + SKU: {code} ({name})")
    conn.commit()

    # ------------------------------------------------------------------
    # 3. Build lookup caches
    # ------------------------------------------------------------------
    client_map = {}
    for row in c.execute("SELECT id, company_name FROM clients").fetchall():
        client_map[row["company_name"]] = row["id"]

    sku_map = {}
    for row in c.execute("SELECT id, code, name, sell_price, zone_id, drawing_number FROM skus").fetchall():
        sku_map[row["code"]] = dict(row)

    office_user = c.execute("SELECT id FROM users WHERE role='office' LIMIT 1").fetchone()
    office_uid = office_user["id"] if office_user else None

    # ------------------------------------------------------------------
    # 4. Seed 50 orders
    # ------------------------------------------------------------------
    today = datetime.now().date()
    created = 0
    skipped = 0

    for (suffix, client_name, status, is_verified, delivery_type,
         is_stock_run, has_del_date, days_offset, notes, items_def) in ORDERS:

        order_number = f"WO-{suffix}"

        # Skip if already exists
        if c.execute("SELECT id FROM orders WHERE order_number=?", [order_number]).fetchone():
            skipped += 1
            continue

        client_id = client_map.get(client_name) if client_name else None
        requested_delivery_date = None
        if has_del_date:
            requested_delivery_date = (today + timedelta(days=days_offset)).isoformat()

        # Calculate total value
        total_value = 0
        for sku_code, qty, _ in items_def:
            sku = sku_map.get(sku_code)
            if sku:
                total_value += qty * sku["sell_price"]

        verified_by = office_uid if is_verified else None

        c.execute("""INSERT INTO orders
            (order_number, client_id, status, is_verified, verified_by, total_value,
             delivery_type, is_stock_run, requested_delivery_date, notes,
             created_at, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)""",
            [order_number, client_id, status, is_verified, verified_by, total_value,
             delivery_type, is_stock_run, requested_delivery_date, notes])
        order_id = c.lastrowid

        # Insert order items
        for sku_code, qty, produced_qty in items_def:
            sku = sku_map.get(sku_code)
            if not sku:
                print(f"  WARNING: SKU {sku_code} not found, skipping item")
                continue

            # Item status follows order status, but if produced_qty == qty => F
            item_status = status
            if produced_qty >= qty and status in ("P", "F"):
                item_status = "F"

            line_total = qty * sku["sell_price"]
            c.execute("""INSERT INTO order_items
                (order_id, sku_id, sku_code, product_name, quantity, produced_quantity,
                 unit_price, line_total, status, zone_id, drawing_number)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                [order_id, sku["id"], sku_code, sku["name"], qty, produced_qty,
                 sku["sell_price"], line_total, item_status, sku["zone_id"],
                 sku["drawing_number"]])

        created += 1
        print(f"  + {order_number} | {client_name or 'STOCK RUN':30s} | {status} | {len(items_def)} items | ${total_value:,.2f}")

    conn.commit()

    # ------------------------------------------------------------------
    # 5. Summary
    # ------------------------------------------------------------------
    total_orders = c.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
    total_items = c.execute("SELECT COUNT(*) FROM order_items").fetchone()[0]
    total_clients = c.execute("SELECT COUNT(*) FROM clients").fetchone()[0]
    total_skus = c.execute("SELECT COUNT(*) FROM skus").fetchone()[0]

    print(f"\n{'='*60}")
    print(f"  SEED COMPLETE")
    print(f"{'='*60}")
    print(f"  Orders created this run:  {created}")
    print(f"  Orders skipped (exist):   {skipped}")
    print(f"  Total orders in DB:       {total_orders}")
    print(f"  Total order items in DB:  {total_items}")
    print(f"  Total clients in DB:      {total_clients}")
    print(f"  Total SKUs in DB:         {total_skus}")
    print(f"{'='*60}")

    # Status breakdown
    print(f"\n  Pipeline breakdown:")
    for status in ["T", "C", "R", "P", "F"]:
        cnt = c.execute("SELECT COUNT(*) FROM orders WHERE order_number LIKE 'WO-%' AND status=?", [status]).fetchone()[0]
        print(f"    {status}: {cnt} orders")

    stock_cnt = c.execute("SELECT COUNT(*) FROM orders WHERE order_number LIKE 'WO-%' AND is_stock_run=1").fetchone()[0]
    multi_cnt = c.execute("""SELECT COUNT(DISTINCT o.id) FROM orders o
        JOIN order_items oi ON oi.order_id = o.id
        WHERE o.order_number LIKE 'WO-%'
        GROUP BY o.id HAVING COUNT(DISTINCT oi.zone_id) > 1""").fetchall()
    print(f"\n  Stock runs:               {stock_cnt}")
    print(f"  Multi-zone orders:        {len(multi_cnt)}")

    conn.close()
    print(f"\n  Done. Restart your app or refresh the browser to see the new data.")


if __name__ == "__main__":
    main()
