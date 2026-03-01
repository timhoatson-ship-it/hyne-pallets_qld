#!/usr/bin/env python3
"""
server.py - Flask server for Hyne Pallets Manufacturing Management System
Wraps the existing CGI handler logic for deployment on Railway/Render/etc.
"""

import os
import sys
import json
import base64
import hashlib
import re
import traceback
import sqlite3
from datetime import datetime, timezone, timedelta
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS

# ---------------------------------------------------------------------------
# Ensure index.html exists (download if missing/truncated)
# ---------------------------------------------------------------------------
import urllib.request
_idx = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static", "index.html")
if not os.path.exists(_idx) or os.path.getsize(_idx) < 100000:
    _url = "https://sites.pplx.app/sites/proxy/eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJwcmVmaXgiOiJ3ZWIvZGlyZWN0LWZpbGVzL2NvbXB1dGVyLzEwNDk1ZjE1LThmNmEtNDcwNy04OWQzLTVjYjA2YjdiMmU4OC9oeW5lLWh0bWwtaG9zdC8iLCJzaWQiOiIxMDQ5NWYxNS04ZjZhLTQ3MDctODlkMy01Y2IwNmI3YjJlODgiLCJleHAiOjE3NzIzOTA5NzZ9.O5s3VCN9xvvrnmbVgZzOd9rvgddMgEggKFD6VzgfbB4/web/direct-files/computer/10495f15-8f6a-4707-89d3-5cb06b7b2e88/hyne-html-host/index.html"
    try:
        print("Downloading index.html...")
        urllib.request.urlretrieve(_url, _idx)
        print(f"Downloaded index.html ({os.path.getsize(_idx)} bytes)")
    except Exception as e:
        print(f"Warning: Could not download index.html: {e}")

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------

app = Flask(__name__, static_folder="static", static_url_path="")
CORS(app)

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data.db")

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def hash_password(password):
    return hashlib.sha256((password + "hyne_salt").encode()).hexdigest()


def row_to_dict(row):
    if row is None:
        return None
    return dict(row)


def rows_to_list(rows):
    return [dict(r) for r in rows]


def init_db():
    """Create all tables and insert seed data if not already present."""
    conn = get_connection()
    c = conn.cursor()

    c.executescript("""
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT UNIQUE,
    password_hash TEXT,
    pin TEXT,
    username TEXT UNIQUE,
    full_name TEXT NOT NULL,
    role TEXT NOT NULL CHECK(role IN ('executive','office','planner','production_manager','floor_worker','qa_lead','dispatch','yard')),
    default_zone_id INTEGER,
    is_active INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS zones (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    code TEXT NOT NULL UNIQUE,
    capacity_metric TEXT NOT NULL CHECK(capacity_metric IN ('units_per_machine','man_hours_per_table','man_hours_per_centre','man_hours_per_station')),
    is_active INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    zone_id INTEGER NOT NULL REFERENCES zones(id),
    name TEXT NOT NULL,
    code TEXT NOT NULL,
    station_type TEXT CHECK(station_type IN ('machine','table','centre','station')),
    is_active INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(zone_id, code)
);

CREATE TABLE IF NOT EXISTS shifts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    start_time TEXT NOT NULL,
    end_time TEXT NOT NULL,
    zone_id INTEGER REFERENCES zones(id),
    is_active INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS trucks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    rego TEXT NOT NULL UNIQUE,
    driver_name TEXT,
    truck_type TEXT DEFAULT 'internal' CHECK(truck_type IN ('internal','contractor')),
    capacity_notes TEXT,
    is_active INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS clients (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_name TEXT NOT NULL,
    contact_name TEXT,
    email TEXT,
    phone TEXT,
    address TEXT,
    payment_terms TEXT,
    myob_uid TEXT UNIQUE,
    is_active INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS client_contacts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id INTEGER NOT NULL REFERENCES clients(id),
    contact_name TEXT NOT NULL,
    email TEXT,
    phone TEXT,
    role_title TEXT,
    email_purpose TEXT CHECK(email_purpose IN ('order_confirmation','delivery_confirmation','invoicing','general','all')),
    receives_sensitive INTEGER DEFAULT 0,
    notes TEXT,
    is_active INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS skus (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    drawing_number TEXT,
    labour_cost REAL DEFAULT 0,
    material_cost REAL DEFAULT 0,
    sell_price REAL DEFAULT 0,
    zone_id INTEGER REFERENCES zones(id),
    is_active INTEGER DEFAULT 1,
    myob_uid TEXT UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_number TEXT NOT NULL UNIQUE,
    client_id INTEGER REFERENCES clients(id),
    status TEXT NOT NULL DEFAULT 'T' CHECK(status IN ('T','C','R','P','F','dispatched','delivered','collected')),
    special_instructions TEXT,
    is_verified INTEGER DEFAULT 0,
    verified_by INTEGER REFERENCES users(id),
    verified_at TIMESTAMP,
    eta_date TEXT,
    previous_eta TEXT,
    eta_set_by INTEGER REFERENCES users(id),
    eta_set_at TIMESTAMP,
    delivery_type TEXT DEFAULT 'delivery' CHECK(delivery_type IN ('delivery','collection')),
    truck_id INTEGER REFERENCES trucks(id),
    dispatch_date TEXT,
    dispatched_at TIMESTAMP,
    myob_invoice_number TEXT,
    myob_uid TEXT UNIQUE,
    total_value REAL DEFAULT 0,
    notes TEXT,
    requested_delivery_date TEXT,
    is_stock_run INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS order_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id INTEGER NOT NULL REFERENCES orders(id),
    sku_id INTEGER REFERENCES skus(id),
    sku_code TEXT,
    product_name TEXT,
    quantity INTEGER NOT NULL,
    produced_quantity INTEGER DEFAULT 0,
    unit_price REAL DEFAULT 0,
    line_total REAL DEFAULT 0,
    status TEXT DEFAULT 'T' CHECK(status IN ('T','C','R','P','F','dispatched')),
    zone_id INTEGER REFERENCES zones(id),
    station_id INTEGER REFERENCES stations(id),
    scheduled_date TEXT,
    eta_date TEXT,
    drawing_number TEXT,
    special_instructions TEXT,
    split_from_item_id INTEGER REFERENCES order_items(id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS labour_rates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sku_id INTEGER REFERENCES skus(id),
    rate_per_unit REAL NOT NULL,
    rate_type TEXT DEFAULT 'standard',
    effective_date TEXT,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS target_labour_rates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER REFERENCES users(id),
    rate_per_hour REAL NOT NULL DEFAULT 55.00,
    is_default INTEGER DEFAULT 0,
    effective_date TEXT,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS production_sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_item_id INTEGER REFERENCES order_items(id),
    station_id INTEGER NOT NULL REFERENCES stations(id),
    zone_id INTEGER NOT NULL REFERENCES zones(id),
    shift_id INTEGER REFERENCES shifts(id),
    status TEXT DEFAULT 'active' CHECK(status IN ('active','paused','completed')),
    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP,
    target_quantity INTEGER,
    produced_quantity INTEGER DEFAULT 0,
    is_sub_assembly INTEGER DEFAULT 0,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS session_workers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id INTEGER NOT NULL REFERENCES production_sessions(id),
    user_id INTEGER NOT NULL REFERENCES users(id),
    scan_on_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    scan_off_time TIMESTAMP,
    is_active INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS production_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id INTEGER NOT NULL REFERENCES production_sessions(id),
    user_id INTEGER REFERENCES users(id),
    quantity_change INTEGER NOT NULL,
    running_total INTEGER NOT NULL,
    logged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS setup_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    station_id INTEGER NOT NULL REFERENCES stations(id),
    order_item_id INTEGER REFERENCES order_items(id),
    setup_type TEXT CHECK(setup_type IN ('machine','jig')),
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    duration_minutes REAL,
    qa_checklist_passed INTEGER DEFAULT 0,
    team_leader_id INTEGER REFERENCES users(id),
    team_leader_signed_at TIMESTAMP,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS pause_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id INTEGER NOT NULL REFERENCES production_sessions(id),
    reason TEXT NOT NULL CHECK(reason IN ('material','cleaning','break','breakdown','forklift','urgent_changeover','other')),
    paused_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    resumed_at TIMESTAMP,
    duration_minutes REAL,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS qa_inspections (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_item_id INTEGER REFERENCES order_items(id),
    session_id INTEGER REFERENCES production_sessions(id),
    inspection_type TEXT CHECK(inspection_type IN ('batch','setup','random_audit','post_production','final')),
    batch_size INTEGER,
    passed INTEGER,
    inspector_id INTEGER REFERENCES users(id),
    inspected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS qa_defects (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    inspection_id INTEGER NOT NULL REFERENCES qa_inspections(id),
    defect_type TEXT CHECK(defect_type IN ('rework','seconds','destroy')),
    quantity INTEGER NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS post_production_processes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    requires_dashboard INTEGER DEFAULT 1,
    triggers_notification INTEGER DEFAULT 1,
    assigned_role TEXT,
    is_active INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS post_production_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_item_id INTEGER REFERENCES order_items(id),
    process_id INTEGER REFERENCES post_production_processes(id),
    facility TEXT,
    operator_id INTEGER REFERENCES users(id),
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    batch_quantity INTEGER,
    certificate_number TEXT,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS accounting_config (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    provider TEXT DEFAULT 'mock' CHECK(provider IN ('mock','myob','xero','manual')),
    api_key TEXT,
    api_secret TEXT,
    access_token TEXT,
    refresh_token TEXT,
    company_file_id TEXT,
    sync_interval_minutes INTEGER DEFAULT 5,
    last_sync_at TIMESTAMP,
    is_connected INTEGER DEFAULT 0,
    config_json TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS accounting_sync_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    direction TEXT CHECK(direction IN ('inbound','outbound')),
    entity_type TEXT,
    entity_id TEXT,
    status TEXT CHECK(status IN ('success','error','conflict')),
    details TEXT,
    synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS notification_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id INTEGER REFERENCES orders(id),
    notification_type TEXT CHECK(notification_type IN ('order_acknowledgement','eta_notification','dispatch_notification','collection_ready')),
    recipient_email TEXT,
    subject TEXT,
    body TEXT,
    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status TEXT DEFAULT 'sent'
);

CREATE TABLE IF NOT EXISTS audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER REFERENCES users(id),
    action TEXT NOT NULL,
    entity_type TEXT,
    entity_id INTEGER,
    old_value TEXT,
    new_value TEXT,
    ip_address TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS schedule_entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id INTEGER REFERENCES orders(id),
    order_item_id INTEGER REFERENCES order_items(id),
    zone_id INTEGER NOT NULL REFERENCES zones(id),
    station_id INTEGER REFERENCES stations(id),
    scheduled_date TEXT NOT NULL,
    shift_id INTEGER REFERENCES shifts(id),
    planned_quantity INTEGER,
    status TEXT DEFAULT 'planned' CHECK(status IN ('planned','in_progress','completed','cancelled')),
    priority INTEGER DEFAULT 0,
    run_order INTEGER DEFAULT 0,
    notes TEXT,
    created_by INTEGER REFERENCES users(id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS inventory (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sku_id INTEGER NOT NULL REFERENCES skus(id) UNIQUE,
    units_on_hand INTEGER DEFAULT 0,
    units_allocated INTEGER DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS station_capacity (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    station_id INTEGER NOT NULL REFERENCES stations(id),
    max_units_per_day INTEGER NOT NULL DEFAULT 3000,
    UNIQUE(station_id)
);

CREATE TABLE IF NOT EXISTS close_days (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    zone_id INTEGER NOT NULL REFERENCES zones(id),
    closed_date TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(zone_id, closed_date)
);

CREATE TABLE IF NOT EXISTS delivery_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id INTEGER REFERENCES orders(id),
    expected_date TEXT,
    actual_date TEXT,
    truck_id INTEGER REFERENCES trucks(id),
    delivery_type TEXT DEFAULT 'delivery' CHECK(delivery_type IN ('delivery','collection')),
    status TEXT DEFAULT 'pending' CHECK(status IN ('pending','loaded','in_transit','delivered','collected')),
    load_sequence INTEGER,
    notes TEXT,
    estimated_minutes INTEGER DEFAULT 30,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS truck_work_orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    truck_id INTEGER NOT NULL REFERENCES trucks(id),
    wo_type TEXT NOT NULL CHECK(wo_type IN ('service','rego','mill_run','alternate_collection','other')),
    title TEXT NOT NULL,
    description TEXT,
    scheduled_date TEXT,
    estimated_minutes INTEGER DEFAULT 60,
    status TEXT DEFAULT 'pending' CHECK(status IN ('pending','in_progress','completed','cancelled')),
    priority TEXT DEFAULT 'normal' CHECK(priority IN ('low','normal','high','urgent')),
    completed_at TIMESTAMP,
    created_by INTEGER REFERENCES users(id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS truck_capacity_config (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    truck_id INTEGER NOT NULL REFERENCES trucks(id),
    day_of_week INTEGER,
    capacity_minutes INTEGER NOT NULL DEFAULT 480,
    overtime_minutes INTEGER DEFAULT 120,
    notes TEXT,
    UNIQUE(truck_id, day_of_week)
);

CREATE TABLE IF NOT EXISTS delivery_addresses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id INTEGER NOT NULL REFERENCES clients(id),
    address_name TEXT,
    street_address TEXT NOT NULL,
    suburb TEXT,
    state TEXT DEFAULT 'QLD',
    postcode TEXT,
    estimated_travel_minutes INTEGER DEFAULT 30,
    estimated_return_minutes INTEGER,
    notes TEXT,
    is_default INTEGER DEFAULT 0,
    is_active INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS contractor_assignments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    delivery_log_id INTEGER REFERENCES delivery_log(id),
    truck_work_order_id INTEGER REFERENCES truck_work_orders(id),
    contractor_name TEXT,
    contractor_phone TEXT,
    contractor_company TEXT,
    on_behalf_of TEXT NOT NULL DEFAULT 'hyne' CHECK(on_behalf_of IN ('hyne','customer')),
    assignment_type TEXT NOT NULL DEFAULT 'delivery' CHECK(assignment_type IN ('delivery','collection')),
    pickup_address TEXT,
    delivery_address TEXT,
    estimated_minutes INTEGER,
    cost_estimate REAL,
    status TEXT DEFAULT 'assigned' CHECK(status IN ('assigned','in_progress','completed','cancelled')),
    notes TEXT,
    assigned_by INTEGER REFERENCES users(id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
""")
    conn.commit()

    # Seed data
    if c.execute("SELECT COUNT(*) FROM zones").fetchone()[0] == 0:
        c.executemany("INSERT INTO zones (name, code, capacity_metric) VALUES (?, ?, ?)", [
            ("Viking", "VIK", "units_per_machine"),
            ("Handmade", "HMP", "man_hours_per_table"),
            ("DTL", "DTL", "man_hours_per_centre"),
            ("Crates", "CRT", "man_hours_per_station"),
        ])
        conn.commit()

    if c.execute("SELECT COUNT(*) FROM stations").fetchone()[0] == 0:
        vik_id = c.execute("SELECT id FROM zones WHERE code='VIK'").fetchone()[0]
        hmp_id = c.execute("SELECT id FROM zones WHERE code='HMP'").fetchone()[0]
        dtl_id = c.execute("SELECT id FROM zones WHERE code='DTL'").fetchone()[0]
        crt_id = c.execute("SELECT id FROM zones WHERE code='CRT'").fetchone()[0]
        stations = (
            [(vik_id, "Machine 505", "M505", "machine"),
             (vik_id, "Machine 504", "M504", "machine"),
             (vik_id, "Champion", "CHAMP", "machine")] +
            [(hmp_id, f"Table {i}", f"T{i:02d}", "table") for i in range(1, 9)] +
            [(dtl_id, "Saw 2", "SAW2", "centre"),
             (dtl_id, "Saw 3", "SAW3", "centre"),
             (dtl_id, "Titan", "TITAN", "centre"),
             (dtl_id, "Dimter 1", "DIM1", "centre"),
             (dtl_id, "Tiger", "TIGER", "centre"),
             (dtl_id, "Notcher", "NOTCH", "centre"),
             (dtl_id, "Dimter 2", "DIM2", "centre")] +
            [(crt_id, f"Station {i}", f"S{i:02d}", "station") for i in range(1, 4)]
        )
        c.executemany("INSERT INTO stations (zone_id, name, code, station_type) VALUES (?,?,?,?)", stations)
        conn.commit()

    if c.execute("SELECT COUNT(*) FROM shifts").fetchone()[0] == 0:
        c.executemany("INSERT INTO shifts (name, start_time, end_time) VALUES (?,?,?)", [
            ("Day Shift", "06:00", "14:00"),
            ("Night Shift", "14:00", "22:00"),
        ])
        conn.commit()

    if c.execute("SELECT COUNT(*) FROM trucks").fetchone()[0] == 0:
        c.executemany("INSERT INTO trucks (name, rego, driver_name, truck_type) VALUES (?,?,?,?)", [
            ("Truck 1", "TRK-001", "Leeroy", "internal"),
            ("Truck 2", "TRK-002", "Usef", "internal"),
            ("Truck 3", "TRK-003", "Ronny", "internal"),
            ("Truck 4", "TRK-004", "Ben", "internal"),
            ("Truck 5", "TRK-005", "Marcus", "internal"),
            ("Truck 6", "TRK-006", "Besher", "internal"),
            ("Truck 7", "TRK-007", "Contractor", "contractor"),
        ])
        conn.commit()

    # Seed default truck capacity (480 min = 8 hrs per day, 120 min overtime)
    if c.execute("SELECT COUNT(*) FROM truck_capacity_config").fetchone()[0] == 0:
        trucks_for_cap = c.execute("SELECT id FROM trucks WHERE is_active=1").fetchall()
        for t in trucks_for_cap:
            for dow in range(7):  # 0=Mon through 6=Sun
                cap = 480 if dow < 6 else 0  # No capacity on Sunday by default
                c.execute("INSERT OR IGNORE INTO truck_capacity_config (truck_id, day_of_week, capacity_minutes, overtime_minutes) VALUES (?,?,?,?)",
                    [t[0], dow, cap, 120])
        conn.commit()

    if c.execute("SELECT COUNT(*) FROM users").fetchone()[0] == 0:
        admin_pw = hash_password("admin123")
        default_pw = hash_password("password123")
        users = [
            ("tim@hynepallets.com.au", admin_pw, None, "tim.hoatson", "Tim Hoatson", "executive"),
            ("sarah@hynepallets.com.au", default_pw, None, "sarah.office", "Sarah Office", "office"),
            ("mike@hynepallets.com.au", default_pw, None, "mike.planner", "Mike Planner", "planner"),
            ("dave@hynepallets.com.au", default_pw, None, "dave.prodmgr", "Dave ProdMgr", "production_manager"),
            (None, None, "123456", "bob.floor1", "Bob Floor1", "floor_worker"),
            (None, None, "234567", "jim.floor2", "Jim Floor2", "floor_worker"),
            (None, None, "345678", "pete.floor3", "Pete Floor3", "floor_worker"),
            ("jane@hynepallets.com.au", default_pw, None, "jane.qa", "Jane QA", "qa_lead"),
            ("tom@hynepallets.com.au", default_pw, None, "tom.dispatch", "Tom Dispatch", "dispatch"),
            ("steve@hynepallets.com.au", default_pw, None, "steve.yard", "Steve Yard", "yard"),
        ]
        c.executemany("INSERT INTO users (email, password_hash, pin, username, full_name, role) VALUES (?,?,?,?,?,?)", users)
        conn.commit()

    if c.execute("SELECT COUNT(*) FROM target_labour_rates").fetchone()[0] == 0:
        c.execute("INSERT INTO target_labour_rates (user_id, rate_per_hour, is_default, notes) VALUES (NULL, 55.00, 1, 'Global default rate')")
        conn.commit()

    if c.execute("SELECT COUNT(*) FROM post_production_processes").fetchone()[0] == 0:
        c.executemany("INSERT INTO post_production_processes (name, requires_dashboard, triggers_notification, assigned_role) VALUES (?,?,?,?)", [
            ("Heat Treatment", 1, 1, "production_manager"),
            ("CCA Treatment", 1, 1, "production_manager"),
            ("Painting", 1, 0, "floor_worker"),
            ("Stencilling", 1, 0, "floor_worker"),
        ])
        conn.commit()

    if c.execute("SELECT COUNT(*) FROM accounting_config").fetchone()[0] == 0:
        c.execute("INSERT INTO accounting_config (provider, sync_interval_minutes, is_connected) VALUES ('mock', 5, 1)")
        conn.commit()

    if c.execute("SELECT COUNT(*) FROM clients").fetchone()[0] == 0:
        c.executemany("INSERT INTO clients (company_name, contact_name, email, phone) VALUES (?,?,?,?)", [
            ("Simon National Carriers", "Simon Carter", "simon@snc.com.au", "07 3000 1111"),
            ("Hisense Australia", "Rachel Wong", "rachel@hisense.com.au", "02 9000 2222"),
            ("Brisbane Transport Co", "Brad Thompson", "brad@brisbanetrans.com.au", "07 3000 3333"),
            ("Pacific Pallets Pty Ltd", "Paul Nguyen", "paul@pacificpallets.com.au", "07 3000 4444"),
        ])
        conn.commit()

    if c.execute("SELECT COUNT(*) FROM skus").fetchone()[0] == 0:
        vik_id = c.execute("SELECT id FROM zones WHERE code='VIK'").fetchone()[0]
        hmp_id = c.execute("SELECT id FROM zones WHERE code='HMP'").fetchone()[0]
        dtl_id = c.execute("SELECT id FROM zones WHERE code='DTL'").fetchone()[0]
        crt_id = c.execute("SELECT id FROM zones WHERE code='CRT'").fetchone()[0]
        skus = [
            ("VP1165743SIM", "1165 x 1165 Stencilled Pallet", "8015", 3.10, 8.50, 14.95, vik_id),
            ("VP1165743HIS", "1165 x 1165 Hisense Pallet", "8015", 3.10, 8.50, 14.95, vik_id),
            ("VP1100PLAIN", "1100 x 1100 Standard Pallet", "7001", 2.85, 7.20, 12.50, vik_id),
            ("VP1200STD", "1200 x 1000 Euro Pallet", "6002", 4.20, 9.80, 17.50, vik_id),
            ("VP1400HD", "1400 x 1100 Heavy Duty Pallet", "5015", 6.50, 14.00, 24.00, vik_id),
            ("VP900CHEP", "900 x 900 CHEP Pallet", "6010", 2.50, 6.80, 11.50, vik_id),
            ("VP1200EXPORT", "1200 x 1200 Export Pallet (VIK)", "6015", 4.50, 10.20, 18.00, vik_id),
            ("VP800HALF", "800 x 600 Half Pallet", "6020", 1.90, 4.50, 8.50, vik_id),
            ("HM0800CST", "800 x 600 Custom Pallet", "3001", 8.00, 12.00, 28.00, hmp_id),
            ("HM1200EXP", "1200 x 1200 Export Pallet", "3050", 12.00, 18.00, 42.00, hmp_id),
            ("DT0900DUN", "900mm Heat Treated Dunnage", "D001", 1.50, 3.20, 6.00, dtl_id),
            ("DT1200GRV", "1200mm Grooved Dunnage", "D002", 1.80, 3.60, 6.80, dtl_id),
            ("CR0001CUS", "Custom Crate", "CR001", 25.00, 45.00, 95.00, crt_id),
        ]
        c.executemany("INSERT INTO skus (code, name, drawing_number, labour_cost, material_cost, sell_price, zone_id) VALUES (?,?,?,?,?,?,?)", skus)
        conn.commit()

    if c.execute("SELECT COUNT(*) FROM orders").fetchone()[0] == 0:
        snc_id = c.execute("SELECT id FROM clients WHERE company_name='Simon National Carriers'").fetchone()[0]
        his_id = c.execute("SELECT id FROM clients WHERE company_name='Hisense Australia'").fetchone()[0]
        bris_id = c.execute("SELECT id FROM clients WHERE company_name='Brisbane Transport Co'").fetchone()[0]
        pac_id = c.execute("SELECT id FROM clients WHERE company_name='Pacific Pallets Pty Ltd'").fetchone()[0]

        # Fetch all SKU info: (id, sell_price, zone_id) keyed by code
        def _sku(code):
            return c.execute("SELECT id, sell_price, zone_id FROM skus WHERE code=?", [code]).fetchone()

        skus_map = {
            "VP1165743SIM": (_sku("VP1165743SIM"), "1165 x 1165 Stencilled Pallet"),
            "VP1165743HIS": (_sku("VP1165743HIS"), "1165 x 1165 Hisense Pallet"),
            "VP1100PLAIN":  (_sku("VP1100PLAIN"),  "1100 x 1100 Standard Pallet"),
            "VP1200STD":    (_sku("VP1200STD"),    "1200 x 1000 Euro Pallet"),
            "VP1400HD":     (_sku("VP1400HD"),     "1400 x 1100 Heavy Duty Pallet"),
            "VP900CHEP":    (_sku("VP900CHEP"),    "900 x 900 CHEP Pallet"),
            "VP1200EXPORT": (_sku("VP1200EXPORT"), "1200 x 1200 Export Pallet (VIK)"),
            "VP800HALF":    (_sku("VP800HALF"),    "800 x 600 Half Pallet"),
            "HM0800CST":    (_sku("HM0800CST"),    "800 x 600 Custom Pallet"),
            "HM1200EXP":    (_sku("HM1200EXP"),    "1200 x 1200 Export Pallet"),
            "DT0900DUN":    (_sku("DT0900DUN"),    "900mm Heat Treated Dunnage"),
            "DT1200GRV":    (_sku("DT1200GRV"),    "1200mm Grooved Dunnage"),
            "CR0001CUS":    (_sku("CR0001CUS"),    "Custom Crate"),
        }

        # Deterministic order definitions: each tuple is
        # (client_id_key, created_at, requested_delivery_date_or_None, item_specs)
        # item_specs: list of (sku_code, quantity)
        # client keys: 0=SNC, 1=HIS, 2=BRIS, 3=PAC
        _clients = [snc_id, his_id, bris_id, pac_id]

        # Pre-defined order data: (client_idx, created_at, req_delivery, [(sku_code, qty), ...])
        _orders_def = [
            # --- Orders 0-9 ---
            (0, "2026-02-23", "2026-03-05", [("VP1165743SIM", 200)]),
            (1, "2026-02-23", "2026-03-07", [("VP1165743HIS", 500)]),
            (2, "2026-02-23", "2026-03-10", [("VP1100PLAIN",  300)]),
            (3, "2026-02-23", "2026-03-12", [("VP1200STD",    150)]),
            (0, "2026-02-23", None,         [("VP1400HD",     100)]),
            (1, "2026-02-23", "2026-03-14", [("VP900CHEP",    400)]),
            (2, "2026-02-24", "2026-03-06", [("VP1200EXPORT", 250)]),
            (3, "2026-02-24", "2026-03-08", [("VP800HALF",    600)]),
            (0, "2026-02-24", "2026-03-15", [("HM0800CST",     30)]),
            (1, "2026-02-24", None,         [("HM1200EXP",     20)]),
            # --- Orders 10-19 ---
            (2, "2026-02-24", "2026-03-09", [("DT0900DUN",    100)]),
            (3, "2026-02-24", "2026-03-11", [("CR0001CUS",      5)]),
            (0, "2026-02-24", "2026-03-16", [("VP1165743SIM", 350)]),
            (1, "2026-02-24", "2026-03-13", [("VP1165743HIS", 800)]),
            (2, "2026-02-25", None,         [("VP1100PLAIN",  500)]),
            (3, "2026-02-25", "2026-03-17", [("VP1200STD",    200)]),
            (0, "2026-02-25", "2026-03-07", [("VP1400HD",      75)]),
            (1, "2026-02-25", "2026-03-18", [("VP900CHEP",    300)]),
            (2, "2026-02-25", "2026-03-10", [("VP1200EXPORT", 180)]),
            (3, "2026-02-25", "2026-03-19", [("VP800HALF",   1000)]),
            # --- Orders 20-29 ---
            (0, "2026-02-25", None,         [("HM0800CST",     50)]),
            (1, "2026-02-25", "2026-03-20", [("HM1200EXP",     40)]),
            (2, "2026-02-25", "2026-03-08", [("DT1200GRV",    200)]),
            (3, "2026-02-25", "2026-03-11", [("CR0001CUS",     10)]),
            (0, "2026-02-26", "2026-03-21", [("VP1165743SIM", 450)]),
            (1, "2026-02-26", "2026-03-12", [("VP1165743HIS", 600)]),
            (2, "2026-02-26", None,         [("VP1100PLAIN",  700)]),
            (3, "2026-02-26", "2026-03-22", [("VP1200STD",    100)]),
            (0, "2026-02-26", "2026-03-09", [("VP1400HD",     120)]),
            (1, "2026-02-26", "2026-03-23", [("VP900CHEP",    500)]),
            # --- Orders 30-39 ---
            (2, "2026-02-26", "2026-03-13", [("VP1200EXPORT", 300)]),
            (3, "2026-02-26", "2026-03-24", [("VP800HALF",    800)]),
            (0, "2026-02-26", None,         [("HM0800CST",     60)]),
            (1, "2026-02-26", "2026-03-25", [("HM1200EXP",     35)]),
            (2, "2026-02-27", "2026-03-14", [("DT0900DUN",    300)]),
            (3, "2026-02-27", "2026-03-26", [("CR0001CUS",      8)]),
            (0, "2026-02-27", "2026-03-06", [("VP1165743SIM", 550)]),
            (1, "2026-02-27", None,         [("VP1165743HIS", 900)]),
            (2, "2026-02-27", "2026-03-15", [("VP1100PLAIN",  400)]),
            (3, "2026-02-27", "2026-03-27", [("VP1200STD",    250)]),
            # --- Orders 40-49 (2-item orders start) ---
            (0, "2026-02-27", "2026-03-10", [("VP1400HD", 80),   ("VP900CHEP", 200)]),
            (1, "2026-02-27", "2026-03-16", [("VP1165743HIS", 400), ("HM1200EXP", 25)]),
            (2, "2026-02-27", None,         [("VP1200EXPORT", 150), ("DT0900DUN", 80)]),
            (3, "2026-02-27", "2026-03-28", [("VP800HALF", 700),  ("CR0001CUS",  6)]),
            (0, "2026-02-28", "2026-03-11", [("VP1165743SIM", 300), ("VP1100PLAIN", 200)]),
            (1, "2026-02-28", "2026-03-17", [("HM0800CST", 40),   ("HM1200EXP", 30)]),
            (2, "2026-02-28", "2026-03-12", [("DT1200GRV", 150),  ("DT0900DUN", 100)]),
            (3, "2026-02-28", None,         [("VP1200STD", 180),  ("VP1400HD",  60)]),
            (0, "2026-02-28", "2026-03-18", [("VP900CHEP", 350),  ("VP800HALF", 400)]),
            (1, "2026-02-28", "2026-03-29", [("VP1165743SIM", 600), ("VP1200EXPORT", 200)]),
            # --- Orders 50-59 ---
            (2, "2026-02-28", "2026-03-13", [("CR0001CUS", 12)]),
            (3, "2026-02-28", "2026-03-19", [("VP1165743HIS", 1000)]),
            (0, "2026-02-28", None,         [("VP1100PLAIN", 600)]),
            (1, "2026-02-28", "2026-03-20", [("VP1400HD", 90)]),
            (2, "2026-03-01", "2026-03-14", [("VP900CHEP", 450)]),
            (3, "2026-03-01", "2026-03-21", [("VP1200EXPORT", 350)]),
            (0, "2026-03-01", "2026-03-07", [("VP800HALF", 900)]),
            (1, "2026-03-01", None,         [("HM0800CST", 70)]),
            (2, "2026-03-01", "2026-03-22", [("HM1200EXP", 50)]),
            (3, "2026-03-01", "2026-03-15", [("DT0900DUN", 250)]),
            # --- Orders 60-69 ---
            (0, "2026-03-01", "2026-03-23", [("DT1200GRV", 300)]),
            (1, "2026-03-01", "2026-03-08", [("CR0001CUS",  15)]),
            (2, "2026-03-01", None,         [("VP1165743SIM", 750)]),
            (3, "2026-03-01", "2026-03-24", [("VP1165743HIS", 1200)]),
            (0, "2026-03-01", "2026-03-16", [("VP1100PLAIN", 800)]),
            (1, "2026-03-01", "2026-03-25", [("VP1200STD", 300)]),
            (2, "2026-03-01", "2026-03-09", [("VP1400HD", 150)]),
            (3, "2026-03-01", None,         [("VP900CHEP", 600)]),
            (0, "2026-03-01", "2026-03-26", [("VP1200EXPORT", 400)]),
            (1, "2026-03-01", "2026-03-17", [("VP800HALF", 1200)]),
            # --- Orders 70-79 (2-item orders) ---
            (2, "2026-03-01", "2026-03-10", [("VP1165743SIM", 500), ("VP900CHEP", 300)]),
            (3, "2026-03-01", "2026-03-27", [("VP1165743HIS", 700), ("DT0900DUN", 150)]),
            (0, "2026-03-01", None,         [("HM0800CST", 45),  ("CR0001CUS",  7)]),
            (1, "2026-03-01", "2026-03-18", [("HM1200EXP", 60),  ("HM0800CST", 35)]),
            (2, "2026-03-01", "2026-03-11", [("VP1200STD", 220), ("VP800HALF", 500)]),
            (3, "2026-03-01", "2026-03-28", [("VP1400HD", 110),  ("VP1200EXPORT", 200)]),
            (0, "2026-03-01", "2026-03-12", [("DT1200GRV", 250), ("DT0900DUN", 180)]),
            (1, "2026-03-01", None,         [("VP1165743SIM", 400), ("VP1100PLAIN", 300)]),
            (2, "2026-03-01", "2026-03-19", [("CR0001CUS", 18),  ("HM1200EXP", 45)]),
            (3, "2026-03-01", "2026-03-29", [("VP900CHEP", 700),  ("VP800HALF", 600)]),
            # --- Orders 80-89 (3-item orders) ---
            (0, "2026-03-01", "2026-03-13", [("VP1165743SIM", 300), ("VP1100PLAIN", 200), ("VP900CHEP", 150)]),
            (1, "2026-03-01", "2026-03-20", [("VP1165743HIS", 500), ("HM1200EXP", 40),   ("CR0001CUS",  5)]),
            (2, "2026-03-01", None,         [("VP1200STD", 200),  ("VP1400HD", 80),     ("VP800HALF", 400)]),
            (3, "2026-03-01", "2026-03-21", [("HM0800CST", 50),  ("HM1200EXP", 30),    ("DT0900DUN", 100)]),
            (0, "2026-03-01", "2026-03-14", [("VP1200EXPORT", 300), ("DT1200GRV", 200), ("CR0001CUS",  8)]),
            (1, "2026-03-01", "2026-03-30", [("VP1165743SIM", 600), ("VP1200STD", 180), ("VP900CHEP", 250)]),
            (2, "2026-03-01", "2026-03-15", [("VP800HALF", 800),  ("HM0800CST", 60),   ("DT0900DUN", 200)]),
            (3, "2026-03-01", None,         [("VP1165743HIS", 400), ("VP1400HD", 90),   ("VP1200EXPORT", 150)]),
            (0, "2026-03-01", "2026-03-22", [("DT0900DUN", 400),  ("DT1200GRV", 350),  ("CR0001CUS", 12)]),
            (1, "2026-03-01", "2026-03-16", [("VP1100PLAIN", 500), ("VP1200STD", 250),  ("HM1200EXP", 55)]),
            # --- Orders 90-99 (back to 1-item) ---
            (2, "2026-03-01", "2026-03-23", [("VP1165743SIM", 850)]),
            (3, "2026-03-01", "2026-03-31", [("VP1165743HIS", 1500)]),
            (0, "2026-03-01", None,         [("VP1100PLAIN", 900)]),
            (1, "2026-03-01", "2026-03-17", [("VP1200STD", 400)]),
            (2, "2026-03-01", "2026-03-24", [("VP1400HD", 200)]),
            (3, "2026-03-01", "2026-03-10", [("VP900CHEP", 750)]),
            (0, "2026-03-01", "2026-03-25", [("VP1200EXPORT", 500)]),
            (1, "2026-03-01", None,         [("VP800HALF", 2000)]),
            (2, "2026-03-01", "2026-03-18", [("HM0800CST", 100)]),
            (3, "2026-03-01", "2026-03-26", [("CR0001CUS",  20)]),
        ]

        all_items = []
        for i, (cli_idx, created_at, req_del, item_specs) in enumerate(_orders_def):
            order_number = f"ORD-{10009510 + i}"
            client_id = _clients[cli_idx]
            # Compute total_value
            total_value = sum(
                qty * skus_map[sku_code][0][1]
                for sku_code, qty in item_specs
            )
            c.execute(
                "INSERT INTO orders (order_number, client_id, status, is_verified, verified_by, "
                "total_value, notes, requested_delivery_date, eta_date, created_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                (order_number, client_id, "T", 0, None, round(total_value, 2),
                 "New order", req_del, None, created_at)
            )
            order_id = c.lastrowid
            for sku_code, qty in item_specs:
                sku_row, product_name = skus_map[sku_code]
                sku_id, sell_price, zone_id = sku_row
                all_items.append((
                    order_id, sku_id, sku_code, product_name,
                    qty, 0, sell_price, round(qty * sell_price, 2), "T", zone_id
                ))
        c.executemany(
            "INSERT INTO order_items (order_id, sku_id, sku_code, product_name, quantity, "
            "produced_quantity, unit_price, line_total, status, zone_id) VALUES (?,?,?,?,?,?,?,?,?,?)",
            all_items
        )
        conn.commit()

    # Seed station_capacity for Viking machines (idempotent)
    if c.execute("SELECT COUNT(*) FROM station_capacity").fetchone()[0] == 0:
        # Get Viking machine station IDs
        for code, max_units in [("M504", 3000), ("M505", 500), ("CHAMP", 500)]:
            row = c.execute("SELECT id FROM stations WHERE code=?", [code]).fetchone()
            if row:
                c.execute("INSERT OR IGNORE INTO station_capacity (station_id, max_units_per_day) VALUES (?,?)", [row[0], max_units])
        # Seed handmade tables
        for i in range(1, 9):
            code = f"T{i:02d}"
            row = c.execute("SELECT id FROM stations WHERE code=?", [code]).fetchone()
            if row:
                c.execute("INSERT OR IGNORE INTO station_capacity (station_id, max_units_per_day) VALUES (?,?)", [row[0], 80])
        # Seed DTL centres
        for code in ["SAW2", "SAW3", "TITAN", "DIM1", "TIGER", "NOTCH", "DIM2"]:
            row = c.execute("SELECT id FROM stations WHERE code=?", [code]).fetchone()
            if row:
                c.execute("INSERT OR IGNORE INTO station_capacity (station_id, max_units_per_day) VALUES (?,?)", [row[0], 100])
        # Seed Crates stations
        for i in range(1, 4):
            code = f"S{i:02d}"
            row = c.execute("SELECT id FROM stations WHERE code=?", [code]).fetchone()
            if row:
                c.execute("INSERT OR IGNORE INTO station_capacity (station_id, max_units_per_day) VALUES (?,?)", [row[0], 20])
        conn.commit()

    conn.close()


# ---------------------------------------------------------------------------
# Migration helper — add columns / tables to existing DBs
# ---------------------------------------------------------------------------

def migrate_db():
    """Run safe ALTER TABLE / CREATE TABLE IF NOT EXISTS migrations on existing DBs."""
    conn = get_connection()
    c = conn.cursor()
    # Add new columns to orders if they don't exist
    existing_cols = {row[1] for row in c.execute("PRAGMA table_info(orders)").fetchall()}
    if 'requested_delivery_date' not in existing_cols:
        c.execute("ALTER TABLE orders ADD COLUMN requested_delivery_date TEXT")
    if 'is_stock_run' not in existing_cols:
        c.execute("ALTER TABLE orders ADD COLUMN is_stock_run INTEGER DEFAULT 0")
    if 'previous_eta' not in existing_cols:
        c.execute("ALTER TABLE orders ADD COLUMN previous_eta TEXT")
    # Add new columns to schedule_entries
    se_cols = {row[1] for row in c.execute("PRAGMA table_info(schedule_entries)").fetchall()}
    if 'priority' not in se_cols:
        c.execute("ALTER TABLE schedule_entries ADD COLUMN priority INTEGER DEFAULT 0")
    if 'run_order' not in se_cols:
        c.execute("ALTER TABLE schedule_entries ADD COLUMN run_order INTEGER DEFAULT 0")
    # Add new columns to order_items
    oi_cols = {row[1] for row in c.execute("PRAGMA table_info(order_items)").fetchall()}
    if 'split_from_item_id' not in oi_cols:
        c.execute("ALTER TABLE order_items ADD COLUMN split_from_item_id INTEGER REFERENCES order_items(id)")
    if 'previous_eta' not in oi_cols:
        c.execute("ALTER TABLE order_items ADD COLUMN previous_eta TEXT")
    if 'eta_set_by' not in oi_cols:
        c.execute("ALTER TABLE order_items ADD COLUMN eta_set_by INTEGER REFERENCES users(id)")
    if 'eta_set_at' not in oi_cols:
        c.execute("ALTER TABLE order_items ADD COLUMN eta_set_at TIMESTAMP")
    if 'docking_completed_at' not in oi_cols:
        c.execute("ALTER TABLE order_items ADD COLUMN docking_completed_at TIMESTAMP")
    if 'requested_delivery_date' not in oi_cols:
        c.execute("ALTER TABLE order_items ADD COLUMN requested_delivery_date TEXT")
    # Add progress column to orders
    if 'progress' not in existing_cols:
        c.execute("ALTER TABLE orders ADD COLUMN progress TEXT")
    # Add new columns to trucks
    truck_cols = {row[1] for row in c.execute("PRAGMA table_info(trucks)").fetchall()}
    if 'driver_name' not in truck_cols:
        c.execute("ALTER TABLE trucks ADD COLUMN driver_name TEXT")
    if 'truck_type' not in truck_cols:
        c.execute("ALTER TABLE trucks ADD COLUMN truck_type TEXT DEFAULT 'internal'")
    # Add estimated_minutes to delivery_log for time-based capacity
    dl_cols = {row[1] for row in c.execute("PRAGMA table_info(delivery_log)").fetchall()}
    if 'estimated_minutes' not in dl_cols:
        c.execute("ALTER TABLE delivery_log ADD COLUMN estimated_minutes INTEGER DEFAULT 30")
    # Seed 7 trucks if only 3 exist
    truck_count = c.execute("SELECT COUNT(*) FROM trucks").fetchone()[0]
    if truck_count < 7:
        # Update existing trucks with driver names
        existing_trucks = c.execute("SELECT id, name FROM trucks ORDER BY id").fetchall()
        driver_map = ['Leeroy', 'Usef', 'Ronny', 'Ben', 'Marcus', 'Besher', 'Contractor']
        for i, t in enumerate(existing_trucks):
            if i < len(driver_map):
                c.execute("UPDATE trucks SET driver_name=? WHERE id=?", [driver_map[i], t[0]])
        # Add missing trucks
        for i in range(truck_count, 7):
            rego = f"TRK-{i+1:03d}"
            try:
                c.execute("INSERT INTO trucks (name, rego, driver_name, truck_type) VALUES (?,?,?,?)",
                    [f"Truck {i+1}", rego, driver_map[i] if i < len(driver_map) else f"Driver {i+1}", 'contractor' if i == 6 else 'internal'])
            except: pass
    conn.commit()
    # Create new tables
    c.executescript("""
    CREATE TABLE IF NOT EXISTS inventory (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sku_id INTEGER NOT NULL REFERENCES skus(id) UNIQUE,
        units_on_hand INTEGER DEFAULT 0,
        units_allocated INTEGER DEFAULT 0,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS station_capacity (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        station_id INTEGER NOT NULL REFERENCES stations(id),
        max_units_per_day INTEGER NOT NULL DEFAULT 3000,
        UNIQUE(station_id)
    );
    CREATE TABLE IF NOT EXISTS close_days (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        zone_id INTEGER NOT NULL REFERENCES zones(id),
        closed_date TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(zone_id, closed_date)
    );
    """)
    conn.commit()
    # Seed station capacity if missing
    if c.execute("SELECT COUNT(*) FROM station_capacity").fetchone()[0] == 0:
        for code, max_units in [("M504", 3000), ("M505", 500), ("CHAMP", 500)]:
            row = c.execute("SELECT id FROM stations WHERE code=?", [code]).fetchone()
            if row:
                c.execute("INSERT OR IGNORE INTO station_capacity (station_id, max_units_per_day) VALUES (?,?)", [row[0], max_units])
        for i in range(1, 9):
            code = f"T{i:02d}"
            row = c.execute("SELECT id FROM stations WHERE code=?", [code]).fetchone()
            if row:
                c.execute("INSERT OR IGNORE INTO station_capacity (station_id, max_units_per_day) VALUES (?,?)", [row[0], 80])
        # DTL centres
        for code in ["SAW2", "SAW3", "TITAN", "DIM1", "TIGER", "NOTCH", "DIM2"]:
            row = c.execute("SELECT id FROM stations WHERE code=?", [code]).fetchone()
            if row:
                c.execute("INSERT OR IGNORE INTO station_capacity (station_id, max_units_per_day) VALUES (?,?)", [row[0], 100])
        # Crates stations
        for i in range(1, 4):
            code = f"S{i:02d}"
            row = c.execute("SELECT id FROM stations WHERE code=?", [code]).fetchone()
            if row:
                c.execute("INSERT OR IGNORE INTO station_capacity (station_id, max_units_per_day) VALUES (?,?)", [row[0], 20])
        conn.commit()
    # Fix QA inspection_type constraint to include 'final'
    try:
        c.execute("INSERT INTO qa_inspections (order_item_id, inspection_type, batch_size, passed, inspector_id) VALUES (NULL, 'final', 0, 0, 1)")
        c.execute("DELETE FROM qa_inspections WHERE order_item_id IS NULL AND inspection_type='final' AND batch_size=0")
    except Exception:
        # Constraint doesn't allow 'final' — need to recreate table
        try:
            existing = c.execute("SELECT * FROM qa_inspections").fetchall()
            cols = [desc[0] for desc in c.description] if c.description else []
            c.execute("DROP TABLE qa_inspections")
            c.execute("""CREATE TABLE qa_inspections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_item_id INTEGER REFERENCES order_items(id),
                session_id INTEGER REFERENCES production_sessions(id),
                inspection_type TEXT CHECK(inspection_type IN ('batch','setup','random_audit','post_production','final')),
                batch_size INTEGER,
                passed INTEGER DEFAULT 1,
                inspector_id INTEGER REFERENCES users(id),
                notes TEXT,
                inspected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )""")
            if existing and cols:
                for row in existing:
                    row_dict = dict(zip(cols, row))
                    c.execute("INSERT INTO qa_inspections (id, order_item_id, session_id, inspection_type, batch_size, passed, inspector_id, notes, inspected_at) VALUES (?,?,?,?,?,?,?,?,?)",
                        [row_dict.get('id'), row_dict.get('order_item_id'), row_dict.get('session_id'), row_dict.get('inspection_type'), row_dict.get('batch_size'), row_dict.get('passed'), row_dict.get('inspector_id'), row_dict.get('notes'), row_dict.get('inspected_at')])
            conn.commit()
        except:
            pass

    # =========================================================================
    # DRIVER APP TABLES & MIGRATIONS
    # =========================================================================
    c.executescript("""
    CREATE TABLE IF NOT EXISTS driver_shifts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        driver_id INTEGER NOT NULL REFERENCES users(id),
        truck_id INTEGER NOT NULL REFERENCES trucks(id),
        shift_date TEXT NOT NULL,
        clock_on_time TIMESTAMP NOT NULL,
        clock_off_time TIMESTAMP,
        safety_acknowledged INTEGER DEFAULT 0,
        safety_acknowledged_at TIMESTAMP,
        safety_checklist TEXT,
        total_hours REAL,
        total_km REAL,
        status TEXT DEFAULT 'active' CHECK(status IN ('active','completed','abandoned')),
        notes TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS delivery_run_stages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        delivery_log_id INTEGER REFERENCES delivery_log(id),
        driver_shift_id INTEGER NOT NULL REFERENCES driver_shifts(id),
        stage TEXT NOT NULL CHECK(stage IN (
            'waiting_to_load','being_loaded','tie_down',
            'driving_to_customer','break',
            'waiting_at_customer','being_unloaded',
            'driving_return',
            'being_loaded_at_customer','being_unloaded_at_depot'
        )),
        started_at TIMESTAMP NOT NULL,
        ended_at TIMESTAMP,
        duration_minutes REAL,
        location_lat REAL,
        location_lng REAL,
        stop_number INTEGER DEFAULT 1,
        notes TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS truck_finance_config (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        truck_id INTEGER NOT NULL REFERENCES trucks(id) UNIQUE,
        driver_hourly_rate REAL DEFAULT 38.50,
        fuel_cost_per_litre REAL DEFAULT 1.85,
        avg_fuel_consumption_per_100km REAL DEFAULT 32.0,
        annual_rego_cost REAL DEFAULT 4200.0,
        annual_insurance_cost REAL DEFAULT 8500.0,
        rm_budget_monthly REAL DEFAULT 2000.0,
        tyre_cost_per_km REAL DEFAULT 0.04,
        operating_days_per_year INTEGER DEFAULT 260,
        running_cost_per_hour REAL DEFAULT 0,
        running_cost_per_km REAL DEFAULT 0,
        notes TEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS delivery_run_costs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        delivery_log_id INTEGER NOT NULL REFERENCES delivery_log(id),
        driver_shift_id INTEGER NOT NULL REFERENCES driver_shifts(id),
        driver_cost REAL DEFAULT 0,
        fuel_cost REAL DEFAULT 0,
        rego_cost REAL DEFAULT 0,
        insurance_cost REAL DEFAULT 0,
        rm_cost REAL DEFAULT 0,
        tyre_cost REAL DEFAULT 0,
        tolls REAL DEFAULT 0,
        other_costs REAL DEFAULT 0,
        total_cost REAL DEFAULT 0,
        total_km REAL,
        total_minutes REAL,
        cost_per_pallet REAL,
        calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS driver_incidents (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        driver_shift_id INTEGER NOT NULL REFERENCES driver_shifts(id),
        delivery_log_id INTEGER REFERENCES delivery_log(id),
        incident_type TEXT NOT NULL CHECK(incident_type IN ('vehicle_damage','load_damage','near_miss','injury','other')),
        description TEXT,
        photo_data TEXT,
        location_lat REAL,
        location_lng REAL,
        reported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    conn.commit()

    # --- FIX 2: Create trackmyride_config table if missing ---
    c.execute("""
        CREATE TABLE IF NOT EXISTS trackmyride_config (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_key TEXT,
            api_key TEXT,
            is_active INTEGER DEFAULT 0,
            truck_device_mapping TEXT,
            last_sync_at TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()

    # --- FIX 4: Migrate driver_incidents CHECK constraint to include new incident types ---
    try:
        c.execute("INSERT INTO driver_incidents (driver_shift_id, incident_type) VALUES (0, 'product_damage')")
        c.execute("DELETE FROM driver_incidents WHERE driver_shift_id=0 AND incident_type='product_damage'")
        conn.commit()
    except Exception:
        # Old constraint doesn't allow new types — recreate with full list
        try:
            existing_incidents = c.execute("SELECT * FROM driver_incidents").fetchall()
            inc_cols = [desc[0] for desc in c.description] if c.description else []
            c.execute("DROP TABLE driver_incidents")
            c.execute("""
                CREATE TABLE driver_incidents (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    driver_shift_id INTEGER NOT NULL REFERENCES driver_shifts(id),
                    delivery_log_id INTEGER REFERENCES delivery_log(id),
                    incident_type TEXT NOT NULL CHECK(incident_type IN ('vehicle_damage','load_damage','near_miss','injury','other','product_damage','road_incident','customer_issue','safety_concern')),
                    description TEXT,
                    photo_data TEXT,
                    location_lat REAL,
                    location_lng REAL,
                    reported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            if existing_incidents and inc_cols:
                for row in existing_incidents:
                    row_dict = dict(zip(inc_cols, row))
                    c.execute("INSERT INTO driver_incidents (id, driver_shift_id, delivery_log_id, incident_type, description, photo_data, location_lat, location_lng, reported_at) VALUES (?,?,?,?,?,?,?,?,?)",
                        [row_dict.get('id'), row_dict.get('driver_shift_id'), row_dict.get('delivery_log_id'), row_dict.get('incident_type'), row_dict.get('description'), row_dict.get('photo_data'), row_dict.get('location_lat'), row_dict.get('location_lng'), row_dict.get('reported_at')])
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"[migrate_db] driver_incidents table recreation failed: {e}")

    # --- FIX 1: Migrate users table to add 'driver' to the role CHECK constraint ---
    # SQLite can't ALTER CHECK constraints, so we recreate the table.
    try:
        # Test whether 'driver' is already allowed
        c.execute("INSERT INTO users (full_name, role) VALUES ('_test_driver_', 'driver')")
        c.execute("DELETE FROM users WHERE full_name='_test_driver_' AND role='driver'")
        conn.commit()
    except Exception:
        # 'driver' not in CHECK — recreate table with updated constraint
        try:
            c.execute("""
                CREATE TABLE IF NOT EXISTS users_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    email TEXT UNIQUE,
                    password_hash TEXT,
                    pin TEXT,
                    username TEXT UNIQUE,
                    full_name TEXT NOT NULL,
                    role TEXT NOT NULL CHECK(role IN ('executive','office','planner','production_manager','floor_worker','qa_lead','dispatch','yard','driver')),
                    default_zone_id INTEGER,
                    is_active INTEGER DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            c.execute("""
                INSERT INTO users_new (id, email, password_hash, pin, username, full_name, role, default_zone_id, is_active, created_at, updated_at)
                SELECT id, email, password_hash, pin, username, full_name,
                    CASE WHEN role = 'dispatch' AND username IN ('leeroy','usef','ronny','ben','marcus','besher') THEN 'driver' ELSE role END,
                    default_zone_id, is_active, created_at, updated_at
                FROM users
            """)
            c.execute("DROP TABLE users")
            c.execute("ALTER TABLE users_new RENAME TO users")
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"[migrate_db] users table recreation failed: {e}")

    # Add driver users if they don't exist
    driver_users = [
        ("leeroy", "Leeroy", "111111"),
        ("usef", "Usef", "222222"),
        ("ronny", "Ronny", "333333"),
        ("ben", "Ben", "444444"),
        ("marcus", "Marcus", "555555"),
        ("besher", "Besher", "666666"),
    ]
    for uname, fname, pin in driver_users:
        existing = c.execute("SELECT id FROM users WHERE username=?", [uname]).fetchone()
        if not existing:
            try:
                c.execute("INSERT INTO users (email, password_hash, pin, username, full_name, role) VALUES (?,?,?,?,?,?)",
                          [None, None, pin, uname, fname, "driver"])
            except Exception:
                pass
    conn.commit()

    # Seed truck_finance_config with defaults if empty
    if c.execute("SELECT COUNT(*) FROM truck_finance_config").fetchone()[0] == 0:
        trucks_list = c.execute("SELECT id FROM trucks WHERE is_active=1 ORDER BY id").fetchall()
        for t in trucks_list:
            try:
                c.execute("""INSERT INTO truck_finance_config
                    (truck_id, driver_hourly_rate, fuel_cost_per_litre, avg_fuel_consumption_per_100km,
                     annual_rego_cost, annual_insurance_cost, rm_budget_monthly, tyre_cost_per_km,
                     operating_days_per_year)
                    VALUES (?,38.50,1.85,32.0,4200.0,8500.0,2000.0,0.04,260)""", [t[0]])
            except Exception:
                pass
        conn.commit()

    conn.close()


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------

SECRET = "hyne_pallets_secret_2026"


def make_token(user_id, role):
    payload = json.dumps({"user_id": user_id, "role": role, "ts": datetime.now(timezone.utc).isoformat()})
    return base64.b64encode(payload.encode()).decode()


def decode_token(token):
    try:
        payload = json.loads(base64.b64decode(token.encode()).decode())
        return payload
    except Exception:
        return None


def get_current_user(conn):
    auth = request.headers.get("Authorization", "")
    token = None
    if auth.startswith("Bearer "):
        token = auth[7:]
    if not token:
        token = request.args.get("_token")
    if not token:
        return None
    payload = decode_token(token)
    if not payload:
        return None
    row = conn.execute("SELECT * FROM users WHERE id=? AND is_active=1", [payload.get("user_id")]).fetchone()
    return row_to_dict(row)


def require_auth(conn):
    user = get_current_user(conn)
    if not user:
        return None
    return user


def log_audit(conn, user_id, action, entity_type=None, entity_id=None, old_val=None, new_val=None):
    ip = request.remote_addr or ""
    conn.execute(
        "INSERT INTO audit_log (user_id, action, entity_type, entity_id, old_value, new_value, ip_address) VALUES (?,?,?,?,?,?,?)",
        [user_id, action, entity_type, entity_id,
         json.dumps(old_val, default=str) if old_val else None,
         json.dumps(new_val, default=str) if new_val else None, ip]
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Route matching helper
# ---------------------------------------------------------------------------

def match(pattern, path):
    regex = re.sub(r":([a-zA-Z_]+)", r"(?P<\1>[^/]+)", pattern)
    regex = "^" + regex + "$"
    m = re.match(regex, path)
    if m:
        return m.groupdict()
    return None


# ---------------------------------------------------------------------------
# Helper to get query params (excluding internal ones)
# ---------------------------------------------------------------------------

def query_params():
    params = {}
    for k, v in request.args.items():
        if k not in ("route", "_token"):
            params[k] = v
    return params


# ---------------------------------------------------------------------------
# Order helper
# ---------------------------------------------------------------------------

def order_full(conn, order_id):
    row = conn.execute(
        "SELECT o.*, c.company_name as client_name, c.email as client_email FROM orders o LEFT JOIN clients c ON c.id=o.client_id WHERE o.id=?",
        [order_id]
    ).fetchone()
    if not row:
        return None
    order = row_to_dict(row)
    items = rows_to_list(conn.execute(
        "SELECT oi.*, s.name as sku_name, z.name as zone_name FROM order_items oi LEFT JOIN skus s ON s.id=oi.sku_id LEFT JOIN zones z ON z.id=oi.zone_id WHERE oi.order_id=? ORDER BY oi.id",
        [order_id]
    ).fetchall())
    order["items"] = items
    # Item-level status breakdown
    breakdown = {}
    for it in items:
        s = it.get("status", "T")
        breakdown[s] = breakdown.get(s, 0) + 1
    order["item_status_breakdown"] = breakdown
    total = len(items)
    finished = sum(1 for it in items if it.get("status") in ('F', 'dispatched'))
    order["progress"] = f"{finished}/{total} items complete" if total > 0 else "0/0 items complete"
    return order


# ---------------------------------------------------------------------------
# Item-level pipeline helpers
# ---------------------------------------------------------------------------

def compute_order_status(conn, order_id):
    """Derive order status from individual order_item statuses.
    Priority: dispatched/delivered/collected (preserved as-is) > F (all F) > P (any P) > R (any R) > C (any C) > T.
    """
    # Check if order is already in a terminal delivered/collected state — preserve it
    order_row = conn.execute("SELECT status FROM orders WHERE id=?", [order_id]).fetchone()
    if not order_row:
        return 'T'
    current = order_row[0]
    if current in ('delivered', 'collected'):
        return current

    items = conn.execute("SELECT status FROM order_items WHERE order_id=?", [order_id]).fetchall()
    if not items:
        return current  # No items — don't change

    statuses = [r[0] for r in items]

    # If any item is dispatched, treat as dispatched
    if all(s in ('dispatched', 'F') for s in statuses):
        if any(s == 'dispatched' for s in statuses):
            return 'dispatched'

    # All finished
    if all(s == 'F' for s in statuses):
        return 'F'

    # Any in production
    if any(s == 'P' for s in statuses):
        return 'P'

    # Any ready
    if any(s == 'R' for s in statuses):
        return 'R'

    # Any cut-listed (docking)
    if any(s == 'C' for s in statuses):
        return 'C'

    return 'T'


def sync_order_status(conn, order_id):
    """Recompute and persist orders.status from item-level statuses.
    Also computes a progress string like '2/5 items complete'.
    """
    new_status = compute_order_status(conn, order_id)
    items = conn.execute("SELECT status FROM order_items WHERE order_id=?", [order_id]).fetchall()
    total = len(items)
    finished = sum(1 for r in items if r[0] in ('F', 'dispatched'))
    progress = f"{finished}/{total} items complete" if total > 0 else "0/0 items complete"
    conn.execute(
        "UPDATE orders SET status=?, progress=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
        [new_status, progress, order_id]
    )
    conn.commit()
    return new_status


# ---------------------------------------------------------------------------
# Static file serving
# ---------------------------------------------------------------------------

@app.route("/")
def serve_index():
    idx = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static", "index.html")
    if not os.path.exists(idx) or os.path.getsize(idx) < 100000:
        dl_url = "https://sites.pplx.app/sites/proxy/eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJwcmVmaXgiOiJ3ZWIvZGlyZWN0LWZpbGVzL2NvbXB1dGVyLzEwNDk1ZjE1LThmNmEtNDcwNy04OWQzLTVjYjA2YjdiMmU4OC9oeW5lLWh0bWwtaG9zdC8iLCJzaWQiOiIxMDQ5NWYxNS04ZjZhLTQ3MDctODlkMy01Y2IwNmI3YjJlODgiLCJleHAiOjE3NzIzOTA5NzZ9.O5s3VCN9xvvrnmbVgZzOd9rvgddMgEggKFD6VzgfbB4/web/direct-files/computer/10495f15-8f6a-4707-89d3-5cb06b7b2e88/hyne-html-host/index.html"
        try:
            urllib.request.urlretrieve(dl_url, idx)
            print(f"Downloaded index.html on first request ({os.path.getsize(idx)} bytes)")
        except Exception as e:
            print(f"Warning: download failed: {e}")
    return send_from_directory("static", "index.html")


@app.route("/<path:path>")
def serve_static(path):
    return send_from_directory("static", path)


# ---------------------------------------------------------------------------
# API Routes
# ---------------------------------------------------------------------------

@app.route("/api/<path:route>", methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"])
def api_handler(route):
    if request.method == "OPTIONS":
        return "", 204

    path = "/" + route
    # Strip trailing slashes
    if len(path) > 1:
        path = path.rstrip("/")

    method = request.method
    params = query_params()
    body = request.get_json(silent=True) or {}

    conn = get_connection()
    try:
        result = dispatch(method, path, params, body, conn)
        return jsonify(result.get("body", {})), result.get("status", 200)
    except Exception as exc:
        tb = traceback.format_exc()
        return jsonify({"error": "Internal server error", "detail": str(exc), "traceback": tb}), 500
    finally:
        conn.close()


def dispatch(method, path, params, body, conn):
    """Route dispatcher - returns dict with 'status' and 'body' keys."""

    # ----- HEALTH CHECK -----
    if method == "GET" and path == "/health":
        return {"status": 200, "body": {
            "status": "ok",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "db_ok": os.path.exists(DB_PATH)
        }}

    # ----- AUTH -----
    if method == "POST" and path == "/auth/login":
        email = body.get("email", "").strip().lower()
        password = body.get("password", "")
        if not email or not password:
            return {"status": 400, "body": {"error": "email and password required"}}
        pw_hash = hash_password(password)
        row = conn.execute("SELECT * FROM users WHERE email=? AND password_hash=? AND is_active=1", [email, pw_hash]).fetchone()
        if not row:
            return {"status": 401, "body": {"error": "Invalid credentials"}}
        user = row_to_dict(row)
        token = make_token(user["id"], user["role"])
        user.pop("password_hash", None)
        user.pop("pin", None)
        return {"status": 200, "body": {"token": token, "user": user}}

    if method == "POST" and path == "/auth/pin-login":
        username = body.get("username", "").strip().lower()
        pin = body.get("pin", "").strip()
        if not username or not pin:
            return {"status": 400, "body": {"error": "username and pin required"}}
        row = conn.execute("SELECT * FROM users WHERE username=? AND pin=? AND is_active=1", [username, pin]).fetchone()
        if not row:
            return {"status": 401, "body": {"error": "Invalid username or PIN"}}
        user = row_to_dict(row)
        token = make_token(user["id"], user["role"])
        user.pop("password_hash", None)
        user.pop("pin", None)
        return {"status": 200, "body": {"token": token, "user": user}}

    if method == "GET" and path == "/auth/me":
        user = require_auth(conn)
        if not user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        user.pop("password_hash", None)
        user.pop("pin", None)
        return {"status": 200, "body": user}

    # All routes below require auth
    current_user = get_current_user(conn)

    # ----- USERS -----
    if method == "GET" and path == "/users":
        rows = conn.execute("SELECT * FROM users ORDER BY full_name").fetchall()
        users = []
        for r in rows_to_list(rows):
            r.pop("password_hash", None)
            r.pop("pin", None)
            users.append(r)
        return {"status": 200, "body": users}

    if method == "POST" and path == "/users":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        for f in ["full_name", "role"]:
            if not body.get(f):
                return {"status": 400, "body": {"error": f"Field '{f}' is required"}}
        email = body.get("email")
        username = body.get("username")
        password = body.get("password")
        pin = body.get("pin")
        pw_hash = hash_password(password) if password else None
        try:
            cur = conn.execute("INSERT INTO users (email, password_hash, pin, username, full_name, role, default_zone_id) VALUES (?,?,?,?,?,?,?)",
                [email, pw_hash, pin, username, body["full_name"], body["role"], body.get("default_zone_id")])
            conn.commit()
            row = conn.execute("SELECT * FROM users WHERE id=?", [cur.lastrowid]).fetchone()
            user = row_to_dict(row)
            user.pop("password_hash", None)
            user.pop("pin", None)
            return {"status": 201, "body": user}
        except Exception as e:
            return {"status": 409, "body": {"error": str(e)}}

    m = match("/users/:id", path)
    if m:
        uid = int(m["id"])
        if method == "PUT":
            if not current_user:
                return {"status": 401, "body": {"error": "Authentication required"}}
            row = conn.execute("SELECT * FROM users WHERE id=?", [uid]).fetchone()
            if not row:
                return {"status": 404, "body": {"error": "User not found"}}
            fields, vals = [], []
            for f in ["email", "username", "full_name", "role", "default_zone_id", "is_active"]:
                if f in body:
                    fields.append(f"{f}=?"); vals.append(body[f])
            if "password" in body:
                fields.append("password_hash=?"); vals.append(hash_password(body["password"]))
            if "pin" in body:
                fields.append("pin=?"); vals.append(body["pin"])
            if not fields:
                return {"status": 400, "body": {"error": "No updatable fields provided"}}
            fields.append("updated_at=CURRENT_TIMESTAMP")
            vals.append(uid)
            conn.execute(f"UPDATE users SET {', '.join(fields)} WHERE id=?", vals)
            conn.commit()
            row = conn.execute("SELECT * FROM users WHERE id=?", [uid]).fetchone()
            user = row_to_dict(row)
            user.pop("password_hash", None)
            user.pop("pin", None)
            return {"status": 200, "body": user}
        if method == "DELETE":
            if not current_user:
                return {"status": 401, "body": {"error": "Authentication required"}}
            row = conn.execute("SELECT id FROM users WHERE id=?", [uid]).fetchone()
            if not row:
                return {"status": 404, "body": {"error": "User not found"}}
            conn.execute("UPDATE users SET is_active=0 WHERE id=?", [uid])
            conn.commit()
            return {"status": 200, "body": {"message": "User deactivated"}}

    # ----- ZONES -----
    if method == "GET" and path == "/zones":
        zones = rows_to_list(conn.execute("SELECT * FROM zones WHERE is_active=1 ORDER BY name").fetchall())
        for z in zones:
            z["stations"] = rows_to_list(conn.execute("SELECT * FROM stations WHERE zone_id=? AND is_active=1 ORDER BY name", [z["id"]]).fetchall())
        return {"status": 200, "body": zones}

    if method == "POST" and path == "/zones":
        for f in ["name", "code", "capacity_metric"]:
            if not body.get(f):
                return {"status": 400, "body": {"error": f"Field '{f}' is required"}}
        try:
            cur = conn.execute("INSERT INTO zones (name, code, capacity_metric) VALUES (?,?,?)", [body["name"], body["code"].upper(), body["capacity_metric"]])
            conn.commit()
            row = conn.execute("SELECT * FROM zones WHERE id=?", [cur.lastrowid]).fetchone()
            return {"status": 201, "body": row_to_dict(row)}
        except Exception as e:
            return {"status": 409, "body": {"error": str(e)}}

    m = match("/zones/:id", path)
    if m and method == "PUT":
        zid = int(m["id"])
        row = conn.execute("SELECT id FROM zones WHERE id=?", [zid]).fetchone()
        if not row:
            return {"status": 404, "body": {"error": "Zone not found"}}
        fields, vals = [], []
        for f in ["name", "code", "capacity_metric", "is_active"]:
            if f in body:
                fields.append(f"{f}=?"); vals.append(body[f] if f != "code" else body[f].upper())
        if not fields:
            return {"status": 400, "body": {"error": "No updatable fields"}}
        vals.append(zid)
        conn.execute(f"UPDATE zones SET {', '.join(fields)} WHERE id=?", vals)
        conn.commit()
        row = conn.execute("SELECT * FROM zones WHERE id=?", [zid]).fetchone()
        z = row_to_dict(row)
        z["stations"] = rows_to_list(conn.execute("SELECT * FROM stations WHERE zone_id=? AND is_active=1 ORDER BY name", [zid]).fetchall())
        return {"status": 200, "body": z}
    if m and method == "DELETE":
        zid = int(m["id"])
        conn.execute("UPDATE zones SET is_active=0 WHERE id=?", [zid])
        conn.commit()
        return {"status": 200, "body": {"message": "Zone deactivated"}}

    m = match("/zones/:id/stations", path)
    if m and method == "GET":
        zone = conn.execute("SELECT id FROM zones WHERE id=?", [int(m["id"])]).fetchone()
        if not zone:
            return {"status": 404, "body": {"error": "Zone not found"}}
        rows = conn.execute("SELECT * FROM stations WHERE zone_id=? AND is_active=1 ORDER BY name", [int(m["id"])]).fetchall()
        return {"status": 200, "body": rows_to_list(rows)}

    m = match("/zones/:id/stations", path)
    if m and method == "POST":
        zid = int(m["id"])
        zone = conn.execute("SELECT id FROM zones WHERE id=?", [zid]).fetchone()
        if not zone:
            return {"status": 404, "body": {"error": "Zone not found"}}
        for f in ["name", "station_type"]:
            if not body.get(f):
                return {"status": 400, "body": {"error": f"Field '{f}' is required"}}
        code = body.get("code") or body["name"].upper().replace(" ","")[:8]
        try:
            cur = conn.execute("INSERT INTO stations (zone_id, name, code, station_type) VALUES (?,?,?,?)",
                [zid, body["name"], code.upper(), body["station_type"]])
            conn.commit()
            row = conn.execute("SELECT * FROM stations WHERE id=?", [cur.lastrowid]).fetchone()
            return {"status": 201, "body": row_to_dict(row)}
        except Exception as e:
            return {"status": 409, "body": {"error": str(e)}}

    if method == "POST" and path == "/stations":
        for f in ["zone_id", "name", "code", "station_type"]:
            if not body.get(f):
                return {"status": 400, "body": {"error": f"Field '{f}' is required"}}
        try:
            cur = conn.execute("INSERT INTO stations (zone_id, name, code, station_type) VALUES (?,?,?,?)",
                [body["zone_id"], body["name"], body["code"].upper(), body["station_type"]])
            conn.commit()
            row = conn.execute("SELECT * FROM stations WHERE id=?", [cur.lastrowid]).fetchone()
            return {"status": 201, "body": row_to_dict(row)}
        except Exception as e:
            return {"status": 409, "body": {"error": str(e)}}

    m = match("/stations/:id", path)
    if m:
        sid = int(m["id"])
        if method == "PUT":
            row = conn.execute("SELECT id FROM stations WHERE id=?", [sid]).fetchone()
            if not row:
                return {"status": 404, "body": {"error": "Station not found"}}
            fields, vals = [], []
            for f in ["name", "station_type", "is_active"]:
                if f in body:
                    fields.append(f"{f}=?"); vals.append(body[f])
            if "code" in body:
                fields.append("code=?"); vals.append(body["code"].upper())
            if not fields:
                return {"status": 400, "body": {"error": "No updatable fields"}}
            vals.append(sid)
            conn.execute(f"UPDATE stations SET {', '.join(fields)} WHERE id=?", vals)
            conn.commit()
            row = conn.execute("SELECT * FROM stations WHERE id=?", [sid]).fetchone()
            return {"status": 200, "body": row_to_dict(row)}
        if method == "DELETE":
            conn.execute("UPDATE stations SET is_active=0 WHERE id=?", [sid])
            conn.commit()
            return {"status": 200, "body": {"message": "Station deactivated"}}

    # ----- ORDERS -----
    if method == "GET" and path == "/orders":
        status = params.get("status")
        client_id = params.get("client_id")
        where, vals = ["1=1"], []
        if status:
            where.append("o.status=?"); vals.append(status)
        if client_id:
            where.append("o.client_id=?"); vals.append(client_id)
        sql = f"SELECT o.*, c.company_name as client_name FROM orders o LEFT JOIN clients c ON c.id=o.client_id WHERE {' AND '.join(where)} ORDER BY o.created_at DESC"
        rows = conn.execute(sql, vals).fetchall()
        orders = rows_to_list(rows)
        for o in orders:
            cnt = conn.execute("SELECT COUNT(*), COALESCE(SUM(quantity),0), COALESCE(SUM(produced_quantity),0) FROM order_items WHERE order_id=?", [o["id"]]).fetchone()
            o["item_count"] = cnt[0]
            o["total_qty"] = cnt[1]
            o["total_produced"] = cnt[2]
            max_sched = conn.execute(
                "SELECT MAX(scheduled_date) FROM schedule_entries WHERE order_id=? AND status!='cancelled'",
                [o["id"]]
            ).fetchone()
            o["mfg_completion_date"] = max_sched[0] if max_sched and max_sched[0] else None
            # Item-level status breakdown
            item_rows = conn.execute(
                "SELECT status, COUNT(*) as cnt FROM order_items WHERE order_id=? GROUP BY status",
                [o["id"]]
            ).fetchall()
            breakdown = {r[0]: r[1] for r in item_rows}
            o["item_status_breakdown"] = breakdown
            # Compute progress from item statuses
            total_items = cnt[0]
            finished_items = sum(v for k, v in breakdown.items() if k in ('F', 'dispatched'))
            o["progress"] = f"{finished_items}/{total_items} items complete" if total_items > 0 else "0/0 items complete"
            # Add SKU codes for floor tablet display
            sku_rows = conn.execute(
                "SELECT DISTINCT sku_code FROM order_items WHERE order_id=? AND sku_code IS NOT NULL AND sku_code != ''",
                [o["id"]]
            ).fetchall()
            o["sku_codes"] = [r[0] for r in sku_rows]
            o["primary_sku"] = sku_rows[0][0] if sku_rows else None
        return {"status": 200, "body": orders}

    if method == "POST" and path == "/orders":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        if not body.get("order_number"):
            return {"status": 400, "body": {"error": "Field 'order_number' is required"}}
        # client_id required unless stock run
        if not body.get("client_id") and not body.get("is_stock_run"):
            return {"status": 400, "body": {"error": "Field 'client_id' is required"}}
        try:
            cur = conn.execute("INSERT INTO orders (order_number, client_id, status, special_instructions, delivery_type, notes, requested_delivery_date, is_stock_run) VALUES (?,?,?,?,?,?,?,?)",
                [body["order_number"], body.get("client_id"), body.get("status", "T"), body.get("special_instructions"), body.get("delivery_type", "delivery"), body.get("notes"),
                 body.get("requested_delivery_date"), body.get("is_stock_run", 0)])
            conn.commit()
            log_audit(conn, current_user["id"], "create_order", "orders", cur.lastrowid, None, body)
            order = order_full(conn, cur.lastrowid)
            return {"status": 201, "body": order}
        except Exception as e:
            return {"status": 409, "body": {"error": str(e)}}

    m = match("/orders/:id", path)
    if m:
        oid = int(m["id"])
        if method == "GET":
            order = order_full(conn, oid)
            if not order:
                return {"status": 404, "body": {"error": "Order not found"}}
            return {"status": 200, "body": order}
        if method == "PUT":
            if not current_user:
                return {"status": 401, "body": {"error": "Authentication required"}}
            row = conn.execute("SELECT * FROM orders WHERE id=?", [oid]).fetchone()
            if not row:
                return {"status": 404, "body": {"error": "Order not found"}}
            old = row_to_dict(row)
            fields, vals = [], []
            for f in ["client_id", "special_instructions", "delivery_type", "truck_id", "dispatch_date", "myob_invoice_number", "myob_uid", "total_value", "notes", "requested_delivery_date", "is_stock_run"]:
                if f in body:
                    fields.append(f"{f}=?"); vals.append(body[f])
            if not fields:
                return {"status": 400, "body": {"error": "No updatable fields"}}
            fields.append("updated_at=CURRENT_TIMESTAMP")
            vals.append(oid)
            conn.execute(f"UPDATE orders SET {', '.join(fields)} WHERE id=?", vals)
            conn.commit()
            log_audit(conn, current_user["id"], "update_order", "orders", oid, old, body)
            return {"status": 200, "body": order_full(conn, oid)}

    m = match("/orders/:id/verify", path)
    if m and method == "PUT":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        oid = int(m["id"])
        row = conn.execute("SELECT * FROM orders WHERE id=?", [oid]).fetchone()
        if not row:
            return {"status": 404, "body": {"error": "Order not found"}}
        # Don't downgrade orders that are already past C (Cut List/Docking)
        if row["status"] not in ('T', 'C'):
            return {"status": 400, "body": {"error": f"Order is already at status '{row['status']}' — cannot re-verify"}}
        # Set is_verified and promote all T-status items to C (enters docking pipeline)
        conn.execute("UPDATE orders SET is_verified=1, verified_by=?, verified_at=CURRENT_TIMESTAMP, updated_at=CURRENT_TIMESTAMP WHERE id=?", [current_user["id"], oid])
        conn.execute("UPDATE order_items SET status='C' WHERE order_id=? AND status='T'", [oid])
        conn.commit()
        # Sync order status from items
        sync_order_status(conn, oid)
        client = conn.execute("SELECT * FROM clients WHERE id=?", [row["client_id"]]).fetchone()
        if client and client["email"]:
            conn.execute("INSERT INTO notification_log (order_id, notification_type, recipient_email, subject, body, status) VALUES (?,?,?,?,?,?)",
                [oid, "order_acknowledgement", client["email"], f"Order {row['order_number']} Acknowledged", f"Your order {row['order_number']} has been received and verified.", "sent"])
            conn.commit()
        log_audit(conn, current_user["id"], "verify_order", "orders", oid)
        return {"status": 200, "body": order_full(conn, oid)}

    m = match("/orders/:id/docking-complete", path)
    if m and method == "PUT":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        allowed = ['planner','production_manager','floor_worker','executive','office','admin','ops_manager']
        if current_user.get("role") not in allowed:
            return {"status": 403, "body": {"error": "Permission denied — requires planner, production manager, or floor team leader"}}
        oid = int(m["id"])
        row = conn.execute("SELECT * FROM orders WHERE id=?", [oid]).fetchone()
        if not row:
            return {"status": 404, "body": {"error": "Order not found"}}
        # Batch docking: promote ALL C-status items on this order to R
        conn.execute(
            "UPDATE order_items SET status='R', docking_completed_at=CURRENT_TIMESTAMP WHERE order_id=? AND status='C'",
            [oid]
        )
        conn.commit()
        # Sync order status from items
        sync_order_status(conn, oid)
        log_audit(conn, current_user["id"], "docking_complete", "orders", oid)
        return {"status": 200, "body": order_full(conn, oid)}

    # Single-item docking complete
    m = match("/order-items/:id/docking-complete", path)
    if m and method == "PUT":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        iid = int(m["id"])
        item_row = conn.execute("SELECT * FROM order_items WHERE id=?", [iid]).fetchone()
        if not item_row:
            return {"status": 404, "body": {"error": "Order item not found"}}
        if item_row["status"] != 'C':
            return {"status": 400, "body": {"error": "Item must be in Cut List/Docking status (C) to complete docking"}}
        conn.execute(
            "UPDATE order_items SET status='R', docking_completed_at=CURRENT_TIMESTAMP WHERE id=?",
            [iid]
        )
        conn.commit()
        # Sync order status
        sync_order_status(conn, item_row["order_id"])
        log_audit(conn, current_user["id"], "item_docking_complete", "order_items", iid)
        updated = conn.execute("SELECT * FROM order_items WHERE id=?", [iid]).fetchone()
        return {"status": 200, "body": row_to_dict(updated)}

    m = match("/orders/:id/status", path)
    if m and method == "PUT":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        oid = int(m["id"])
        new_status = body.get("status")
        valid = ["T", "C", "R", "P", "F", "dispatched", "delivered", "collected"]
        if new_status not in valid:
            return {"status": 400, "body": {"error": f"Invalid status. Must be one of: {valid}"}}
        row = conn.execute("SELECT * FROM orders WHERE id=?", [oid]).fetchone()
        if not row:
            return {"status": 404, "body": {"error": "Order not found"}}
        old_status = row["status"]
        # DOCKING GATE: Block C→R via general status endpoint — must use /docking-complete
        if old_status == "C" and new_status == "R":
            return {"status": 400, "body": {"error": "Cannot promote C→R directly. Use 'Docking Complete' action instead — docking is a mandatory gate."}}
        # For terminal statuses (dispatched/delivered/collected), set directly on order
        # For pipeline statuses (P/F), also update item statuses to stay in sync
        if new_status in ('dispatched', 'delivered', 'collected'):
            conn.execute("UPDATE orders SET status=?, updated_at=CURRENT_TIMESTAMP WHERE id=?", [new_status, oid])
            if new_status == 'dispatched':
                conn.execute("UPDATE order_items SET status='dispatched' WHERE order_id=? AND status='F'", [oid])
            conn.commit()
        elif new_status == 'P':
            # Promote R items to P
            conn.execute("UPDATE order_items SET status='P' WHERE order_id=? AND status='R'", [oid])
            conn.commit()
            sync_order_status(conn, oid)
        elif new_status == 'F':
            # Promote all non-dispatched items to F
            conn.execute("UPDATE order_items SET status='F' WHERE order_id=? AND status NOT IN ('dispatched')", [oid])
            conn.commit()
            sync_order_status(conn, oid)
        else:
            conn.execute("UPDATE orders SET status=?, updated_at=CURRENT_TIMESTAMP WHERE id=?", [new_status, oid])
            conn.commit()
        log_audit(conn, current_user["id"], f"status_change_{old_status}_to_{new_status}", "orders", oid)
        return {"status": 200, "body": order_full(conn, oid)}

    m = match("/orders/:id/eta", path)
    if m and method == "PUT":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        oid = int(m["id"])
        eta = body.get("eta_date")
        if not eta:
            return {"status": 400, "body": {"error": "eta_date required"}}
        row = conn.execute("SELECT * FROM orders WHERE id=?", [oid]).fetchone()
        if not row:
            return {"status": 404, "body": {"error": "Order not found"}}
        if row["is_stock_run"]:
            return {"status": 400, "body": {"error": "Stock runs do not require ETAs"}}
        # Set ETA on order-level (backward compat) AND batch-set on items that have no eta_date
        conn.execute("UPDATE orders SET eta_date=?, eta_set_by=?, eta_set_at=CURRENT_TIMESTAMP, updated_at=CURRENT_TIMESTAMP WHERE id=?", [eta, current_user["id"], oid])
        conn.execute(
            "UPDATE order_items SET eta_date=?, eta_set_by=?, eta_set_at=CURRENT_TIMESTAMP WHERE order_id=? AND (eta_date IS NULL OR eta_date='')",
            [eta, current_user["id"], oid]
        )
        conn.commit()
        order_row = conn.execute("SELECT * FROM orders WHERE id=?", [oid]).fetchone()
        # Auto-create/update delivery_log entry so dispatch has visibility
        existing_dl = conn.execute("SELECT id FROM delivery_log WHERE order_id=?", [oid]).fetchone()
        if existing_dl:
            conn.execute("UPDATE delivery_log SET expected_date=?, updated_at=CURRENT_TIMESTAMP WHERE order_id=?", [eta, oid])
        else:
            delivery_type = order_row["delivery_type"] or "delivery"
            conn.execute("INSERT INTO delivery_log (order_id, expected_date, delivery_type, status, notes) VALUES (?,?,?,?,?)",
                [oid, eta, delivery_type, "pending", "ETA set by office — awaiting production completion"])
        conn.commit()
        client = conn.execute("SELECT * FROM clients WHERE id=?", [order_row["client_id"]]).fetchone()
        if client and client["email"]:
            conn.execute("INSERT INTO notification_log (order_id, notification_type, recipient_email, subject, body, status) VALUES (?,?,?,?,?,?)",
                [oid, "eta_notification", client["email"], f"ETA Update for Order {order_row['order_number']}", f"Your order {order_row['order_number']} ETA has been set to {eta}.", "sent"])
            conn.commit()
        return {"status": 200, "body": order_full(conn, oid)}

    # Per-item ETA endpoint
    m = match("/order-items/:id/eta", path)
    if m and method == "PUT":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        iid = int(m["id"])
        eta = body.get("eta_date")
        if not eta:
            return {"status": 400, "body": {"error": "eta_date required"}}
        item_row = conn.execute("SELECT * FROM order_items WHERE id=?", [iid]).fetchone()
        if not item_row:
            return {"status": 404, "body": {"error": "Order item not found"}}
        conn.execute(
            "UPDATE order_items SET eta_date=?, eta_set_by=?, eta_set_at=CURRENT_TIMESTAMP WHERE id=?",
            [eta, current_user["id"], iid]
        )
        conn.commit()
        updated = conn.execute("SELECT * FROM order_items WHERE id=?", [iid]).fetchone()
        return {"status": 200, "body": row_to_dict(updated)}

    # Batch ETA for order
    m = match("/orders/:id/eta-batch", path)
    if m and method == "PUT":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        oid = int(m["id"])
        # body.item_etas = [{item_id, eta_date}, ...] OR body.eta_date for blanket
        row = conn.execute("SELECT * FROM orders WHERE id=?", [oid]).fetchone()
        if not row:
            return {"status": 404, "body": {"error": "Order not found"}}
        item_etas = body.get("item_etas", [])
        blanket_eta = body.get("eta_date")
        if item_etas:
            for ie in item_etas:
                iid = ie.get("item_id")
                eta = ie.get("eta_date")
                if iid and eta:
                    conn.execute(
                        "UPDATE order_items SET eta_date=?, eta_set_by=?, eta_set_at=CURRENT_TIMESTAMP WHERE id=? AND order_id=?",
                        [eta, current_user["id"], iid, oid]
                    )
        elif blanket_eta:
            conn.execute(
                "UPDATE order_items SET eta_date=?, eta_set_by=?, eta_set_at=CURRENT_TIMESTAMP WHERE order_id=?",
                [blanket_eta, current_user["id"], oid]
            )
            conn.execute("UPDATE orders SET eta_date=?, eta_set_by=?, eta_set_at=CURRENT_TIMESTAMP, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                [blanket_eta, current_user["id"], oid])
        conn.commit()
        return {"status": 200, "body": order_full(conn, oid)}

    # ----- ORDER ITEMS -----
    m = match("/orders/:id/items", path)
    if m:
        oid = int(m["id"])
        if method == "GET":
            order = conn.execute("SELECT id FROM orders WHERE id=?", [oid]).fetchone()
            if not order:
                return {"status": 404, "body": {"error": "Order not found"}}
            rows = conn.execute("SELECT oi.*, s.name as sku_name, z.name as zone_name, st.name as station_name FROM order_items oi LEFT JOIN skus s ON s.id=oi.sku_id LEFT JOIN zones z ON z.id=oi.zone_id LEFT JOIN stations st ON st.id=oi.station_id WHERE oi.order_id=? ORDER BY oi.id", [oid]).fetchall()
            return {"status": 200, "body": rows_to_list(rows)}
        if method == "POST":
            order = conn.execute("SELECT id FROM orders WHERE id=?", [oid]).fetchone()
            if not order:
                return {"status": 404, "body": {"error": "Order not found"}}
            if not body.get("quantity"):
                return {"status": 400, "body": {"error": "quantity required"}}
            sku_id = body.get("sku_id")
            sku_code = body.get("sku_code")
            product_name = body.get("product_name")
            unit_price = body.get("unit_price", 0)
            quantity = int(body["quantity"])
            if sku_id:
                sku = conn.execute("SELECT * FROM skus WHERE id=?", [sku_id]).fetchone()
                if sku:
                    sku = row_to_dict(sku)
                    sku_code = sku_code or sku["code"]
                    product_name = product_name or sku["name"]
                    unit_price = unit_price or sku["sell_price"]
                    body.setdefault("zone_id", sku["zone_id"])
                    body.setdefault("drawing_number", sku["drawing_number"])
            line_total = quantity * float(unit_price)
            try:
                cur = conn.execute("INSERT INTO order_items (order_id, sku_id, sku_code, product_name, quantity, unit_price, line_total, zone_id, station_id, scheduled_date, eta_date, drawing_number, special_instructions) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    [oid, sku_id, sku_code, product_name, quantity, unit_price, line_total, body.get("zone_id"), body.get("station_id"), body.get("scheduled_date"), body.get("eta_date"), body.get("drawing_number"), body.get("special_instructions")])
                conn.commit()
                row = conn.execute("SELECT * FROM order_items WHERE id=?", [cur.lastrowid]).fetchone()
                return {"status": 201, "body": row_to_dict(row)}
            except Exception as e:
                return {"status": 409, "body": {"error": str(e)}}

    m = match("/order-items/:id", path)
    if m and method == "PUT":
        iid = int(m["id"])
        row = conn.execute("SELECT * FROM order_items WHERE id=?", [iid]).fetchone()
        if not row:
            return {"status": 404, "body": {"error": "Order item not found"}}
        fields, vals = [], []
        for f in ["sku_id", "sku_code", "product_name", "quantity", "produced_quantity", "unit_price", "line_total", "status", "zone_id", "station_id", "scheduled_date", "eta_date", "drawing_number", "special_instructions", "requested_delivery_date"]:
            if f in body:
                fields.append(f"{f}=?"); vals.append(body[f])
        if not fields:
            return {"status": 400, "body": {"error": "No updatable fields"}}
        vals.append(iid)
        conn.execute(f"UPDATE order_items SET {', '.join(fields)} WHERE id=?", vals)
        conn.commit()
        row = conn.execute("SELECT * FROM order_items WHERE id=?", [iid]).fetchone()
        return {"status": 200, "body": row_to_dict(row)}

    # ----- SCHEDULE -----
    if method == "GET" and path == "/schedule":
        where, vals = ["1=1"], []
        if params.get("date_from"):
            where.append("se.scheduled_date >= ?"); vals.append(params["date_from"])
        if params.get("date_to"):
            where.append("se.scheduled_date <= ?"); vals.append(params["date_to"])
        if params.get("zone_id"):
            where.append("se.zone_id=?"); vals.append(params["zone_id"])
        if params.get("date"):
            where.append("se.scheduled_date=?"); vals.append(params["date"])
        sql = f"""SELECT se.*, z.name as zone_name, z.code as zone_code, st.name as station_name, sh.name as shift_name,
                  oi.sku_code, oi.product_name, oi.quantity as item_quantity, oi.status as item_status,
                  o.order_number, o.status as order_status, o.client_id, c.company_name as client_name
                  FROM schedule_entries se LEFT JOIN zones z ON z.id=se.zone_id LEFT JOIN stations st ON st.id=se.station_id
                  LEFT JOIN shifts sh ON sh.id=se.shift_id LEFT JOIN order_items oi ON oi.id=se.order_item_id
                  LEFT JOIN orders o ON o.id=se.order_id LEFT JOIN clients c ON c.id=o.client_id
                  WHERE {' AND '.join(where)} ORDER BY se.scheduled_date, se.zone_id, se.station_id"""
        rows = conn.execute(sql, vals).fetchall()
        return {"status": 200, "body": rows_to_list(rows)}

    if method == "POST" and path == "/schedule":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        for f in ["zone_id", "scheduled_date"]:
            if not body.get(f):
                return {"status": 400, "body": {"error": f"Field '{f}' is required"}}
        order_id = body.get("order_id")
        order_item_id = body.get("order_item_id")
        # New path: order_item_id provided directly (from planning board drag)
        if order_item_id:
            item = conn.execute("SELECT * FROM order_items WHERE id=?", [order_item_id]).fetchone()
            if not item:
                return {"status": 404, "body": {"error": "Order item not found"}}
            item = row_to_dict(item)
            oid = item["order_id"]
            try:
                cur = conn.execute("INSERT INTO schedule_entries (order_id, order_item_id, zone_id, station_id, scheduled_date, planned_quantity, notes, created_by) VALUES (?,?,?,?,?,?,?,?)",
                    [oid, order_item_id, body["zone_id"], body.get("station_id"), body["scheduled_date"],
                     body.get("planned_quantity") or item["quantity"], body.get("notes"), current_user["id"]])
                # Update item schedule info — promote T→C (docking) but NOT to R
                # Docking (C→R) is a manual gate — planner/prod manager must release
                conn.execute("UPDATE order_items SET status=CASE WHEN status='T' THEN 'C' ELSE status END, station_id=?, scheduled_date=? WHERE id=? AND status IN ('T','C')",
                    [body.get("station_id"), body["scheduled_date"], order_item_id])
                conn.commit()
                # Sync order status from items (replaces direct status='C' set)
                sync_order_status(conn, oid)
                row = conn.execute("SELECT * FROM schedule_entries WHERE id=?", [cur.lastrowid]).fetchone()
                return {"status": 201, "body": row_to_dict(row)}
            except Exception as e:
                return {"status": 409, "body": {"error": str(e)}}
        if not order_id:
            return {"status": 400, "body": {"error": "order_id or order_item_id is required"}}
        try:
            items = conn.execute("SELECT * FROM order_items WHERE order_id=? AND zone_id=?", [order_id, body["zone_id"]]).fetchall()
            if not items:
                items = conn.execute("SELECT * FROM order_items WHERE order_id=?", [order_id]).fetchall()
            if not items:
                return {"status": 400, "body": {"error": "No line items found for this order"}}
            created_entries = []
            for item in items:
                cur = conn.execute("INSERT INTO schedule_entries (order_id, order_item_id, zone_id, station_id, scheduled_date, shift_id, planned_quantity, notes, created_by) VALUES (?,?,?,?,?,?,?,?,?)",
                    [order_id, item["id"], body["zone_id"], body.get("station_id"), body["scheduled_date"], body.get("shift_id"), item["quantity"] or body.get("planned_quantity"), body.get("notes"), current_user["id"]])
                created_entries.append(cur.lastrowid)
                # Update item schedule info — promote T→C (docking) but NOT to R
                # Docking (C→R) is a manual gate — planner/prod manager must release
                conn.execute("UPDATE order_items SET status=CASE WHEN status='T' THEN 'C' ELSE status END, station_id=?, scheduled_date=? WHERE id=? AND status IN ('T','C')",
                    [body.get("station_id"), body["scheduled_date"], item["id"]])
            conn.commit()
            # Sync order status from items (replaces direct status='C' set)
            sync_order_status(conn, order_id)
            if len(created_entries) == 1:
                row = conn.execute("SELECT * FROM schedule_entries WHERE id=?", [created_entries[0]]).fetchone()
                return {"status": 201, "body": row_to_dict(row)}
            else:
                rows = conn.execute(f"SELECT * FROM schedule_entries WHERE id IN ({','.join('?' * len(created_entries))})", created_entries).fetchall()
                return {"status": 201, "body": {"entries": rows_to_list(rows), "count": len(created_entries)}}
        except Exception as e:
            return {"status": 409, "body": {"error": str(e)}}

    m = match("/schedule/:id", path)
    if m:
        sid = int(m["id"])
        if method == "PUT":
            row = conn.execute("SELECT id FROM schedule_entries WHERE id=?", [sid]).fetchone()
            if not row:
                return {"status": 404, "body": {"error": "Schedule entry not found"}}
            fields, vals = [], []
            for f in ["order_item_id", "zone_id", "station_id", "scheduled_date", "shift_id", "planned_quantity", "status", "notes", "priority", "run_order"]:
                if f in body:
                    fields.append(f"{f}=?"); vals.append(body[f])
            if not fields:
                return {"status": 400, "body": {"error": "No updatable fields"}}
            fields.append("updated_at=CURRENT_TIMESTAMP")
            vals.append(sid)
            conn.execute(f"UPDATE schedule_entries SET {', '.join(fields)} WHERE id=?", vals)
            conn.commit()
            row = conn.execute("SELECT * FROM schedule_entries WHERE id=?", [sid]).fetchone()
            return {"status": 200, "body": row_to_dict(row)}
        if method == "DELETE":
            row = conn.execute("SELECT id FROM schedule_entries WHERE id=?", [sid]).fetchone()
            if not row:
                return {"status": 404, "body": {"error": "Schedule entry not found"}}
            conn.execute("DELETE FROM schedule_entries WHERE id=?", [sid])
            conn.commit()
            return {"status": 200, "body": {"message": "Schedule entry deleted"}}

    m = match("/schedule/:id/reschedule", path)
    if m and method == "PUT":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        sid = int(m["id"])
        entry = conn.execute("SELECT se.*, o.eta_date as original_eta, o.is_stock_run, o.order_number FROM schedule_entries se LEFT JOIN orders o ON o.id=se.order_id WHERE se.id=?", [sid]).fetchone()
        if not entry:
            return {"status": 404, "body": {"error": "Schedule entry not found"}}
        entry = row_to_dict(entry)
        new_date = body.get("scheduled_date")
        new_station = body.get("station_id")
        reset_eta = body.get("reset_eta", False)
        silent = body.get("silent", False)
        if not new_date:
            return {"status": 400, "body": {"error": "scheduled_date required"}}
        # Update the schedule entry
        fields = ["scheduled_date=?", "updated_at=CURRENT_TIMESTAMP"]
        vals = [new_date]
        if new_station is not None:
            fields.append("station_id=?")
            vals.append(new_station)
        vals.append(sid)
        conn.execute(f"UPDATE schedule_entries SET {', '.join(fields)} WHERE id=?", vals)
        # Also update the order_item's scheduled_date
        if entry.get("order_item_id"):
            conn.execute("UPDATE order_items SET scheduled_date=? WHERE id=?", [new_date, entry["order_item_id"]])
        # If reset_eta requested AND not a stock run, clear ETA so office sees it again
        if reset_eta and not entry.get("is_stock_run"):
            old_eta = entry.get("original_eta")
            if silent and old_eta:
                # Silently adjust ETA by same delta as schedule change
                try:
                    old_date_dt = datetime.strptime(entry["scheduled_date"], "%Y-%m-%d")
                    new_dt = datetime.strptime(new_date, "%Y-%m-%d")
                    delta = (new_dt - old_date_dt).days
                    old_eta_dt = datetime.strptime(old_eta, "%Y-%m-%d")
                    new_eta = (old_eta_dt + timedelta(days=delta)).strftime("%Y-%m-%d")
                    conn.execute("UPDATE orders SET eta_date=?, previous_eta=?, updated_at=CURRENT_TIMESTAMP WHERE id=?", [new_eta, old_eta, entry["order_id"]])
                except Exception:
                    # Fallback: just clear ETA normally
                    conn.execute("UPDATE orders SET eta_date=NULL, previous_eta=?, updated_at=CURRENT_TIMESTAMP WHERE id=?", [old_eta, entry["order_id"]])
            else:
                conn.execute("UPDATE orders SET eta_date=NULL, previous_eta=?, updated_at=CURRENT_TIMESTAMP WHERE id=?", [old_eta, entry["order_id"]])
        conn.commit()
        log_audit(conn, current_user["id"], "reschedule", "schedule_entries", sid)
        updated = conn.execute("SELECT se.*, o.eta_date, o.previous_eta, o.is_stock_run FROM schedule_entries se LEFT JOIN orders o ON o.id=se.order_id WHERE se.id=?", [sid]).fetchone()
        return {"status": 200, "body": row_to_dict(updated)}

    # ----- PRODUCTION FLOOR OVERVIEW -----
    if method == "GET" and path == "/production/floor-overview":
        zones = rows_to_list(conn.execute("SELECT * FROM zones WHERE is_active=1 ORDER BY id").fetchall())
        result = []
        for zone in zones:
            # Get stations for this zone
            stations = rows_to_list(conn.execute("SELECT * FROM stations WHERE zone_id=? AND is_active=1 ORDER BY id", [zone["id"]]).fetchall())
            zone_stations = []
            for station in stations:
                # Active/paused sessions at this station
                sessions = rows_to_list(conn.execute("""
                    SELECT ps.*, oi.sku_code, oi.product_name, oi.quantity as order_qty, oi.produced_quantity,
                           oi.drawing_number, oi.status as item_status,
                           o.order_number, o.special_instructions,
                           c.company_name as client_name
                    FROM production_sessions ps
                    LEFT JOIN order_items oi ON oi.id=ps.order_item_id
                    LEFT JOIN orders o ON o.id=oi.order_id
                    LEFT JOIN clients c ON c.id=o.client_id
                    WHERE ps.station_id=? AND ps.status IN ('active','paused')
                    ORDER BY ps.start_time DESC
                """, [station["id"]]).fetchall())
                for s in sessions:
                    s["workers"] = rows_to_list(conn.execute("""
                        SELECT sw.*, u.full_name, u.username
                        FROM session_workers sw JOIN users u ON u.id=sw.user_id
                        WHERE sw.session_id=? AND sw.is_active=1
                    """, [s["id"]]).fetchall())
                # Also get scheduled (docked) work orders for this station
                scheduled = rows_to_list(conn.execute("""
                    SELECT se.*, oi.sku_code, oi.product_name, oi.quantity, oi.drawing_number, oi.status as item_status,
                           o.order_number, c.company_name as client_name
                    FROM schedule_entries se
                    LEFT JOIN order_items oi ON oi.id=se.order_item_id
                    LEFT JOIN orders o ON o.id=se.order_id
                    LEFT JOIN clients c ON c.id=o.client_id
                    WHERE se.station_id=? AND se.status IN ('planned','in_progress')
                      AND oi.status IN ('C','R','P')
                    ORDER BY se.priority DESC, se.scheduled_date ASC
                """, [station["id"]]).fetchall())
                zone_stations.append({
                    "station": station,
                    "active_sessions": sessions,
                    "queued_work": scheduled
                })
            result.append({
                "zone": zone,
                "stations": zone_stations
            })
        return {"status": 200, "body": result}

    # ----- PRODUCTION -----
    if method == "GET" and path == "/production/sessions":
        status = params.get("status", "active")
        where, vals = ["ps.status=?"], [status]
        if params.get("zone_id"):
            where.append("ps.zone_id=?"); vals.append(params["zone_id"])
        sql = f"SELECT ps.*, z.name as zone_name, st.name as station_name, oi.sku_code, oi.product_name FROM production_sessions ps LEFT JOIN zones z ON z.id=ps.zone_id LEFT JOIN stations st ON st.id=ps.station_id LEFT JOIN order_items oi ON oi.id=ps.order_item_id WHERE {' AND '.join(where)} ORDER BY ps.start_time DESC"
        rows = conn.execute(sql, vals).fetchall()
        sessions = rows_to_list(rows)
        for s in sessions:
            s["workers"] = rows_to_list(conn.execute("SELECT sw.*, u.full_name, u.username FROM session_workers sw JOIN users u ON u.id=sw.user_id WHERE sw.session_id=? AND sw.is_active=1", [s["id"]]).fetchall())
        return {"status": 200, "body": sessions}

    if method == "POST" and path == "/production/sessions":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        for f in ["station_id", "zone_id"]:
            if not body.get(f):
                return {"status": 400, "body": {"error": f"Field '{f}' is required"}}
        try:
            cur = conn.execute("INSERT INTO production_sessions (order_item_id, station_id, zone_id, shift_id, target_quantity, is_sub_assembly, notes) VALUES (?,?,?,?,?,?,?)",
                [body.get("order_item_id"), body["station_id"], body["zone_id"], body.get("shift_id"), body.get("target_quantity"), body.get("is_sub_assembly", 0), body.get("notes")])
            session_id = cur.lastrowid
            conn.commit()
            conn.execute("INSERT INTO session_workers (session_id, user_id) VALUES (?,?)", [session_id, current_user["id"]])
            conn.commit()
            # Promote item status R→P and sync order status
            order_item_id = body.get("order_item_id")
            if order_item_id:
                item_row = conn.execute("SELECT * FROM order_items WHERE id=?", [order_item_id]).fetchone()
                if item_row and item_row["status"] == 'R':
                    conn.execute("UPDATE order_items SET status='P' WHERE id=?", [order_item_id])
                    conn.commit()
                    sync_order_status(conn, item_row["order_id"])
            row = conn.execute("SELECT * FROM production_sessions WHERE id=?", [session_id]).fetchone()
            return {"status": 201, "body": row_to_dict(row)}
        except Exception as e:
            return {"status": 409, "body": {"error": str(e)}}

    m = match("/production/sessions/:id/log", path)
    if m and method == "PUT":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        sid = int(m["id"])
        qty_change = body.get("quantity_change")
        if qty_change is None:
            return {"status": 400, "body": {"error": "quantity_change required"}}
        session = conn.execute("SELECT * FROM production_sessions WHERE id=?", [sid]).fetchone()
        if not session:
            return {"status": 404, "body": {"error": "Session not found"}}
        if session["status"] != "active":
            return {"status": 400, "body": {"error": "Session is not active"}}
        new_total = max(0, (session["produced_quantity"] or 0) + int(qty_change))
        conn.execute("INSERT INTO production_logs (session_id, user_id, quantity_change, running_total) VALUES (?,?,?,?)", [sid, current_user["id"], qty_change, new_total])
        conn.execute("UPDATE production_sessions SET produced_quantity=? WHERE id=?", [new_total, sid])
        conn.commit()
        return {"status": 200, "body": {"session_id": sid, "quantity_change": qty_change, "running_total": new_total}}

    m = match("/production/sessions/:id/pause", path)
    if m and method == "PUT":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        sid = int(m["id"])
        reason = body.get("reason")
        valid_reasons = ["material","cleaning","break","breakdown","forklift","urgent_changeover","other"]
        if reason not in valid_reasons:
            return {"status": 400, "body": {"error": f"reason must be one of: {valid_reasons}"}}
        session = conn.execute("SELECT * FROM production_sessions WHERE id=?", [sid]).fetchone()
        if not session:
            return {"status": 404, "body": {"error": "Session not found"}}
        if session["status"] != "active":
            return {"status": 400, "body": {"error": "Session is not active"}}
        conn.execute("UPDATE production_sessions SET status='paused' WHERE id=?", [sid])
        conn.execute("INSERT INTO pause_logs (session_id, reason, notes) VALUES (?,?,?)", [sid, reason, body.get("notes")])
        conn.commit()
        return {"status": 200, "body": {"session_id": sid, "status": "paused", "reason": reason}}

    m = match("/production/sessions/:id/resume", path)
    if m and method == "PUT":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        sid = int(m["id"])
        session = conn.execute("SELECT * FROM production_sessions WHERE id=?", [sid]).fetchone()
        if not session:
            return {"status": 404, "body": {"error": "Session not found"}}
        if session["status"] != "paused":
            return {"status": 400, "body": {"error": "Session is not paused"}}
        conn.execute("UPDATE pause_logs SET resumed_at=CURRENT_TIMESTAMP, duration_minutes = ROUND((julianday('now') - julianday(paused_at)) * 1440, 2) WHERE session_id=? AND resumed_at IS NULL", [sid])
        conn.execute("UPDATE production_sessions SET status='active' WHERE id=?", [sid])
        conn.commit()
        return {"status": 200, "body": {"session_id": sid, "status": "active"}}

    m = match("/production/sessions/:id/complete", path)
    if m and method == "PUT":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        sid = int(m["id"])
        session = conn.execute("SELECT * FROM production_sessions WHERE id=?", [sid]).fetchone()
        if not session:
            return {"status": 404, "body": {"error": "Session not found"}}
        final_qty = body.get("produced_quantity", session["produced_quantity"])
        force_complete = body.get("force_complete", False)
        conn.execute("UPDATE production_sessions SET status='completed', end_time=CURRENT_TIMESTAMP, produced_quantity=? WHERE id=?", [final_qty, sid])
        conn.execute("UPDATE session_workers SET scan_off_time=CURRENT_TIMESTAMP, is_active=0 WHERE session_id=? AND is_active=1", [sid])
        if session["order_item_id"]:
            qty_delta = final_qty - (session["produced_quantity"] or 0)
            conn.execute("UPDATE order_items SET produced_quantity=produced_quantity+? WHERE id=?", [qty_delta, session["order_item_id"]])
            conn.commit()
            # QA is the gate that releases to dispatch
            # Create QA inspection when target is met OR when force-completed by authorized user
            item_row = conn.execute("SELECT * FROM order_items WHERE id=?", [session["order_item_id"]]).fetchone()
            target_met = item_row and item_row["produced_quantity"] >= item_row["quantity"]
            should_qa = (target_met or force_complete) and item_row and item_row["status"] not in ('F', 'dispatched')
            if should_qa:
                note = 'Auto-created: production target met' if target_met else f'Force-completed at {item_row["produced_quantity"]}/{item_row["quantity"]} units'
                conn.execute(
                    "INSERT INTO qa_inspections (order_item_id, session_id, inspection_type, batch_size, passed, inspector_id, notes) VALUES (?,?,?,?,?,?,?)",
                    [session["order_item_id"], sid, 'final', item_row["produced_quantity"], 0, None, note]
                )
                conn.commit()
            else:
                conn.commit()
        else:
            conn.commit()
        log_audit(conn, current_user["id"], "complete_session", "production_sessions", sid)
        row = conn.execute("SELECT * FROM production_sessions WHERE id=?", [sid]).fetchone()
        return {"status": 200, "body": row_to_dict(row)}

    m = match("/production/sessions/:id/workers", path)
    if m and method == "POST":
        sid = int(m["id"])
        user_id = body.get("user_id")
        if not user_id:
            return {"status": 400, "body": {"error": "user_id required"}}
        session = conn.execute("SELECT id FROM production_sessions WHERE id=?", [sid]).fetchone()
        if not session:
            return {"status": 404, "body": {"error": "Session not found"}}
        user = conn.execute("SELECT id FROM users WHERE id=?", [user_id]).fetchone()
        if not user:
            return {"status": 404, "body": {"error": "User not found"}}
        try:
            cur = conn.execute("INSERT INTO session_workers (session_id, user_id) VALUES (?,?)", [sid, user_id])
            conn.commit()
            row = conn.execute("SELECT * FROM session_workers WHERE id=?", [cur.lastrowid]).fetchone()
            return {"status": 201, "body": row_to_dict(row)}
        except Exception as e:
            return {"status": 409, "body": {"error": str(e)}}

    # ----- SETUP -----
    if method == "POST" and path == "/setup":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        for f in ["station_id", "setup_type"]:
            if not body.get(f):
                return {"status": 400, "body": {"error": f"Field '{f}' is required"}}
        try:
            cur = conn.execute("INSERT INTO setup_logs (station_id, order_item_id, setup_type, notes) VALUES (?,?,?,?)",
                [body["station_id"], body.get("order_item_id"), body["setup_type"], body.get("notes")])
            conn.commit()
            row = conn.execute("SELECT * FROM setup_logs WHERE id=?", [cur.lastrowid]).fetchone()
            return {"status": 201, "body": row_to_dict(row)}
        except Exception as e:
            return {"status": 409, "body": {"error": str(e)}}

    m = match("/setup/:id/complete", path)
    if m and method == "PUT":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        sid = int(m["id"])
        row = conn.execute("SELECT * FROM setup_logs WHERE id=?", [sid]).fetchone()
        if not row:
            return {"status": 404, "body": {"error": "Setup log not found"}}
        qa_passed = body.get("qa_checklist_passed", 0)
        conn.execute("UPDATE setup_logs SET completed_at=CURRENT_TIMESTAMP, duration_minutes=ROUND((julianday('now')-julianday(started_at))*1440,2), qa_checklist_passed=?, team_leader_id=?, team_leader_signed_at=CURRENT_TIMESTAMP WHERE id=?",
            [qa_passed, current_user["id"], sid])
        conn.commit()
        row = conn.execute("SELECT * FROM setup_logs WHERE id=?", [sid]).fetchone()
        return {"status": 200, "body": row_to_dict(row)}

    # ----- QA -----
    if method == "GET" and path == "/qa/inspections":
        where, vals = ["1=1"], []
        if params.get("order_item_id"):
            where.append("qi.order_item_id=?"); vals.append(params["order_item_id"])
        if params.get("inspector_id"):
            where.append("qi.inspector_id=?"); vals.append(params["inspector_id"])
        if params.get("passed") is not None and params.get("passed") != '':
            where.append("qi.passed=?"); vals.append(int(params["passed"]))
        rows = conn.execute(f"""
            SELECT qi.*, u.full_name as inspector_name,
                   oi.sku_code, oi.product_name, oi.quantity as item_quantity, oi.produced_quantity, oi.status as item_status, oi.drawing_number,
                   o.order_number, o.id as order_id, o.status as order_status,
                   c.company_name as client_name
            FROM qa_inspections qi
            LEFT JOIN users u ON u.id=qi.inspector_id
            LEFT JOIN order_items oi ON oi.id=qi.order_item_id
            LEFT JOIN orders o ON o.id=oi.order_id
            LEFT JOIN clients c ON c.id=o.client_id
            WHERE {' AND '.join(where)}
            ORDER BY qi.inspected_at DESC, qi.id DESC
        """, vals).fetchall()
        return {"status": 200, "body": rows_to_list(rows)}

    if method == "POST" and path == "/qa/inspections":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        try:
            cur = conn.execute("INSERT INTO qa_inspections (order_item_id, session_id, inspection_type, batch_size, passed, inspector_id, notes) VALUES (?,?,?,?,?,?,?)",
                [body.get("order_item_id"), body.get("session_id"), body.get("inspection_type", "batch"), body.get("batch_size"), body.get("passed"), body.get("inspector_id", current_user["id"]), body.get("notes")])
            conn.commit()
            row = conn.execute("SELECT * FROM qa_inspections WHERE id=?", [cur.lastrowid]).fetchone()
            return {"status": 201, "body": row_to_dict(row)}
        except Exception as e:
            return {"status": 409, "body": {"error": str(e)}}

    m = match("/qa/inspections/:id/defects", path)
    if m and method == "POST":
        iid = int(m["id"])
        insp = conn.execute("SELECT id FROM qa_inspections WHERE id=?", [iid]).fetchone()
        if not insp:
            return {"status": 404, "body": {"error": "Inspection not found"}}
        for f in ["defect_type", "quantity"]:
            if not body.get(f):
                return {"status": 400, "body": {"error": f"Field '{f}' is required"}}
        cur = conn.execute("INSERT INTO qa_defects (inspection_id, defect_type, quantity, description) VALUES (?,?,?,?)",
            [iid, body["defect_type"], body["quantity"], body.get("description")])
        conn.commit()
        row = conn.execute("SELECT * FROM qa_defects WHERE id=?", [cur.lastrowid]).fetchone()
        return {"status": 201, "body": row_to_dict(row)}

    m = match("/qa/inspections/:id/approve", path)
    if m and method == "PUT":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        iid = int(m["id"])
        insp = conn.execute("SELECT * FROM qa_inspections WHERE id=?", [iid]).fetchone()
        if not insp:
            return {"status": 404, "body": {"error": "Inspection not found"}}
        conn.execute("UPDATE qa_inspections SET passed=1, inspector_id=?, inspected_at=CURRENT_TIMESTAMP WHERE id=?", [current_user["id"], iid])
        conn.commit()
        # QA approval is the gate: promote item P→F and sync order status
        if insp["order_item_id"]:
            item = conn.execute("SELECT * FROM order_items WHERE id=?", [insp["order_item_id"]]).fetchone()
            if item and item["status"] not in ('F', 'dispatched'):
                conn.execute("UPDATE order_items SET status='F' WHERE id=?", [insp["order_item_id"]])
                conn.commit()
                sync_order_status(conn, item["order_id"])
        row = conn.execute("SELECT * FROM qa_inspections WHERE id=?", [iid]).fetchone()
        return {"status": 200, "body": row_to_dict(row)}

    # ----- DISPATCH -----
    if method == "GET" and path == "/dispatch":
        date = params.get("date", datetime.now().strftime("%Y-%m-%d"))
        deliveries = rows_to_list(conn.execute("SELECT dl.*, o.order_number, c.company_name as client_name, t.name as truck_name FROM delivery_log dl LEFT JOIN orders o ON o.id=dl.order_id LEFT JOIN clients c ON c.id=o.client_id LEFT JOIN trucks t ON t.id=dl.truck_id WHERE dl.expected_date=? ORDER BY dl.load_sequence, dl.id", [date]).fetchall())
        # Collections: orders that are delivery_type='collection' AND have at least one item with status='F'
        collections_raw = rows_to_list(conn.execute("""
            SELECT DISTINCT o.*, c.company_name as client_name
            FROM orders o
            LEFT JOIN clients c ON c.id=o.client_id
            WHERE o.delivery_type='collection'
              AND o.status NOT IN ('collected','dispatched','delivered')
              AND EXISTS (
                  SELECT 1 FROM order_items oi
                  WHERE oi.order_id=o.id AND oi.status='F'
              )
            ORDER BY o.updated_at DESC
        """).fetchall())
        # Attach item_status_breakdown and progress to collections
        for o in collections_raw:
            item_rows = conn.execute(
                "SELECT status, COUNT(*) as cnt FROM order_items WHERE order_id=? GROUP BY status",
                [o["id"]]).fetchall()
            breakdown = {r["status"]: r["cnt"] for r in item_rows}
            total = sum(breakdown.values())
            done = breakdown.get('F', 0) + breakdown.get('dispatched', 0)
            o["item_status_breakdown"] = breakdown
            o["progress"] = f"{done}/{total}" if total else "0/0"
        collections = collections_raw
        # Incoming production: orders with any item having eta_date set, still in pipeline
        incoming_raw = rows_to_list(conn.execute("""
            SELECT DISTINCT o.*, c.company_name as client_name,
                   (SELECT MAX(se.scheduled_date) FROM schedule_entries se WHERE se.order_id=o.id AND se.status!='cancelled') as mfg_completion_date
            FROM orders o
            LEFT JOIN clients c ON c.id=o.client_id
            WHERE o.status NOT IN ('dispatched','delivered','collected')
              AND (
                  o.eta_date IS NOT NULL
                  OR EXISTS (
                      SELECT 1 FROM order_items oi
                      WHERE oi.order_id=o.id AND oi.eta_date IS NOT NULL
                  )
              )
            ORDER BY o.eta_date ASC
        """).fetchall())
        # Add item counts and item_status_breakdown for incoming production
        for o in incoming_raw:
            cnt = conn.execute("SELECT COUNT(*), COALESCE(SUM(quantity),0) FROM order_items WHERE order_id=?", [o["id"]]).fetchone()
            o["item_count"] = cnt[0]
            o["total_qty"] = cnt[1]
            item_rows = conn.execute(
                "SELECT status, COUNT(*) as cnt FROM order_items WHERE order_id=? GROUP BY status",
                [o["id"]]).fetchall()
            breakdown = {r["status"]: r["cnt"] for r in item_rows}
            total = sum(breakdown.values())
            done = breakdown.get('F', 0) + breakdown.get('dispatched', 0)
            o["item_status_breakdown"] = breakdown
            o["progress"] = f"{done}/{total}" if total else "0/0"
        incoming = incoming_raw
        return {"status": 200, "body": {"date": date, "deliveries": deliveries, "collections": collections, "incoming_production": incoming}}

    if method == "GET" and path == "/delivery-log":
        where, vals = ["1=1"], []
        if params.get("order_id"):
            where.append("dl.order_id=?"); vals.append(params["order_id"])
        if params.get("status"):
            where.append("dl.status=?"); vals.append(params["status"])
        rows = conn.execute(f"SELECT dl.*, o.order_number, c.company_name, t.name as truck_name FROM delivery_log dl LEFT JOIN orders o ON o.id=dl.order_id LEFT JOIN clients c ON c.id=o.client_id LEFT JOIN trucks t ON t.id=dl.truck_id WHERE {' AND '.join(where)} ORDER BY dl.expected_date DESC, dl.load_sequence", vals).fetchall()
        return {"status": 200, "body": rows_to_list(rows)}

    if method == "POST" and path == "/delivery-log":
        if not body.get("order_id"):
            return {"status": 400, "body": {"error": "order_id required"}}
        try:
            cur = conn.execute("INSERT INTO delivery_log (order_id, expected_date, truck_id, delivery_type, load_sequence, notes) VALUES (?,?,?,?,?,?)",
                [body["order_id"], body.get("expected_date"), body.get("truck_id"), body.get("delivery_type", "delivery"), body.get("load_sequence"), body.get("notes")])
            conn.commit()
            row = conn.execute("SELECT * FROM delivery_log WHERE id=?", [cur.lastrowid]).fetchone()
            return {"status": 201, "body": row_to_dict(row)}
        except Exception as e:
            return {"status": 409, "body": {"error": str(e)}}

    m = match("/delivery-log/:id", path)
    if m and method == "PUT":
        lid = int(m["id"])
        row = conn.execute("SELECT id FROM delivery_log WHERE id=?", [lid]).fetchone()
        if not row:
            return {"status": 404, "body": {"error": "Delivery log entry not found"}}
        fields, vals = [], []
        for f in ["expected_date", "actual_date", "truck_id", "delivery_type", "status", "load_sequence", "notes"]:
            if f in body:
                fields.append(f"{f}=?"); vals.append(body[f])
        if not fields:
            return {"status": 400, "body": {"error": "No updatable fields"}}
        fields.append("updated_at=CURRENT_TIMESTAMP")
        vals.append(lid)
        conn.execute(f"UPDATE delivery_log SET {', '.join(fields)} WHERE id=?", vals)
        conn.commit()
        row = conn.execute("SELECT * FROM delivery_log WHERE id=?", [lid]).fetchone()
        return {"status": 200, "body": row_to_dict(row)}

    # ----- TRUCKS -----
    if method == "GET" and path == "/trucks":
        rows = conn.execute("SELECT * FROM trucks WHERE is_active=1 ORDER BY id").fetchall()
        result = rows_to_list(rows)
        for t in result:
            caps = rows_to_list(conn.execute("SELECT * FROM truck_capacity_config WHERE truck_id=? ORDER BY day_of_week", [t["id"]]).fetchall())
            t["capacity_config"] = {str(c["day_of_week"]): c for c in caps}
        return {"status": 200, "body": result}

    # ----- DISPATCH PLANNING (date-range, truck-based) -----
    if method == "GET" and path == "/dispatch-planning":
        date_from = params.get("date_from", datetime.now().strftime("%Y-%m-%d"))
        date_to = params.get("date_to", date_from)
        truck_id = params.get("truck_id")  # optional filter

        # Get all trucks
        all_trucks = rows_to_list(conn.execute("SELECT * FROM trucks WHERE is_active=1 ORDER BY id").fetchall())

        # Attach capacity config to each truck
        for t in all_trucks:
            caps = rows_to_list(conn.execute("SELECT * FROM truck_capacity_config WHERE truck_id=? ORDER BY day_of_week", [t["id"]]).fetchall())
            t["capacity_config"] = {c["day_of_week"]: c for c in caps}

        # Get delivery_log entries in date range
        dl_where = ["dl.expected_date >= ?", "dl.expected_date <= ?"]
        dl_vals = [date_from, date_to]
        if truck_id:
            dl_where.append("dl.truck_id = ?")
            dl_vals.append(int(truck_id))

        deliveries = rows_to_list(conn.execute(f"""
            SELECT dl.*, o.order_number, o.delivery_type as order_delivery_type, o.special_instructions,
                   o.requested_delivery_date, o.eta_date as order_eta,
                   c.company_name as client_name, c.address as client_address, c.phone as client_phone,
                   t.name as truck_name, t.driver_name, t.rego as truck_rego
            FROM delivery_log dl
            LEFT JOIN orders o ON o.id=dl.order_id
            LEFT JOIN clients c ON c.id=o.client_id
            LEFT JOIN trucks t ON t.id=dl.truck_id
            WHERE {' AND '.join(dl_where)}
            ORDER BY dl.expected_date, dl.load_sequence, dl.id
        """, dl_vals).fetchall())

        # Attach item info to each delivery
        for d in deliveries:
            if d.get("order_id"):
                items = rows_to_list(conn.execute("""
                    SELECT oi.id, oi.sku_code, oi.product_name, oi.quantity, oi.produced_quantity, oi.status,
                           oi.eta_date as item_eta
                    FROM order_items oi WHERE oi.order_id=?
                """, [d["order_id"]]).fetchall())
                d["items"] = items
                total = len(items)
                done = sum(1 for i in items if i["status"] in ('F', 'dispatched'))
                d["progress"] = f"{done}/{total}"
                d["all_finished"] = done == total
            else:
                d["items"] = []
                d["progress"] = "0/0"
                d["all_finished"] = False
            # Attach contractor assignments
            d["contractor_assignments"] = rows_to_list(conn.execute(
                "SELECT * FROM contractor_assignments WHERE delivery_log_id=? AND status!='cancelled'",
                [d["id"]]).fetchall()) if d.get("id") else []

        # Collections in date range (delivery_type='collection', expected_date in range)
        col_where = ["1=1"]
        col_vals = []
        collections_raw = rows_to_list(conn.execute(f"""
            SELECT DISTINCT o.*, c.company_name as client_name, c.address as client_address, c.phone as client_phone,
                   dl.id as dl_id, dl.expected_date, dl.status as dl_status
            FROM orders o
            LEFT JOIN clients c ON c.id=o.client_id
            LEFT JOIN delivery_log dl ON dl.order_id=o.id
            WHERE o.delivery_type='collection'
              AND o.status NOT IN ('collected','delivered')
              AND (
                  (dl.expected_date >= ? AND dl.expected_date <= ?)
                  OR (dl.expected_date IS NULL AND EXISTS (
                      SELECT 1 FROM order_items oi WHERE oi.order_id=o.id AND oi.status='F'
                  ))
              )
            ORDER BY COALESCE(dl.expected_date, '9999-12-31'), o.updated_at DESC
        """, [date_from, date_to]).fetchall())

        for o in collections_raw:
            item_rows = conn.execute(
                "SELECT status, COUNT(*) as cnt FROM order_items WHERE order_id=? GROUP BY status",
                [o["id"]]).fetchall()
            breakdown = {r["status"]: r["cnt"] for r in item_rows}
            total = sum(breakdown.values())
            done = breakdown.get('F', 0) + breakdown.get('dispatched', 0)
            o["progress"] = f"{done}/{total}" if total else "0/0"
            o["all_finished"] = done == total
            # Attach item list with SKU codes for collection cards
            o["items"] = rows_to_list(conn.execute(
                "SELECT id, sku_code, product_name, quantity, status FROM order_items WHERE order_id=?",
                [o["id"]]).fetchall())

        # Unassigned deliveries (no truck, but in date range)
        unassigned = [d for d in deliveries if not d.get("truck_id")]
        assigned = [d for d in deliveries if d.get("truck_id")]

        # Incoming production: orders with ETA in date range, not yet all finished
        incoming_raw = rows_to_list(conn.execute("""
            SELECT DISTINCT o.*, c.company_name as client_name,
                   o.eta_date,
                   (SELECT MAX(se.scheduled_date) FROM schedule_entries se WHERE se.order_id=o.id AND se.status!='cancelled') as mfg_completion_date
            FROM orders o
            LEFT JOIN clients c ON c.id=o.client_id
            WHERE o.status NOT IN ('dispatched','delivered','collected','F')
              AND o.eta_date IS NOT NULL
              AND o.eta_date >= ? AND o.eta_date <= ?
            ORDER BY o.eta_date ASC
        """, [date_from, date_to]).fetchall())

        for o in incoming_raw:
            cnt = conn.execute("SELECT COUNT(*), COALESCE(SUM(quantity),0) FROM order_items WHERE order_id=?", [o["id"]]).fetchone()
            o["item_count"] = cnt[0]
            o["total_qty"] = cnt[1]
            item_rows = conn.execute(
                "SELECT status, COUNT(*) as cnt FROM order_items WHERE order_id=? GROUP BY status",
                [o["id"]]).fetchall()
            breakdown = {r["status"]: r["cnt"] for r in item_rows}
            total = sum(breakdown.values())
            done = breakdown.get('F', 0) + breakdown.get('dispatched', 0)
            o["progress"] = f"{done}/{total}" if total else "0/0"

        # Build day-by-day structure
        from datetime import date as date_type
        d_from = datetime.strptime(date_from, "%Y-%m-%d").date()
        d_to = datetime.strptime(date_to, "%Y-%m-%d").date()
        days = []
        current = d_from
        while current <= d_to:
            ds = current.strftime("%Y-%m-%d")
            day_deliveries = [d for d in assigned if d.get("expected_date") == ds]
            day_collections = [c for c in collections_raw if c.get("expected_date") == ds]
            day_incoming = [i for i in incoming_raw if i.get("eta_date") == ds]
            # Group deliveries by truck
            truck_slots = {}
            for t in all_trucks:
                truck_entries = [d for d in day_deliveries if d.get("truck_id") == t["id"]]
                truck_slots[t["id"]] = {
                    "truck": t,
                    "entries": sorted(truck_entries, key=lambda x: x.get("load_sequence") or 999)
                }
            # Get truck work orders for this day
            day_twos = rows_to_list(conn.execute(
                "SELECT * FROM truck_work_orders WHERE scheduled_date=? AND status!='cancelled' ORDER BY priority DESC, id",
                [ds]).fetchall())
            # Calculate capacity per truck for this day
            dow = datetime.strptime(ds, "%Y-%m-%d").weekday()  # 0=Mon
            for tid, slot in truck_slots.items():
                delivery_mins = sum(e.get("estimated_minutes") or 30 for e in slot["entries"])
                truck_wo_mins = sum(tw.get("estimated_minutes") or 60 for tw in day_twos if tw["truck_id"] == tid)
                slot["truck_work_orders"] = [tw for tw in day_twos if tw["truck_id"] == tid]
                total_mins = delivery_mins + truck_wo_mins
                cap_config = slot["truck"].get("capacity_config", {}).get(dow)
                cap = cap_config["capacity_minutes"] if cap_config else 480
                ot = cap_config["overtime_minutes"] if cap_config else 120
                slot["capacity"] = {
                    "scheduled_minutes": total_mins,
                    "capacity_minutes": cap,
                    "overtime_minutes": ot,
                    "remaining_minutes": cap - total_mins,
                    "is_over_capacity": total_mins > cap,
                    "is_overtime": total_mins > cap and total_mins <= cap + ot,
                    "is_exceeded": total_mins > cap + ot,
                }
            days.append({
                "date": ds,
                "day_label": datetime.strptime(ds, "%Y-%m-%d").strftime("%a %d %b"),
                "truck_slots": truck_slots,
                "collections": day_collections,
                "incoming": day_incoming
            })
            current += timedelta(days=1)

        return {"status": 200, "body": {
            "date_from": date_from,
            "date_to": date_to,
            "trucks": all_trucks,
            "days": days,
            "unassigned": unassigned,
            "collections_unscheduled": [c for c in collections_raw if not c.get("expected_date")],
        }}

    # ----- DISPATCH RUN SHEET (all trucks, all days, load order) -----
    if method == "GET" and path == "/dispatch-runsheet":
        date_from = params.get("date_from", datetime.now().strftime("%Y-%m-%d"))
        date_to = params.get("date_to", date_from)

        all_trucks = rows_to_list(conn.execute("SELECT * FROM trucks WHERE is_active=1 ORDER BY id").fetchall())

        deliveries = rows_to_list(conn.execute("""
            SELECT dl.*, o.order_number, o.special_instructions,
                   c.company_name as client_name, c.address as client_address, c.phone as client_phone,
                   t.name as truck_name, t.driver_name, t.rego as truck_rego
            FROM delivery_log dl
            LEFT JOIN orders o ON o.id=dl.order_id
            LEFT JOIN clients c ON c.id=o.client_id
            LEFT JOIN trucks t ON t.id=dl.truck_id
            WHERE dl.expected_date >= ? AND dl.expected_date <= ? AND dl.truck_id IS NOT NULL
            ORDER BY dl.expected_date, t.id, dl.load_sequence, dl.id
        """, [date_from, date_to]).fetchall())

        # Attach items to each delivery
        for d in deliveries:
            if d.get("order_id"):
                items = rows_to_list(conn.execute("""
                    SELECT oi.sku_code, oi.product_name, oi.quantity, oi.status
                    FROM order_items oi WHERE oi.order_id=?
                """, [d["order_id"]]).fetchall())
                d["items"] = items

        # Build structure: days → trucks → deliveries
        d_from = datetime.strptime(date_from, "%Y-%m-%d").date()
        d_to = datetime.strptime(date_to, "%Y-%m-%d").date()
        days = []
        current = d_from
        while current <= d_to:
            ds = current.strftime("%Y-%m-%d")
            day_label = datetime.strptime(ds, "%Y-%m-%d").strftime("%A %d %b")
            truck_runs = []
            for t in all_trucks:
                truck_deliveries = [d for d in deliveries if d.get("expected_date") == ds and d.get("truck_id") == t["id"]]
                if truck_deliveries:
                    truck_runs.append({
                        "truck": t,
                        "deliveries": sorted(truck_deliveries, key=lambda x: x.get("load_sequence") or 999)
                    })
            days.append({
                "date": ds,
                "day_label": day_label,
                "truck_runs": truck_runs
            })
            current += timedelta(days=1)

        return {"status": 200, "body": {
            "date_from": date_from,
            "date_to": date_to,
            "trucks": all_trucks,
            "days": days
        }}

    # ----- Assign truck to delivery_log entry -----
    if method == "PUT" and path == "/dispatch-assign":
        dl_id = body.get("delivery_log_id")
        truck_id = body.get("truck_id")
        load_seq = body.get("load_sequence")
        expected_date = body.get("expected_date")
        if not dl_id:
            return {"status": 400, "body": {"error": "delivery_log_id required"}}
        fields, vals = [], []
        if truck_id is not None:
            fields.append("truck_id=?"); vals.append(truck_id if truck_id else None)
        if load_seq is not None:
            fields.append("load_sequence=?"); vals.append(load_seq)
        if expected_date:
            fields.append("expected_date=?"); vals.append(expected_date)
        if fields:
            fields.append("updated_at=CURRENT_TIMESTAMP")
            vals.append(int(dl_id))
            conn.execute(f"UPDATE delivery_log SET {', '.join(fields)} WHERE id=?", vals)
            conn.commit()
        row = conn.execute("""
            SELECT dl.*, o.order_number, c.company_name as client_name, t.name as truck_name, t.driver_name
            FROM delivery_log dl
            LEFT JOIN orders o ON o.id=dl.order_id
            LEFT JOIN clients c ON c.id=o.client_id
            LEFT JOIN trucks t ON t.id=dl.truck_id
            WHERE dl.id=?
        """, [int(dl_id)]).fetchone()
        return {"status": 200, "body": row_to_dict(row)}

    # ----- TRUCK WORK ORDERS -----
    if method == "GET" and path == "/truck-work-orders":
        where, vals = ["1=1"], []
        if params.get("truck_id"):
            where.append("truck_id=?"); vals.append(int(params["truck_id"]))
        if params.get("status"):
            where.append("status=?"); vals.append(params["status"])
        if params.get("date_from"):
            where.append("scheduled_date>=?"); vals.append(params["date_from"])
        if params.get("date_to"):
            where.append("scheduled_date<=?"); vals.append(params["date_to"])
        rows = conn.execute(f"SELECT * FROM truck_work_orders WHERE {' AND '.join(where)} ORDER BY scheduled_date, priority DESC, id", vals).fetchall()
        return {"status": 200, "body": rows_to_list(rows)}

    if method == "POST" and path == "/truck-work-orders":
        if not body.get("truck_id") or not body.get("wo_type") or not body.get("title"):
            return {"status": 400, "body": {"error": "truck_id, wo_type, title required"}}
        user_id = body.get("user_id")
        cur = conn.execute(
            "INSERT INTO truck_work_orders (truck_id, wo_type, title, description, scheduled_date, estimated_minutes, priority, created_by) VALUES (?,?,?,?,?,?,?,?)",
            [body["truck_id"], body["wo_type"], body["title"], body.get("description"),
             body.get("scheduled_date"), body.get("estimated_minutes", 60),
             body.get("priority", "normal"), user_id])
        conn.commit()
        row = conn.execute("SELECT * FROM truck_work_orders WHERE id=?", [cur.lastrowid]).fetchone()
        return {"status": 201, "body": row_to_dict(row)}

    m = match("/truck-work-orders/:id", path)
    if m:
        two_id = int(m["id"])
        if method == "PUT":
            allowed = ["wo_type", "title", "description", "scheduled_date", "estimated_minutes",
                       "status", "priority", "completed_at", "truck_id"]
            fields, vals = [], []
            for f in allowed:
                if f in body:
                    fields.append(f"{f}=?"); vals.append(body[f])
            if body.get("status") == "completed" and "completed_at" not in body:
                fields.append("completed_at=CURRENT_TIMESTAMP")
            if fields:
                fields.append("updated_at=CURRENT_TIMESTAMP")
                vals.append(two_id)
                conn.execute(f"UPDATE truck_work_orders SET {', '.join(fields)} WHERE id=?", vals)
                conn.commit()
            row = conn.execute("SELECT * FROM truck_work_orders WHERE id=?", [two_id]).fetchone()
            return {"status": 200, "body": row_to_dict(row)}
        if method == "DELETE":
            conn.execute("UPDATE truck_work_orders SET status='cancelled', updated_at=CURRENT_TIMESTAMP WHERE id=?", [two_id])
            conn.commit()
            return {"status": 200, "body": {"ok": True}}

    # ----- TRUCK CAPACITY CONFIG -----
    if method == "GET" and path == "/truck-capacity":
        if params.get("truck_id"):
            rows = conn.execute("SELECT * FROM truck_capacity_config WHERE truck_id=? ORDER BY day_of_week", [int(params["truck_id"])]).fetchall()
        else:
            rows = conn.execute("SELECT * FROM truck_capacity_config ORDER BY truck_id, day_of_week").fetchall()
        return {"status": 200, "body": rows_to_list(rows)}

    if method == "PUT" and path == "/truck-capacity":
        if body.get("truck_id") is None or body.get("day_of_week") is None or body.get("capacity_minutes") is None:
            return {"status": 400, "body": {"error": "truck_id, day_of_week, capacity_minutes required"}}
        conn.execute(
            "INSERT INTO truck_capacity_config (truck_id, day_of_week, capacity_minutes, overtime_minutes, notes) VALUES (?,?,?,?,?) "
            "ON CONFLICT(truck_id, day_of_week) DO UPDATE SET capacity_minutes=excluded.capacity_minutes, overtime_minutes=excluded.overtime_minutes, notes=excluded.notes",
            [body["truck_id"], body["day_of_week"], body["capacity_minutes"],
             body.get("overtime_minutes", 120), body.get("notes")])
        conn.commit()
        row = conn.execute("SELECT * FROM truck_capacity_config WHERE truck_id=? AND day_of_week=?",
                           [body["truck_id"], body["day_of_week"]]).fetchone()
        return {"status": 200, "body": row_to_dict(row)}

    if method == "GET" and path == "/truck-capacity-check":
        truck_id = params.get("truck_id")
        check_date = params.get("date")
        if not truck_id or not check_date:
            return {"status": 400, "body": {"error": "truck_id and date required"}}
        truck_id = int(truck_id)
        dow = datetime.strptime(check_date, "%Y-%m-%d").weekday()  # 0=Mon
        # Sum delivery_log estimated_minutes for this truck+date
        dl_row = conn.execute(
            "SELECT COALESCE(SUM(COALESCE(estimated_minutes, 30)), 0) FROM delivery_log WHERE truck_id=? AND expected_date=?",
            [truck_id, check_date]).fetchone()
        delivery_mins = dl_row[0] if dl_row else 0
        # Sum truck_work_orders estimated_minutes for this truck+date (non-cancelled)
        wo_row = conn.execute(
            "SELECT COALESCE(SUM(COALESCE(estimated_minutes, 60)), 0) FROM truck_work_orders WHERE truck_id=? AND scheduled_date=? AND status!='cancelled'",
            [truck_id, check_date]).fetchone()
        wo_mins = wo_row[0] if wo_row else 0
        total_mins = delivery_mins + wo_mins
        # Get capacity config for this truck+dow
        cap_row = conn.execute(
            "SELECT * FROM truck_capacity_config WHERE truck_id=? AND day_of_week=?",
            [truck_id, dow]).fetchone()
        cap = cap_row["capacity_minutes"] if cap_row else 480
        ot = cap_row["overtime_minutes"] if cap_row else 120
        return {"status": 200, "body": {
            "truck_id": truck_id,
            "date": check_date,
            "day_of_week": dow,
            "total_scheduled_minutes": total_mins,
            "delivery_minutes": delivery_mins,
            "work_order_minutes": wo_mins,
            "capacity_minutes": cap,
            "overtime_minutes": ot,
            "remaining_minutes": cap - total_mins,
            "is_over_capacity": total_mins > cap,
            "is_overtime": total_mins > cap and total_mins <= cap + ot,
            "is_exceeded": total_mins > cap + ot,
        }}

    # ----- DELIVERY ADDRESSES -----
    if method == "GET" and path == "/delivery-addresses":
        where, vals = ["is_active=1"], []
        if params.get("client_id"):
            where.append("client_id=?"); vals.append(int(params["client_id"]))
        rows = conn.execute(f"SELECT * FROM delivery_addresses WHERE {' AND '.join(where)} ORDER BY is_default DESC, id", vals).fetchall()
        return {"status": 200, "body": rows_to_list(rows)}

    if method == "POST" and path == "/delivery-addresses":
        if not body.get("client_id") or not body.get("street_address"):
            return {"status": 400, "body": {"error": "client_id and street_address required"}}
        # If marking as default, clear existing defaults for this client
        if body.get("is_default"):
            conn.execute("UPDATE delivery_addresses SET is_default=0 WHERE client_id=?", [body["client_id"]])
        cur = conn.execute(
            "INSERT INTO delivery_addresses (client_id, address_name, street_address, suburb, state, postcode, estimated_travel_minutes, estimated_return_minutes, notes, is_default) VALUES (?,?,?,?,?,?,?,?,?,?)",
            [body["client_id"], body.get("address_name"), body["street_address"],
             body.get("suburb"), body.get("state", "QLD"), body.get("postcode"),
             body.get("estimated_travel_minutes", 30), body.get("estimated_return_minutes"),
             body.get("notes"), 1 if body.get("is_default") else 0])
        conn.commit()
        row = conn.execute("SELECT * FROM delivery_addresses WHERE id=?", [cur.lastrowid]).fetchone()
        return {"status": 201, "body": row_to_dict(row)}

    m = match("/delivery-addresses/:id", path)
    if m:
        da_id = int(m["id"])
        if method == "PUT":
            allowed = ["address_name", "street_address", "suburb", "state", "postcode",
                       "estimated_travel_minutes", "estimated_return_minutes", "notes", "is_default", "is_active"]
            fields, vals = [], []
            for f in allowed:
                if f in body:
                    fields.append(f"{f}=?"); vals.append(body[f])
            # If marking as default, clear others for this client
            if body.get("is_default"):
                existing = conn.execute("SELECT client_id FROM delivery_addresses WHERE id=?", [da_id]).fetchone()
                if existing:
                    conn.execute("UPDATE delivery_addresses SET is_default=0 WHERE client_id=?", [existing["client_id"]])
            if fields:
                vals.append(da_id)
                conn.execute(f"UPDATE delivery_addresses SET {', '.join(fields)} WHERE id=?", vals)
                conn.commit()
            row = conn.execute("SELECT * FROM delivery_addresses WHERE id=?", [da_id]).fetchone()
            return {"status": 200, "body": row_to_dict(row)}
        if method == "DELETE":
            conn.execute("UPDATE delivery_addresses SET is_active=0 WHERE id=?", [da_id])
            conn.commit()
            return {"status": 200, "body": {"ok": True}}

    # ----- CONTRACTOR ASSIGNMENTS -----
    if method == "GET" and path == "/contractor-assignments":
        where, vals = ["status!='cancelled'"], []
        if params.get("delivery_log_id"):
            where.append("delivery_log_id=?"); vals.append(int(params["delivery_log_id"]))
        if params.get("status"):
            where[0] = "1=1"  # override default filter
            where.append("status=?"); vals.append(params["status"])
        rows = conn.execute(f"SELECT * FROM contractor_assignments WHERE {' AND '.join(where)} ORDER BY created_at DESC", vals).fetchall()
        return {"status": 200, "body": rows_to_list(rows)}

    if method == "POST" and path == "/contractor-assignments":
        user_id = body.get("assigned_by")
        cur = conn.execute(
            "INSERT INTO contractor_assignments (delivery_log_id, truck_work_order_id, contractor_name, contractor_phone, contractor_company, on_behalf_of, assignment_type, pickup_address, delivery_address, estimated_minutes, cost_estimate, notes, assigned_by) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [body.get("delivery_log_id"), body.get("truck_work_order_id"),
             body.get("contractor_name"), body.get("contractor_phone"), body.get("contractor_company"),
             body.get("on_behalf_of", "hyne"), body.get("assignment_type", "delivery"),
             body.get("pickup_address"), body.get("delivery_address"),
             body.get("estimated_minutes"), body.get("cost_estimate"),
             body.get("notes"), user_id])
        conn.commit()
        row = conn.execute("SELECT * FROM contractor_assignments WHERE id=?", [cur.lastrowid]).fetchone()
        return {"status": 201, "body": row_to_dict(row)}

    m = match("/contractor-assignments/:id", path)
    if m:
        ca_id = int(m["id"])
        if method == "PUT":
            allowed = ["contractor_name", "contractor_phone", "contractor_company", "on_behalf_of",
                       "assignment_type", "pickup_address", "delivery_address", "estimated_minutes",
                       "cost_estimate", "status", "notes", "delivery_log_id", "truck_work_order_id"]
            fields, vals = [], []
            for f in allowed:
                if f in body:
                    fields.append(f"{f}=?"); vals.append(body[f])
            if fields:
                fields.append("updated_at=CURRENT_TIMESTAMP")
                vals.append(ca_id)
                conn.execute(f"UPDATE contractor_assignments SET {', '.join(fields)} WHERE id=?", vals)
                conn.commit()
            row = conn.execute("SELECT * FROM contractor_assignments WHERE id=?", [ca_id]).fetchone()
            return {"status": 200, "body": row_to_dict(row)}
        if method == "DELETE":
            conn.execute("UPDATE contractor_assignments SET status='cancelled', updated_at=CURRENT_TIMESTAMP WHERE id=?", [ca_id])
            conn.commit()
            return {"status": 200, "body": {"ok": True}}

    # ----- CLIENTS -----
    if method == "GET" and path == "/clients":
        is_active = params.get("is_active", "1")
        rows = conn.execute("SELECT * FROM clients WHERE is_active=? ORDER BY company_name", [is_active]).fetchall()
        return {"status": 200, "body": rows_to_list(rows)}

    if method == "POST" and path == "/clients":
        if not body.get("company_name"):
            return {"status": 400, "body": {"error": "company_name required"}}
        try:
            cur = conn.execute("INSERT INTO clients (company_name, contact_name, email, phone, address, payment_terms, myob_uid) VALUES (?,?,?,?,?,?,?)",
                [body["company_name"], body.get("contact_name"), body.get("email"), body.get("phone"), body.get("address"), body.get("payment_terms"), body.get("myob_uid")])
            conn.commit()
            row = conn.execute("SELECT * FROM clients WHERE id=?", [cur.lastrowid]).fetchone()
            return {"status": 201, "body": row_to_dict(row)}
        except Exception as e:
            return {"status": 409, "body": {"error": str(e)}}

    m = match("/clients/:id", path)
    if m:
        cid = int(m["id"])
        if method == "GET":
            client = conn.execute("SELECT * FROM clients WHERE id=?", [cid]).fetchone()
            if not client:
                return {"status": 404, "body": {"error": "Client not found"}}
            c = row_to_dict(client)
            c["contacts"] = rows_to_list(conn.execute("SELECT * FROM client_contacts WHERE client_id=? AND is_active=1 ORDER BY id", [cid]).fetchall())
            return {"status": 200, "body": c}
        if method == "PUT":
            if not current_user:
                return {"status": 401, "body": {"error": "Authentication required"}}
            client = conn.execute("SELECT id FROM clients WHERE id=?", [cid]).fetchone()
            if not client:
                return {"status": 404, "body": {"error": "Client not found"}}
            fields, vals = [], []
            for f in ["company_name", "contact_name", "email", "phone", "address", "payment_terms"]:
                if f in body:
                    fields.append(f"{f}=?"); vals.append(body[f])
            if fields:
                vals.append(cid)
                conn.execute(f"UPDATE clients SET {', '.join(fields)} WHERE id=?", vals)
                conn.commit()
            row = conn.execute("SELECT * FROM clients WHERE id=?", [cid]).fetchone()
            result = row_to_dict(row)
            result["contacts"] = rows_to_list(conn.execute("SELECT * FROM client_contacts WHERE client_id=? AND is_active=1 ORDER BY id", [cid]).fetchall())
            return {"status": 200, "body": result}

    m = match("/clients/:id/contacts", path)
    if m:
        cid = int(m["id"])
        if method == "GET":
            contacts = rows_to_list(conn.execute("SELECT * FROM client_contacts WHERE client_id=? AND is_active=1 ORDER BY id", [cid]).fetchall())
            return {"status": 200, "body": contacts}
        if method == "POST":
            if not current_user:
                return {"status": 401, "body": {"error": "Authentication required"}}
            cur = conn.execute("INSERT INTO client_contacts (client_id, contact_name, email, phone, role_title, email_purpose, receives_sensitive, notes) VALUES (?,?,?,?,?,?,?,?)",
                [cid, body.get("contact_name",""), body.get("email"), body.get("phone"), body.get("role_title"), body.get("email_purpose","general"), body.get("receives_sensitive",0), body.get("notes")])
            conn.commit()
            row = conn.execute("SELECT * FROM client_contacts WHERE id=?", [cur.lastrowid]).fetchone()
            return {"status": 201, "body": row_to_dict(row)}

    m = match("/clients/:cid/contacts/:id", path)
    if m:
        contact_id = int(m["id"])
        if method == "PUT":
            if not current_user:
                return {"status": 401, "body": {"error": "Authentication required"}}
            fields, vals = [], []
            for f in ["contact_name", "email", "phone", "role_title", "email_purpose", "receives_sensitive", "notes", "is_active"]:
                if f in body:
                    fields.append(f"{f}=?"); vals.append(body[f])
            if fields:
                vals.append(contact_id)
                conn.execute(f"UPDATE client_contacts SET {', '.join(fields)} WHERE id=?", vals)
                conn.commit()
            row = conn.execute("SELECT * FROM client_contacts WHERE id=?", [contact_id]).fetchone()
            return {"status": 200, "body": row_to_dict(row)}
        if method == "DELETE":
            if not current_user:
                return {"status": 401, "body": {"error": "Authentication required"}}
            conn.execute("UPDATE client_contacts SET is_active=0 WHERE id=?", [contact_id])
            conn.commit()
            return {"status": 200, "body": {"deleted": True}}

    # ----- SKUS -----
    if method == "GET" and path == "/skus":
        where, vals = ["is_active=1"], []
        if params.get("zone_id"):
            where.append("zone_id=?"); vals.append(params["zone_id"])
        if params.get("search"):
            where.append("(code LIKE ? OR name LIKE ?)")
            term = f"%{params['search']}%"
            vals += [term, term]
        rows = conn.execute(f"SELECT * FROM skus WHERE {' AND '.join(where)} ORDER BY code", vals).fetchall()
        return {"status": 200, "body": rows_to_list(rows)}

    if method == "POST" and path == "/skus":
        for f in ["code", "name"]:
            if not body.get(f):
                return {"status": 400, "body": {"error": f"Field '{f}' is required"}}
        try:
            cur = conn.execute("INSERT INTO skus (code, name, drawing_number, labour_cost, material_cost, sell_price, zone_id, myob_uid) VALUES (?,?,?,?,?,?,?,?)",
                [body["code"].upper(), body["name"], body.get("drawing_number"), body.get("labour_cost", 0), body.get("material_cost", 0), body.get("sell_price", 0), body.get("zone_id"), body.get("myob_uid")])
            conn.commit()
            row = conn.execute("SELECT * FROM skus WHERE id=?", [cur.lastrowid]).fetchone()
            return {"status": 201, "body": row_to_dict(row)}
        except Exception as e:
            return {"status": 409, "body": {"error": str(e)}}

    # ----- STATS -----
    if method == "GET" and path == "/stats/production":
        today = datetime.now().strftime("%Y-%m-%d")
        zone_stats = rows_to_list(conn.execute("SELECT z.name as zone_name, z.code, COUNT(ps.id) as sessions_today, COALESCE(SUM(ps.produced_quantity),0) as units_produced FROM zones z LEFT JOIN production_sessions ps ON ps.zone_id=z.id AND DATE(ps.start_time)=? WHERE z.is_active=1 GROUP BY z.id, z.name, z.code ORDER BY z.name", [today]).fetchall())
        # Order-level pipeline (for backward compat)
        pipeline = rows_to_list(conn.execute("SELECT status, COUNT(*) as count, COALESCE(SUM(total_value),0) as value FROM orders GROUP BY status").fetchall())
        # Item-level pipeline counts
        item_pipeline = rows_to_list(conn.execute("""
            SELECT oi.status, COUNT(*) as item_count, COALESCE(SUM(oi.quantity),0) as total_qty
            FROM order_items oi
            JOIN orders o ON o.id=oi.order_id
            WHERE o.status NOT IN ('delivered','collected')
            GROUP BY oi.status
        """).fetchall())
        active_sessions = conn.execute("SELECT COUNT(*) FROM production_sessions WHERE status='active'").fetchone()[0]
        today_value = conn.execute("SELECT COALESCE(SUM(ps.produced_quantity * s.sell_price),0) FROM production_sessions ps JOIN order_items oi ON oi.id=ps.order_item_id JOIN skus s ON s.id=oi.sku_id WHERE DATE(ps.start_time)=? AND ps.status='completed'", [today]).fetchone()[0]
        return {"status": 200, "body": {"date": today, "zone_stats": zone_stats, "pipeline": pipeline, "item_pipeline": item_pipeline, "active_sessions": active_sessions, "today_completed_value": round(today_value, 2)}}

    if method == "GET" and path == "/stats/orders":
        rows = conn.execute("SELECT status, COUNT(*) as count, COALESCE(SUM(total_value),0) as total_value FROM orders GROUP BY status ORDER BY status").fetchall()
        status_labels = {"T": "New/Tendered", "C": "Cut List", "R": "Ready", "P": "In Production", "F": "Finished", "dispatched": "Dispatched", "delivered": "Delivered", "collected": "Collected"}
        result = []
        for r in rows_to_list(rows):
            r["label"] = status_labels.get(r["status"], r["status"])
            result.append(r)
        totals = conn.execute("SELECT COUNT(*) as total_orders, COALESCE(SUM(total_value),0) as total_value FROM orders").fetchone()
        return {"status": 200, "body": {"by_status": result, "totals": {"orders": totals[0], "value": round(totals[1], 2)}}}

    # ----- ACCOUNTING -----
    if method == "GET" and path == "/accounting/config":
        row = conn.execute("SELECT * FROM accounting_config LIMIT 1").fetchone()
        if not row:
            return {"status": 200, "body": {}}
        cfg = row_to_dict(row)
        if cfg.get("api_key"):
            cfg["api_key"] = "****"
        if cfg.get("api_secret"):
            cfg["api_secret"] = "****"
        return {"status": 200, "body": cfg}

    if method == "PUT" and path == "/accounting/config":
        row = conn.execute("SELECT id FROM accounting_config LIMIT 1").fetchone()
        if not row:
            return {"status": 404, "body": {"error": "Accounting config not found"}}
        cfg_id = row["id"]
        fields, vals = [], []
        for f in ["provider", "api_key", "api_secret", "access_token", "refresh_token", "company_file_id", "sync_interval_minutes", "is_connected", "config_json"]:
            if f in body:
                fields.append(f"{f}=?"); vals.append(body[f])
        if not fields:
            return {"status": 400, "body": {"error": "No updatable fields"}}
        fields.append("updated_at=CURRENT_TIMESTAMP")
        vals.append(cfg_id)
        conn.execute(f"UPDATE accounting_config SET {', '.join(fields)} WHERE id=?", vals)
        conn.commit()
        row = conn.execute("SELECT * FROM accounting_config WHERE id=?", [cfg_id]).fetchone()
        cfg = row_to_dict(row)
        if cfg.get("api_key"):
            cfg["api_key"] = "****"
        if cfg.get("api_secret"):
            cfg["api_secret"] = "****"
        return {"status": 200, "body": cfg}

    if method == "POST" and path == "/accounting/sync":
        row = conn.execute("SELECT * FROM accounting_config LIMIT 1").fetchone()
        provider = row["provider"] if row else "mock"
        conn.execute("INSERT INTO accounting_sync_log (direction, entity_type, entity_id, status, details) VALUES (?,?,?,?,?)",
            ["outbound", "sync", "all", "success", f"Mock sync triggered for provider: {provider}"])
        conn.execute("UPDATE accounting_config SET last_sync_at=CURRENT_TIMESTAMP WHERE id=1")
        conn.commit()
        return {"status": 200, "body": {"message": "Sync triggered", "provider": provider, "status": "success"}}

    if method == "GET" and path == "/accounting/sync-log":
        limit = int(params.get("limit", 50))
        rows = conn.execute("SELECT * FROM accounting_sync_log ORDER BY synced_at DESC LIMIT ?", [limit]).fetchall()
        return {"status": 200, "body": rows_to_list(rows)}

    # ----- NOTIFICATIONS -----
    if method == "GET" and path == "/notifications":
        where, vals = ["1=1"], []
        if params.get("order_id"):
            where.append("order_id=?"); vals.append(params["order_id"])
        if params.get("type"):
            where.append("notification_type=?"); vals.append(params["type"])
        limit = int(params.get("limit", 50))
        rows = conn.execute(f"SELECT * FROM notification_log WHERE {' AND '.join(where)} ORDER BY sent_at DESC LIMIT ?", vals + [limit]).fetchall()
        return {"status": 200, "body": rows_to_list(rows)}

    if method == "POST" and path == "/notifications":
        for f in ["notification_type", "recipient_email"]:
            if not body.get(f):
                return {"status": 400, "body": {"error": f"Field '{f}' is required"}}
        cur = conn.execute("INSERT INTO notification_log (order_id, notification_type, recipient_email, subject, body, status) VALUES (?,?,?,?,?,?)",
            [body.get("order_id"), body["notification_type"], body["recipient_email"], body.get("subject", ""), body.get("body", ""), "sent"])
        conn.commit()
        row = conn.execute("SELECT * FROM notification_log WHERE id=?", [cur.lastrowid]).fetchone()
        return {"status": 201, "body": row_to_dict(row)}

    # ----- AUDIT LOG -----
    if method == "GET" and path == "/audit-log":
        where, vals = ["1=1"], []
        if params.get("entity_type"):
            where.append("entity_type=?"); vals.append(params["entity_type"])
        if params.get("user_id"):
            where.append("user_id=?"); vals.append(params["user_id"])
        limit = int(params.get("limit", 100))
        rows = conn.execute(f"SELECT al.*, u.full_name as user_name FROM audit_log al LEFT JOIN users u ON u.id=al.user_id WHERE {' AND '.join(where)} ORDER BY al.created_at DESC LIMIT ?", vals + [limit]).fetchall()
        return {"status": 200, "body": rows_to_list(rows)}

    # ----- INVENTORY -----
    if method == "GET" and path == "/inventory":
        rows = conn.execute("""
            SELECT inv.*, s.code as sku_code, s.name as sku_name, s.zone_id
            FROM inventory inv JOIN skus s ON s.id=inv.sku_id
            ORDER BY s.code
        """).fetchall()
        return {"status": 200, "body": rows_to_list(rows)}

    m = match("/inventory/:sku_id", path)
    if m and method == "PUT":
        sku_id = int(m["sku_id"])
        units_on_hand = body.get("units_on_hand")
        if units_on_hand is None:
            return {"status": 400, "body": {"error": "units_on_hand required"}}
        conn.execute("""
            INSERT INTO inventory (sku_id, units_on_hand, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(sku_id) DO UPDATE SET units_on_hand=excluded.units_on_hand, updated_at=CURRENT_TIMESTAMP
        """, [sku_id, int(units_on_hand)])
        conn.commit()
        row = conn.execute("SELECT inv.*, s.code as sku_code, s.name as sku_name FROM inventory inv JOIN skus s ON s.id=inv.sku_id WHERE inv.sku_id=?", [sku_id]).fetchone()
        return {"status": 200, "body": row_to_dict(row)}

    # ----- STATION CAPACITY -----
    if method == "GET" and path == "/station-capacity":
        station_id_filter = params.get("station_id")
        if station_id_filter:
            row = conn.execute(
                "SELECT sc.*, s.name as station_name FROM station_capacity sc JOIN stations s ON s.id=sc.station_id WHERE sc.station_id=?",
                [int(station_id_filter)]).fetchone()
            if not row:
                return {"status": 200, "body": {"station_id": int(station_id_filter), "max_units_per_day": None}}
            return {"status": 200, "body": row_to_dict(row)}
        rows = conn.execute("""
            SELECT sc.*, s.name as station_name, s.code as station_code, z.name as zone_name, z.code as zone_code
            FROM station_capacity sc JOIN stations s ON s.id=sc.station_id JOIN zones z ON z.id=s.zone_id
            ORDER BY z.name, s.name
        """).fetchall()
        return {"status": 200, "body": rows_to_list(rows)}

    if method == "PUT" and path == "/station-capacity":
        station_id = body.get("station_id")
        max_units = body.get("max_units_per_day")
        if station_id is None or max_units is None:
            return {"status": 400, "body": {"error": "station_id and max_units_per_day required"}}
        conn.execute("""
            INSERT INTO station_capacity (station_id, max_units_per_day)
            VALUES (?, ?)
            ON CONFLICT(station_id) DO UPDATE SET max_units_per_day=excluded.max_units_per_day
        """, [int(station_id), int(max_units)])
        conn.commit()
        row = conn.execute("SELECT sc.*, s.name as station_name FROM station_capacity sc JOIN stations s ON s.id=sc.station_id WHERE sc.station_id=?", [int(station_id)]).fetchone()
        return {"status": 200, "body": row_to_dict(row)}

    m = match("/station-capacity/:station_id", path)
    if m and method == "PUT":
        station_id = int(m["station_id"])
        max_units = body.get("max_units_per_day")
        if max_units is None:
            return {"status": 400, "body": {"error": "max_units_per_day required"}}
        conn.execute("""
            INSERT INTO station_capacity (station_id, max_units_per_day)
            VALUES (?, ?)
            ON CONFLICT(station_id) DO UPDATE SET max_units_per_day=excluded.max_units_per_day
        """, [station_id, int(max_units)])
        conn.commit()
        row = conn.execute("SELECT sc.*, s.name as station_name FROM station_capacity sc JOIN stations s ON s.id=sc.station_id WHERE sc.station_id=?", [station_id]).fetchone()
        return {"status": 200, "body": row_to_dict(row)}

    # ----- LABOUR CONFIG -----
    if method == "GET" and path == "/labour-config":
        default_rate = conn.execute("SELECT * FROM target_labour_rates WHERE is_default=1 LIMIT 1").fetchone()
        user_rates = rows_to_list(conn.execute(
            "SELECT tlr.*, u.full_name, u.username FROM target_labour_rates tlr LEFT JOIN users u ON u.id=tlr.user_id WHERE tlr.is_default=0 ORDER BY tlr.id"
        ).fetchall())
        return {"status": 200, "body": {
            "default_rate": row_to_dict(default_rate) if default_rate else {"rate_per_hour": 55.0, "is_default": 1},
            "user_rates": user_rates
        }}

    if method == "PUT" and path == "/labour-config":
        rate = body.get("rate_per_hour")
        user_id = body.get("user_id")  # None = update default
        if rate is None:
            return {"status": 400, "body": {"error": "rate_per_hour required"}}
        if user_id is None:
            # Upsert default rate
            existing = conn.execute("SELECT id FROM target_labour_rates WHERE is_default=1").fetchone()
            if existing:
                conn.execute("UPDATE target_labour_rates SET rate_per_hour=? WHERE is_default=1", [float(rate)])
            else:
                conn.execute("INSERT INTO target_labour_rates (rate_per_hour, is_default, notes) VALUES (?,1,'Global default rate')", [float(rate)])
        else:
            existing = conn.execute("SELECT id FROM target_labour_rates WHERE user_id=?", [int(user_id)]).fetchone()
            if existing:
                conn.execute("UPDATE target_labour_rates SET rate_per_hour=? WHERE user_id=?", [float(rate), int(user_id)])
            else:
                conn.execute("INSERT INTO target_labour_rates (user_id, rate_per_hour, is_default) VALUES (?,?,0)", [int(user_id), float(rate)])
        conn.commit()
        return {"status": 200, "body": {"ok": True, "rate_per_hour": float(rate)}}

    # ----- CLOSE DAYS -----
    if method == "POST" and path == "/planning/close-day":
        zone_id = body.get("zone_id")
        closed_date = body.get("closed_date")
        if not zone_id or not closed_date:
            return {"status": 400, "body": {"error": "zone_id and closed_date required"}}
        try:
            conn.execute("INSERT INTO close_days (zone_id, closed_date) VALUES (?,?)", [zone_id, closed_date])
            conn.commit()
        except Exception:
            pass  # Already exists — idempotent
        return {"status": 200, "body": {"zone_id": zone_id, "closed_date": closed_date, "closed": True}}

    if method == "DELETE" and path == "/planning/close-day":
        zone_id = body.get("zone_id")
        closed_date = body.get("closed_date")
        if not zone_id or not closed_date:
            return {"status": 400, "body": {"error": "zone_id and closed_date required"}}
        conn.execute("DELETE FROM close_days WHERE zone_id=? AND closed_date=?", [zone_id, closed_date])
        conn.commit()
        return {"status": 200, "body": {"zone_id": zone_id, "closed_date": closed_date, "closed": False}}

    # ----- ORDER ITEM SPLIT -----
    m = match("/order-items/:id/split", path)
    if m and method == "POST":
        iid = int(m["id"])
        new_qty = body.get("new_quantity")
        if not new_qty or int(new_qty) <= 0:
            return {"status": 400, "body": {"error": "new_quantity required and must be > 0"}}
        new_qty = int(new_qty)
        orig = conn.execute("SELECT * FROM order_items WHERE id=?", [iid]).fetchone()
        if not orig:
            return {"status": 404, "body": {"error": "Order item not found"}}
        orig = row_to_dict(orig)
        if new_qty >= orig["quantity"]:
            return {"status": 400, "body": {"error": "new_quantity must be less than original quantity"}}
        remaining = orig["quantity"] - new_qty
        # Reduce original
        conn.execute("UPDATE order_items SET quantity=?, line_total=quantity*unit_price WHERE id=?", [remaining, iid])
        # Create split item
        cur = conn.execute("""
            INSERT INTO order_items (order_id, sku_id, sku_code, product_name, quantity, unit_price, line_total,
                zone_id, station_id, scheduled_date, eta_date, drawing_number, special_instructions, split_from_item_id)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            [orig["order_id"], orig["sku_id"], orig["sku_code"], orig["product_name"],
             new_qty, orig["unit_price"], new_qty * (orig["unit_price"] or 0),
             orig["zone_id"], orig["station_id"], orig["scheduled_date"], orig["eta_date"],
             orig["drawing_number"], orig["special_instructions"], iid])
        conn.commit()
        new_item = row_to_dict(conn.execute("SELECT * FROM order_items WHERE id=?", [cur.lastrowid]).fetchone())
        orig_updated = row_to_dict(conn.execute("SELECT * FROM order_items WHERE id=?", [iid]).fetchone())
        return {"status": 201, "body": {"original": orig_updated, "split": new_item}}

    # ----- STOCK COMPLETE -----
    m = match("/orders/:id/stock-complete", path)
    if m and method == "POST":
        oid = int(m["id"])
        order = conn.execute("SELECT * FROM orders WHERE id=? AND is_stock_run=1", [oid]).fetchone()
        if not order:
            return {"status": 404, "body": {"error": "Stock run order not found"}}
        # Get order items and update each to 'F'
        items = conn.execute("SELECT * FROM order_items WHERE order_id=?", [oid]).fetchall()
        for item in items:
            item = row_to_dict(item)
            if item.get("sku_id"):
                qty = body.get("produced_quantity", item["quantity"])
                conn.execute("""
                    INSERT INTO inventory (sku_id, units_on_hand, updated_at)
                    VALUES (?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(sku_id) DO UPDATE SET units_on_hand=units_on_hand+?, updated_at=CURRENT_TIMESTAMP
                """, [item["sku_id"], int(qty), int(qty)])
            # Set item status to F
            conn.execute("UPDATE order_items SET status='F' WHERE id=? AND status NOT IN ('F','dispatched')", [item["id"]])
        # Sync order status from items (will compute F since all items are F)
        sync_order_status(conn, oid)
        return {"status": 200, "body": order_full(conn, oid)}

    # ----- CAPACITY CHECK (pre-drop validation) -----
    if method == "GET" and path == "/capacity-check":
        station_id = params.get("station_id")
        scheduled_date = params.get("scheduled_date")
        additional_qty = int(params.get("additional_quantity", 0))
        zone_id = params.get("zone_id")
        if not station_id or not scheduled_date:
            return {"status": 400, "body": {"error": "station_id and scheduled_date required"}}
        station_id = int(station_id)
        # Get station capacity limit
        cap_row = conn.execute("SELECT max_units_per_day FROM station_capacity WHERE station_id=?", [station_id]).fetchone()
        max_capacity = cap_row[0] if cap_row else 9999
        # Get current total already scheduled on this station+date
        cur_total_row = conn.execute(
            "SELECT COALESCE(SUM(planned_quantity),0) FROM schedule_entries WHERE station_id=? AND scheduled_date=?",
            [station_id, scheduled_date]).fetchone()
        current_total = cur_total_row[0] if cur_total_row else 0
        new_total = current_total + additional_qty
        remaining_capacity = max(0, max_capacity - current_total)
        return {"status": 200, "body": {
            "station_id": station_id,
            "scheduled_date": scheduled_date,
            "max_capacity": max_capacity,
            "current_total": current_total,
            "additional_quantity": additional_qty,
            "new_total": new_total,
            "would_exceed": new_total > max_capacity,
            "remaining_capacity": remaining_capacity
        }}

    # ----- PLANNING / VIKING -----
    if method == "GET" and path == "/planning/viking":
        week_start = params.get("week_start")
        if not week_start:
            # Default to current Monday
            today = datetime.now()
            days_since_monday = today.weekday()
            monday = today.replace(hour=0, minute=0, second=0, microsecond=0)
            monday = monday.replace(day=monday.day - days_since_monday)
            week_start = monday.strftime("%Y-%m-%d")
        # Parse week start and compute Mon-Sat
        ws = datetime.strptime(week_start, "%Y-%m-%d")
        num_days = min(int(params.get("num_days", 6)), 21)  # Default 6 (Mon-Sat), max 21
        days = [ws + timedelta(days=i) for i in range(num_days)]
        day_strings = [d.strftime("%Y-%m-%d") for d in days]
        day_names = [d.strftime("%A") for d in days]

        # Get Viking zone
        vik_zone = conn.execute("SELECT * FROM zones WHERE code='VIK'").fetchone()
        if not vik_zone:
            return {"status": 404, "body": {"error": "Viking zone not found"}}
        vik_zone = row_to_dict(vik_zone)
        zone_id = vik_zone["id"]

        # Get Viking machines with capacity
        machines = rows_to_list(conn.execute("""
            SELECT s.*, sc.max_units_per_day
            FROM stations s
            LEFT JOIN station_capacity sc ON sc.station_id=s.id
            WHERE s.zone_id=? AND s.is_active=1
            ORDER BY s.id
        """, [zone_id]).fetchall())

        # Get close days for this week
        close_day_rows = conn.execute(
            "SELECT closed_date FROM close_days WHERE zone_id=? AND closed_date >= ? AND closed_date <= ?",
            [zone_id, day_strings[0], day_strings[-1]]
        ).fetchall()
        close_days_set = {row[0] for row in close_day_rows}

        # Get all schedule entries for Viking this week
        entries = rows_to_list(conn.execute("""
            SELECT se.*,
                   oi.sku_code, oi.product_name, oi.quantity as item_quantity, oi.sku_id, oi.status as item_status, oi.split_from_item_id,
                   o.order_number, o.status as order_status, o.is_stock_run,
                   o.requested_delivery_date, o.client_id, c.company_name as client_name
            FROM schedule_entries se
            LEFT JOIN order_items oi ON oi.id=se.order_item_id
            LEFT JOIN orders o ON o.id=se.order_id
            LEFT JOIN clients c ON c.id=o.client_id
            WHERE se.zone_id=? AND se.scheduled_date >= ? AND se.scheduled_date <= ?
            ORDER BY se.priority DESC, se.run_order ASC, se.id ASC
        """, [zone_id, day_strings[0], day_strings[-1]]).fetchall())

        # Get inventory map
        inv_map = {row["sku_id"]: row["units_on_hand"]
                   for row in rows_to_list(conn.execute("SELECT * FROM inventory").fetchall())}

        # Build per-day, per-machine structure
        result_days = []
        for i, date_str in enumerate(day_strings):
            day_entries = [e for e in entries if e["scheduled_date"] == date_str]
            total_planned = sum(e.get("planned_quantity") or 0 for e in day_entries)
            machine_slots = {}
            for m_obj in machines:
                mid = m_obj["id"]
                m_entries = [e for e in day_entries if e.get("station_id") == mid]
                # Sort: priority desc, run_order asc
                m_entries.sort(key=lambda x: (-int(x.get("priority") or 0), int(x.get("run_order") or 0)))
                machine_total = sum(e.get("planned_quantity") or 0 for e in m_entries)
                machine_slots[mid] = {
                    "entries": m_entries,
                    "total": machine_total,
                    "over_capacity": machine_total > (m_obj.get("max_units_per_day") or 9999)
                }
            result_days.append({
                "date": date_str,
                "day_name": day_names[i],
                "is_closed": date_str in close_days_set,
                "total_planned": total_planned,
                "machine_slots": machine_slots
            })

        # Intake queue: Viking order items NOT in schedule, status not F/dispatched/delivered/collected
        # Unscheduled = no schedule_entry for the item in Viking zone
        scheduled_item_ids = {e["order_item_id"] for e in entries if e.get("order_item_id")}
        # Also items scheduled in other weeks
        all_vik_sched = {row[0] for row in conn.execute(
            "SELECT order_item_id FROM schedule_entries WHERE zone_id=? AND order_item_id IS NOT NULL",
            [zone_id]).fetchall()}
        intake_raw = rows_to_list(conn.execute("""
            SELECT oi.*, o.order_number, o.status as order_status, o.is_stock_run,
                   o.requested_delivery_date, o.eta_date, c.company_name as client_name
            FROM order_items oi
            JOIN orders o ON o.id=oi.order_id
            LEFT JOIN clients c ON c.id=o.client_id
            WHERE oi.zone_id=? AND o.status NOT IN ('F','dispatched','delivered','collected')
              AND oi.status NOT IN ('F','dispatched')
        """, [zone_id]).fetchall())
        intake_queue = [
            dict(item, inventory_on_hand=inv_map.get(item.get("sku_id"), 0))
            for item in intake_raw
            if item["id"] not in all_vik_sched
        ]
        # Sort: priority (has requested_delivery_date) first, then by created_at desc
        intake_queue.sort(key=lambda x: (
            0 if x.get("requested_delivery_date") else 1,
            x.get("created_at", "") or ""
        ))

        return {"status": 200, "body": {
            "zone": vik_zone,
            "week_start": week_start,
            "machines": machines,
            "days": result_days,
            "intake_queue": intake_queue,
            "close_days": list(close_days_set),
            "docking_queue": rows_to_list(conn.execute("""
                SELECT oi.*, o.order_number, o.status as order_status, o.is_stock_run,
                       o.requested_delivery_date, c.company_name as client_name
                FROM order_items oi
                JOIN orders o ON o.id=oi.order_id
                LEFT JOIN clients c ON c.id=o.client_id
                WHERE oi.zone_id=? AND oi.status='C'
            """, [zone_id]).fetchall())
        }}

    # ----- PLANNING / HANDMADE -----
    if method == "GET" and path == "/planning/handmade":
        week_start = params.get("week_start")
        if not week_start:
            today = datetime.now()
            days_since_monday = today.weekday()
            monday = today.replace(hour=0, minute=0, second=0, microsecond=0)
            monday = monday.replace(day=monday.day - days_since_monday)
            week_start = monday.strftime("%Y-%m-%d")
        ws = datetime.strptime(week_start, "%Y-%m-%d")
        num_days = min(int(params.get("num_days", 6)), 21)  # Default 6 (Mon-Sat), max 21
        days = [ws + timedelta(days=i) for i in range(num_days)]
        day_strings = [d.strftime("%Y-%m-%d") for d in days]
        day_names = [d.strftime("%A") for d in days]

        hmp_zone = conn.execute("SELECT * FROM zones WHERE code='HMP'").fetchone()
        if not hmp_zone:
            return {"status": 404, "body": {"error": "Handmade zone not found"}}
        hmp_zone = row_to_dict(hmp_zone)
        zone_id = hmp_zone["id"]

        tables = rows_to_list(conn.execute("""
            SELECT s.*, sc.max_units_per_day
            FROM stations s LEFT JOIN station_capacity sc ON sc.station_id=s.id
            WHERE s.zone_id=? AND s.is_active=1 ORDER BY s.id
        """, [zone_id]).fetchall())

        close_day_rows = conn.execute(
            "SELECT closed_date FROM close_days WHERE zone_id=? AND closed_date >= ? AND closed_date <= ?",
            [zone_id, day_strings[0], day_strings[-1]]
        ).fetchall()
        close_days_set = {row[0] for row in close_day_rows}

        entries = rows_to_list(conn.execute("""
            SELECT se.*,
                   oi.sku_code, oi.product_name, oi.quantity as item_quantity, oi.sku_id, oi.status as item_status, oi.split_from_item_id,
                   o.order_number, o.status as order_status, o.is_stock_run,
                   o.requested_delivery_date, o.client_id, c.company_name as client_name
            FROM schedule_entries se
            LEFT JOIN order_items oi ON oi.id=se.order_item_id
            LEFT JOIN orders o ON o.id=se.order_id
            LEFT JOIN clients c ON c.id=o.client_id
            WHERE se.zone_id=? AND se.scheduled_date >= ? AND se.scheduled_date <= ?
            ORDER BY se.priority DESC, se.run_order ASC, se.id ASC
        """, [zone_id, day_strings[0], day_strings[-1]]).fetchall())

        inv_map = {row["sku_id"]: row["units_on_hand"]
                   for row in rows_to_list(conn.execute("SELECT * FROM inventory").fetchall())}

        result_days = []
        for i, date_str in enumerate(day_strings):
            day_entries = [e for e in entries if e["scheduled_date"] == date_str]
            total_planned = sum(e.get("planned_quantity") or 0 for e in day_entries)
            table_slots = {}
            for t_obj in tables:
                tid = t_obj["id"]
                t_entries = [e for e in day_entries if e.get("station_id") == tid]
                t_entries.sort(key=lambda x: (-int(x.get("priority") or 0), int(x.get("run_order") or 0)))
                table_total = sum(e.get("planned_quantity") or 0 for e in t_entries)
                table_slots[tid] = {
                    "entries": t_entries,
                    "total": table_total,
                    "over_capacity": table_total > (t_obj.get("max_units_per_day") or 9999)
                }
            result_days.append({
                "date": date_str,
                "day_name": day_names[i],
                "is_closed": date_str in close_days_set,
                "total_planned": total_planned,
                "machine_slots": table_slots
            })

        all_hmp_sched = {row[0] for row in conn.execute(
            "SELECT order_item_id FROM schedule_entries WHERE zone_id=? AND order_item_id IS NOT NULL",
            [zone_id]).fetchall()}
        intake_raw = rows_to_list(conn.execute("""
            SELECT oi.*, o.order_number, o.status as order_status, o.is_stock_run,
                   o.requested_delivery_date, o.eta_date, c.company_name as client_name
            FROM order_items oi
            JOIN orders o ON o.id=oi.order_id
            LEFT JOIN clients c ON c.id=o.client_id
            WHERE oi.zone_id=? AND o.status NOT IN ('F','dispatched','delivered','collected')
              AND oi.status NOT IN ('F','dispatched')
        """, [zone_id]).fetchall())
        intake_queue = [
            dict(item, inventory_on_hand=inv_map.get(item.get("sku_id"), 0))
            for item in intake_raw
            if item["id"] not in all_hmp_sched
        ]
        intake_queue.sort(key=lambda x: (
            0 if x.get("requested_delivery_date") else 1,
            x.get("created_at", "") or ""
        ))

        return {"status": 200, "body": {
            "zone": hmp_zone,
            "week_start": week_start,
            "machines": tables,
            "days": result_days,
            "intake_queue": intake_queue,
            "close_days": list(close_days_set),
            "docking_queue": rows_to_list(conn.execute("""
                SELECT oi.*, o.order_number, o.status as order_status, o.is_stock_run,
                       o.requested_delivery_date, c.company_name as client_name
                FROM order_items oi
                JOIN orders o ON o.id=oi.order_id
                LEFT JOIN clients c ON c.id=o.client_id
                WHERE oi.zone_id=? AND oi.status='C'
            """, [zone_id]).fetchall())
        }}

    # ----- PLANNING / GENERIC ZONE HELPER (DTL, Crates) -----
    def _planning_zone(zone_code):
        """Generic planning endpoint for any zone - reusable for DTL, Crates, etc."""
        week_start_p = params.get("week_start")
        if not week_start_p:
            today_p = datetime.now()
            days_since_mon = today_p.weekday()
            mon = today_p.replace(hour=0, minute=0, second=0, microsecond=0)
            mon = mon.replace(day=mon.day - days_since_mon)
            week_start_p = mon.strftime("%Y-%m-%d")
        ws_p = datetime.strptime(week_start_p, "%Y-%m-%d")
        num_days = min(int(params.get("num_days", 6)), 21)  # Default 6 (Mon-Sat), max 21
        day_list = [ws_p + timedelta(days=i) for i in range(num_days)]
        day_strs = [d.strftime("%Y-%m-%d") for d in day_list]
        day_nms = [d.strftime("%A") for d in day_list]

        z_row = conn.execute("SELECT * FROM zones WHERE code=?", [zone_code]).fetchone()
        if not z_row:
            return {"status": 404, "body": {"error": f"Zone {zone_code} not found"}}
        z_row = row_to_dict(z_row)
        zid = z_row["id"]

        stations_list = rows_to_list(conn.execute("""
            SELECT s.*, sc.max_units_per_day
            FROM stations s LEFT JOIN station_capacity sc ON sc.station_id=s.id
            WHERE s.zone_id=? AND s.is_active=1 ORDER BY s.id
        """, [zid]).fetchall())

        cd_rows = conn.execute(
            "SELECT closed_date FROM close_days WHERE zone_id=? AND closed_date >= ? AND closed_date <= ?",
            [zid, day_strs[0], day_strs[-1]]).fetchall()
        cd_set = {r[0] for r in cd_rows}

        ents = rows_to_list(conn.execute("""
            SELECT se.*,
                   oi.sku_code, oi.product_name, oi.quantity as item_quantity, oi.sku_id, oi.status as item_status, oi.split_from_item_id,
                   o.order_number, o.status as order_status, o.is_stock_run,
                   o.requested_delivery_date, o.client_id, c.company_name as client_name
            FROM schedule_entries se
            LEFT JOIN order_items oi ON oi.id=se.order_item_id
            LEFT JOIN orders o ON o.id=se.order_id
            LEFT JOIN clients c ON c.id=o.client_id
            WHERE se.zone_id=? AND se.scheduled_date >= ? AND se.scheduled_date <= ?
            ORDER BY se.priority DESC, se.run_order ASC, se.id ASC
        """, [zid, day_strs[0], day_strs[-1]]).fetchall())

        inv_mp = {r["sku_id"]: r["units_on_hand"]
                  for r in rows_to_list(conn.execute("SELECT * FROM inventory").fetchall())}

        res_days = []
        for i, ds in enumerate(day_strs):
            de = [e for e in ents if e["scheduled_date"] == ds]
            tp = sum(e.get("planned_quantity") or 0 for e in de)
            slots = {}
            for st in stations_list:
                sid = st["id"]
                se = sorted([e for e in de if e.get("station_id") == sid],
                            key=lambda x: (-int(x.get("priority") or 0), int(x.get("run_order") or 0)))
                st_total = sum(e.get("planned_quantity") or 0 for e in se)
                slots[sid] = {"entries": se, "total": st_total,
                              "over_capacity": st_total > (st.get("max_units_per_day") or 9999)}
            res_days.append({"date": ds, "day_name": day_nms[i], "is_closed": ds in cd_set,
                             "total_planned": tp, "machine_slots": slots})

        all_sched = {r[0] for r in conn.execute(
            "SELECT order_item_id FROM schedule_entries WHERE zone_id=? AND order_item_id IS NOT NULL",
            [zid]).fetchall()}
        iq_raw = rows_to_list(conn.execute("""
            SELECT oi.*, o.order_number, o.status as order_status, o.is_stock_run,
                   o.requested_delivery_date, o.eta_date, c.company_name as client_name
            FROM order_items oi JOIN orders o ON o.id=oi.order_id
            LEFT JOIN clients c ON c.id=o.client_id
            WHERE oi.zone_id=? AND o.status NOT IN ('F','dispatched','delivered','collected')
              AND oi.status NOT IN ('F','dispatched')
        """, [zid]).fetchall())
        iq = [dict(it, inventory_on_hand=inv_mp.get(it.get("sku_id"), 0))
              for it in iq_raw if it["id"] not in all_sched]
        iq.sort(key=lambda x: (0 if x.get("requested_delivery_date") else 1, x.get("created_at", "") or ""))

        return {"status": 200, "body": {
            "zone": z_row, "week_start": week_start_p, "machines": stations_list,
            "days": res_days, "intake_queue": iq, "close_days": list(cd_set),
            "docking_queue": rows_to_list(conn.execute("""
                SELECT oi.*, o.order_number, o.status as order_status, o.is_stock_run,
                       o.requested_delivery_date, c.company_name as client_name
                FROM order_items oi
                JOIN orders o ON o.id=oi.order_id
                LEFT JOIN clients c ON c.id=o.client_id
                WHERE oi.zone_id=? AND oi.status='C'
            """, [zid]).fetchall())
        }}

    if method == "GET" and path == "/planning/dtl":
        return _planning_zone("DTL")

    if method == "GET" and path == "/planning/crates":
        return _planning_zone("CRT")

    # ----- DEBUG -----
    if method == "GET" and path == "/debug":
        return {"status": 200, "body": {"db_path": DB_PATH, "db_exists": os.path.exists(DB_PATH), "cwd": os.getcwd()}}

    # =========================================================================
    # DRIVER APP ENDPOINTS
    # =========================================================================

    # ----- DRIVER PIN LOGIN (PIN-only, no username needed) -----
    # NOTE (FIX 6 — PIN uniqueness): The users.pin column has no UNIQUE constraint.
    # Adding one would require a table migration and could break existing DBs with NULL PINs
    # (email-login users have pin=NULL, and UNIQUE in SQLite allows multiple NULLs).
    # Risk: if two users share the same PIN, the first row returned by the query below
    # wins. Ensure driver PINs are unique via application-level validation in user management.
    if method == "POST" and path == "/driver/pin-login":
        pin = body.get("pin", "").strip()
        if not pin or len(pin) != 6:
            return {"status": 400, "body": {"error": "6-digit PIN required"}}
        row = conn.execute("SELECT * FROM users WHERE pin=? AND is_active=1", [pin]).fetchone()
        if not row:
            return {"status": 401, "body": {"error": "Invalid PIN"}}
        user = row_to_dict(row)
        token = make_token(user["id"], user["role"])
        # Get default truck for this driver
        truck = conn.execute("SELECT * FROM trucks WHERE driver_name=? AND is_active=1",
                             [user["full_name"]]).fetchone()
        user.pop("password_hash", None)
        user.pop("pin", None)
        result = {"token": token, "user": user}
        if truck:
            result["default_truck"] = row_to_dict(truck)
        return {"status": 200, "body": result}

    # ----- DRIVER CLOCK ON -----
    if method == "POST" and path == "/driver/clock-on":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        truck_id = body.get("truck_id")
        safety_checks = body.get("safety_checks", {})
        if not truck_id:
            return {"status": 400, "body": {"error": "truck_id required"}}
        # Check no active shift
        active = conn.execute(
            "SELECT id FROM driver_shifts WHERE driver_id=? AND status='active'",
            [current_user["id"]]).fetchone()
        if active:
            return {"status": 409, "body": {"error": "Already clocked on", "shift_id": active[0]}}
        now = datetime.now(timezone.utc).isoformat()
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        cur = conn.execute(
            """INSERT INTO driver_shifts (driver_id, truck_id, shift_date, clock_on_time,
               safety_acknowledged, safety_acknowledged_at, safety_checklist, status)
               VALUES (?,?,?,?,1,?,?,?)""",
            [current_user["id"], truck_id, today, now, now,
             json.dumps(safety_checks), "active"])
        conn.commit()
        shift = row_to_dict(conn.execute("SELECT * FROM driver_shifts WHERE id=?",
                                         [cur.lastrowid]).fetchone())
        return {"status": 201, "body": shift}

    # ----- DRIVER CLOCK OFF -----
    if method == "POST" and path == "/driver/clock-off":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        shift = conn.execute(
            "SELECT * FROM driver_shifts WHERE driver_id=? AND status='active'",
            [current_user["id"]]).fetchone()
        if not shift:
            return {"status": 404, "body": {"error": "No active shift"}}
        shift_dict = row_to_dict(shift)
        now = datetime.now(timezone.utc).isoformat()
        # Calculate total hours
        try:
            clock_on = datetime.fromisoformat(shift_dict["clock_on_time"].replace("Z", "+00:00"))
            clock_off = datetime.fromisoformat(now.replace("Z", "+00:00"))
            total_hours = (clock_off - clock_on).total_seconds() / 3600
        except:
            total_hours = 0
        conn.execute(
            "UPDATE driver_shifts SET clock_off_time=?, status='completed', total_hours=? WHERE id=?",
            [now, round(total_hours, 2), shift_dict["id"]])
        conn.commit()
        updated = row_to_dict(conn.execute("SELECT * FROM driver_shifts WHERE id=?",
                                           [shift_dict["id"]]).fetchone())
        return {"status": 200, "body": updated}

    # ----- GET ACTIVE SHIFT -----
    if method == "GET" and path == "/driver/shift":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        shift = conn.execute(
            "SELECT ds.*, t.name as truck_name, t.rego as truck_rego FROM driver_shifts ds LEFT JOIN trucks t ON t.id=ds.truck_id WHERE ds.driver_id=? AND ds.status='active'",
            [current_user["id"]]).fetchone()
        if not shift:
            return {"status": 200, "body": None}
        return {"status": 200, "body": row_to_dict(shift)}

    # ----- GET SHIFT HISTORY -----
    if method == "GET" and path == "/driver/shift-history":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        rows = conn.execute(
            "SELECT ds.*, t.name as truck_name FROM driver_shifts ds LEFT JOIN trucks t ON t.id=ds.truck_id WHERE ds.driver_id=? ORDER BY ds.created_at DESC LIMIT 30",
            [current_user["id"]]).fetchall()
        return {"status": 200, "body": rows_to_list(rows)}

    # ----- GET DRIVER LOAD (deliveries for truck today) -----
    if method == "GET" and path == "/driver/load":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        truck_id = params.get("truck_id")
        date = params.get("date", datetime.now(timezone.utc).strftime("%Y-%m-%d"))
        if not truck_id:
            return {"status": 400, "body": {"error": "truck_id required"}}
        rows = conn.execute("""
            SELECT dl.*, o.order_number, o.status as order_status, o.delivery_type as order_delivery_type,
                   c.company_name as client_name, c.phone as client_phone,
                   da.street_address, da.suburb, da.state, da.postcode,
                   da.estimated_travel_minutes, da.estimated_return_minutes,
                   t.name as truck_name, t.driver_name
            FROM delivery_log dl
            LEFT JOIN orders o ON o.id = dl.order_id
            LEFT JOIN clients c ON c.id = o.client_id
            LEFT JOIN delivery_addresses da ON da.client_id = o.client_id AND da.is_default = 1
            LEFT JOIN trucks t ON t.id = dl.truck_id
            WHERE dl.truck_id = ? AND dl.expected_date = ?
            ORDER BY dl.load_sequence ASC, dl.id ASC
        """, [truck_id, date]).fetchall()
        deliveries = []
        for r in rows_to_list(rows):
            # Get order items for each delivery
            if r.get("order_id"):
                items = rows_to_list(conn.execute(
                    "SELECT oi.*, s.name as sku_name FROM order_items oi LEFT JOIN skus s ON s.id=oi.sku_id WHERE oi.order_id=? ORDER BY oi.id",
                    [r["order_id"]]).fetchall())
                r["items"] = items
                r["total_qty"] = sum(it.get("quantity", 0) for it in items)
            else:
                r["items"] = []
                r["total_qty"] = 0
            # Get stages for this delivery
            stages = rows_to_list(conn.execute(
                "SELECT * FROM delivery_run_stages WHERE delivery_log_id=? ORDER BY started_at",
                [r["id"]]).fetchall())
            r["stages"] = stages
            deliveries.append(r)
        return {"status": 200, "body": deliveries}

    # ----- GET UPCOMING RUNS -----
    if method == "GET" and path == "/driver/upcoming":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        truck_id = params.get("truck_id")
        date = params.get("date", datetime.now(timezone.utc).strftime("%Y-%m-%d"))
        if not truck_id:
            return {"status": 400, "body": {"error": "truck_id required"}}
        rows = conn.execute("""
            SELECT dl.*, o.order_number, c.company_name as client_name,
                   da.street_address, da.suburb, da.state, da.postcode,
                   da.estimated_travel_minutes
            FROM delivery_log dl
            LEFT JOIN orders o ON o.id = dl.order_id
            LEFT JOIN clients c ON c.id = o.client_id
            LEFT JOIN delivery_addresses da ON da.client_id = o.client_id AND da.is_default = 1
            WHERE dl.truck_id = ? AND dl.expected_date >= ? AND dl.status = 'pending'
            ORDER BY dl.expected_date ASC, dl.load_sequence ASC
        """, [truck_id, date]).fetchall()
        return {"status": 200, "body": rows_to_list(rows)}

    # ----- START STAGE -----
    if method == "POST" and path == "/driver/stage/start":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        delivery_log_id = body.get("delivery_log_id")
        stage = body.get("stage")
        shift_id = body.get("shift_id")
        if not stage or not shift_id:
            return {"status": 400, "body": {"error": "stage and shift_id required"}}
        now = datetime.now(timezone.utc).isoformat()
        stop_number = body.get("stop_number", 1)
        cur = conn.execute(
            """INSERT INTO delivery_run_stages
               (delivery_log_id, driver_shift_id, stage, started_at, location_lat, location_lng, stop_number)
               VALUES (?,?,?,?,?,?,?)""",
            [delivery_log_id, shift_id, stage, now,
             body.get("lat"), body.get("lng"), stop_number])
        conn.commit()
        row = row_to_dict(conn.execute("SELECT * FROM delivery_run_stages WHERE id=?",
                                       [cur.lastrowid]).fetchone())
        return {"status": 201, "body": row}

    # ----- END STAGE -----
    if method == "POST" and path == "/driver/stage/end":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        stage_id = body.get("stage_id")
        if not stage_id:
            return {"status": 400, "body": {"error": "stage_id required"}}
        stage_row = conn.execute("SELECT * FROM delivery_run_stages WHERE id=?", [stage_id]).fetchone()
        if not stage_row:
            return {"status": 404, "body": {"error": "Stage not found"}}
        stage_dict = row_to_dict(stage_row)
        now = datetime.now(timezone.utc).isoformat()
        try:
            started = datetime.fromisoformat(stage_dict["started_at"].replace("Z", "+00:00"))
            ended = datetime.fromisoformat(now.replace("Z", "+00:00"))
            duration = (ended - started).total_seconds() / 60
        except:
            duration = 0
        conn.execute(
            "UPDATE delivery_run_stages SET ended_at=?, duration_minutes=? WHERE id=?",
            [now, round(duration, 2), stage_id])
        conn.commit()
        updated = row_to_dict(conn.execute("SELECT * FROM delivery_run_stages WHERE id=?",
                                           [stage_id]).fetchone())
        return {"status": 200, "body": updated}

    # ----- GET STAGES FOR DELIVERY -----
    if method == "GET" and path == "/driver/stages":
        delivery_log_id = params.get("delivery_log_id")
        shift_id = params.get("shift_id")
        if delivery_log_id:
            rows = conn.execute(
                "SELECT * FROM delivery_run_stages WHERE delivery_log_id=? ORDER BY started_at",
                [delivery_log_id]).fetchall()
        elif shift_id:
            rows = conn.execute(
                "SELECT * FROM delivery_run_stages WHERE driver_shift_id=? ORDER BY started_at",
                [shift_id]).fetchall()
        else:
            return {"status": 400, "body": {"error": "delivery_log_id or shift_id required"}}
        return {"status": 200, "body": rows_to_list(rows)}

    # ----- START/END BREAK -----
    if method == "POST" and path == "/driver/break/start":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        shift_id = body.get("shift_id")
        now = datetime.now(timezone.utc).isoformat()
        cur = conn.execute(
            """INSERT INTO delivery_run_stages
               (delivery_log_id, driver_shift_id, stage, started_at, location_lat, location_lng)
               VALUES (?,?,?,?,?,?)""",
            [body.get("delivery_log_id"), shift_id, "break", now,
             body.get("lat"), body.get("lng")])
        conn.commit()
        return {"status": 201, "body": row_to_dict(
            conn.execute("SELECT * FROM delivery_run_stages WHERE id=?", [cur.lastrowid]).fetchone())}

    if method == "POST" and path == "/driver/break/end":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        stage_id = body.get("stage_id")
        if not stage_id:
            return {"status": 400, "body": {"error": "stage_id required"}}
        now = datetime.now(timezone.utc).isoformat()
        stage_row = conn.execute("SELECT * FROM delivery_run_stages WHERE id=?", [stage_id]).fetchone()
        if not stage_row:
            return {"status": 404, "body": {"error": "Break stage not found"}}
        sd = row_to_dict(stage_row)
        try:
            started = datetime.fromisoformat(sd["started_at"].replace("Z", "+00:00"))
            ended = datetime.fromisoformat(now.replace("Z", "+00:00"))
            duration = (ended - started).total_seconds() / 60
        except:
            duration = 0
        conn.execute("UPDATE delivery_run_stages SET ended_at=?, duration_minutes=? WHERE id=?",
                     [now, round(duration, 2), stage_id])
        conn.commit()
        return {"status": 200, "body": row_to_dict(
            conn.execute("SELECT * FROM delivery_run_stages WHERE id=?", [stage_id]).fetchone())}

    # ----- UPDATE DELIVERY STATUS -----
    if method == "PUT" and path == "/driver/delivery/status":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        dl_id = body.get("delivery_log_id")
        new_status = body.get("status")
        if not dl_id or not new_status:
            return {"status": 400, "body": {"error": "delivery_log_id and status required"}}
        conn.execute("UPDATE delivery_log SET status=?, updated_at=? WHERE id=?",
                     [new_status, datetime.now(timezone.utc).isoformat(), dl_id])
        # Also update the order status if delivery is complete
        if new_status in ("delivered", "collected"):
            dl_row = conn.execute("SELECT order_id FROM delivery_log WHERE id=?", [dl_id]).fetchone()
            if dl_row and dl_row[0]:
                conn.execute("UPDATE orders SET status=?, dispatched_at=? WHERE id=?",
                             [new_status, datetime.now(timezone.utc).isoformat(), dl_row[0]])
        conn.commit()
        return {"status": 200, "body": {"ok": True}}

    # ----- COMPLETE DELIVERY (triggers cost calc) -----
    if method == "POST" and path == "/driver/delivery/complete":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        dl_id = body.get("delivery_log_id")
        shift_id = body.get("shift_id")
        total_km = body.get("total_km", 0)
        tolls = body.get("tolls", 0)
        if not dl_id or not shift_id:
            return {"status": 400, "body": {"error": "delivery_log_id and shift_id required"}}
        # Calculate cost
        shift = conn.execute("SELECT * FROM driver_shifts WHERE id=?", [shift_id]).fetchone()
        if not shift:
            return {"status": 404, "body": {"error": "Shift not found"}}
        shift_d = row_to_dict(shift)
        finance = conn.execute("SELECT * FROM truck_finance_config WHERE truck_id=?",
                               [shift_d["truck_id"]]).fetchone()
        # Sum stage durations for this delivery
        total_mins = conn.execute(
            "SELECT COALESCE(SUM(duration_minutes), 0) FROM delivery_run_stages WHERE delivery_log_id=? AND driver_shift_id=?",
            [dl_id, shift_id]).fetchone()[0]
        costs = {"driver_cost": 0, "fuel_cost": 0, "rego_cost": 0, "insurance_cost": 0,
                 "rm_cost": 0, "tyre_cost": 0, "tolls": tolls, "total_cost": 0}
        if finance:
            f = row_to_dict(finance)
            hours = total_mins / 60 if total_mins else 0
            costs["driver_cost"] = round(hours * f["driver_hourly_rate"], 2)
            costs["fuel_cost"] = round((total_km / 100) * f["avg_fuel_consumption_per_100km"] * f["fuel_cost_per_litre"], 2) if total_km else 0
            op_days = f["operating_days_per_year"] or 230
            costs["rego_cost"] = round(f["annual_rego_cost"] / op_days, 2)
            costs["insurance_cost"] = round(f["annual_insurance_cost"] / op_days, 2)
            costs["rm_cost"] = round(f["rm_budget_monthly"] / (op_days / 12), 2)
            costs["tyre_cost"] = round(total_km * f["tyre_cost_per_km"], 2) if total_km else 0
        costs["total_cost"] = round(sum([costs["driver_cost"], costs["fuel_cost"], costs["rego_cost"],
                                         costs["insurance_cost"], costs["rm_cost"], costs["tyre_cost"],
                                         costs["tolls"]]), 2)
        # Insert cost record
        conn.execute("""INSERT INTO delivery_run_costs
            (delivery_log_id, driver_shift_id, driver_cost, fuel_cost, rego_cost, insurance_cost,
             rm_cost, tyre_cost, tolls, total_cost, total_km, total_minutes)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            [dl_id, shift_id, costs["driver_cost"], costs["fuel_cost"], costs["rego_cost"],
             costs["insurance_cost"], costs["rm_cost"], costs["tyre_cost"], costs["tolls"],
             costs["total_cost"], total_km, total_mins])
        # Check if this is a collection or delivery
        dl_row = conn.execute("SELECT delivery_type FROM delivery_log WHERE id=?", [dl_id]).fetchone()
        final_status = "collected" if dl_row and dl_row[0] == "collection" else "delivered"
        conn.execute("UPDATE delivery_log SET status=?, actual_date=?, updated_at=? WHERE id=?",
                     [final_status, datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                      datetime.now(timezone.utc).isoformat(), dl_id])
        # Update the parent order status too
        dl_order = conn.execute("SELECT order_id FROM delivery_log WHERE id=?", [dl_id]).fetchone()
        if dl_order and dl_order[0]:
            conn.execute("UPDATE orders SET status=?, dispatched_at=? WHERE id=?",
                         [final_status, datetime.now(timezone.utc).isoformat(), dl_order[0]])
        conn.commit()
        return {"status": 200, "body": costs}

    # ----- TRUCK FINANCE CONFIG -----
    if method == "GET" and path == "/truck-finance":
        truck_id = params.get("truck_id")
        if truck_id:
            row = conn.execute("SELECT tf.*, t.name as truck_name FROM truck_finance_config tf LEFT JOIN trucks t ON t.id=tf.truck_id WHERE tf.truck_id=?",
                               [truck_id]).fetchone()
            if not row:
                return {"status": 404, "body": {"error": "Finance config not found"}}
            return {"status": 200, "body": row_to_dict(row)}
        rows = conn.execute("SELECT tf.*, t.name as truck_name FROM truck_finance_config tf LEFT JOIN trucks t ON t.id=tf.truck_id ORDER BY tf.truck_id").fetchall()
        return {"status": 200, "body": rows_to_list(rows)}

    if method == "PUT" and path == "/truck-finance":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        truck_id = body.get("truck_id")
        if not truck_id:
            return {"status": 400, "body": {"error": "truck_id required"}}
        fields = ["driver_hourly_rate", "fuel_cost_per_litre", "avg_fuel_consumption_per_100km",
                  "annual_rego_cost", "annual_insurance_cost", "rm_budget_monthly",
                  "tyre_cost_per_km", "operating_days_per_year", "running_cost_per_hour",
                  "running_cost_per_km", "notes"]
        updates = []
        vals = []
        for f in fields:
            if f in body:
                updates.append(f"{f}=?")
                vals.append(body[f])
        if updates:
            vals.append(datetime.now(timezone.utc).isoformat())
            vals.append(truck_id)
            conn.execute(f"UPDATE truck_finance_config SET {', '.join(updates)}, updated_at=? WHERE truck_id=?", vals)
            conn.commit()
        row = conn.execute("SELECT * FROM truck_finance_config WHERE truck_id=?", [truck_id]).fetchone()
        return {"status": 200, "body": row_to_dict(row) if row else {}}

    # ----- DELIVERY COSTS -----
    if method == "GET" and path == "/delivery-costs":
        dl_id = params.get("delivery_log_id")
        if dl_id:
            row = conn.execute("SELECT * FROM delivery_run_costs WHERE delivery_log_id=?", [dl_id]).fetchone()
            return {"status": 200, "body": row_to_dict(row) if row else None}
        shift_id = params.get("shift_id")
        if shift_id:
            rows = conn.execute("SELECT * FROM delivery_run_costs WHERE driver_shift_id=?", [shift_id]).fetchall()
            return {"status": 200, "body": rows_to_list(rows)}
        return {"status": 400, "body": {"error": "delivery_log_id or shift_id required"}}

    # ----- TRUCKS LIST (enhanced for driver app) -----
    if method == "GET" and path == "/driver/trucks":
        rows = conn.execute("SELECT * FROM trucks WHERE is_active=1 ORDER BY id").fetchall()
        return {"status": 200, "body": rows_to_list(rows)}

    # ----- REPORT INCIDENT -----
    if method == "POST" and path == "/driver/incident":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        shift_id = body.get("shift_id")
        incident_type = body.get("incident_type")
        description = body.get("description", "")
        if not shift_id or not incident_type:
            return {"status": 400, "body": {"error": "shift_id and incident_type required"}}
        now = datetime.now(timezone.utc).isoformat()
        cur = conn.execute(
            """INSERT INTO driver_incidents (driver_shift_id, delivery_log_id, incident_type, description, photo_data, location_lat, location_lng, reported_at)
               VALUES (?,?,?,?,?,?,?,?)""",
            [shift_id, body.get("delivery_log_id"), incident_type, description,
             body.get("photo_data"), body.get("lat"), body.get("lng"), now])
        conn.commit()
        return {"status": 201, "body": row_to_dict(conn.execute("SELECT * FROM driver_incidents WHERE id=?", [cur.lastrowid]).fetchone())}

    # ----- GET DRIVER RUN SHEET -----
    if method == "GET" and path == "/driver/runsheet":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        truck_id = params.get("truck_id")
        date = params.get("date", datetime.now(timezone.utc).strftime("%Y-%m-%d"))
        if not truck_id:
            return {"status": 400, "body": {"error": "truck_id required"}}
        rows = conn.execute("""
            SELECT dl.*, o.order_number, o.delivery_type as order_delivery_type,
                   c.company_name as client_name,
                   da.street_address, da.suburb, da.state, da.postcode,
                   da.estimated_travel_minutes, da.estimated_return_minutes
            FROM delivery_log dl
            LEFT JOIN orders o ON o.id = dl.order_id
            LEFT JOIN clients c ON c.id = o.client_id
            LEFT JOIN delivery_addresses da ON da.client_id = o.client_id AND da.is_default = 1
            WHERE dl.truck_id = ? AND dl.expected_date = ?
            ORDER BY dl.load_sequence ASC, dl.id ASC
        """, [truck_id, date]).fetchall()
        stops = []
        cumulative_mins = 0
        for r in rows_to_list(rows):
            travel = r.get("estimated_travel_minutes") or r.get("estimated_minutes") or 30
            site_time = 30  # default 30 min at site
            cumulative_mins += travel + site_time
            r["cumulative_minutes"] = cumulative_mins
            r["estimated_arrival_minutes"] = cumulative_mins - site_time
            items = rows_to_list(conn.execute(
                "SELECT oi.sku_code, oi.product_name, oi.quantity FROM order_items oi WHERE oi.order_id=? ORDER BY oi.id",
                [r.get("order_id")]).fetchall()) if r.get("order_id") else []
            r["items"] = items
            r["total_qty"] = sum(it.get("quantity", 0) for it in items)
            stops.append(r)
        return {"status": 200, "body": {"stops": stops, "total_stops": len(stops), "total_estimated_minutes": cumulative_mins}}

    # 404
    return {"status": 404, "body": {"error": f"Route not found: {method} {path}"}}


# ---------------------------------------------------------------------------
# Init and run
# ---------------------------------------------------------------------------

# Always init DB on import (gunicorn imports the module, doesn't run __main__)
init_db()
migrate_db()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
