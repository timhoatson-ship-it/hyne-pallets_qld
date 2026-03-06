#!/usr/bin/env python3
"""
server.py - Flask server for Hyne Pallets Manufacturing Management System
Wraps the existing CGI handler logic for deployment on Railway/Render/etc.
"""

import os
import sys
import json
import logging
import base64
import hashlib
import re
import sqlite3
import hmac
import time
from datetime import datetime, timezone, timedelta
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------

app = Flask(__name__, static_folder="static", static_url_path="")
CORS(app, origins=[
    os.environ.get("CORS_ORIGIN", "https://web-production-8779e.up.railway.app"),  # Set CORS_ORIGIN env var in Railway
    "https://web-production-8779e.up.railway.app",
])

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data.db")

# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def safe_int(val, default=0):
    try:
        return int(val)
    except (TypeError, ValueError):
        return default

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def get_connection():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA busy_timeout = 5000")
    return conn


def hash_password(password):
    """Hash password using werkzeug's PBKDF2 (with per-user salt + work factor)."""
    from werkzeug.security import generate_password_hash
    return generate_password_hash(password, method='pbkdf2:sha256', salt_length=16)


def check_password(stored_hash, password):
    """Verify password against stored hash. Supports both new werkzeug and legacy SHA256."""
    from werkzeug.security import check_password_hash
    if stored_hash and stored_hash.startswith("pbkdf2:"):
        return check_password_hash(stored_hash, password)
    # Legacy SHA256 fallback for existing accounts
    legacy = hashlib.sha256((password + "hyne_salt").encode()).hexdigest()
    return stored_hash == legacy


def row_to_dict(row):
    if row is None:
        return None
    return dict(row)


def rows_to_list(rows):
    return [dict(r) for r in rows]


def send_email_smtp(to_email, subject, body_text, body_html=None):
    """Send an email via SMTP. Returns (success: bool, error_msg: str or None)."""
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart

    try:
        conn_db = get_connection()
        cfg = conn_db.execute("SELECT * FROM email_config WHERE is_active=1 LIMIT 1").fetchone()
        conn_db.close()
        if not cfg:
            return (False, "Email not configured — SMTP settings not active")
        cfg = dict(cfg)

        if not cfg.get("smtp_host") or not cfg.get("smtp_user"):
            return (False, "SMTP host and user are required")

        msg = MIMEMultipart("alternative")
        msg["From"] = f"{cfg.get('from_name', 'Hyne Pallets')} <{cfg.get('from_email', cfg['smtp_user'])}>"
        msg["To"] = to_email
        msg["Subject"] = subject

        msg.attach(MIMEText(body_text, "plain"))
        if body_html:
            msg.attach(MIMEText(body_html, "html"))

        server = smtplib.SMTP(cfg["smtp_host"], int(cfg.get("smtp_port", 587)))
        if cfg.get("smtp_use_tls", 1):
            server.starttls()
        server.login(cfg["smtp_user"], _smtp_decrypt(cfg.get("smtp_password", "")))
        server.send_message(msg)
        server.quit()
        return (True, None)
    except Exception as e:
        return (False, str(e))



def send_email_smtp_async(*args, **kwargs):
    """Send email in background thread to avoid blocking HTTP response."""
    import threading
    t = threading.Thread(target=send_email_smtp, args=args, kwargs=kwargs, daemon=True)
    t.start()

def log_and_send_notification(conn, order_id, notification_type, recipient_email, subject, body_text, body_html=None):
    """Insert notification record and attempt SMTP delivery."""
    now = datetime.now(timezone.utc).isoformat()
    cur = conn.execute("""INSERT INTO notification_log
        (order_id, notification_type, recipient_email, subject, body, status, delivery_status)
        VALUES (?,?,?,?,?,?,?)""",
        [order_id, notification_type, recipient_email, subject, body_text, "queued", "queued"])
    conn.commit()
    notif_id = cur.lastrowid

    # Attempt SMTP delivery
    send_email_smtp_async(recipient_email, subject, body_text, body_html)
    success, err = True, None  # Async — assume queued OK
    if success:
        conn.execute("UPDATE notification_log SET status='sent', delivery_status='delivered', delivered_at=?, attempted_at=? WHERE id=?",
            [now, now, notif_id])
    else:
        conn.execute("UPDATE notification_log SET status='failed', delivery_status='failed', error_message=?, attempted_at=? WHERE id=?",
            [err, now, notif_id])
    conn.commit()
    return notif_id


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
    setup_type TEXT CHECK(setup_type IN ('machine','jig','machine_setup','jig_setup')),
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
    reason TEXT NOT NULL CHECK(reason IN ('wait_material','tool_breakdown','machine_fault','lunch','smoko_break','qa_hold','waiting_instructions','other')),
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

CREATE TABLE IF NOT EXISTS drawing_files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sku_id INTEGER REFERENCES skus(id),
    order_item_id INTEGER REFERENCES order_items(id),
    file_name TEXT NOT NULL,
    file_type TEXT CHECK(file_type IN ('pdf','image')),
    file_data TEXT NOT NULL,
    uploaded_by INTEGER REFERENCES users(id),
    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    notes TEXT
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
    status TEXT DEFAULT 'queued',
    delivery_status TEXT DEFAULT 'queued',
    delivered_at TIMESTAMP,
    attempted_at TIMESTAMP,
    error_message TEXT
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

CREATE TABLE IF NOT EXISTS login_attempts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    identifier TEXT NOT NULL,
    attempt_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    success BOOLEAN DEFAULT 0
);

CREATE TABLE IF NOT EXISTS schedule_entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id INTEGER REFERENCES orders(id),
    order_item_id INTEGER REFERENCES order_items(id),
    zone_id INTEGER NOT NULL REFERENCES zones(id),
    station_id INTEGER REFERENCES stations(id),
    planned_station_id INTEGER REFERENCES stations(id),
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
    run_id INTEGER REFERENCES dispatch_runs(id),
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

CREATE TABLE IF NOT EXISTS dispatch_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    truck_id INTEGER NOT NULL REFERENCES trucks(id),
    run_date TEXT NOT NULL,
    run_number INTEGER NOT NULL DEFAULT 1,
    driver_id INTEGER REFERENCES users(id),
    status TEXT DEFAULT 'planned' CHECK(status IN ('planned','loading','in_transit','completed','cancelled')),
    departure_time TEXT,
    return_time TEXT,
    notes TEXT,
    created_by INTEGER REFERENCES users(id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(truck_id, run_date, run_number)
);

CREATE TABLE IF NOT EXISTS qa_audits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    station_id INTEGER,
    zone_id INTEGER,
    auditor_id INTEGER REFERENCES users(id),
    order_item_id INTEGER REFERENCES order_items(id),
    session_id INTEGER REFERENCES production_sessions(id),
    audit_type TEXT DEFAULT 'spot_check' CHECK(audit_type IN ('spot_check','scheduled','random')),
    batch_size INTEGER DEFAULT 1,
    passed INTEGER DEFAULT 0,
    notes TEXT,
    photos TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS production_log_summary (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    zone_id INTEGER NOT NULL REFERENCES zones(id),
    log_date TEXT NOT NULL,
    station_id INTEGER REFERENCES stations(id),
    total_planned INTEGER DEFAULT 0,
    total_produced INTEGER DEFAULT 0,
    total_sessions INTEGER DEFAULT 0,
    total_labour_minutes REAL DEFAULT 0,
    summary_json TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(zone_id, log_date, station_id)
);

-- ===== TIMBER INVENTORY TABLES (Block 5) =====

CREATE TABLE IF NOT EXISTS timber_suppliers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    abn TEXT,
    contact_name TEXT,
    contact_email TEXT,
    contact_phone TEXT,
    default_terms TEXT,
    is_active BOOLEAN DEFAULT 1,
    approval_status TEXT DEFAULT 'approved',
    created_by TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS timber_specs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    myob_code TEXT UNIQUE,
    type_prefix TEXT NOT NULL,
    grade_codes TEXT,
    width_mm INTEGER,
    thickness_mm INTEGER,
    length_mm INTEGER,
    suffix_flags TEXT,
    description TEXT NOT NULL,
    is_active BOOLEAN DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS timber_grade_codes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT UNIQUE NOT NULL,
    full_name TEXT NOT NULL,
    description TEXT,
    is_active BOOLEAN DEFAULT 1
);

CREATE TABLE IF NOT EXISTS timber_deliveries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    supplier_id INTEGER REFERENCES timber_suppliers(id),
    delivery_date DATE NOT NULL,
    docket_number TEXT,
    docket_photo_path TEXT,
    ocr_raw_text TEXT,
    status TEXT DEFAULT 'pending',
    notes TEXT,
    created_by TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS timber_delivery_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    delivery_id INTEGER REFERENCES timber_deliveries(id),
    spec_id INTEGER REFERENCES timber_specs(id),
    description TEXT,
    expected_packs INTEGER NOT NULL DEFAULT 0,
    assigned_packs INTEGER NOT NULL DEFAULT 0,
    pcs_per_pack INTEGER,
    cost_per_m3 REAL,
    total_amount REAL,
    lineal_metres_per_pack REAL,
    status TEXT DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS timber_packs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    qr_code TEXT UNIQUE,
    delivery_item_id INTEGER REFERENCES timber_delivery_items(id),
    spec_id INTEGER NOT NULL REFERENCES timber_specs(id),
    supplier_id INTEGER NOT NULL REFERENCES timber_suppliers(id),
    received_date DATE NOT NULL,
    received_by TEXT,
    pcs_per_pack INTEGER,
    m3_volume REAL NOT NULL,
    lineal_metres REAL,
    cost_per_m3 REAL,
    pack_cost_total REAL,
    status TEXT DEFAULT 'inventory',
    yard_zone_id INTEGER,
    pack_type TEXT DEFAULT 'full',
    assigned_worker TEXT,
    destination_zone TEXT,
    is_test_data BOOLEAN DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    consumed_at TIMESTAMP,
    consumed_by TEXT
);

CREATE TABLE IF NOT EXISTS timber_consumption (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pack_id INTEGER NOT NULL REFERENCES timber_packs(id),
    consumed_by TEXT NOT NULL,
    consumed_by_user_id INTEGER,
    consumed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    destination TEXT,
    destination_zone TEXT,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS timber_consumption_undo (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pack_id INTEGER NOT NULL REFERENCES timber_packs(id),
    original_consumption_id INTEGER,
    undone_by TEXT NOT NULL,
    undone_by_user_id INTEGER,
    undone_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    reason TEXT
);

CREATE TABLE IF NOT EXISTS timber_chainsaw_allocations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pack_id INTEGER NOT NULL REFERENCES timber_packs(id),
    allocated_by TEXT NOT NULL,
    allocated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status TEXT DEFAULT 'allocated'
);

CREATE TABLE IF NOT EXISTS timber_cost_imports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    import_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    imported_by TEXT NOT NULL,
    file_name TEXT,
    period_month INTEGER,
    period_year INTEGER,
    status TEXT DEFAULT 'pending',
    total_rows INTEGER DEFAULT 0,
    matched_rows INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS timber_cost_import_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    import_id INTEGER REFERENCES timber_cost_imports(id),
    myob_code TEXT,
    supplier_name TEXT,
    date TEXT,
    quantity REAL,
    description TEXT,
    amount REAL,
    tax TEXT,
    status_field TEXT,
    mapped_pack_id INTEGER
);

CREATE TABLE IF NOT EXISTS timber_stocktakes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stocktake_date DATE NOT NULL,
    conducted_by TEXT,
    status TEXT DEFAULT 'in_progress',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS timber_stocktake_counts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stocktake_id INTEGER REFERENCES timber_stocktakes(id),
    spec_id INTEGER REFERENCES timber_specs(id),
    supplier_id INTEGER REFERENCES timber_suppliers(id),
    system_packs INTEGER,
    system_m3 REAL,
    physical_packs INTEGER,
    physical_m3 REAL,
    variance_packs INTEGER,
    variance_m3 REAL,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS timber_low_stock_alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    spec_id INTEGER REFERENCES timber_specs(id),
    threshold_value REAL NOT NULL,
    threshold_unit TEXT DEFAULT 'm3',
    is_active BOOLEAN DEFAULT 1
);

CREATE TABLE IF NOT EXISTS timber_alert_recipients (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT NOT NULL,
    name TEXT,
    is_active BOOLEAN DEFAULT 1
);

CREATE TABLE IF NOT EXISTS timber_yard_zones (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    zone_name TEXT NOT NULL,
    description TEXT,
    is_active BOOLEAN DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS timber_pack_children (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    parent_pack_id INTEGER REFERENCES timber_packs(id),
    child_spec_id INTEGER REFERENCES timber_specs(id),
    docked_length_mm INTEGER,
    quantity INTEGER,
    m3_volume REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS timber_myob_accounts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_code TEXT NOT NULL,
    account_name TEXT NOT NULL,
    category TEXT
);

CREATE TABLE IF NOT EXISTS timber_config (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    description TEXT
);

CREATE TABLE IF NOT EXISTS timber_supplier_approvals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    supplier_name TEXT NOT NULL,
    abn TEXT,
    contact_name TEXT,
    contact_email TEXT,
    contact_phone TEXT,
    requested_by TEXT NOT NULL,
    requested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    approved_by TEXT,
    approved_at TIMESTAMP,
    status TEXT DEFAULT 'pending'
);
""")
    conn.commit()

    # Seed data
    if c.execute("SELECT COUNT(*) FROM zones").fetchone()[0] == 0:
        c.executemany("INSERT OR IGNORE INTO zones (name, code, capacity_metric) VALUES (?, ?, ?)", [
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
        c.executemany("INSERT OR IGNORE INTO stations (zone_id, name, code, station_type) VALUES (?,?,?,?)", stations)
        conn.commit()

    if c.execute("SELECT COUNT(*) FROM shifts").fetchone()[0] == 0:
        c.executemany("INSERT OR IGNORE INTO shifts (name, start_time, end_time) VALUES (?,?,?)", [
            ("Day Shift", "06:00", "14:00"),
            ("Night Shift", "14:00", "22:00"),
        ])
        conn.commit()

    if c.execute("SELECT COUNT(*) FROM trucks").fetchone()[0] == 0:
        c.executemany("INSERT OR IGNORE INTO trucks (name, rego, driver_name, truck_type) VALUES (?,?,?,?)", [
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
        admin_pw = hash_password(os.environ.get("ADMIN_DEFAULT_PW", "admin123"))
        default_pw = hash_password(os.environ.get("DEFAULT_USER_PW", "password123"))
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
        c.executemany("INSERT OR REPLACE INTO users (email, password_hash, pin, username, full_name, role) VALUES (?,?,?,?,?,?)", users)
        conn.commit()

    if c.execute("SELECT COUNT(*) FROM target_labour_rates").fetchone()[0] == 0:
        c.execute("INSERT OR IGNORE INTO target_labour_rates (user_id, rate_per_hour, is_default, notes) VALUES (NULL, 55.00, 1, 'Global default rate')")
        conn.commit()

    if c.execute("SELECT COUNT(*) FROM post_production_processes").fetchone()[0] == 0:
        c.executemany("INSERT OR IGNORE INTO post_production_processes (name, requires_dashboard, triggers_notification, assigned_role) VALUES (?,?,?,?)", [
            ("Heat Treatment", 1, 1, "production_manager"),
            ("CCA Treatment", 1, 1, "production_manager"),
            ("Painting", 1, 0, "floor_worker"),
            ("Stencilling", 1, 0, "floor_worker"),
        ])
        conn.commit()

    if c.execute("SELECT COUNT(*) FROM accounting_config").fetchone()[0] == 0:
        c.execute("INSERT OR IGNORE INTO accounting_config (provider, sync_interval_minutes, is_connected) VALUES ('mock', 5, 1)")
        conn.commit()

    if c.execute("SELECT COUNT(*) FROM clients").fetchone()[0] == 0:
        c.executemany("INSERT OR IGNORE INTO clients (company_name, contact_name, email, phone) VALUES (?,?,?,?)", [
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
        c.executemany("INSERT OR IGNORE INTO skus (code, name, drawing_number, labour_cost, material_cost, sell_price, zone_id) VALUES (?,?,?,?,?,?,?)", skus)
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
    if 'planned_station_id' not in se_cols:
        c.execute("ALTER TABLE schedule_entries ADD COLUMN planned_station_id INTEGER")
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
    try:
        c.execute("ALTER TABLE order_items ADD COLUMN cut_list_issued INTEGER DEFAULT 0")
    except Exception:
        pass
    if 'requested_delivery_date' not in oi_cols:
        c.execute("ALTER TABLE order_items ADD COLUMN requested_delivery_date TEXT")
    # Add progress column to orders
    if 'progress' not in existing_cols:
        c.execute("ALTER TABLE orders ADD COLUMN progress TEXT")
    if 'needs_reverify' not in existing_cols:
        c.execute("ALTER TABLE orders ADD COLUMN needs_reverify INTEGER DEFAULT 0")
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
            except Exception: pass
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
        except Exception as _mig_err:
            logging.debug('Migration note: %s', _mig_err)

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

    # Block 2 migrations
    try:
        c.execute("SELECT needs_reverify FROM setup_logs LIMIT 1")
    except Exception:         c.execute("ALTER TABLE setup_logs ADD COLUMN needs_reverify INTEGER DEFAULT 0")
    try:
        c.execute("SELECT qa_checklist_json FROM setup_logs LIMIT 1")
    except Exception:         c.execute("ALTER TABLE setup_logs ADD COLUMN qa_checklist_json TEXT")
    try:
        c.execute("SELECT is_sub_assembly_mode FROM production_sessions LIMIT 1")
    except Exception:         c.execute("ALTER TABLE production_sessions ADD COLUMN is_sub_assembly_mode INTEGER DEFAULT 0")
    try:
        c.execute("SELECT sub_assembly_count FROM production_sessions LIMIT 1")
    except Exception:         c.execute("ALTER TABLE production_sessions ADD COLUMN sub_assembly_count INTEGER DEFAULT 0")
    try:
        c.execute("SELECT status FROM post_production_log LIMIT 1")
    except Exception:         c.execute("ALTER TABLE post_production_log ADD COLUMN status TEXT DEFAULT 'pending'")
    try:
        c.execute("SELECT description FROM post_production_processes LIMIT 1")
    except Exception:         c.execute("ALTER TABLE post_production_processes ADD COLUMN description TEXT")
    try:
        c.execute("SELECT quantity FROM post_production_log LIMIT 1")
    except Exception:         c.execute("ALTER TABLE post_production_log ADD COLUMN quantity INTEGER")

    c.executescript("""
    CREATE TABLE IF NOT EXISTS qa_audits (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        station_id INTEGER,
        zone_id INTEGER,
        auditor_id INTEGER REFERENCES users(id),
        order_item_id INTEGER REFERENCES order_items(id),
        session_id INTEGER REFERENCES production_sessions(id),
        audit_type TEXT DEFAULT 'spot_check' CHECK(audit_type IN ('spot_check','scheduled','random')),
        batch_size INTEGER DEFAULT 1,
        passed INTEGER DEFAULT 0,
        notes TEXT,
        photos TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS production_log_summary (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        zone_id INTEGER NOT NULL REFERENCES zones(id),
        log_date TEXT NOT NULL,
        station_id INTEGER REFERENCES stations(id),
        total_planned INTEGER DEFAULT 0,
        total_produced INTEGER DEFAULT 0,
        total_sessions INTEGER DEFAULT 0,
        total_labour_minutes REAL DEFAULT 0,
        summary_json TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(zone_id, log_date, station_id)
    );
    """)
    conn.commit()

    # --- Block 3: Add odometer + photo fields to delivery_run_stages ---
    for col_sql in [
        "ALTER TABLE delivery_run_stages ADD COLUMN odometer_start REAL",
        "ALTER TABLE delivery_run_stages ADD COLUMN odometer_end REAL",
        "ALTER TABLE delivery_run_stages ADD COLUMN manual_km REAL",
        "ALTER TABLE delivery_run_stages ADD COLUMN photo_data TEXT",
        "ALTER TABLE delivery_run_stages ADD COLUMN gps_accuracy REAL",
    ]:
        try:
            c.execute(col_sql)
            conn.commit()
        except Exception as _mig_err:
            logging.debug('Migration note: %s', _mig_err)

    c.execute("""
        CREATE TABLE IF NOT EXISTS delivery_photos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            delivery_log_id INTEGER REFERENCES delivery_log(id),
            driver_shift_id INTEGER NOT NULL REFERENCES driver_shifts(id),
            photo_type TEXT NOT NULL CHECK(photo_type IN ('pod','damage','load','pre_trip','incident','other')),
            photo_data TEXT NOT NULL,
            caption TEXT,
            location_lat REAL,
            location_lng REAL,
            taken_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()

    # Fix delivery_photos: allow NULL delivery_log_id (for pre-trip photos, etc.)
    try:
        c.execute("INSERT INTO delivery_photos (driver_shift_id, photo_type, photo_data) VALUES (0, 'other', 'test')")
        c.execute("DELETE FROM delivery_photos WHERE driver_shift_id=0 AND photo_data='test'")
        conn.commit()
    except Exception:
        # If it fails, the NOT NULL constraint is there — recreate table
        try:
            existing = c.execute("SELECT * FROM delivery_photos").fetchall()
            cols = [desc[0] for desc in c.description] if c.description else []
            c.execute("DROP TABLE delivery_photos")
            c.execute("""
                CREATE TABLE delivery_photos (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    delivery_log_id INTEGER REFERENCES delivery_log(id),
                    driver_shift_id INTEGER NOT NULL REFERENCES driver_shifts(id),
                    photo_type TEXT NOT NULL CHECK(photo_type IN ('pod','damage','load','pre_trip','incident','other')),
                    photo_data TEXT NOT NULL,
                    caption TEXT,
                    location_lat REAL,
                    location_lng REAL,
                    taken_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            if existing and cols:
                for row in existing:
                    rd = dict(zip(cols, row))
                    c.execute("INSERT INTO delivery_photos (id, delivery_log_id, driver_shift_id, photo_type, photo_data, caption, location_lat, location_lng, taken_at) VALUES (?,?,?,?,?,?,?,?,?)",
                        [rd.get('id'), rd.get('delivery_log_id'), rd.get('driver_shift_id'), rd.get('photo_type'), rd.get('photo_data'), rd.get('caption'), rd.get('location_lat'), rd.get('location_lng'), rd.get('taken_at')])
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"[migrate_db] delivery_photos recreation failed: {e}")

    c.execute("""
        CREATE TABLE IF NOT EXISTS driver_fatigue_config (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            max_driving_hours_before_break REAL DEFAULT 5.0,
            mandatory_break_minutes INTEGER DEFAULT 30,
            max_shift_hours REAL DEFAULT 12.0,
            warning_threshold_hours REAL DEFAULT 11.0,
            is_active INTEGER DEFAULT 1,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    # Seed default row if empty
    if not c.execute("SELECT id FROM driver_fatigue_config LIMIT 1").fetchone():
        c.execute("INSERT INTO driver_fatigue_config (max_driving_hours_before_break, mandatory_break_minutes, max_shift_hours, warning_threshold_hours) VALUES (5.0, 30, 12.0, 11.0)")
        conn.commit()

    c.execute("""
        CREATE TABLE IF NOT EXISTS driver_logbook (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            driver_shift_id INTEGER NOT NULL REFERENCES driver_shifts(id),
            delivery_log_id INTEGER REFERENCES delivery_log(id),
            event_type TEXT NOT NULL CHECK(event_type IN (
                'shift_start','shift_end','depart_depot','arrive_customer','depart_customer',
                'arrive_depot','break_start','break_end','refuel','incident','other'
            )),
            odometer_reading REAL,
            manual_km REAL,
            location_lat REAL,
            location_lng REAL,
            location_description TEXT,
            notes TEXT,
            recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()

    c.execute("""
        CREATE TABLE IF NOT EXISTS safety_checklist_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            item_text TEXT NOT NULL,
            category TEXT DEFAULT 'pre_trip' CHECK(category IN ('pre_trip','loading','unloading','end_of_day')),
            is_mandatory INTEGER DEFAULT 1,
            sort_order INTEGER DEFAULT 0,
            is_active INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    # Seed 6 mandatory pre-trip items if empty
    if not c.execute("SELECT id FROM safety_checklist_items LIMIT 1").fetchone():
        items = [
            ("Vehicle walk-around inspection completed", "pre_trip", 1, 1),
            ("Tyre condition and pressure checked", "pre_trip", 1, 2),
            ("All lights and indicators working", "pre_trip", 1, 3),
            ("Load restraints and tie-downs inspected", "pre_trip", 1, 4),
            ("Fire extinguisher present and serviceable", "pre_trip", 1, 5),
            ("Fit for duty — no fatigue, drugs, or alcohol", "pre_trip", 1, 6),
        ]
        for text, cat, mandatory, order in items:
            c.execute("INSERT INTO safety_checklist_items (item_text, category, is_mandatory, sort_order) VALUES (?,?,?,?)",
                      [text, cat, mandatory, order])
        conn.commit()

    # --- Block 3: Add columns to driver_shifts for enhanced tracking ---
    for col_sql in [
        "ALTER TABLE driver_shifts ADD COLUMN odometer_start REAL",
        "ALTER TABLE driver_shifts ADD COLUMN odometer_end REAL",
        "ALTER TABLE driver_shifts ADD COLUMN total_driving_minutes REAL DEFAULT 0",
        "ALTER TABLE driver_shifts ADD COLUMN last_break_at TIMESTAMP",
        "ALTER TABLE driver_shifts ADD COLUMN fatigue_warnings INTEGER DEFAULT 0",
    ]:
        try:
            c.execute(col_sql)
            conn.commit()
        except Exception as _mig_err:
            logging.debug('Migration note: %s', _mig_err)

    # --- Block 3: Add columns to trackmyride_config for enhanced config ---
    for col_sql in [
        "ALTER TABLE trackmyride_config ADD COLUMN geofence_radius_m INTEGER DEFAULT 200",
        "ALTER TABLE trackmyride_config ADD COLUMN auto_stage_enabled INTEGER DEFAULT 0",
        "ALTER TABLE trackmyride_config ADD COLUMN playback_enabled INTEGER DEFAULT 0",
        "ALTER TABLE trackmyride_config ADD COLUMN refuel_tracking_enabled INTEGER DEFAULT 0",
    ]:
        try:
            c.execute(col_sql)
            conn.commit()
        except Exception as _mig_err:
            logging.debug('Migration note: %s', _mig_err)

    c.execute("""
        CREATE TABLE IF NOT EXISTS trackmyride_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            truck_id INTEGER NOT NULL REFERENCES trucks(id),
            device_id TEXT,
            event_type TEXT CHECK(event_type IN ('position','geofence_enter','geofence_exit','ignition_on','ignition_off','refuel','speeding','idle')),
            latitude REAL,
            longitude REAL,
            speed REAL,
            heading REAL,
            odometer REAL,
            raw_payload TEXT,
            event_time TIMESTAMP,
            received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()

    c.execute("""
        CREATE TABLE IF NOT EXISTS trackmyride_geofences (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            type TEXT DEFAULT 'circle' CHECK(type IN ('circle','polygon')),
            latitude REAL,
            longitude REAL,
            radius_m INTEGER DEFAULT 200,
            polygon_points TEXT,
            linked_client_id INTEGER REFERENCES clients(id),
            linked_type TEXT DEFAULT 'customer' CHECK(linked_type IN ('depot','customer','supplier')),
            is_active INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()

    # --- Block 4: email_config table ---
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS email_config (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            smtp_host TEXT DEFAULT '',
            smtp_port INTEGER DEFAULT 587,
            smtp_user TEXT DEFAULT '',
            smtp_password TEXT DEFAULT '',
            smtp_use_tls INTEGER DEFAULT 1,
            from_name TEXT DEFAULT 'Hyne Pallets',
            from_email TEXT DEFAULT 'dispatch@hynepallets.com.au',
            is_active INTEGER DEFAULT 0,
            test_email_sent_at TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    # Seed default inactive row if empty
    if not c.execute("SELECT id FROM email_config LIMIT 1").fetchone():
        c.execute("INSERT INTO email_config (smtp_host, smtp_port, from_name, from_email, is_active) VALUES ('','587','Hyne Pallets','dispatch@hynepallets.com.au',0)")
        conn.commit()

    # --- Block 4: add delivery_status + error_message columns to notification_log ---
    for col_sql in [
        "ALTER TABLE notification_log ADD COLUMN delivery_status TEXT DEFAULT 'queued'",
        "ALTER TABLE notification_log ADD COLUMN error_message TEXT",
        "ALTER TABLE notification_log ADD COLUMN attempted_at TIMESTAMP",
        "ALTER TABLE notification_log ADD COLUMN delivered_at TIMESTAMP",
    ]:
        try:
            c.execute(col_sql)
            conn.commit()
        except Exception as _mig_err:
            logging.debug('Migration note: %s', _mig_err)

    # --- Block 4: expand notification_type CHECK constraint ---
    try:
        c.execute("INSERT INTO notification_log (notification_type, recipient_email, subject, body, status) VALUES ('daily_report','test@test.com','test','test','test')")
        c.execute("DELETE FROM notification_log WHERE recipient_email='test@test.com' AND subject='test' AND body='test'")
        conn.commit()
    except Exception:
        # Recreate table with expanded CHECK
        try:
            existing = c.execute("SELECT * FROM notification_log").fetchall()
            cols = [desc[0] for desc in c.description] if c.description else []
            c.execute("DROP TABLE notification_log")
            c.execute("""
                CREATE TABLE notification_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_id INTEGER REFERENCES orders(id),
                    notification_type TEXT CHECK(notification_type IN (
                        'order_acknowledgement','eta_notification','dispatch_notification',
                        'collection_ready','daily_report','shift_summary','qa_alert',
                        'fatigue_alert','custom'
                    )),
                    recipient_email TEXT,
                    subject TEXT,
                    body TEXT,
                    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status TEXT DEFAULT 'queued',
                    delivery_status TEXT DEFAULT 'queued',
                    error_message TEXT,
                    attempted_at TIMESTAMP,
                    delivered_at TIMESTAMP
                )
            """)
            if existing and cols:
                for row in existing:
                    rd = dict(zip(cols, row))
                    c.execute("""INSERT INTO notification_log (id, order_id, notification_type, recipient_email, subject, body, sent_at, status)
                        VALUES (?,?,?,?,?,?,?,?)""",
                        [rd.get('id'), rd.get('order_id'), rd.get('notification_type'), rd.get('recipient_email'),
                         rd.get('subject'), rd.get('body'), rd.get('sent_at'), rd.get('status')])
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"[migrate_db] notification_log recreation failed: {e}")

    # ===== TIMBER INVENTORY SEED DATA (Block 5) =====

    # Create timber tables (idempotent via IF NOT EXISTS)
    conn.execute("""CREATE TABLE IF NOT EXISTS timber_suppliers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL, abn TEXT, contact_name TEXT, contact_email TEXT,
        contact_phone TEXT, default_terms TEXT, is_active BOOLEAN DEFAULT 1,
        approval_status TEXT DEFAULT 'approved', created_by TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS timber_specs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        myob_code TEXT UNIQUE, type_prefix TEXT NOT NULL, grade_codes TEXT,
        width_mm INTEGER, thickness_mm INTEGER, length_mm INTEGER,
        suffix_flags TEXT, description TEXT NOT NULL, is_active BOOLEAN DEFAULT 1,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS timber_grade_codes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        code TEXT UNIQUE NOT NULL, full_name TEXT NOT NULL,
        description TEXT, is_active BOOLEAN DEFAULT 1)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS timber_packs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        qr_code TEXT UNIQUE, delivery_item_id INTEGER, spec_id INTEGER NOT NULL,
        supplier_id INTEGER NOT NULL, received_date DATE NOT NULL,
        received_by TEXT, pcs_per_pack INTEGER, m3_volume REAL NOT NULL,
        lineal_metres REAL, cost_per_m3 REAL, pack_cost_total REAL,
        status TEXT DEFAULT 'inventory', yard_zone_id INTEGER,
        pack_type TEXT DEFAULT 'full', assigned_worker TEXT,
        destination_zone TEXT, is_test_data BOOLEAN DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        consumed_at TIMESTAMP, consumed_by TEXT)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS timber_config (
        key TEXT PRIMARY KEY, value TEXT NOT NULL, description TEXT)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS timber_myob_accounts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        account_code TEXT NOT NULL, account_name TEXT NOT NULL, category TEXT)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS timber_supplier_approvals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        supplier_name TEXT NOT NULL, abn TEXT, contact_name TEXT,
        contact_email TEXT, contact_phone TEXT, requested_by TEXT NOT NULL,
        requested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        approved_by TEXT, approved_at TIMESTAMP, status TEXT DEFAULT 'pending')""")
    conn.execute("""CREATE TABLE IF NOT EXISTS timber_deliveries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        supplier_id INTEGER, delivery_date DATE NOT NULL, docket_number TEXT,
        docket_photo_path TEXT, ocr_raw_text TEXT, status TEXT DEFAULT 'pending',
        notes TEXT, created_by TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        completed_at TIMESTAMP)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS timber_delivery_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        delivery_id INTEGER, spec_id INTEGER, description TEXT,
        expected_packs INTEGER NOT NULL DEFAULT 0, assigned_packs INTEGER NOT NULL DEFAULT 0,
        pcs_per_pack INTEGER, cost_per_m3 REAL, total_amount REAL,
        lineal_metres_per_pack REAL, status TEXT DEFAULT 'pending',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS timber_consumption (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        pack_id INTEGER NOT NULL, consumed_by TEXT NOT NULL,
        consumed_by_user_id INTEGER, consumed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        destination TEXT, destination_zone TEXT, notes TEXT)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS timber_consumption_undo (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        pack_id INTEGER NOT NULL, original_consumption_id INTEGER,
        undone_by TEXT NOT NULL, undone_by_user_id INTEGER,
        undone_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, reason TEXT)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS timber_chainsaw_allocations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        pack_id INTEGER NOT NULL, allocated_by TEXT NOT NULL,
        allocated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, status TEXT DEFAULT 'allocated')""")
    conn.execute("""CREATE TABLE IF NOT EXISTS timber_cost_imports (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        import_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP, imported_by TEXT NOT NULL,
        file_name TEXT, period_month INTEGER, period_year INTEGER,
        status TEXT DEFAULT 'pending', total_rows INTEGER DEFAULT 0, matched_rows INTEGER DEFAULT 0)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS timber_cost_import_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        import_id INTEGER, myob_code TEXT, supplier_name TEXT, date TEXT,
        quantity REAL, description TEXT, amount REAL, tax TEXT,
        status_field TEXT, mapped_pack_id INTEGER)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS timber_stocktakes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        stocktake_date DATE NOT NULL, conducted_by TEXT,
        status TEXT DEFAULT 'in_progress', created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        completed_at TIMESTAMP)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS timber_stocktake_counts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        stocktake_id INTEGER, spec_id INTEGER, supplier_id INTEGER,
        system_packs INTEGER, system_m3 REAL, physical_packs INTEGER, physical_m3 REAL,
        variance_packs INTEGER, variance_m3 REAL, notes TEXT)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS timber_low_stock_alerts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        spec_id INTEGER, threshold_value REAL NOT NULL,
        threshold_unit TEXT DEFAULT 'm3', is_active BOOLEAN DEFAULT 1)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS timber_alert_recipients (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT NOT NULL, name TEXT, is_active BOOLEAN DEFAULT 1)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS timber_yard_zones (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        zone_name TEXT NOT NULL, description TEXT, is_active BOOLEAN DEFAULT 1,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS timber_pack_children (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        parent_pack_id INTEGER, child_spec_id INTEGER, docked_length_mm INTEGER,
        quantity INTEGER, m3_volume REAL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
    conn.commit()

    # Seed timber suppliers
    if conn.execute("SELECT COUNT(*) FROM timber_suppliers").fetchone()[0] == 0:
        suppliers = [
            ("21 Timbers Pty Ltd", None),
            ("AAM Timber – Narangba (Permalog)", None),
            ("AKD Queensland", None),
            ("Allied Natural Wood Enterprises Pty Ltd", None),
            ("Allied Timber Products QLD Pty Ltd", None),
            ("Hyne & Son Pty Ltd", None),
            ("Pinewood Products", None),
            ("Praslas Australia Pty Ltd", None),
            ("Pro-Pine Pty Ltd", None),
            ("Superior Wood Pty Ltd", None),
            ("T & R Carter Trust", None),
            ("Total Sawmilling & Timber Pty Ltd", None),
            ("Vida Wood Australia Pty Ltd", None),
        ]
        for name, abn in suppliers:
            conn.execute(
                "INSERT INTO timber_suppliers (name, abn, approval_status) VALUES (?, ?, 'approved')",
                [name, abn]
            )
        conn.commit()

    # Seed grade codes
    if conn.execute("SELECT COUNT(*) FROM timber_grade_codes").fetchone()[0] == 0:
        grades = [
            ("GOS", "Green Off-Saw", "Not dried, fresh from mill"),
            ("KD", "Kiln Dried", "Dried in kiln"),
            ("FD", "Fall Down/Downgrade", "Reject or lower grade material"),
            ("HW", "Hardwood", "Hardwood species"),
            ("UTE", "Utility Grade", "General utility timber"),
            ("CCA", "CCA Treated", "Copper Chrome Arsenate treatment"),
            ("T2", "Treated Timber", "General treatment designation"),
            ("DTL", "Docked To Length", "Pre-cut to specific length"),
            ("DR", "Dressed", "Planed/dressed timber"),
            ("GR", "Grooved", "Grooved finish"),
        ]
        for code, full_name, desc in grades:
            conn.execute(
                "INSERT INTO timber_grade_codes (code, full_name, description) VALUES (?, ?, ?)",
                [code, full_name, desc]
            )
        conn.commit()

    # Seed MYOB item codes (165 codes)
    if conn.execute("SELECT COUNT(*) FROM timber_specs").fetchone()[0] == 0:
        def _parse_myob(code):
            """Parse a MYOB timber code using corrected 3+2or3+4 pattern."""
            import re as _re
            c = code.strip()
            if c in ("PERMALOG", "PERMALOG01"):
                return {"myob_code": c, "type_prefix": c, "grade_codes": "CCA",
                        "width_mm": None, "thickness_mm": None, "length_mm": None,
                        "suffix_flags": None, "description": "CCA/ACQ Treated Permalog timber"}
            for pfx in ("RSGOS", "RSKD", "RSHW", "UTE"):
                if not c.startswith(pfx):
                    continue
                rest = c[len(pfx):]
                grade = {"RSGOS": "GOS", "RSKD": "KD", "UTE": "UTE", "RSHW": "HW"}[pfx]
                if pfx == "RSHW":
                    m = _re.match(r'^(\d{3})(\d{2,3})$', rest)
                    if m:
                        w, t = int(m.group(1)), int(m.group(2))
                        return {"myob_code": c, "type_prefix": "RSHW", "grade_codes": "HW",
                                "width_mm": w, "thickness_mm": t, "length_mm": None,
                                "suffix_flags": None,
                                "description": f"Rough Sawn Hardwood {w}x{t}"}
                m = _re.match(r'^(\d{3})(\d{2,3})DTL(\d{3,4})$', rest)
                if m:
                    w, t, dl = int(m.group(1)), int(m.group(2)), int(m.group(3))
                    d = f"Utility Timber {w}x{t} Docked To {dl}mm" if pfx == "UTE" else f"Rough Sawn {grade} {w}x{t} Docked To {dl}mm"
                    return {"myob_code": c, "type_prefix": pfx, "grade_codes": grade,
                            "width_mm": w, "thickness_mm": t, "length_mm": dl,
                            "suffix_flags": "DTL", "description": d}
                m = _re.match(r'^(\d{3})(\d{2,3})RDM$', rest)
                if m:
                    w, t = int(m.group(1)), int(m.group(2))
                    d = f"Utility Timber {w}x{t} Random Length" if pfx == "UTE" else f"Rough Sawn {grade} {w}x{t} Random Length"
                    return {"myob_code": c, "type_prefix": pfx, "grade_codes": grade,
                            "width_mm": w, "thickness_mm": t, "length_mm": None,
                            "suffix_flags": "RDM", "description": d}
                m = _re.match(r'^(\d{3})(\d{2,3})(\d{4})GR$', rest)
                if m:
                    w, t, l = int(m.group(1)), int(m.group(2)), int(m.group(3))
                    return {"myob_code": c, "type_prefix": pfx, "grade_codes": grade,
                            "width_mm": w, "thickness_mm": t, "length_mm": l,
                            "suffix_flags": "GR",
                            "description": f"Rough Sawn {grade} {w}x{t}x{l} Grooved"}
                m = _re.match(r'^(\d{3})(\d{3})(\d{4})$', rest)
                if m:
                    w, t, l = int(m.group(1)), int(m.group(2)), int(m.group(3))
                    d = f"Utility Timber {w}x{t}x{l}" if pfx == "UTE" else f"Rough Sawn {grade} {w}x{t}x{l}"
                    return {"myob_code": c, "type_prefix": pfx, "grade_codes": grade,
                            "width_mm": w, "thickness_mm": t, "length_mm": l,
                            "suffix_flags": None, "description": d}
                m = _re.match(r'^(\d{3})(\d{2})(\d{4})$', rest)
                if m:
                    w, t, l = int(m.group(1)), int(m.group(2)), int(m.group(3))
                    d = f"Utility Timber {w}x{t}x{l}" if pfx == "UTE" else f"Rough Sawn {grade} {w}x{t}x{l}"
                    return {"myob_code": c, "type_prefix": pfx, "grade_codes": grade,
                            "width_mm": w, "thickness_mm": t, "length_mm": l,
                            "suffix_flags": None, "description": d}
                break
            return {"myob_code": c, "type_prefix": c[:5] if len(c) >= 5 else c,
                    "grade_codes": None, "width_mm": None, "thickness_mm": None,
                    "length_mm": None, "suffix_flags": None, "description": c}

        myob_codes = [
            "PERMALOG", "PERMALOG01",
            "RSGOS0500254900", "RSGOS0500505400",
            "RSGOS0700161200", "RSGOS0700162400",
            "RSGOS0700251200", "RSGOS0700252400", "RSGOS0700253600",
            "RSGOS0700254800", "RSGOS0700381200", "RSGOS0700501200",
            "RSGOS0700502400",
            "RSGOS0750161200", "RSGOS0750162400", "RSGOS0750162700",
            "RSGOS0750251200", "RSGOS0750252400", "RSGOS0750253000",
            "RSGOS0750253600", "RSGOS0750254200", "RSGOS0750254300",
            "RSGOS0750254500", "RSGOS0750254800", "RSGOS0750254900",
            "RSGOS0750255400", "RSGOS0750255500", "RSGOS0750256000",
            "RSGOS0750382400", "RSGOS0750384800", "RSGOS0750384900",
            "RSGOS0750385400", "RSGOS0750385500", "RSGOS0750386000",
            "RSGOS0750403600", "RSGOS0750501200", "RSGOS0750502400",
            "RSGOS0750502400GR", "RSGOS0750502700", "RSGOS0750504800",
            "RSGOS0750504900", "RSGOS0750505400", "RSGOS0750505500",
            "RSGOS075050DTL0850", "RSGOS0750751200", "RSGOS0750752400",
            "RSGOS0750754900", "RSGOS0750755500",
            "RSGOS0900321200", "RSGOS0900322400", "RSGOS0900452400",
            "RSGOS0960836000",
            "RSGOS1000153600", "RSGOS1000154200", "RSGOS1000154800",
            "RSGOS1000155400", "RSGOS1000156000",
            "RSGOS1000161200", "RSGOS1000162400", "RSGOS1000162700",
            "RSGOS1000163000", "RSGOS1000164800",
            "RSGOS100016DTL1165",
            "RSGOS1000191200", "RSGOS1000192400", "RSGOS1000193600",
            "RSGOS1000194800",
            "RSGOS1000251200", "RSGOS1000252400", "RSGOS1000253000",
            "RSGOS1000253100", "RSGOS1000253600", "RSGOS1000253900",
            "RSGOS1000254200", "RSGOS1000254300", "RSGOS1000254800",
            "RSGOS1000254900", "RSGOS1000255400", "RSGOS1000255500",
            "RSGOS1000256000",
            "RSGOS1000381200", "RSGOS1000382400", "RSGOS1000384800",
            "RSGOS1000386000",
            "RSGOS1000406000",
            "RSGOS1000501200", "RSGOS1000502400", "RSGOS1000502700",
            "RSGOS1000503100", "RSGOS1000503600", "RSGOS1000504300",
            "RSGOS1000504800", "RSGOS1000504900", "RSGOS1000505400",
            "RSGOS1000505500", "RSGOS1000506000",
            "RSGOS1000752400",
            "RSGOS1001002400",
            "RSGOS1500161200", "RSGOS1500162400", "RSGOS1500163000",
            "RSGOS150016DTL1165",
            "RSGOS1500191200", "RSGOS1500192400",
            "RSGOS1500251200", "RSGOS1500252400", "RSGOS1500253100",
            "RSGOS1500253600", "RSGOS1500253900", "RSGOS1500254800",
            "RSGOS1500254900", "RSGOS1500255400", "RSGOS1500255500",
            "RSGOS1500256000",
            "RSGOS150025RDM",
            "RSGOS2000253600", "RSGOS2000254800", "RSGOS2000255400",
            "RSGOS2000256000", "RSGOS2000384800",
            "RSGOS2000502400", "RSGOS2000504800",
            "RSGOS2002004800",
            "RSHW100025", "RSHW100050", "RSHW100100",
            "RSHW150025",
            "RSKD0750381200", "RSKD0750382400", "RSKD1000382400",
            "UTE0700352400", "UTE0700352700", "UTE0700353000",
            "UTE0700353600", "UTE0700354800", "UTE0700355400",
            "UTE0700356000",
            "UTE070035DTL1100", "UTE070035RDM",
            "UTE0900351200", "UTE0900351500", "UTE0900351800",
            "UTE0900352400", "UTE0900353000", "UTE0900353600",
            "UTE0900354200", "UTE0900354800", "UTE0900355400",
            "UTE0900356000",
            "UTE090035RDM",
            "UTE0900451200", "UTE0900452400", "UTE0900452700",
            "UTE0900453000", "UTE0900453600", "UTE0900454200",
            "UTE0900454800", "UTE0900455400", "UTE0900456000",
            "UTE140454800", "UTE1900453600", "UTE1900454800",
        ]
        for code in myob_codes:
            info = _parse_myob(code)
            conn.execute(
                """INSERT OR IGNORE INTO timber_specs
                   (myob_code, type_prefix, grade_codes, width_mm, thickness_mm,
                    length_mm, suffix_flags, description)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                [info["myob_code"], info["type_prefix"], info["grade_codes"],
                 info["width_mm"], info["thickness_mm"], info["length_mm"],
                 info["suffix_flags"], info["description"]]
            )
        conn.commit()

    # Seed config
    if conn.execute("SELECT COUNT(*) FROM timber_config").fetchone()[0] == 0:
        configs = [
            ("fifo_threshold_days", "14", "FIFO advisory threshold in days"),
            ("cost_visible_roles", "executive,finance", "Roles that can see cost data"),
            ("qr_prefix", "EP-", "Prefix for QR pack IDs"),
            ("qr_sequence_start", "1", "Current QR sequence number"),
            ("default_unit", "m3", "Default display unit"),
        ]
        for key, val, desc in configs:
            conn.execute(
                "INSERT OR IGNORE INTO timber_config (key, value, description) VALUES (?, ?, ?)",
                [key, val, desc]
            )
        conn.commit()

    # Seed MYOB accounts
    if conn.execute("SELECT COUNT(*) FROM timber_myob_accounts").fetchone()[0] == 0:
        accounts = [
            ("1-9005", "Full Packs", "inventory"),
            ("1-9006", "Part Packs", "inventory"),
            ("1-9020", "Docked Viking", "inventory"),
            ("5-2701", "Timber Purchases", "expense"),
            ("5-2901", "Freight", "freight"),
        ]
        for code, name, cat in accounts:
            conn.execute(
                "INSERT INTO timber_myob_accounts (account_code, account_name, category) VALUES (?, ?, ?)",
                [code, name, cat]
            )
        conn.commit()

    # Seed 1 test pack (spec RSGOS1000256000, supplier Allied Timber Products QLD)
    if conn.execute("SELECT COUNT(*) FROM timber_packs WHERE is_test_data=1").fetchone()[0] == 0:
        spec_row = conn.execute(
            "SELECT id FROM timber_specs WHERE myob_code='RSGOS1000256000'"
        ).fetchone()
        sup_row = conn.execute(
            "SELECT id FROM timber_suppliers WHERE name='Allied Timber Products QLD Pty Ltd'"
        ).fetchone()
        if spec_row and sup_row:
            conn.execute(
                """INSERT OR IGNORE INTO timber_packs
                   (qr_code, spec_id, supplier_id, received_date, pcs_per_pack,
                    m3_volume, status, is_test_data)
                   VALUES ('EP-00001', ?, ?, date('now'), 330, 49.5, 'inventory', 1)""",
                [spec_row[0], sup_row[0]]
            )
            conn.commit()

    # --- notification_log: add missing columns ---
    for col, defn in [("delivery_status", "TEXT DEFAULT 'queued'"), ("delivered_at", "TIMESTAMP"), ("attempted_at", "TIMESTAMP"), ("error_message", "TEXT")]:
        try:
            conn.execute(f"ALTER TABLE notification_log ADD COLUMN {col} {defn}")
            conn.commit()
        except Exception:
            pass

    # --- login_attempts table ---
    try:
        conn.execute("CREATE TABLE IF NOT EXISTS login_attempts (id INTEGER PRIMARY KEY AUTOINCREMENT, identifier TEXT NOT NULL, attempt_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, success BOOLEAN DEFAULT 0)")
        conn.commit()
    except Exception:
        pass

    # --- BLOCK 2 PHASE 1: Dispatch runs + Kanban ---
    c.execute("""
    CREATE TABLE IF NOT EXISTS dispatch_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        truck_id INTEGER NOT NULL REFERENCES trucks(id),
        run_date TEXT NOT NULL,
        run_number INTEGER NOT NULL DEFAULT 1,
        driver_id INTEGER REFERENCES users(id),
        status TEXT DEFAULT 'planned' CHECK(status IN ('planned','loading','in_transit','completed','cancelled')),
        departure_time TEXT,
        return_time TEXT,
        notes TEXT,
        created_by INTEGER REFERENCES users(id),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(truck_id, run_date, run_number)
    )""")

    # Add run_id to delivery_log
    dl_cols = {row[1] for row in c.execute("PRAGMA table_info(delivery_log)").fetchall()}
    if 'run_id' not in dl_cols:
        c.execute("ALTER TABLE delivery_log ADD COLUMN run_id INTEGER REFERENCES dispatch_runs(id)")

    # Add kanban_status to order_items (computed but cached for performance)
    oi_cols2 = {row[1] for row in c.execute("PRAGMA table_info(order_items)").fetchall()}
    if 'kanban_status' not in oi_cols2:
        c.execute("ALTER TABLE order_items ADD COLUMN kanban_status TEXT DEFAULT 'red_pending'")

    # Add kanban_status to orders
    o_cols2 = {row[1] for row in c.execute("PRAGMA table_info(orders)").fetchall()}
    if 'kanban_status' not in o_cols2:
        c.execute("ALTER TABLE orders ADD COLUMN kanban_status TEXT DEFAULT 'red_pending'")

    # --- Block 2 Phase 2: Migrate pause_logs to new reason values ---
    try:
        # Check if old constraint exists by looking for old reason values in the table
        old_reasons_exist = False
        try:
            # Try inserting a value that only the old constraint would block (new constraint would also block it)
            # Instead, check the sqlite_master for the old constraint text
            tbl_sql = conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='pause_logs'"
            ).fetchone()
            if tbl_sql and "urgent_changeover" in (tbl_sql[0] or ""):
                old_reasons_exist = True
        except Exception:
            pass

        if old_reasons_exist:
            # Map old reason values to new ones
            reason_map = {
                "material": "wait_material",
                "cleaning": "wait_material",
                "break": "smoko_break",
                "breakdown": "tool_breakdown",
                "forklift": "wait_material",
                "urgent_changeover": "waiting_instructions",
                "other": "other",
            }
            # Fetch all existing rows
            existing = conn.execute("SELECT * FROM pause_logs").fetchall()
            cols = [d[0] for d in conn.execute("PRAGMA table_info(pause_logs)").fetchall()]
            # Recreate table with new constraint
            conn.execute("DROP TABLE IF EXISTS pause_logs_old")
            conn.execute("ALTER TABLE pause_logs RENAME TO pause_logs_old")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS pause_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id INTEGER NOT NULL REFERENCES production_sessions(id),
                    reason TEXT NOT NULL CHECK(reason IN ('wait_material','tool_breakdown','machine_fault','lunch','smoko_break','qa_hold','waiting_instructions','other')),
                    paused_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    resumed_at TIMESTAMP,
                    duration_minutes REAL,
                    notes TEXT
                )
            """)
            # Re-insert data with mapped reasons
            for row in existing:
                rd = dict(zip(cols, row))
                old_r = rd.get("reason", "other")
                new_r = reason_map.get(old_r, "other")
                conn.execute(
                    "INSERT INTO pause_logs (id, session_id, reason, paused_at, resumed_at, duration_minutes, notes) VALUES (?,?,?,?,?,?,?)",
                    [rd.get("id"), rd.get("session_id"), new_r, rd.get("paused_at"),
                     rd.get("resumed_at"), rd.get("duration_minutes"), rd.get("notes")]
                )
            conn.execute("DROP TABLE pause_logs_old")
            conn.commit()
            print("[migrate_db] pause_logs migrated to new reason values")
        else:
            # Table already has new constraint or doesn't exist — ensure it exists with new constraint
            conn.execute("""
                CREATE TABLE IF NOT EXISTS pause_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id INTEGER NOT NULL REFERENCES production_sessions(id),
                    reason TEXT NOT NULL CHECK(reason IN ('wait_material','tool_breakdown','machine_fault','lunch','smoko_break','qa_hold','waiting_instructions','other')),
                    paused_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    resumed_at TIMESTAMP,
                    duration_minutes REAL,
                    notes TEXT
                )
            """)
            conn.commit()
    except Exception as e:
        print(f"[migrate_db] pause_logs migration failed: {e}")
        conn.rollback()

    # --- Block 2 Phase 2: drawing_files table ---
    conn.execute("""
        CREATE TABLE IF NOT EXISTS drawing_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sku_id INTEGER REFERENCES skus(id),
            order_item_id INTEGER REFERENCES order_items(id),
            file_name TEXT NOT NULL,
            file_type TEXT CHECK(file_type IN ('pdf','image')),
            file_data TEXT NOT NULL,
            uploaded_by INTEGER REFERENCES users(id),
            uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            notes TEXT
        )
    """)
    conn.commit()

    # --- P1 Fix: Add labour_mins_per_unit to skus if missing ---
    sku_cols = {row[1] for row in conn.execute("PRAGMA table_info(skus)").fetchall()}
    if 'labour_mins_per_unit' not in sku_cols:
        conn.execute("ALTER TABLE skus ADD COLUMN labour_mins_per_unit REAL DEFAULT 0")
        conn.commit()

    # --- P1 Fix: Add updated_at to order_items if missing ---
    oi_cols2 = {row[1] for row in conn.execute("PRAGMA table_info(order_items)").fetchall()}
    if 'updated_at' not in oi_cols2:
        conn.execute("ALTER TABLE order_items ADD COLUMN updated_at TIMESTAMP")
        conn.commit()

    # --- P1 Fix: Add kanban_status to orders/order_items if missing ---
    if 'kanban_status' not in existing_cols:
        conn.execute("ALTER TABLE orders ADD COLUMN kanban_status TEXT")
    oi_cols3 = {row[1] for row in conn.execute("PRAGMA table_info(order_items)").fetchall()}
    if 'kanban_status' not in oi_cols3:
        conn.execute("ALTER TABLE order_items ADD COLUMN kanban_status TEXT")
    conn.commit()

    # --- P1 Fix: Enforce PIN uniqueness via unique index (allows multiple NULLs) ---
    try:
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_pin_unique ON users(pin) WHERE pin IS NOT NULL")
        conn.commit()
    except Exception as e:
        print(f"[migrate_db] PIN uniqueness index: {e}")
        conn.rollback()

    # --- P1 Fix: Recreate users table with expanded role CHECK if needed ---
    # SQLite cannot ALTER CHECK constraints, so we recreate the table.
    try:
        # Test if yardsman role is accepted
        conn.execute("INSERT INTO users (full_name, role, username) VALUES ('__test_role__', 'yardsman', '__test_yardsman__')")
        conn.execute("DELETE FROM users WHERE username='__test_yardsman__'")
        conn.commit()
    except Exception:
        conn.rollback()
        # Need to recreate the table with expanded CHECK
        try:
            c = conn.cursor()
            c.execute("""
                CREATE TABLE IF NOT EXISTS users_expanded (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    email TEXT UNIQUE,
                    password_hash TEXT,
                    pin TEXT,
                    username TEXT UNIQUE,
                    full_name TEXT NOT NULL,
                    role TEXT NOT NULL CHECK(role IN (
                        'executive','office','planner','production_manager',
                        'floor_worker','qa_lead','dispatch','yard','driver',
                        'yardsman','chainsaw_operator','team_leader','ops_manager'
                    )),
                    default_zone_id INTEGER,
                    is_active INTEGER DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            c.execute("""
                INSERT INTO users_expanded (id, email, password_hash, pin, username, full_name, role, default_zone_id, is_active, created_at, updated_at)
                SELECT id, email, password_hash, pin, username, full_name, role, default_zone_id, is_active, created_at, updated_at
                FROM users
            """)
            c.execute("DROP TABLE users")
            c.execute("ALTER TABLE users_expanded RENAME TO users")
            conn.commit()
            print("[migrate_db] users table recreated with expanded role CHECK")
        except Exception as e:
            conn.rollback()
            print(f"[migrate_db] users table role expansion failed: {e}")

    conn.commit()

    # ----- Performance Indexes (Bug #15 fix) -----
    try:
        index_stmts = [
            "CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items(order_id)",
            "CREATE INDEX IF NOT EXISTS idx_order_items_status ON order_items(status)",
            "CREATE INDEX IF NOT EXISTS idx_order_items_zone_id ON order_items(zone_id)",
            "CREATE INDEX IF NOT EXISTS idx_order_items_sku_id ON order_items(sku_id)",
            "CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status)",
            "CREATE INDEX IF NOT EXISTS idx_orders_client_id ON orders(client_id)",
            "CREATE INDEX IF NOT EXISTS idx_schedule_entries_zone_id ON schedule_entries(zone_id)",
            "CREATE INDEX IF NOT EXISTS idx_schedule_entries_station_id ON schedule_entries(station_id)",
            "CREATE INDEX IF NOT EXISTS idx_schedule_entries_date ON schedule_entries(scheduled_date)",
            "CREATE INDEX IF NOT EXISTS idx_schedule_entries_item ON schedule_entries(order_item_id)",
            "CREATE INDEX IF NOT EXISTS idx_production_sessions_zone ON production_sessions(zone_id)",
            "CREATE INDEX IF NOT EXISTS idx_production_sessions_station ON production_sessions(station_id)",
            "CREATE INDEX IF NOT EXISTS idx_production_sessions_status ON production_sessions(status)",
            "CREATE INDEX IF NOT EXISTS idx_production_logs_session ON production_logs(session_id)",
            "CREATE INDEX IF NOT EXISTS idx_session_workers_session ON session_workers(session_id)",
            "CREATE INDEX IF NOT EXISTS idx_session_workers_user ON session_workers(user_id)",
            "CREATE INDEX IF NOT EXISTS idx_delivery_log_order ON delivery_log(order_id)",
            "CREATE INDEX IF NOT EXISTS idx_delivery_log_truck ON delivery_log(truck_id)",
            "CREATE INDEX IF NOT EXISTS idx_delivery_log_date ON delivery_log(expected_date)",
            "CREATE INDEX IF NOT EXISTS idx_qa_inspections_item ON qa_inspections(order_item_id)",
            "CREATE INDEX IF NOT EXISTS idx_stations_zone ON stations(zone_id)",
            "CREATE INDEX IF NOT EXISTS idx_skus_zone ON skus(zone_id)",
            "CREATE INDEX IF NOT EXISTS idx_skus_code ON skus(code)",
            "CREATE INDEX IF NOT EXISTS idx_audit_log_entity ON audit_log(entity_type, entity_id)",
            "CREATE INDEX IF NOT EXISTS idx_audit_log_user ON audit_log(user_id)",
            "CREATE INDEX IF NOT EXISTS idx_timber_packs_spec ON timber_packs(spec_id)",
            "CREATE INDEX IF NOT EXISTS idx_timber_packs_status ON timber_packs(status)",
            "CREATE INDEX IF NOT EXISTS idx_dispatch_runs_date ON dispatch_runs(run_date)",
        ]
        for stmt in index_stmts:
            conn.execute(stmt)
        conn.commit()
    except Exception as e:
        logging.info("Index creation note: %s", e)

    conn.close()


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------

JWT_SECRET = os.environ.get("JWT_SECRET", "")
if not JWT_SECRET:
    print("\n[WARNING] JWT_SECRET environment variable is not set.")
    print("Set JWT_SECRET in your environment before starting the server.")
    print("Example: export JWT_SECRET=$(python3 -c 'import secrets; print(secrets.token_hex(32))')\n")
    # Use a fallback so the server can still start (login will fail without a real secret)
    import sys
    print("\n[FATAL] JWT_SECRET environment variable is not set. Server cannot start securely.")
    print("Set JWT_SECRET in your environment before starting the server.")
    print("Example: export JWT_SECRET=$(python3 -c 'import secrets; print(secrets.token_hex(32))')\n")
    sys.exit(1)
JWT_EXPIRY_SECONDS = 86400 * 7  # 7 days

def _smtp_encrypt(plaintext):
    """Simple XOR obfuscation with JWT_SECRET — not true encryption but prevents casual DB reads."""
    if not plaintext:
        return ""
    key = (JWT_SECRET or "fallback").encode()
    encrypted = bytearray()
    for i, ch in enumerate(plaintext.encode()):
        encrypted.append(ch ^ key[i % len(key)])
    import base64
    return "ENC:" + base64.b64encode(bytes(encrypted)).decode()

def _smtp_decrypt(stored):
    """Reverse the XOR obfuscation."""
    if not stored or not stored.startswith("ENC:"):
        return stored  # Legacy plaintext — return as-is
    import base64
    key = (JWT_SECRET or "fallback").encode()
    encrypted = base64.b64decode(stored[4:])
    decrypted = bytearray()
    for i, ch in enumerate(encrypted):
        decrypted.append(ch ^ key[i % len(key)])
    return decrypted.decode()




def make_token(user_id, role):
    """Create HMAC-signed JWT token."""
    header = base64.urlsafe_b64encode(json.dumps({"alg": "HS256", "typ": "JWT"}).encode()).decode().rstrip("=")
    payload_data = {"user_id": user_id, "role": role, "exp": int(time.time()) + JWT_EXPIRY_SECONDS, "iat": int(time.time())}
    payload = base64.urlsafe_b64encode(json.dumps(payload_data).encode()).decode().rstrip("=")
    signature = hmac.new(JWT_SECRET.encode(), f"{header}.{payload}".encode(), hashlib.sha256).hexdigest()
    return f"{header}.{payload}.{signature}"


def decode_token(token):
    """Verify HMAC signature and decode JWT token. Returns payload dict or None."""
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return None
        header_b64, payload_b64, sig = parts
        # Verify signature
        expected_sig = hmac.new(JWT_SECRET.encode(), f"{header_b64}.{payload_b64}".encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(sig, expected_sig):
            return None
        # Decode payload (add padding)
        payload_b64 += "=" * (4 - len(payload_b64) % 4)
        payload_data = json.loads(base64.urlsafe_b64decode(payload_b64.encode()).decode())
        # Check expiry
        if payload_data.get("exp", 0) < int(time.time()):
            return None
        return payload_data
    except Exception:
        return None


def get_current_user(conn):
    auth = request.headers.get("Authorization", "")
    token = None
    if auth.startswith("Bearer "):
        token = auth[7:]
    # B-029: _token query param removed for security (tokens must use Authorization header)
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


def check_rate_limit(conn, identifier, max_attempts=10, window_minutes=60):
    """Check if identifier is rate-limited. Returns (allowed: bool, attempts_remaining: int)."""
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=window_minutes)).isoformat()
    count = conn.execute("SELECT COUNT(*) FROM login_attempts WHERE identifier=? AND attempt_at>? AND success=0", [identifier, cutoff]).fetchone()[0]
    return (count < max_attempts, max(0, max_attempts - count))


def record_login_attempt(conn, identifier, success=False):
    """Record a login attempt."""
    conn.execute("INSERT INTO login_attempts (identifier, success, attempt_at) VALUES (?,?,?)",
        [identifier, 1 if success else 0, datetime.now(timezone.utc).isoformat()])
    conn.commit()
    # Clean old attempts (older than 24h)
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    conn.execute("DELETE FROM login_attempts WHERE attempt_at<?", [cutoff])
    conn.commit()


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
# TIMBER MODULE: MYOB code parser
# ---------------------------------------------------------------------------

def parse_myob_code(code):
    """Parse a MYOB timber item code into structured fields.
    Returns dict with type_prefix, grade_codes, width_mm, thickness_mm,
    length_mm, suffix_flags, description.
    Encoding: 3-digit zero-padded width + 2-or-3-digit thickness + 4-digit length.
    e.g. RSGOS1000256000 = 100mm x 25mm x 6000mm
    """
    import re as _re
    c = code.strip()
    # PERMALOG variants
    if c in ("PERMALOG", "PERMALOG01"):
        return {
            "myob_code": c,
            "type_prefix": c,
            "grade_codes": "CCA",
            "width_mm": None,
            "thickness_mm": None,
            "length_mm": None,
            "suffix_flags": None,
            "description": "CCA/ACQ Treated Permalog timber",
        }

    for pfx in ("RSGOS", "RSKD", "RSHW", "UTE"):
        if not c.startswith(pfx):
            continue
        rest = c[len(pfx):]
        grade = {"RSGOS": "GOS", "RSKD": "KD", "UTE": "UTE", "RSHW": "HW"}[pfx]

        # RSHW: 3-digit width + 2-or-3-digit thickness (no length)
        if pfx == "RSHW":
            m = _re.match(r'^(\d{3})(\d{2,3})$', rest)
            if m:
                w, t = int(m.group(1)), int(m.group(2))
                return {
                    "myob_code": c, "type_prefix": "RSHW", "grade_codes": "HW",
                    "width_mm": w, "thickness_mm": t, "length_mm": None,
                    "suffix_flags": None,
                    "description": f"Rough Sawn Hardwood {w}x{t}",
                }

        # DTL: 3+2or3+DTL+3-4 digits
        m = _re.match(r'^(\d{3})(\d{2,3})DTL(\d{3,4})$', rest)
        if m:
            w, t, dl = int(m.group(1)), int(m.group(2)), int(m.group(3))
            desc = (f"Utility Timber {w}x{t} Docked To {dl}mm" if pfx == "UTE"
                    else f"Rough Sawn {grade} {w}x{t} Docked To {dl}mm")
            return {
                "myob_code": c, "type_prefix": pfx, "grade_codes": grade,
                "width_mm": w, "thickness_mm": t, "length_mm": dl,
                "suffix_flags": "DTL", "description": desc,
            }

        # RDM: 3+2or3+RDM
        m = _re.match(r'^(\d{3})(\d{2,3})RDM$', rest)
        if m:
            w, t = int(m.group(1)), int(m.group(2))
            desc = (f"Utility Timber {w}x{t} Random Length" if pfx == "UTE"
                    else f"Rough Sawn {grade} {w}x{t} Random Length")
            return {
                "myob_code": c, "type_prefix": pfx, "grade_codes": grade,
                "width_mm": w, "thickness_mm": t, "length_mm": None,
                "suffix_flags": "RDM", "description": desc,
            }

        # GR: 3+2or3+4+GR
        m = _re.match(r'^(\d{3})(\d{2,3})(\d{4})GR$', rest)
        if m:
            w, t, l = int(m.group(1)), int(m.group(2)), int(m.group(3))
            return {
                "myob_code": c, "type_prefix": pfx, "grade_codes": grade,
                "width_mm": w, "thickness_mm": t, "length_mm": l,
                "suffix_flags": "GR",
                "description": f"Rough Sawn {grade} {w}x{t}x{l} Grooved",
            }

        # Standard: try 3+3+4 first, then 3+2+4
        m = _re.match(r'^(\d{3})(\d{3})(\d{4})$', rest)
        if m:
            w, t, l = int(m.group(1)), int(m.group(2)), int(m.group(3))
            desc = (f"Utility Timber {w}x{t}x{l}" if pfx == "UTE"
                    else f"Rough Sawn {grade} {w}x{t}x{l}")
            return {
                "myob_code": c, "type_prefix": pfx, "grade_codes": grade,
                "width_mm": w, "thickness_mm": t, "length_mm": l,
                "suffix_flags": None, "description": desc,
            }
        m = _re.match(r'^(\d{3})(\d{2})(\d{4})$', rest)
        if m:
            w, t, l = int(m.group(1)), int(m.group(2)), int(m.group(3))
            desc = (f"Utility Timber {w}x{t}x{l}" if pfx == "UTE"
                    else f"Rough Sawn {grade} {w}x{t}x{l}")
            return {
                "myob_code": c, "type_prefix": pfx, "grade_codes": grade,
                "width_mm": w, "thickness_mm": t, "length_mm": l,
                "suffix_flags": None, "description": desc,
            }
        break

    # Fallback
    return {
        "myob_code": c,
        "type_prefix": c[:5] if len(c) >= 5 else c,
        "grade_codes": None,
        "width_mm": None, "thickness_mm": None, "length_mm": None,
        "suffix_flags": None,
        "description": c,
    }


def _check_low_stock(conn, spec_id):
    """Check if spec_id has fallen below any alert threshold. Returns list of alert msgs."""
    alerts = []
    rows = conn.execute(
        "SELECT la.id, la.threshold_value, la.threshold_unit, ts.description "
        "FROM timber_low_stock_alerts la "
        "JOIN timber_specs ts ON ts.id = la.spec_id "
        "WHERE la.spec_id=? AND la.is_active=1",
        [spec_id]
    ).fetchall()
    for row in rows:
        threshold = row[1]; unit = row[2]; desc = row[3]
        if unit == "m3":
            current = conn.execute(
                "SELECT COALESCE(SUM(m3_volume),0) FROM timber_packs WHERE spec_id=? AND status='inventory'",
                [spec_id]
            ).fetchone()[0]
        else:  # packs
            current = conn.execute(
                "SELECT COUNT(*) FROM timber_packs WHERE spec_id=? AND status='inventory'",
                [spec_id]
            ).fetchone()[0]
        if current < threshold:
            alerts.append(f"LOW STOCK: {desc} — {current:.2f} {unit} remaining (threshold {threshold} {unit})")
            # Attempt to email alert recipients
            try:
                recipients = conn.execute(
                    "SELECT email FROM timber_alert_recipients WHERE is_active=1"
                ).fetchall()
                for r in recipients:
                    send_email_smtp_async(
                        r[0],
                        f"Low Stock Alert: {desc}",
                        f"Timber stock alert:\n{desc}\nCurrent: {current:.2f} {unit}\nThreshold: {threshold} {unit}"
                    )
            except Exception:
                pass
    return alerts


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
# Health check for Railway
# ---------------------------------------------------------------------------

@app.route("/health")
def health_check():
    return jsonify({"status": "ok"}), 200


# ---------------------------------------------------------------------------
# Static file serving
# ---------------------------------------------------------------------------


@app.after_request
def add_security_headers(response):
    response.headers['Strict-Transport-Security'] = 'max-age=31536000; includeSubDomains'
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['X-Frame-Options'] = 'DENY'
    return response

@app.route("/")
def serve_index():
    return send_from_directory("static", "index.html")


@app.route("/<path:path>")
def serve_static(path):
    return send_from_directory("static", path)


# ---------------------------------------------------------------------------
# Kanban status helpers
# ---------------------------------------------------------------------------

def compute_kanban_status(order_status, item_status, has_inventory=False, has_dispatch_run=False, is_dispatched=False, is_delivered=False):
    """Compute Kanban traffic light status.
    Returns: (color_key, label) tuple
    color_key: red_pending, amber_production, amber_docking, green_planning, green_dispatch, blue, red_delivered
    """
    if is_delivered:
        return ('red_delivered', 'Delivered')
    if is_dispatched:
        return ('blue', 'Dispatched')
    if item_status == 'F' and has_dispatch_run:
        return ('green_dispatch', 'Ready to Dispatch')
    if item_status == 'F' and not has_dispatch_run:
        return ('green_planning', 'Planning')
    if has_inventory and not is_dispatched:
        return ('green_planning', 'Planning')
    if item_status in ('P', 'R'):
        return ('amber_production', 'In Production')
    if item_status == 'C':
        return ('amber_docking', 'In Docking')
    return ('red_pending', 'Pending Stock')


KANBAN_COLORS = {
    'red_pending':   {'color': '#dc2626', 'label': 'Pending Stock',      'hex': '#dc2626'},
    'amber_production': {'color': '#f59e0b', 'label': 'In Production',   'hex': '#f59e0b'},
    'amber_docking':  {'color': '#f59e0b', 'label': 'In Docking',         'hex': '#f59e0b'},
    'green_planning':{'color': '#22c55e', 'label': 'Planning',           'hex': '#22c55e'},
    'green_dispatch':{'color': '#16a34a', 'label': 'Ready to Dispatch',  'hex': '#16a34a'},
    'blue':          {'color': '#2563eb', 'label': 'Dispatched',         'hex': '#2563eb'},
    'red_delivered': {'color': '#dc2626', 'label': 'Delivered',          'hex': '#dc2626'},
}


def kanban_full_info(key):
    """Return full kanban info dict for a given key."""
    return KANBAN_COLORS.get(key, KANBAN_COLORS['red_pending'])


def update_kanban_statuses(conn, order_id):
    """Recompute and cache kanban statuses for all items in an order."""
    items = conn.execute("""
        SELECT oi.id, oi.status,
               CASE WHEN dl.id IS NOT NULL AND dl.status='delivered' THEN 1 ELSE 0 END as is_delivered,
               CASE WHEN dl.id IS NOT NULL AND dl.status IN ('loaded','in_transit') THEN 1 ELSE 0 END as is_dispatched,
               CASE WHEN dl.run_id IS NOT NULL THEN 1 ELSE 0 END as has_dispatch_run,
               CASE WHEN inv.units_on_hand > inv.units_allocated AND inv.units_on_hand > 0 THEN 1 ELSE 0 END as has_inventory
        FROM order_items oi
        LEFT JOIN orders o ON o.id = oi.order_id
        LEFT JOIN (SELECT order_id, MAX(id) as id, MAX(run_id) as run_id, status FROM delivery_log GROUP BY order_id) dl ON dl.order_id = o.id
        LEFT JOIN inventory inv ON inv.sku_id = oi.sku_id
        WHERE oi.order_id = ?
    """, [order_id]).fetchall()

    worst_color_rank = {'red_pending': 0, 'red_delivered': 6, 'amber_docking': 1, 'amber_production': 2, 'green_planning': 3, 'green_dispatch': 4, 'blue': 5}
    order_kanban = 'blue'
    order_kanban_rank = 99

    for item in items:
        color, label = compute_kanban_status(
            None, item['status'] if isinstance(item, dict) else item[1],
            has_inventory=bool(item['has_inventory'] if isinstance(item, dict) else item[5]),
            has_dispatch_run=bool(item['has_dispatch_run'] if isinstance(item, dict) else item[4]),
            is_dispatched=bool(item['is_dispatched'] if isinstance(item, dict) else item[3]),
            is_delivered=bool(item['is_delivered'] if isinstance(item, dict) else item[2])
        )
        item_id = item['id'] if isinstance(item, dict) else item[0]
        conn.execute("UPDATE order_items SET kanban_status=? WHERE id=?", [color, item_id])
        rank = worst_color_rank.get(color, 0)
        if rank < order_kanban_rank:
            order_kanban = color
            order_kanban_rank = rank

    conn.execute("UPDATE orders SET kanban_status=? WHERE id=?", [order_kanban, order_id])
    conn.commit()


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
        resp = jsonify(result.get("body", {}))
        resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
        return resp, result.get("status", 200)
    except Exception as exc:
        import logging
        logging.exception("Unhandled error in dispatch")
        return jsonify({"error": "Internal server error"}), 500
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
        allowed, remaining = check_rate_limit(conn, f"email:{email}")
        if not allowed:
            return {"status": 429, "body": {"error": "Too many attempts. Try again later."}}
        row = conn.execute("SELECT * FROM users WHERE email=? AND is_active=1", [email]).fetchone()
        if not row or not check_password(row_to_dict(row).get("password_hash", ""), password):
            record_login_attempt(conn, f"email:{email}", False)
            return {"status": 401, "body": {"error": "Invalid credentials"}}
        user = row_to_dict(row)
        token = make_token(user["id"], user["role"])
        record_login_attempt(conn, f"email:{email}", True)
        # Upgrade legacy SHA256 hash to werkzeug PBKDF2 on successful login
        if user.get("password_hash") and not user["password_hash"].startswith("pbkdf2:"):
            conn.execute("UPDATE users SET password_hash=? WHERE id=?", [hash_password(password), user["id"]])
            conn.commit()
        user.pop("password_hash", None)
        user.pop("pin", None)
        return {"status": 200, "body": {"token": token, "user": user}}

    if method == "POST" and path == "/auth/pin-login":
        username = body.get("username", "").strip().lower()
        pin = body.get("pin", "").strip()
        if not username or not pin:
            return {"status": 400, "body": {"error": "username and pin required"}}
        allowed, remaining = check_rate_limit(conn, f"pin:{pin}:{request.remote_addr}")
        if not allowed:
            return {"status": 429, "body": {"error": "Too many attempts. Try again later."}}
        row = conn.execute("SELECT * FROM users WHERE username=? AND pin=? AND is_active=1", [username, pin]).fetchone()
        if not row:
            record_login_attempt(conn, f"pin:{pin}:{request.remote_addr}", False)
            return {"status": 401, "body": {"error": "Invalid username or PIN"}}
        user = row_to_dict(row)
        token = make_token(user["id"], user["role"])
        record_login_attempt(conn, f"pin:{pin}:{request.remote_addr}", True)
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
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
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
        if current_user["role"] not in ("executive", "office"):
            return {"status": 403, "body": {"error": "Admin role required to manage users"}}
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
            if current_user["role"] not in ("executive", "office"):
                return {"status": 403, "body": {"error": "Admin role required to manage users"}}
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
            if current_user["role"] not in ("executive", "office"):
                return {"status": 403, "body": {"error": "Only executive/office users can deactivate accounts"}}
            if uid == current_user["id"]:
                return {"status": 400, "body": {"error": "Cannot deactivate your own account"}}
            row = conn.execute("SELECT id FROM users WHERE id=?", [uid]).fetchone()
            if not row:
                return {"status": 404, "body": {"error": "User not found"}}
            conn.execute("UPDATE users SET is_active=0 WHERE id=?", [uid])
            conn.commit()
            return {"status": 200, "body": {"message": "User deactivated"}}

    # ----- CHANGE PASSWORD (self-service + admin reset) -----
    if method == "POST" and path == "/admin/change-password":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        target_user_id = body.get("user_id", current_user["id"])
        new_password = body.get("new_password", "").strip()
        current_password = body.get("current_password", "").strip()
        if not new_password or len(new_password) < 6:
            return {"status": 400, "body": {"error": "New password must be at least 6 characters"}}
        # Self-change: verify current password
        if target_user_id == current_user["id"]:
            row = conn.execute("SELECT password_hash FROM users WHERE id=?", [current_user["id"]]).fetchone()
            if row and not check_password(row[0], current_password):
                return {"status": 403, "body": {"error": "Current password is incorrect"}}
        else:
            # Admin resetting another user's password
            if current_user["role"] not in ("executive", "office"):
                return {"status": 403, "body": {"error": "Only executive/office can reset other users' passwords"}}
        new_hash = hash_password(new_password)
        conn.execute("UPDATE users SET password_hash=?, updated_at=CURRENT_TIMESTAMP WHERE id=?", [new_hash, target_user_id])
        conn.commit()
        target_name = conn.execute("SELECT full_name FROM users WHERE id=?", [target_user_id]).fetchone()
        log_audit(conn, current_user["id"], "CHANGE_PASSWORD", "users", target_user_id, None, {"changed_by": current_user["full_name"]})
        return {"status": 200, "body": {"message": f"Password updated for {target_name[0] if target_name else 'user'}"}}

    # ----- ZONES -----
    if method == "GET" and path == "/zones":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        zones = rows_to_list(conn.execute("SELECT * FROM zones WHERE is_active=1 ORDER BY name").fetchall())
        for z in zones:
            z["stations"] = rows_to_list(conn.execute("SELECT * FROM stations WHERE zone_id=? AND is_active=1 ORDER BY name", [z["id"]]).fetchall())
        return {"status": 200, "body": zones}

    if method == "POST" and path == "/zones":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        if current_user["role"] not in ("executive", "office"):
            return {"status": 403, "body": {"error": "Only executive/office roles can create zones"}}
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
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
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
        # Cascade: if zone deactivated, deactivate its stations
        if body.get("is_active") == 0 or body.get("is_active") == "0":
            conn.execute("UPDATE stations SET is_active=0 WHERE zone_id=?", [zid])
            conn.execute("DELETE FROM schedule_entries WHERE station_id IN (SELECT id FROM stations WHERE zone_id=? AND is_active=0) OR planned_station_id IN (SELECT id FROM stations WHERE zone_id=? AND is_active=0)", [zid, zid])
            conn.execute("DELETE FROM station_capacity WHERE station_id IN (SELECT id FROM stations WHERE zone_id=? AND is_active=0)", [zid])
            conn.commit()
        row = conn.execute("SELECT * FROM zones WHERE id=?", [zid]).fetchone()
        z = row_to_dict(row)
        z["stations"] = rows_to_list(conn.execute("SELECT * FROM stations WHERE zone_id=? AND is_active=1 ORDER BY name", [zid]).fetchall())
        return {"status": 200, "body": z}
    if m and method == "DELETE":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        zid = int(m["id"])
        conn.execute("UPDATE zones SET is_active=0 WHERE id=?", [zid])
        conn.execute("UPDATE stations SET is_active=0 WHERE zone_id=?", [zid])
        conn.execute("DELETE FROM schedule_entries WHERE station_id IN (SELECT id FROM stations WHERE zone_id=? AND is_active=0) OR planned_station_id IN (SELECT id FROM stations WHERE zone_id=? AND is_active=0)", [zid, zid])
        conn.execute("DELETE FROM station_capacity WHERE station_id IN (SELECT id FROM stations WHERE zone_id=? AND is_active=0)", [zid])
        conn.commit()
        return {"status": 200, "body": {"message": "Zone deactivated"}}

    m = match("/zones/:id/stations", path)
    if m and method == "GET":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        zone = conn.execute("SELECT id FROM zones WHERE id=?", [int(m["id"])]).fetchone()
        if not zone:
            return {"status": 404, "body": {"error": "Zone not found"}}
        rows = conn.execute("SELECT * FROM stations WHERE zone_id=? AND is_active=1 ORDER BY name", [int(m["id"])]).fetchall()
        return {"status": 200, "body": rows_to_list(rows)}

    m = match("/zones/:id/stations", path)
    if m and method == "POST":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
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

    if method == "GET" and path == "/stations":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        where, vals = ["is_active=1"], []
        if params.get("zone_id"):
            where.append("zone_id=?"); vals.append(params["zone_id"])
        rows = rows_to_list(conn.execute(f"SELECT * FROM stations WHERE {' AND '.join(where)} ORDER BY name", vals).fetchall())
        return {"status": 200, "body": rows}

    if method == "POST" and path == "/stations":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
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
            if not current_user:
                return {"status": 401, "body": {"error": "Authentication required"}}
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
            # Cascade: if station deactivated, clean up references
            if body.get("is_active") == 0 or body.get("is_active") == "0":
                conn.execute("DELETE FROM schedule_entries WHERE station_id=? OR planned_station_id=?", [sid, sid])
                conn.execute("DELETE FROM station_capacity WHERE station_id=?", [sid])
                conn.commit()
            row = conn.execute("SELECT * FROM stations WHERE id=?", [sid]).fetchone()
            return {"status": 200, "body": row_to_dict(row)}
        if method == "DELETE":
            if not current_user:
                return {"status": 401, "body": {"error": "Authentication required"}}
            conn.execute("UPDATE stations SET is_active=0 WHERE id=?", [sid])
            conn.execute("DELETE FROM schedule_entries WHERE station_id=? OR planned_station_id=?", [sid, sid])
            conn.execute("DELETE FROM station_capacity WHERE station_id=?", [sid])
            conn.commit()
            return {"status": 200, "body": {"message": "Station deactivated"}}

    # ----- ORDERS -----
    if method == "GET" and path == "/orders":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
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
        if current_user["role"] not in ("executive", "office", "planner"):
            return {"status": 403, "body": {"error": "Only office/planner roles can create orders"}}
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
            if not current_user:
                return {"status": 401, "body": {"error": "Authentication required"}}
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
            # If this order was verified and a substantive field changed, flag for re-verification
            substantive_fields = {"client_id", "total_value", "notes", "special_instructions"}
            if old.get("is_verified") == 1 and any(f in substantive_fields for f in body.keys()):
                conn.execute("UPDATE orders SET needs_reverify=1 WHERE id=?", [oid])
                conn.commit()
                log_audit(conn, current_user["id"], "order_modified_needs_reverify", "orders", oid, old, body)
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
        # Don't downgrade orders that are already past T
        if row["status"] not in ('T',):
            return {"status": 400, "body": {"error": f"Order is already at status '{row['status']}' — cannot re-verify"}}
        # Set is_verified ONLY — items stay at T until planner drags them onto the Planning Board
        # (which creates a schedule_entry and promotes T→C into the docking pipeline)
        conn.execute("UPDATE orders SET is_verified=1, needs_reverify=0, verified_by=?, verified_at=CURRENT_TIMESTAMP, updated_at=CURRENT_TIMESTAMP WHERE id=?", [current_user["id"], oid])
        conn.commit()
        client = conn.execute("SELECT * FROM clients WHERE id=?", [row["client_id"]]).fetchone()
        if client and client["email"]:
            log_and_send_notification(conn, oid, "order_acknowledgement", client["email"],
                f"Order {row['order_number']} Acknowledged",
                f"Your order {row['order_number']} has been received and verified.",
                f"<div style='font-family:Arial;max-width:500px;margin:auto;padding:24px;'><div style='background:#07324C;color:white;padding:16px;border-radius:8px 8px 0 0;text-align:center;'><h2 style='margin:0'>Order Acknowledged</h2></div><div style='padding:16px;border:1px solid #e5e7eb;border-top:none;border-radius:0 0 8px 8px;'><p>Your order <strong>{row['order_number']}</strong> has been received and verified.</p></div></div>")
        log_audit(conn, current_user["id"], "verify_order", "orders", oid)
        return {"status": 200, "body": order_full(conn, oid)}

    m = match("/orders/:id/docking-complete", path)
    if m and method == "PUT":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        allowed = ['planner','production_manager','floor_worker','executive','office','ops_manager']
        if current_user.get("role") not in allowed:
            return {"status": 403, "body": {"error": "Permission denied — requires planner, production manager, or floor team leader"}}
        oid = int(m["id"])
        row = conn.execute("SELECT * FROM orders WHERE id=?", [oid]).fetchone()
        if not row:
            return {"status": 404, "body": {"error": "Order not found"}}
        force = body.get("force", False)
        if force:
            # Force-complete: promote T and C items to R (planners/managers only)
            force_allowed = ['planner','production_manager','executive','office','ops_manager']
            if current_user.get("role") not in force_allowed:
                return {"status": 403, "body": {"error": "Force docking complete requires planner, production_manager, ops_manager, executive, or admin role"}}
            conn.execute(
                "UPDATE order_items SET status='R', docking_completed_at=CURRENT_TIMESTAMP, updated_at=CURRENT_TIMESTAMP WHERE order_id=? AND status IN ('T','C')",
                [oid]
            )
            conn.commit()
            sync_order_status(conn, oid)
            log_audit(conn, current_user["id"], "docking_complete_forced", "orders", oid)
        else:
            # Standard: promote only C-status items to R
            conn.execute(
                "UPDATE order_items SET status='R', docking_completed_at=CURRENT_TIMESTAMP, updated_at=CURRENT_TIMESTAMP WHERE order_id=? AND status='C'",
                [oid]
            )
            conn.commit()
            sync_order_status(conn, oid)
            log_audit(conn, current_user["id"], "docking_complete", "orders", oid)
        return {"status": 200, "body": order_full(conn, oid)}

    # Cut list issued flag
    m = match("/order-items/:id/cut-list-issued", path)
    if m and method == "PUT":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        iid = int(m["id"])
        row = conn.execute("SELECT * FROM order_items WHERE id=?", [iid]).fetchone()
        if not row:
            return {"status": 404, "body": {"error": "Order item not found"}}
        conn.execute("UPDATE order_items SET cut_list_issued=1, updated_at=CURRENT_TIMESTAMP WHERE id=?", [iid])
        conn.commit()
        log_audit(conn, current_user["id"], "cut_list_issued", "order_items", iid)
        return {"status": 200, "body": {"success": True}}

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
            "UPDATE order_items SET status='R', docking_completed_at=CURRENT_TIMESTAMP, updated_at=CURRENT_TIMESTAMP WHERE id=?",
            [iid]
        )
        conn.commit()
        # Sync order status
        sync_order_status(conn, item_row["order_id"])
        log_audit(conn, current_user["id"], "item_docking_complete", "order_items", iid)
        updated = conn.execute("SELECT * FROM order_items WHERE id=?", [iid]).fetchone()
        return {"status": 200, "body": row_to_dict(updated)}

    if method == "GET" and path == "/docking/log":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        # All items that have been through docking (status C or beyond, or docking_completed_at set)
        # Order by last action date descending (uses docking_completed_at, updated_at, or created_at)
        logs = rows_to_list(conn.execute("""
            SELECT oi.id, oi.order_id, oi.sku_code, oi.quantity, oi.status,
                   oi.docking_completed_at, oi.created_at as item_created,
                   o.order_number, c.company_name as client_name, o.status as order_status,
                   o.delivery_type, o.created_at as order_date,
                   z.name as zone_name,
                   COALESCE(oi.docking_completed_at, oi.created_at) as last_action_at,
                   CASE
                       WHEN oi.status = 'T' THEN 'Awaiting Cut List'
                       WHEN oi.status = 'C' THEN 'In Docking'
                       WHEN oi.status = 'R' AND oi.docking_completed_at IS NOT NULL THEN 'Docking Complete'
                       WHEN oi.status = 'R' THEN 'Ready for Production'
                       WHEN oi.status = 'P' THEN 'In Production / Packing'
                       WHEN oi.status = 'F' THEN 'Fulfilled'
                       ELSE oi.status
                   END as docking_status
            FROM order_items oi
            JOIN orders o ON o.id = oi.order_id
            LEFT JOIN clients c ON c.id = o.client_id
            LEFT JOIN zones z ON z.id = oi.zone_id
            WHERE (oi.status IN ('C','R','P','F') OR oi.docking_completed_at IS NOT NULL)
                  AND o.status NOT IN ('cancelled','archived')
            ORDER BY COALESCE(oi.docking_completed_at, oi.created_at) DESC
        """))
        return {"status": 200, "body": {"logs": logs}}

    if method == "GET" and path == "/docking/jobs":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        # Items currently in docking status (C) — jobs the chainsaw worker can complete
        jobs = rows_to_list(conn.execute("""
            SELECT oi.id, oi.order_id, oi.sku_code, oi.quantity, oi.status,
                   oi.created_at,
                   o.order_number, c.company_name as client_name, o.delivery_type,
                   z.name as zone_name
            FROM order_items oi
            JOIN orders o ON o.id = oi.order_id
            LEFT JOIN clients c ON c.id = o.client_id
            LEFT JOIN zones z ON z.id = oi.zone_id
            WHERE oi.status = 'C' AND o.status NOT IN ('cancelled','archived')
            ORDER BY oi.created_at ASC
        """))
        return {"status": 200, "body": {"jobs": jobs}}

    if method == "GET" and path == "/docking/board":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        zone_filter = params.get("zone_id")
        base_q = """
            SELECT oi.*, o.order_number, o.client_id, c.company_name as client_name,
                   s.name as sku_name, z.name as zone_name, z.code as zone_code,
                   se.scheduled_date, se.station_id as sched_station_id
            FROM order_items oi
            JOIN orders o ON o.id = oi.order_id
            LEFT JOIN clients c ON c.id = o.client_id
            LEFT JOIN skus s ON s.id = oi.sku_id
            LEFT JOIN zones z ON z.id = oi.zone_id
            LEFT JOIN schedule_entries se ON se.order_item_id = oi.id
        """

        # Docking Required: status='C', has schedule entry, cut_list_issued=0
        q1 = base_q + " WHERE oi.status='C' AND se.id IS NOT NULL AND oi.cut_list_issued=0"
        if zone_filter:
            q1 += " AND oi.zone_id=?"
            params_q1 = [int(zone_filter)]
        else:
            params_q1 = []
        q1 += " ORDER BY se.scheduled_date ASC, o.order_number"
        docking_required = rows_to_list(conn.execute(q1, params_q1).fetchall())

        # Cut List Issued: status='C', cut_list_issued=1
        q2 = base_q + " WHERE oi.status='C' AND oi.cut_list_issued=1"
        if zone_filter:
            q2 += " AND oi.zone_id=?"
            params_q2 = [int(zone_filter)]
        else:
            params_q2 = []
        q2 += " ORDER BY se.scheduled_date ASC, o.order_number"
        cut_list_issued = rows_to_list(conn.execute(q2, params_q2).fetchall())

        # Docking Complete: status='R' (recently completed)
        q3 = base_q + " WHERE oi.status='R'"
        if zone_filter:
            q3 += " AND oi.zone_id=?"
            params_q3 = [int(zone_filter)]
        else:
            params_q3 = []
        q3 += " ORDER BY COALESCE(oi.docking_completed_at, oi.updated_at, oi.created_at) DESC LIMIT 50"
        docking_complete = rows_to_list(conn.execute(q3, params_q3).fetchall())

        return {"status": 200, "body": {
            "docking_required": docking_required,
            "cut_list_issued": cut_list_issued,
            "docking_complete": docking_complete
        }}

    # ----- DELIVERY TYPE TOGGLE -----
    m = match("/orders/:id/delivery-type", path)
    if m and method == "PUT":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        oid = int(m["id"])
        new_type = body.get("delivery_type")
        if new_type not in ("delivery", "collection"):
            return {"status": 400, "body": {"error": "delivery_type must be 'delivery' or 'collection'"}}
        row = conn.execute("SELECT * FROM orders WHERE id=?", [oid]).fetchone()
        if not row:
            return {"status": 404, "body": {"error": "Order not found"}}
        old_type = row["delivery_type"]
        conn.execute("UPDATE orders SET delivery_type=?, updated_at=CURRENT_TIMESTAMP WHERE id=?", [new_type, oid])
        # Also update matching delivery_log entry
        conn.execute("UPDATE delivery_log SET delivery_type=?, updated_at=CURRENT_TIMESTAMP WHERE order_id=?", [new_type, oid])
        conn.commit()
        log_audit(conn, current_user["id"], f"delivery_type_changed_{old_type}_to_{new_type}", "orders", oid)
        return {"status": 200, "body": order_full(conn, oid)}

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
            extra_fields = ", dispatched_at=CURRENT_TIMESTAMP" if new_status == 'dispatched' else ""
            conn.execute(f"UPDATE orders SET status=?, updated_at=CURRENT_TIMESTAMP{extra_fields} WHERE id=?", [new_status, oid])
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
            # Build per-SKU email body with all order items
            all_items = conn.execute(
                "SELECT sku_code, product_name, quantity, eta_date FROM order_items WHERE order_id=? ORDER BY id",
                [oid]
            ).fetchall()
            item_lines = []
            for it in all_items:
                sku = it["sku_code"] or "Unknown SKU"
                pname = it["product_name"] or ""
                qty = it["quantity"] or 0
                item_eta = it["eta_date"] or eta
                item_lines.append(f"  - {sku} ({pname}) x {qty}: ETA {item_eta}")
            items_text = "\n".join(item_lines) if item_lines else f"  All items: ETA {eta}"
            email_body = f"Order {order_row['order_number']} ETA Update:\n{items_text}"
            log_and_send_notification(conn, oid, "eta_notification", client["email"],
                f"ETA Update for Order {order_row['order_number']}", email_body)
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
        # Log per-item ETA notification
        order_row = conn.execute("SELECT * FROM orders WHERE id=?", [item_row["order_id"]]).fetchone()
        if order_row:
            client = conn.execute("SELECT * FROM clients WHERE id=?", [order_row["client_id"]]).fetchone()
            if client and client["email"]:
                sku = item_row["sku_code"] or "Unknown SKU"
                pname = item_row["product_name"] or ""
                qty = item_row["quantity"] or 0
                email_body = f"Order {order_row['order_number']} — Item ETA Update:\n  - {sku} ({pname}) x {qty}: ETA {eta}"
                log_and_send_notification(conn, order_row["id"], "eta_notification", client["email"],
                    f"Item ETA Update for Order {order_row['order_number']}", email_body)
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
            if not current_user:
                return {"status": 401, "body": {"error": "Authentication required"}}
            order = conn.execute("SELECT id FROM orders WHERE id=?", [oid]).fetchone()
            if not order:
                return {"status": 404, "body": {"error": "Order not found"}}
            rows = conn.execute("SELECT oi.*, s.name as sku_name, z.name as zone_name, st.name as station_name FROM order_items oi LEFT JOIN skus s ON s.id=oi.sku_id LEFT JOIN zones z ON z.id=oi.zone_id LEFT JOIN stations st ON st.id=oi.station_id WHERE oi.order_id=? ORDER BY oi.id", [oid]).fetchall()
            return {"status": 200, "body": rows_to_list(rows)}
        if method == "POST":
            if not current_user:
                return {"status": 401, "body": {"error": "Authentication required"}}
            order = conn.execute("SELECT id FROM orders WHERE id=?", [oid]).fetchone()
            if not order:
                return {"status": 404, "body": {"error": "Order not found"}}
            if not body.get("quantity"):
                return {"status": 400, "body": {"error": "quantity required"}}
            sku_id = body.get("sku_id")
            sku_code = body.get("sku_code")
            product_name = body.get("product_name")
            unit_price = body.get("unit_price", 0)
            try:
                quantity = int(body["quantity"])
            except (TypeError, ValueError):
                return {"status": 400, "body": {"error": "quantity must be a valid integer"}}
            if sku_id:
                sku = conn.execute("SELECT * FROM skus WHERE id=?", [sku_id]).fetchone()
                if sku:
                    sku = row_to_dict(sku)
                    sku_code = sku_code or sku["code"]
                    product_name = product_name or sku["name"]
                    unit_price = unit_price or sku["sell_price"]
                    body.setdefault("zone_id", sku["zone_id"])
                    body.setdefault("drawing_number", sku["drawing_number"])
            # SKU prefix auto-routing if no zone_id set
            if not body.get("zone_id") and sku_code:
                prefix_upper = (sku_code or "").upper()
                zone_route = None
                if prefix_upper.startswith("CR"):
                    zone_route = "CRT"
                elif prefix_upper.startswith("HMP") or prefix_upper.startswith("2MP") or prefix_upper.startswith("1MP"):
                    zone_route = "HMP"
                elif prefix_upper.startswith("VP"):
                    zone_route = "VIK"
                elif prefix_upper.startswith("DTL"):
                    zone_route = "DTL"
                if zone_route:
                    z_row = conn.execute("SELECT id FROM zones WHERE code=?", [zone_route]).fetchone()
                    if z_row:
                        body["zone_id"] = z_row["id"]
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
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        iid = int(m["id"])
        row = conn.execute("SELECT * FROM order_items WHERE id=?", [iid]).fetchone()
        if not row:
            return {"status": 404, "body": {"error": "Order item not found"}}
        fields, vals = [], []
        for f in ["sku_id", "sku_code", "product_name", "quantity", "produced_quantity", "unit_price", "line_total", "zone_id", "station_id", "scheduled_date", "eta_date", "drawing_number", "special_instructions", "requested_delivery_date"]:
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
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
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
                  oi.sku_code, oi.product_name, oi.quantity as item_quantity, oi.status as item_status, oi.cut_list_issued,
                  o.order_number, o.status as order_status, o.client_id, c.company_name as client_name
                  FROM schedule_entries se LEFT JOIN zones z ON z.id=se.zone_id LEFT JOIN stations st ON st.id=se.station_id
                  LEFT JOIN shifts sh ON sh.id=se.shift_id LEFT JOIN order_items oi ON oi.id=se.order_item_id
                  LEFT JOIN orders o ON o.id=se.order_id LEFT JOIN clients c ON c.id=o.client_id
                  WHERE {' AND '.join(where)} ORDER BY se.scheduled_date, se.zone_id, COALESCE(se.station_id, se.planned_station_id)"""
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
            # Check for duplicate schedule entry for this item in this zone
            existing = conn.execute("SELECT id FROM schedule_entries WHERE order_item_id=? AND zone_id=?", [order_item_id, body["zone_id"]]).fetchone()
            if existing:
                return {"status": 409, "body": {"error": "This item is already scheduled in this zone", "existing_id": existing["id"]}}
            try:
                cur = conn.execute("INSERT INTO schedule_entries (order_id, order_item_id, zone_id, planned_station_id, scheduled_date, planned_quantity, notes, created_by) VALUES (?,?,?,?,?,?,?,?)",
                    [oid, order_item_id, body["zone_id"], body.get("planned_station_id"), body["scheduled_date"],
                     body.get("planned_quantity") or item["quantity"], body.get("notes"), current_user["id"]])
                # Update item schedule info — promote T→C (docking) but NOT to R
                # Docking (C→R) is a manual gate — planner/prod manager must release
                conn.execute("UPDATE order_items SET status=CASE WHEN status='T' THEN 'C' ELSE status END, scheduled_date=? WHERE id=? AND status IN ('T','C')",
                    [body["scheduled_date"], order_item_id])
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
            # Skip items that already have a schedule entry in this zone
            item_ids = [item["id"] for item in items]
            existing_ids = {row[0] for row in conn.execute(
                f"SELECT order_item_id FROM schedule_entries WHERE order_item_id IN ({','.join('?' * len(item_ids))}) AND zone_id=?",
                item_ids + [body["zone_id"]]
            ).fetchall()}
            created_entries = []
            for item in items:
                if item["id"] in existing_ids:
                    continue  # skip already-scheduled items
                cur = conn.execute("INSERT INTO schedule_entries (order_id, order_item_id, zone_id, planned_station_id, scheduled_date, shift_id, planned_quantity, notes, created_by) VALUES (?,?,?,?,?,?,?,?,?)",
                    [order_id, item["id"], body["zone_id"], body.get("planned_station_id"), body["scheduled_date"], body.get("shift_id"), item["quantity"] or body.get("planned_quantity"), body.get("notes"), current_user["id"]])
                created_entries.append(cur.lastrowid)
                # Update item schedule info — promote T→C (docking) but NOT to R
                # Docking (C→R) is a manual gate — planner/prod manager must release
                conn.execute("UPDATE order_items SET status=CASE WHEN status='T' THEN 'C' ELSE status END, scheduled_date=? WHERE id=? AND status IN ('T','C')",
                    [body["scheduled_date"], item["id"]])
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
            if not current_user:
                return {"status": 401, "body": {"error": "Authentication required"}}
            row = conn.execute("SELECT id FROM schedule_entries WHERE id=?", [sid]).fetchone()
            if not row:
                return {"status": 404, "body": {"error": "Schedule entry not found"}}
            fields, vals = [], []
            for f in ["order_item_id", "zone_id", "station_id", "planned_station_id", "scheduled_date", "shift_id", "planned_quantity", "status", "notes", "priority", "run_order"]:
                if f in body:
                    fields.append(f"{f}=?"); vals.append(body[f])
            if not fields:
                return {"status": 400, "body": {"error": "No updatable fields"}}
            fields.append("updated_at=CURRENT_TIMESTAMP")
            vals.append(sid)
            conn.execute(f"UPDATE schedule_entries SET {', '.join(fields)} WHERE id=?", vals)
            # Sync station_id to order_items when station changes (Station Allocation flow)
            if "station_id" in body:
                se_row = conn.execute("SELECT order_item_id FROM schedule_entries WHERE id=?", [sid]).fetchone()
                if se_row and se_row["order_item_id"]:
                    conn.execute("UPDATE order_items SET station_id=? WHERE id=?", [body["station_id"], se_row["order_item_id"]])
            conn.commit()
            row = conn.execute("SELECT * FROM schedule_entries WHERE id=?", [sid]).fetchone()
            return {"status": 200, "body": row_to_dict(row)}
        if method == "DELETE":
            if not current_user:
                return {"status": 401, "body": {"error": "Authentication required"}}
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
        new_station = body.get("planned_station_id", body.get("station_id"))  # accept planned_station_id; fall back to station_id for backwards compat
        reset_eta = body.get("reset_eta", False)
        silent = body.get("silent", False)
        if not new_date:
            return {"status": 400, "body": {"error": "scheduled_date required"}}
        # Update the schedule entry
        fields = ["scheduled_date=?", "updated_at=CURRENT_TIMESTAMP"]
        vals = [new_date]
        if new_station is not None:
            fields.append("planned_station_id=?")
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
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
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
                    WHERE (se.station_id=? OR (se.station_id IS NULL AND se.planned_station_id=?))
                      AND se.status IN ('planned','in_progress')
                      AND oi.status IN ('R','P')
                    ORDER BY se.priority DESC, se.scheduled_date ASC
                """, [station["id"], station["id"]]).fetchall())
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
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
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

    m = match("/production/sessions/:id", path)
    if m and method == "GET":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        sid = int(m["id"])
        session = conn.execute("SELECT * FROM production_sessions WHERE id=?", [sid]).fetchone()
        if not session:
            return {"status": 404, "body": {"error": "Session not found"}}
        s = row_to_dict(session)
        s["workers"] = rows_to_list(conn.execute("SELECT sw.*, u.full_name, u.username FROM session_workers sw JOIN users u ON u.id=sw.user_id WHERE sw.session_id=? AND sw.is_active=1", [sid]).fetchall())
        # Calculate pause info
        pauses = rows_to_list(conn.execute("SELECT * FROM pause_logs WHERE session_id=? ORDER BY paused_at DESC", [sid]).fetchall())
        s["pause_logs"] = pauses
        open_pause = next((p for p in pauses if p.get("resumed_at") is None), None)
        if open_pause:
            s["paused_at"] = open_pause["paused_at"]
        return {"status": 200, "body": s}

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
        valid_reasons = ["wait_material","tool_breakdown","machine_fault","lunch","smoko_break","qa_hold","waiting_instructions","other"]
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
            # Sum ALL completed session quantities for this item (including this one)
            total_produced = conn.execute(
                "SELECT COALESCE(SUM(produced_quantity),0) FROM production_sessions WHERE order_item_id=? AND status='completed'",
                [session["order_item_id"]]
            ).fetchone()[0]
            conn.execute("UPDATE order_items SET produced_quantity=? WHERE id=?", [total_produced, session["order_item_id"]])
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

    m = match("/production/sessions/:id/sub-assembly", path)
    if m and method == "PUT":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        sid = int(m["id"])
        mode = body.get("is_sub_assembly_mode", 0)
        conn.execute("UPDATE production_sessions SET is_sub_assembly_mode=? WHERE id=?", [mode, sid])
        conn.commit()
        return {"status": 200, "body": {"session_id": sid, "is_sub_assembly_mode": mode}}

    m = match("/production/sessions/:id/sub-assembly-log", path)
    if m and method == "PUT":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        sid = int(m["id"])
        qty_change = body.get("quantity_change", 0)
        session = conn.execute("SELECT * FROM production_sessions WHERE id=?", [sid]).fetchone()
        if not session:
            return {"status": 404, "body": {"error": "Session not found"}}
        new_sub_total = max(0, (session["sub_assembly_count"] or 0) + int(qty_change))
        conn.execute("UPDATE production_sessions SET sub_assembly_count=? WHERE id=?", [new_sub_total, sid])
        conn.execute("INSERT INTO production_logs (session_id, user_id, quantity_change, running_total) VALUES (?,?,?,?)",
            [sid, current_user["id"], qty_change, new_sub_total])
        conn.commit()
        return {"status": 200, "body": {"session_id": sid, "sub_assembly_count": new_sub_total, "quantity_change": qty_change}}

    m = match("/production/sessions/:id/workers", path)
    if m and method == "POST":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
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

    m = match("/production/sessions/:id/workers/:wid", path)
    if m and method == "DELETE":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        sid = int(m["id"])
        wid = int(m["wid"])
        conn.execute("UPDATE session_workers SET scan_off_time=CURRENT_TIMESTAMP, is_active=0 WHERE session_id=? AND user_id=?", [sid, wid])
        conn.commit()
        return {"status": 200, "body": {"message": "Worker scanned off", "session_id": sid, "user_id": wid}}

    # ----- SETUP -----
    if method == "POST" and path == "/setup":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        for f in ["station_id", "setup_type"]:
            if not body.get(f):
                return {"status": 400, "body": {"error": f"Field '{f}' is required"}}
        try:
            cur = conn.execute("""INSERT INTO setup_logs (station_id, order_item_id, setup_type, notes, qa_checklist_json)
                VALUES (?,?,?,?,?)""",
                [body["station_id"], body.get("order_item_id"), body["setup_type"], body.get("notes"),
                 json.dumps(body.get("qa_checklist", []))])
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
        checklist = body.get("qa_checklist", [])
        conn.execute("""UPDATE setup_logs SET completed_at=CURRENT_TIMESTAMP,
            duration_minutes=ROUND((julianday('now')-julianday(started_at))*1440,2),
            qa_checklist_passed=?, qa_checklist_json=?, team_leader_id=?, team_leader_signed_at=CURRENT_TIMESTAMP
            WHERE id=?""",
            [qa_passed, json.dumps(checklist), current_user["id"], sid])
        conn.commit()
        row = conn.execute("SELECT * FROM setup_logs WHERE id=?", [sid]).fetchone()
        return {"status": 200, "body": row_to_dict(row)}

    m = match("/setup/:id/reverify", path)
    if m and method == "PUT":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        sid = int(m["id"])
        row = conn.execute("SELECT * FROM setup_logs WHERE id=?", [sid]).fetchone()
        if not row:
            return {"status": 404, "body": {"error": "Setup log not found"}}
        checklist = body.get("qa_checklist", [])
        passed = body.get("qa_checklist_passed", 0)
        conn.execute("""UPDATE setup_logs SET needs_reverify=0, qa_checklist_passed=?,
            qa_checklist_json=?, team_leader_id=?, team_leader_signed_at=CURRENT_TIMESTAMP WHERE id=?""",
            [passed, json.dumps(checklist), current_user["id"], sid])
        conn.commit()
        row = conn.execute("SELECT * FROM setup_logs WHERE id=?", [sid]).fetchone()
        return {"status": 200, "body": row_to_dict(row)}

    # ----- QA -----
    if method == "GET" and path == "/qa/inspections":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
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
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
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
        if current_user["role"] not in ("qa_lead", "production_manager", "executive", "office"):
            return {"status": 403, "body": {"error": "QA lead or manager role required to approve inspections"}}
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

    # ----- POST-PRODUCTION PROCESSES -----
    if method == "GET" and path == "/post-production/processes":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        rows = conn.execute("SELECT * FROM post_production_processes WHERE is_active=1 ORDER BY name").fetchall()
        return {"status": 200, "body": rows_to_list(rows)}

    if method == "POST" and path == "/post-production/processes":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        cur = conn.execute("INSERT INTO post_production_processes (name, description, requires_dashboard, triggers_notification) VALUES (?,?,?,?)",
            [body.get("name"), body.get("description"), body.get("requires_dashboard", 1), body.get("triggers_notification", 1)])
        conn.commit()
        return {"status": 201, "body": row_to_dict(conn.execute("SELECT * FROM post_production_processes WHERE id=?", [cur.lastrowid]).fetchone())}

    if method == "GET" and path == "/post-production/log":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        where, vals = ["1=1"], []
        if params.get("order_item_id"):
            where.append("ppl.order_item_id=?"); vals.append(params["order_item_id"])
        if params.get("process_id"):
            where.append("ppl.process_id=?"); vals.append(params["process_id"])
        if params.get("status"):
            where.append("ppl.status=?"); vals.append(params["status"])
        rows = conn.execute(f"""
            SELECT ppl.*, pp.name as process_name, oi.sku_code, oi.product_name, oi.quantity as item_qty,
                   o.order_number, c.company_name as client_name, u.full_name as operator_name
            FROM post_production_log ppl
            LEFT JOIN post_production_processes pp ON pp.id=ppl.process_id
            LEFT JOIN order_items oi ON oi.id=ppl.order_item_id
            LEFT JOIN orders o ON o.id=oi.order_id
            LEFT JOIN clients c ON c.id=o.client_id
            LEFT JOIN users u ON u.id=ppl.operator_id
            WHERE {' AND '.join(where)}
            ORDER BY ppl.started_at DESC
        """, vals).fetchall()
        return {"status": 200, "body": rows_to_list(rows)}

    if method == "POST" and path == "/post-production/log":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        cur = conn.execute("""INSERT INTO post_production_log (order_item_id, process_id, operator_id, quantity, facility, notes, status)
            VALUES (?,?,?,?,?,?,?)""",
            [body.get("order_item_id"), body.get("process_id"), current_user["id"],
             body.get("quantity"), body.get("facility"), body.get("notes"), "in_progress"])
        conn.commit()
        return {"status": 201, "body": row_to_dict(conn.execute("SELECT * FROM post_production_log WHERE id=?", [cur.lastrowid]).fetchone())}

    m = match("/post-production/log/:id/complete", path)
    if m and method == "PUT":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        pid = int(m["id"])
        conn.execute("UPDATE post_production_log SET status='completed', completed_at=CURRENT_TIMESTAMP WHERE id=?", [pid])
        conn.commit()
        entry = conn.execute("SELECT * FROM post_production_log WHERE id=?", [pid]).fetchone()
        if entry and entry["order_item_id"]:
            oi_row = conn.execute("SELECT order_id FROM order_items WHERE id=?", [entry["order_item_id"]]).fetchone()
            if oi_row:
                log_and_send_notification(conn, oi_row["order_id"], "dispatch_notification",
                    "office@hynepallets.com.au", "Post-production complete", "Process completed for item")
        return {"status": 200, "body": row_to_dict(conn.execute("SELECT * FROM post_production_log WHERE id=?", [pid]).fetchone())}

    # ----- QA AUDITS (management spot checks) -----
    if method == "GET" and path == "/qa/audits":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        where, vals = ["1=1"], []
        if params.get("zone_id"):
            where.append("qa.zone_id=?"); vals.append(params["zone_id"])
        if params.get("station_id"):
            where.append("qa.station_id=?"); vals.append(params["station_id"])
        if params.get("auditor_id"):
            where.append("qa.auditor_id=?"); vals.append(params["auditor_id"])
        rows = conn.execute(f"""
            SELECT qa.*, u.full_name as auditor_name, z.name as zone_name, st.name as station_name,
                   oi.sku_code, oi.product_name
            FROM qa_audits qa
            LEFT JOIN users u ON u.id=qa.auditor_id
            LEFT JOIN zones z ON z.id=qa.zone_id
            LEFT JOIN stations st ON st.id=qa.station_id
            LEFT JOIN order_items oi ON oi.id=qa.order_item_id
            WHERE {' AND '.join(where)}
            ORDER BY qa.created_at DESC
        """, vals).fetchall()
        return {"status": 200, "body": rows_to_list(rows)}

    if method == "POST" and path == "/qa/audits":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        if current_user["role"] not in ("executive", "production_manager", "qa_lead"):
            return {"status": 403, "body": {"error": "Only management can perform audits"}}
        cur = conn.execute("""INSERT INTO qa_audits (station_id, zone_id, auditor_id, order_item_id, session_id,
            audit_type, batch_size, passed, notes, photos) VALUES (?,?,?,?,?,?,?,?,?,?)""",
            [body.get("station_id"), body.get("zone_id"), current_user["id"],
             body.get("order_item_id"), body.get("session_id"),
             body.get("audit_type", "spot_check"), body.get("batch_size", 1),
             body.get("passed", 0), body.get("notes"), body.get("photos")])
        conn.commit()
        row = conn.execute("SELECT * FROM qa_audits WHERE id=?", [cur.lastrowid]).fetchone()
        log_audit(conn, current_user["id"], "qa_audit", "qa_audits", cur.lastrowid)
        return {"status": 201, "body": row_to_dict(row)}

    # ----- WORKER APP ENDPOINTS (Block 2 Phase 2) -----

    # GET /production/worker-station-data
    if method == "GET" and path == "/production/worker-station-data":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        station_id = params.get("station_id")
        zone_id = params.get("zone_id")
        if not station_id or not zone_id:
            return {"status": 400, "body": {"error": "station_id and zone_id required"}}
        # Get scheduled work orders for this station
        scheduled = rows_to_list(conn.execute("""
            SELECT se.*, oi.sku_code, oi.product_name, oi.quantity, oi.produced_quantity, oi.drawing_number,
                   oi.status as item_status, oi.id as order_item_id,
                   o.order_number, o.special_instructions, c.company_name as client_name,
                   s.sell_price, s.labour_mins_per_unit
            FROM schedule_entries se
            LEFT JOIN order_items oi ON oi.id=se.order_item_id
            LEFT JOIN orders o ON o.id=se.order_id
            LEFT JOIN clients c ON c.id=o.client_id
            LEFT JOIN skus s ON s.id=oi.sku_id
            WHERE se.station_id=? AND se.status IN ('planned','in_progress')
              AND oi.status IN ('R','P')
            ORDER BY se.priority DESC, se.scheduled_date ASC
        """, [station_id]).fetchall())
        # Get active/paused sessions at this station
        active_sessions = rows_to_list(conn.execute("""
            SELECT ps.*, oi.sku_code, oi.product_name, oi.quantity as order_qty,
                   oi.drawing_number, o.order_number, c.company_name as client_name,
                   s.sell_price, s.labour_mins_per_unit
            FROM production_sessions ps
            LEFT JOIN order_items oi ON oi.id=ps.order_item_id
            LEFT JOIN orders o ON o.id=oi.order_id
            LEFT JOIN clients c ON c.id=o.client_id
            LEFT JOIN skus s ON s.id=oi.sku_id
            WHERE ps.station_id=? AND ps.status IN ('active','paused')
            ORDER BY ps.start_time DESC
        """, [station_id]).fetchall())
        for s in active_sessions:
            s["workers"] = rows_to_list(conn.execute("""
                SELECT sw.*, u.full_name, u.username
                FROM session_workers sw JOIN users u ON u.id=sw.user_id
                WHERE sw.session_id=? AND sw.is_active=1
            """, [s["id"]]).fetchall())
            s["pause_logs"] = rows_to_list(conn.execute(
                "SELECT * FROM pause_logs WHERE session_id=? ORDER BY paused_at DESC", [s["id"]]).fetchall())
        # Get station and zone info
        station = row_to_dict(conn.execute("SELECT * FROM stations WHERE id=?", [station_id]).fetchone()) if station_id else None
        zone = row_to_dict(conn.execute("SELECT * FROM zones WHERE id=?", [zone_id]).fetchone()) if zone_id else None
        return {"status": 200, "body": {"station": station, "zone": zone, "scheduled_work": scheduled, "active_sessions": active_sessions}}

    # GET /production/combined-progress/:item_id
    m = match("/production/combined-progress/:item_id", path)
    if m and method == "GET":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        item_id = int(m["item_id"])
        sessions = rows_to_list(conn.execute("""
            SELECT ps.*, st.name as station_name, z.name as zone_name
            FROM production_sessions ps
            LEFT JOIN stations st ON st.id=ps.station_id
            LEFT JOIN zones z ON z.id=ps.zone_id
            WHERE ps.order_item_id=? AND ps.status IN ('active','paused','completed')
            ORDER BY ps.start_time DESC
        """, [item_id]).fetchall())
        total_produced = sum(s["produced_quantity"] or 0 for s in sessions)
        item = row_to_dict(conn.execute("SELECT * FROM order_items WHERE id=?", [item_id]).fetchone())
        return {"status": 200, "body": {"order_item_id": item_id, "total_produced": total_produced, "target": item["quantity"] if item else 0, "sessions": sessions}}

    # GET /production/shift-summary
    if method == "GET" and path == "/production/shift-summary":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        station_id = params.get("station_id")
        zone_id = params.get("zone_id")
        if not station_id:
            return {"status": 400, "body": {"error": "station_id required"}}
        # Get all sessions at this station today
        active = rows_to_list(conn.execute(
            "SELECT * FROM production_sessions WHERE station_id=? AND status IN ('active','paused')", [station_id]).fetchall())
        completed_today = rows_to_list(conn.execute(
            "SELECT * FROM production_sessions WHERE station_id=? AND status='completed' AND DATE(end_time)=DATE('now')", [station_id]).fetchall())
        all_sessions = active + completed_today
        total_produced = sum(s.get("produced_quantity") or 0 for s in all_sessions)
        # Calculate total run seconds
        total_run_secs = 0
        for s in all_sessions:
            start = s.get("start_time")
            end = s.get("end_time")
            if start:
                from datetime import datetime as dt2
                try:
                    s_dt = dt2.fromisoformat(start.replace("Z",""))
                    e_dt = dt2.fromisoformat(end.replace("Z","")) if end else dt2.utcnow()
                    gross_secs = (e_dt - s_dt).total_seconds()
                    # Subtract pauses
                    pause_mins = sum(p["duration_minutes"] or 0 for p in rows_to_list(conn.execute(
                        "SELECT duration_minutes FROM pause_logs WHERE session_id=?", [s["id"]]).fetchall()))
                    total_run_secs += max(0, gross_secs - pause_mins * 60)
                except Exception:                     pass
        return {"status": 200, "body": {
            "station_id": int(station_id),
            "sessions_active": len(active),
            "sessions_closed": len(completed_today),
            "total_produced": total_produced,
            "total_run_seconds": round(total_run_secs)
        }}

    # POST /production/floor-event (worker event logging)
    if method == "POST" and path == "/production/floor-event":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        event_type = body.get("event_type")
        if not event_type:
            return {"status": 400, "body": {"error": "event_type required"}}
        log_audit(conn, current_user["id"], event_type, body.get("entity_type", "worker_app"), body.get("entity_id"), None, json.dumps({k:v for k,v in body.items() if k not in ("event_type","entity_type","entity_id")}))
        return {"status": 200, "body": {"logged": True, "event_type": event_type}}

    # POST /production/qa-check (floor QA alert acknowledgement)
    if method == "POST" and path == "/production/qa-check":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        session_id = body.get("session_id")
        if not session_id:
            return {"status": 400, "body": {"error": "session_id required"}}
        session = conn.execute("SELECT * FROM production_sessions WHERE id=?", [session_id]).fetchone()
        if not session:
            return {"status": 404, "body": {"error": "Session not found"}}
        try:
            cur = conn.execute("""INSERT INTO qa_inspections (order_item_id, session_id, inspection_type, batch_size, passed, inspector_id, notes)
                VALUES (?,?,'batch',?,?,?,?)""",
                [session["order_item_id"], session_id, body.get("pallet_count", 0), body.get("passed", 1), current_user["id"], body.get("notes", f"QA check at {body.get('pallet_count', 0)} pallets")])
            conn.commit()
            return {"status": 201, "body": {"id": cur.lastrowid, "logged": True}}
        except Exception as e:
            return {"status": 500, "body": {"error": str(e)}}

    # POST /production/shift-changeover
    if method == "POST" and path == "/production/shift-changeover":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        station_id = body.get("station_id")
        zone_id = body.get("zone_id")
        if not station_id:
            return {"status": 400, "body": {"error": "station_id required"}}
        # Complete all active sessions at this station
        active = rows_to_list(conn.execute(
            "SELECT * FROM production_sessions WHERE station_id=? AND status IN ('active','paused')", [station_id]).fetchall())
        changeover_results = []
        for session in active:
            # Close any open pauses
            conn.execute("UPDATE pause_logs SET resumed_at=CURRENT_TIMESTAMP, duration_minutes=ROUND((julianday('now')-julianday(paused_at))*1440,2) WHERE session_id=? AND resumed_at IS NULL", [session["id"]])
            # Mark session completed
            conn.execute("UPDATE production_sessions SET status='completed', end_time=CURRENT_TIMESTAMP WHERE id=?", [session["id"]])
            conn.execute("UPDATE session_workers SET scan_off_time=CURRENT_TIMESTAMP, is_active=0 WHERE session_id=? AND is_active=1", [session["id"]])
            # Update order item produced quantity
            if session["order_item_id"]:
                total = conn.execute("SELECT COALESCE(SUM(produced_quantity),0) FROM production_sessions WHERE order_item_id=? AND status='completed'", [session["order_item_id"]]).fetchone()[0]
                conn.execute("UPDATE order_items SET produced_quantity=? WHERE id=?", [total, session["order_item_id"]])
            # QA check on shift changeover
            if session["order_item_id"]:
                try:
                    conn.execute("""INSERT INTO qa_inspections (order_item_id, session_id, inspection_type, batch_size, inspector_id, notes)
                        VALUES (?,?,'batch',?,?,?)""",
                        [session["order_item_id"], session["id"], session["produced_quantity"] or 0, current_user["id"], "Auto-created: shift changeover QA check"])
                except Exception:                     pass
            changeover_results.append({"session_id": session["id"], "produced": session["produced_quantity"], "order_item_id": session["order_item_id"]})
        conn.commit()
        log_audit(conn, current_user["id"], "shift_changeover", "stations", station_id)
        return {"status": 200, "body": {"station_id": station_id, "sessions_closed": len(changeover_results), "details": changeover_results}}

    # GET /production/drawings (list, no file_data)
    if method == "GET" and path == "/production/drawings":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        where, vals = ["1=1"], []
        if params.get("sku_id"):
            where.append("sku_id=?"); vals.append(params["sku_id"])
        if params.get("order_item_id"):
            where.append("order_item_id=?"); vals.append(params["order_item_id"])
        rows = rows_to_list(conn.execute(f"SELECT id, sku_id, order_item_id, file_name, file_type, uploaded_by, uploaded_at, notes FROM drawing_files WHERE {' AND '.join(where)} ORDER BY uploaded_at DESC", vals).fetchall())
        return {"status": 200, "body": rows}

    # GET /production/drawings/:id (single drawing with file_data)
    m = match("/production/drawings/:id", path)
    if m and method == "GET":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        did = int(m["id"])
        row = conn.execute("SELECT * FROM drawing_files WHERE id=?", [did]).fetchone()
        if not row:
            return {"status": 404, "body": {"error": "Drawing not found"}}
        return {"status": 200, "body": row_to_dict(row)}

    # POST /production/drawings (upload new drawing)
    if method == "POST" and path == "/production/drawings":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        for f in ["file_name", "file_data"]:
            if not body.get(f):
                return {"status": 400, "body": {"error": f"Field '{f}' required"}}
        cur = conn.execute("INSERT INTO drawing_files (sku_id, order_item_id, file_name, file_type, file_data, uploaded_by, notes) VALUES (?,?,?,?,?,?,?)",
            [body.get("sku_id"), body.get("order_item_id"), body["file_name"], body.get("file_type", "image"), body["file_data"], current_user["id"], body.get("notes")])
        conn.commit()
        row = conn.execute("SELECT id, sku_id, order_item_id, file_name, file_type, uploaded_by, uploaded_at, notes FROM drawing_files WHERE id=?", [cur.lastrowid]).fetchone()
        return {"status": 201, "body": row_to_dict(row)}

    # DELETE /production/drawings/:id
    m = match("/production/drawings/:id", path)
    if m and method == "DELETE":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        did = int(m["id"])
        conn.execute("DELETE FROM drawing_files WHERE id=?", [did])
        conn.commit()
        return {"status": 200, "body": {"deleted": did}}

    # GET /production/session-summary/:id
    m = match("/production/session-summary/:id", path)
    if m and method == "GET":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        sid = int(m["id"])
        session = row_to_dict(conn.execute("""
            SELECT ps.*, oi.sku_code, oi.product_name, oi.quantity as order_qty, oi.drawing_number,
                   o.order_number, c.company_name as client_name, st.name as station_name, z.name as zone_name
            FROM production_sessions ps
            LEFT JOIN order_items oi ON oi.id=ps.order_item_id
            LEFT JOIN orders o ON o.id=oi.order_id
            LEFT JOIN clients c ON c.id=o.client_id
            LEFT JOIN stations st ON st.id=ps.station_id
            LEFT JOIN zones z ON z.id=ps.zone_id
            WHERE ps.id=?
        """, [sid]).fetchone())
        if not session:
            return {"status": 404, "body": {"error": "Session not found"}}
        # Calculate times
        pauses = rows_to_list(conn.execute("SELECT * FROM pause_logs WHERE session_id=? ORDER BY paused_at", [sid]).fetchall())
        total_pause_mins = sum(p["duration_minutes"] or 0 for p in pauses)
        workers = rows_to_list(conn.execute("SELECT sw.*, u.full_name FROM session_workers sw JOIN users u ON u.id=sw.user_id WHERE sw.session_id=?", [sid]).fetchall())
        # Net run time
        start = session.get("start_time")
        end = session.get("end_time")
        if start and end:
            from datetime import datetime as dt2
            try:
                s_dt = dt2.fromisoformat(start.replace("Z",""))
                e_dt = dt2.fromisoformat(end.replace("Z",""))
                gross_mins = (e_dt - s_dt).total_seconds() / 60
            except Exception:                 gross_mins = 0
        else:
            gross_mins = 0
        net_mins = max(0, gross_mins - total_pause_mins)
        net_hours = net_mins / 60
        labour_cost = round(net_hours * 55 * max(1, len([w for w in workers if w.get("is_active") is not None])), 2)
        variance = (session.get("produced_quantity") or 0) - (session.get("target_quantity") or 0)
        session["pause_logs"] = pauses
        session["workers"] = workers
        session["total_pause_minutes"] = round(total_pause_mins, 2)
        session["net_run_minutes"] = round(net_mins, 2)
        session["gross_minutes"] = round(gross_mins, 2)
        session["labour_cost"] = labour_cost
        session["variance"] = variance
        return {"status": 200, "body": session}

    # GET /skus/search (SKU autocomplete)
    if method == "GET" and path == "/skus/search":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        q = params.get("q", "")
        zone_id = params.get("zone_id")
        where, vals = [], []
        if q:
            where.append("(s.code LIKE ? OR s.name LIKE ?)"); vals.extend([f"%{q}%", f"%{q}%"])
        if zone_id:
            where.append("s.zone_id=?"); vals.append(zone_id)
        where_str = " AND ".join(where) if where else "1=1"
        rows = rows_to_list(conn.execute(f"SELECT s.*, z.name as zone_name FROM skus s LEFT JOIN zones z ON z.id=s.zone_id WHERE {where_str} ORDER BY s.code LIMIT 50", vals).fetchall())
        return {"status": 200, "body": rows}

    # ----- DISPATCH -----
    if method == "GET" and path == "/dispatch":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        date = params.get("date", datetime.now(timezone.utc).strftime("%Y-%m-%d"))
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
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        where, vals = ["1=1"], []
        if params.get("order_id"):
            where.append("dl.order_id=?"); vals.append(params["order_id"])
        if params.get("status"):
            where.append("dl.status=?"); vals.append(params["status"])
        rows = conn.execute(f"SELECT dl.*, o.order_number, c.company_name, t.name as truck_name FROM delivery_log dl LEFT JOIN orders o ON o.id=dl.order_id LEFT JOIN clients c ON c.id=o.client_id LEFT JOIN trucks t ON t.id=dl.truck_id WHERE {' AND '.join(where)} ORDER BY dl.expected_date DESC, dl.load_sequence", vals).fetchall()
        return {"status": 200, "body": rows_to_list(rows)}

    if method == "POST" and path == "/delivery-log":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
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
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        lid = int(m["id"])
        row = conn.execute("SELECT id FROM delivery_log WHERE id=?", [lid]).fetchone()
        if not row:
            return {"status": 404, "body": {"error": "Delivery log entry not found"}}
        fields, vals = [], []
        for f in ["expected_date", "actual_date", "truck_id", "delivery_type", "status", "load_sequence", "notes", "estimated_minutes"]:
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
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        rows = conn.execute("SELECT * FROM trucks WHERE is_active=1 ORDER BY id").fetchall()
        result = rows_to_list(rows)
        for t in result:
            caps = rows_to_list(conn.execute("SELECT * FROM truck_capacity_config WHERE truck_id=? ORDER BY day_of_week", [t["id"]]).fetchall())
            t["capacity_config"] = {str(c["day_of_week"]): c for c in caps}
        return {"status": 200, "body": result}

    if method == "POST" and path == "/trucks":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        if current_user["role"] not in ("executive", "office", "dispatch"):
            return {"status": 403, "body": {"error": "Insufficient role"}}
        name = body.get("name")
        if not name:
            return {"status": 400, "body": {"error": "Truck name is required"}}
        try:
            cur = conn.execute(
                "INSERT INTO trucks (name, rego, capacity_pallets, notes) VALUES (?,?,?,?)",
                [name, body.get("rego"), body.get("capacity_pallets", 0), body.get("notes")]
            )
            conn.commit()
            row = conn.execute("SELECT * FROM trucks WHERE id=?", [cur.lastrowid]).fetchone()
            return {"status": 201, "body": row_to_dict(row)}
        except Exception as e:
            return {"status": 409, "body": {"error": str(e)}}

    m = match("/trucks/:id", path)
    if m and method == "PUT":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        if current_user["role"] not in ("executive", "office", "dispatch"):
            return {"status": 403, "body": {"error": "Insufficient role"}}
        tid = int(m["id"])
        row = conn.execute("SELECT id FROM trucks WHERE id=?", [tid]).fetchone()
        if not row:
            return {"status": 404, "body": {"error": "Truck not found"}}
        fields, vals = [], []
        for f in ["name", "rego", "capacity_pallets", "notes", "is_active"]:
            if f in body:
                fields.append(f"{f}=?"); vals.append(body[f])
        if not fields:
            return {"status": 400, "body": {"error": "No updatable fields"}}
        fields.append("updated_at=CURRENT_TIMESTAMP")
        vals.append(tid)
        conn.execute(f"UPDATE trucks SET {', '.join(fields)} WHERE id=?", vals)
        conn.commit()
        row = conn.execute("SELECT * FROM trucks WHERE id=?", [tid]).fetchone()
        return {"status": 200, "body": row_to_dict(row)}
    if m and method == "DELETE":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        if current_user["role"] not in ("executive", "office"):
            return {"status": 403, "body": {"error": "Insufficient role"}}
        tid = int(m["id"])
        conn.execute("UPDATE trucks SET is_active=0 WHERE id=?", [tid])
        conn.commit()
        return {"status": 200, "body": {"message": "Truck deactivated"}}

    # ----- DISPATCH PLANNING (date-range, truck-based) -----
    if method == "GET" and path == "/dispatch-planning":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        date_from = params.get("date_from", datetime.now(timezone.utc).strftime("%Y-%m-%d"))
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
            SELECT dl.*, o.order_number, o.delivery_type as order_delivery_type, o.special_instructions, o.kanban_status as order_kanban,
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
                           oi.eta_date as item_eta, oi.kanban_status
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
            # Get dispatch runs for this day
            day_runs = rows_to_list(conn.execute("""
                SELECT dr.*, u.full_name as driver_display_name
                FROM dispatch_runs dr
                LEFT JOIN users u ON u.id=dr.driver_id
                WHERE dr.run_date=? AND dr.status!='cancelled'
                ORDER BY dr.truck_id, dr.run_number
            """, [ds]).fetchall())
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
                slot["runs"] = [r for r in day_runs if r["truck_id"] == tid]
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

    # ----- DISPATCH PLANNING V2 (new Excel-style grid endpoint) -----
    if method == "GET" and path == "/dispatch-planning-v2":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        date_from = params.get("date_from", datetime.now(timezone.utc).strftime("%Y-%m-%d"))
        date_to = params.get("date_to", date_from)

        # Get all active trucks
        all_trucks = rows_to_list(conn.execute("SELECT * FROM trucks WHERE is_active=1 ORDER BY id").fetchall())

        # Attach capacity config to each truck
        for t in all_trucks:
            caps = rows_to_list(conn.execute("SELECT * FROM truck_capacity_config WHERE truck_id=? ORDER BY day_of_week", [t["id"]]).fetchall())
            t["capacity_config"] = {c["day_of_week"]: c for c in caps}

        # Get drivers (users with dispatch role or any role that can drive)
        drivers = rows_to_list(conn.execute(
            "SELECT id, full_name, username, role FROM users WHERE is_active=1 ORDER BY full_name"
        ).fetchall())

        # Get delivery_log entries in date range (assigned ones)
        deliveries = rows_to_list(conn.execute("""
            SELECT dl.*, o.order_number, o.delivery_type as order_delivery_type, o.special_instructions,
                   o.kanban_status as order_kanban, o.requested_delivery_date, o.eta_date as order_eta,
                   c.company_name as client_name, c.address as client_address, c.phone as client_phone,
                   t.name as truck_name, t.driver_name, t.rego as truck_rego
            FROM delivery_log dl
            LEFT JOIN orders o ON o.id=dl.order_id
            LEFT JOIN clients c ON c.id=o.client_id
            LEFT JOIN trucks t ON t.id=dl.truck_id
            WHERE dl.expected_date >= ? AND dl.expected_date <= ?
            ORDER BY dl.expected_date, dl.load_sequence, dl.id
        """, [date_from, date_to]).fetchall())

        # Attach items to each delivery
        for d in deliveries:
            if d.get("order_id"):
                items = rows_to_list(conn.execute("""
                    SELECT oi.id, oi.sku_code, oi.product_name, oi.quantity, oi.produced_quantity,
                           oi.status, oi.kanban_status
                    FROM order_items oi WHERE oi.order_id=?
                """, [d["order_id"]]).fetchall())
                d["items"] = items
                total = len(items)
                done = sum(1 for i in items if i["status"] in ('F', 'dispatched'))
                d["progress"] = f"{done}/{total}"
                d["all_finished"] = done == total
                # Build SKU summary
                sku_parts = []
                for it in items[:3]:
                    code = it.get("sku_code") or it.get("product_name") or "?"
                    qty = it.get("quantity", 0)
                    sku_parts.append(f"{code} \u00d7{qty}")
                d["sku_summary"] = ", ".join(sku_parts)
                if len(items) > 3:
                    d["sku_summary"] += f" +{len(items)-3} more"
            else:
                d["items"] = []
                d["progress"] = "0/0"
                d["all_finished"] = False
                d["sku_summary"] = ""

            # Enhance kanban info
            kanban_key = d.get("order_kanban") or "red_pending"
            kanban_map = {
                "red_pending":   {"color": "#dc2626", "label": "Pending Stock"},
                "amber_production": {"color": "#f59e0b", "label": "In Production"},
                "amber_docking":  {"color": "#f59e0b", "label": "In Docking"},
                "green_planning":{"color": "#22c55e", "label": "Planning"},
                "green_dispatch":{"color": "#16a34a", "label": "Ready to Dispatch"},
                "blue":          {"color": "#2563eb", "label": "Dispatched"},
                "red_delivered": {"color": "#dc2626", "label": "Delivered"},
            }
            km = kanban_map.get(kanban_key, kanban_map["red_pending"])
            d["kanban_color"] = km["color"]
            d["kanban_label"] = km["label"]

        # Unassigned deliveries (no truck/date assigned — for intake queue)
        unassigned_raw = rows_to_list(conn.execute("""
            SELECT dl.*, o.order_number, o.delivery_type as order_delivery_type, o.special_instructions,
                   o.kanban_status as order_kanban, o.requested_delivery_date,
                   c.company_name as client_name, c.address as client_address
            FROM delivery_log dl
            LEFT JOIN orders o ON o.id=dl.order_id
            LEFT JOIN clients c ON c.id=o.client_id
            WHERE (dl.truck_id IS NULL OR dl.expected_date IS NULL)
              AND dl.status NOT IN ('delivered','cancelled')
              AND o.status NOT IN ('delivered','collected','cancelled')
            ORDER BY dl.id DESC
            LIMIT 100
        """).fetchall())
        for d in unassigned_raw:
            if d.get("order_id"):
                items = rows_to_list(conn.execute(
                    "SELECT oi.sku_code, oi.product_name, oi.quantity, oi.status, oi.kanban_status FROM order_items oi WHERE oi.order_id=?",
                    [d["order_id"]]).fetchall())
                d["items"] = items
                total = len(items); done = sum(1 for i in items if i["status"] in ('F','dispatched'))
                d["progress"] = f"{done}/{total}"; d["all_finished"] = done == total
                sku_parts = []
                for it in items[:3]:
                    code = it.get("sku_code") or it.get("product_name") or "?"
                    sku_parts.append(f"{code} \u00d7{it.get('quantity',0)}")
                d["sku_summary"] = ", ".join(sku_parts)
                if len(items) > 3: d["sku_summary"] += f" +{len(items)-3} more"
            else:
                d["items"] = []; d["progress"] = "0/0"; d["all_finished"] = False; d["sku_summary"] = ""
            kanban_key = d.get("order_kanban") or "red_pending"
            km = {"red_pending":{"color":"#dc2626","label":"Pending Stock"},"amber_production":{"color":"#f59e0b","label":"In Production"},
                  "amber_docking":{"color":"#f59e0b","label":"In Docking"},"green_planning":{"color":"#22c55e","label":"Planning"},
                  "green_dispatch":{"color":"#16a34a","label":"Ready to Dispatch"},"blue":{"color":"#2563eb","label":"Dispatched"},
                  "red_delivered":{"color":"#dc2626","label":"Delivered"}}.get(kanban_key, {"color":"#dc2626","label":"Pending Stock"})
            d["kanban_color"] = km["color"]; d["kanban_label"] = km["label"]

        assigned = [d for d in deliveries if d.get("truck_id")]

        # Build day-by-day structure
        from datetime import date as date_type
        d_from = datetime.strptime(date_from, "%Y-%m-%d").date()
        d_to = datetime.strptime(date_to, "%Y-%m-%d").date()
        days_v2 = []
        current = d_from
        while current <= d_to:
            ds = current.strftime("%Y-%m-%d")
            dow = current.weekday()  # 0=Mon
            day_name = current.strftime("%A")
            day_label = current.strftime("%a %-d %b")

            # Get dispatch runs for this day
            day_runs = rows_to_list(conn.execute("""
                SELECT dr.*, u.full_name as driver_display_name
                FROM dispatch_runs dr
                LEFT JOIN users u ON u.id=dr.driver_id
                WHERE dr.run_date=? AND dr.status!='cancelled'
                ORDER BY dr.truck_id, dr.run_number
            """, [ds]).fetchall())

            # Get truck work orders for this day
            day_twos = rows_to_list(conn.execute(
                "SELECT * FROM truck_work_orders WHERE scheduled_date=? AND status!='cancelled' ORDER BY priority DESC, id",
                [ds]).fetchall())

            # Build cells: {truck_id: {driver_id, runs, truck_work_orders, capacity}}
            cells = {}
            for t in all_trucks:
                tid = t["id"]
                truck_runs = [r for r in day_runs if r["truck_id"] == tid]
                truck_entries = [d for d in assigned if d.get("expected_date") == ds and d.get("truck_id") == tid]
                truck_twos = [tw for tw in day_twos if tw["truck_id"] == tid]

                # Group entries by run_id
                runs_out = []
                for run in truck_runs:
                    run_entries = [e for e in truck_entries if e.get("run_id") == run["id"]]
                    run_entries_out = []
                    for e in run_entries:
                        kanban_key = e.get("order_kanban") or "red_pending"
                        km_entry = {"red_pending":{"color":"#dc2626","label":"Pending Stock"},
                                    "amber_production":{"color":"#f59e0b","label":"In Production"},
                                    "amber_docking":{"color":"#f59e0b","label":"In Docking"},
                                    "green_planning":{"color":"#22c55e","label":"Planning"},
                                    "green_dispatch":{"color":"#16a34a","label":"Ready to Dispatch"},
                                    "blue":{"color":"#2563eb","label":"Dispatched"},
                                    "red_delivered":{"color":"#dc2626","label":"Delivered"}}.get(kanban_key,{"color":"#dc2626","label":"Pending Stock"})
                        run_entries_out.append({
                            "delivery_log_id": e.get("id"),
                            "order_number": e.get("order_number"),
                            "client_name": e.get("client_name"),
                            "sku_summary": e.get("sku_summary", ""),
                            "kanban_status": kanban_key,
                            "kanban_label": km_entry["label"],
                            "kanban_color": km_entry["color"],
                            "load_sequence": e.get("load_sequence"),
                            "estimated_minutes": e.get("estimated_minutes") or 30,
                            "status": e.get("status", "pending"),
                            "all_finished": e.get("all_finished", False),
                            "items": e.get("items", []),
                            "client_address": e.get("client_address"),
                            "order_kanban": kanban_key,
                        })
                    runs_out.append({
                        "id": run["id"],
                        "run_number": run["run_number"],
                        "status": run.get("status", "planned"),
                        "driver_id": run.get("driver_id"),
                        "driver_name": run.get("driver_display_name") or "",
                        "departure_time": run.get("departure_time"),
                        "notes": run.get("notes", ""),
                        "entries": run_entries_out,
                    })

                # Unassigned entries for this truck/day (no run)
                unrun_entries = [e for e in truck_entries if not e.get("run_id")]

                # Capacity
                delivery_mins = sum((e.get("estimated_minutes") or 30) for e in truck_entries)
                truck_wo_mins = sum((tw.get("estimated_minutes") or 60) for tw in truck_twos)
                total_mins = delivery_mins + truck_wo_mins
                cap_config = t.get("capacity_config", {}).get(dow)
                cap = cap_config["capacity_minutes"] if cap_config else 480
                ot = cap_config["overtime_minutes"] if cap_config else 120

                # Get driver from first run (if any)
                cell_driver_id = truck_runs[0].get("driver_id") if truck_runs else None
                cell_driver_name = truck_runs[0].get("driver_display_name") if truck_runs else t.get("driver_name", "")

                cells[tid] = {
                    "driver_id": cell_driver_id,
                    "driver_name": cell_driver_name or "",
                    "runs": runs_out,
                    "unrun_entries": [{"delivery_log_id":e.get("id"),"order_number":e.get("order_number"),
                                       "client_name":e.get("client_name"),"sku_summary":e.get("sku_summary",""),
                                       "kanban_status":e.get("order_kanban","red_pending"),
                                       "kanban_color":e.get("kanban_color","#dc2626"),
                                       "kanban_label":e.get("kanban_label","Pending Stock"),
                                       "estimated_minutes":e.get("estimated_minutes") or 30,
                                       "status":e.get("status","pending"),"all_finished":e.get("all_finished",False)
                                       } for e in unrun_entries],
                    "truck_work_orders": truck_twos,
                    "capacity": {
                        "capacity_minutes": cap,
                        "scheduled_minutes": total_mins,
                        "overtime_minutes": ot,
                        "remaining_minutes": cap - total_mins,
                        "is_over_capacity": total_mins > cap,
                        "is_overtime": total_mins > cap and total_mins <= cap + ot,
                        "is_exceeded": total_mins > cap + ot,
                    }
                }

            days_v2.append({
                "date": ds,
                "day_label": day_label,
                "day_name": day_name,
                "day_of_week": dow,
                "cells": cells,
                "incoming": [],  # can be populated if needed
            })
            current += timedelta(days=1)

        return {"status": 200, "body": {
            "date_from": date_from,
            "date_to": date_to,
            "trucks": all_trucks,
            "drivers": drivers,
            "days": days_v2,
            "unassigned": unassigned_raw,
        }}

    # ----- DISPATCH RUN SHEET (all trucks, all days, load order) -----
    if method == "GET" and path == "/dispatch-runsheet":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        date_from = params.get("date_from", datetime.now(timezone.utc).strftime("%Y-%m-%d"))
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
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
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
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
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
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
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
            if not current_user:
                return {"status": 401, "body": {"error": "Authentication required"}}
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
            if not current_user:
                return {"status": 401, "body": {"error": "Authentication required"}}
            conn.execute("UPDATE truck_work_orders SET status='cancelled', updated_at=CURRENT_TIMESTAMP WHERE id=?", [two_id])
            conn.commit()
            return {"status": 200, "body": {"ok": True}}

    # ----- TRUCK CAPACITY CONFIG -----
    if method == "GET" and path == "/truck-capacity":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        if params.get("truck_id"):
            rows = conn.execute("SELECT * FROM truck_capacity_config WHERE truck_id=? ORDER BY day_of_week", [int(params["truck_id"])]).fetchall()
        else:
            rows = conn.execute("SELECT * FROM truck_capacity_config ORDER BY truck_id, day_of_week").fetchall()
        return {"status": 200, "body": rows_to_list(rows)}

    if method == "PUT" and path == "/truck-capacity":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        if current_user.get("role") not in ("executive", "production_manager", "ops_manager"):
            return {"status": 403, "body": {"error": "Insufficient permissions"}}
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
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
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

    # ----- DISPATCH RUNS (multi-run support) -----
    if method == "GET" and path == "/dispatch-runs":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        where, vals = ["1=1"], []
        if params.get("truck_id"):
            where.append("truck_id=?"); vals.append(int(params["truck_id"]))
        if params.get("run_date"):
            where.append("run_date=?"); vals.append(params["run_date"])
        if params.get("date_from"):
            where.append("run_date>=?"); vals.append(params["date_from"])
        if params.get("date_to"):
            where.append("run_date<=?"); vals.append(params["date_to"])
        rows = conn.execute(f"""
            SELECT dr.*, t.name as truck_name, t.rego as truck_rego, u.full_name as driver_name
            FROM dispatch_runs dr
            LEFT JOIN trucks t ON t.id=dr.truck_id
            LEFT JOIN users u ON u.id=dr.driver_id
            WHERE {' AND '.join(where)}
            ORDER BY dr.run_date, dr.truck_id, dr.run_number
        """, vals).fetchall()
        return {"status": 200, "body": rows_to_list(rows)}

    if method == "POST" and path == "/dispatch-runs":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        truck_id = body.get("truck_id")
        run_date = body.get("run_date")
        if not truck_id or not run_date:
            return {"status": 400, "body": {"error": "truck_id and run_date required"}}
        # Auto-increment run_number
        max_run = conn.execute(
            "SELECT COALESCE(MAX(run_number), 0) FROM dispatch_runs WHERE truck_id=? AND run_date=?",
            [int(truck_id), run_date]).fetchone()[0]
        if max_run >= 3:
            return {"status": 400, "body": {"error": "Maximum 3 runs per truck per day"}}
        run_number = max_run + 1
        driver_id = body.get("driver_id")
        notes = body.get("notes", "")
        created_by = current_user["id"]
        conn.execute("""
            INSERT INTO dispatch_runs (truck_id, run_date, run_number, driver_id, notes, created_by)
            VALUES (?,?,?,?,?,?)
        """, [int(truck_id), run_date, run_number, driver_id, notes, created_by])
        conn.commit()
        new_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        row = conn.execute("SELECT * FROM dispatch_runs WHERE id=?", [new_id]).fetchone()
        return {"status": 201, "body": row_to_dict(row)}

    m = match("/dispatch-runs/:id", path)
    if m:
        run_id = int(m["id"])
        if method == "PUT":
            if not current_user:
                return {"status": 401, "body": {"error": "Authentication required"}}
            fields, vals = [], []
            for f in ["status", "driver_id", "departure_time", "return_time", "notes"]:
                if f in body:
                    fields.append(f"{f}=?"); vals.append(body[f])
            if fields:
                fields.append("updated_at=CURRENT_TIMESTAMP")
                vals.append(run_id)
                conn.execute(f"UPDATE dispatch_runs SET {', '.join(fields)} WHERE id=?", vals)
                conn.commit()
            row = conn.execute("SELECT * FROM dispatch_runs WHERE id=?", [run_id]).fetchone()
            return {"status": 200, "body": row_to_dict(row)}
        if method == "DELETE":
            if not current_user:
                return {"status": 401, "body": {"error": "Authentication required"}}
            # Unlink any delivery_log entries first
            conn.execute("UPDATE delivery_log SET run_id=NULL WHERE run_id=?", [run_id])
            conn.execute("DELETE FROM dispatch_runs WHERE id=?", [run_id])
            conn.commit()
            return {"status": 200, "body": {"deleted": True}}

    # ----- Assign driver to a dispatch run (or all runs for truck/day) -----
    m2 = match("/dispatch-runs/:id/driver", path)
    if m2 and method == "PUT":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        run_id = int(m2["id"])
        driver_id = body.get("driver_id")
        # Optionally propagate to all runs for this truck/day
        propagate = body.get("propagate_to_day", False)
        conn.execute("UPDATE dispatch_runs SET driver_id=?, updated_at=CURRENT_TIMESTAMP WHERE id=?", [driver_id, run_id])
        if propagate:
            run_row = conn.execute("SELECT truck_id, run_date FROM dispatch_runs WHERE id=?", [run_id]).fetchone()
            if run_row:
                tid = run_row[0] if not isinstance(run_row, dict) else run_row["truck_id"]
                rdate = run_row[1] if not isinstance(run_row, dict) else run_row["run_date"]
                conn.execute("UPDATE dispatch_runs SET driver_id=?, updated_at=CURRENT_TIMESTAMP WHERE truck_id=? AND run_date=?", [driver_id, tid, rdate])
        conn.commit()
        row = conn.execute("SELECT * FROM dispatch_runs WHERE id=?", [run_id]).fetchone()
        return {"status": 200, "body": row_to_dict(row)}

    # ----- Assign driver to all runs for a truck/day -----
    if method == "PUT" and path == "/dispatch-driver-assign":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        truck_id_val = body.get("truck_id")
        run_date_val = body.get("run_date")
        driver_id_val = body.get("driver_id")
        if not truck_id_val or not run_date_val:
            return {"status": 400, "body": {"error": "truck_id and run_date required"}}
        conn.execute("UPDATE dispatch_runs SET driver_id=?, updated_at=CURRENT_TIMESTAMP WHERE truck_id=? AND run_date=?",
                     [driver_id_val, int(truck_id_val), run_date_val])
        conn.commit()
        return {"status": 200, "body": {"updated": True}}

    # ----- Assign delivery_log to a dispatch run -----
    if method == "PUT" and path == "/dispatch-run-assign":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        dl_id = body.get("delivery_log_id")
        run_id_val = body.get("run_id")
        sequence = body.get("sequence")
        if not dl_id:
            return {"status": 400, "body": {"error": "delivery_log_id required"}}
        fields, vals = [], []
        if run_id_val is not None:
            fields.append("run_id=?"); vals.append(run_id_val if run_id_val else None)
        if sequence is not None:
            fields.append("load_sequence=?"); vals.append(sequence)
        if fields:
            fields.append("updated_at=CURRENT_TIMESTAMP")
            vals.append(int(dl_id))
            conn.execute(f"UPDATE delivery_log SET {', '.join(fields)} WHERE id=?", vals)
            conn.commit()
        row = conn.execute("SELECT * FROM delivery_log WHERE id=?", [int(dl_id)]).fetchone()
        return {"status": 200, "body": row_to_dict(row)}

    # ----- DISPATCH ACTION (office confirms truck departed) -----
    m = match("/dispatch-runs/:id/dispatch", path)
    if m and method == "PUT":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        run_id = int(m["id"])
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        conn.execute("UPDATE dispatch_runs SET status='in_transit', departure_time=?, updated_at=CURRENT_TIMESTAMP WHERE id=?", [now, run_id])
        # Update all linked delivery_log entries
        conn.execute("UPDATE delivery_log SET status='in_transit', updated_at=CURRENT_TIMESTAMP WHERE run_id=?", [run_id])
        # Update linked orders to 'dispatched'
        dl_rows = conn.execute("SELECT order_id FROM delivery_log WHERE run_id=? AND order_id IS NOT NULL", [run_id]).fetchall()
        for dlr in dl_rows:
            oid = dlr[0] if not isinstance(dlr, dict) else dlr["order_id"]
            conn.execute("UPDATE orders SET status='dispatched', dispatched_at=CURRENT_TIMESTAMP WHERE id=? AND status NOT IN ('delivered','collected')", [oid])
            conn.execute("UPDATE order_items SET status='dispatched' WHERE order_id=? AND status='F'", [oid])
            update_kanban_statuses(conn, oid)
        conn.commit()
        return {"status": 200, "body": {"dispatched": True, "run_id": run_id}}

    # ----- DELIVERY CONFIRMED (driver confirms) -----
    m = match("/delivery-log/:id/delivered", path)
    if m and method == "PUT":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        dl_id = int(m["id"])
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        conn.execute("UPDATE delivery_log SET status='delivered', actual_date=?, updated_at=CURRENT_TIMESTAMP WHERE id=?", [now[:10], dl_id])
        dl = conn.execute("SELECT * FROM delivery_log WHERE id=?", [dl_id]).fetchone()
        order_id = dl["order_id"] if isinstance(dl, dict) else dl[1]
        if order_id:
            conn.execute("UPDATE orders SET status='delivered' WHERE id=?", [order_id])
            conn.execute("UPDATE order_items SET status='delivered' WHERE order_id=? AND status='dispatched'", [order_id])
            update_kanban_statuses(conn, order_id)
        conn.commit()
        return {"status": 200, "body": {"delivered": True, "delivery_log_id": dl_id}}

    # ----- INVENTORY ALLOCATION (fast-track: stock -> dispatch ready) -----
    if method == "POST" and path == "/inventory-allocate":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        order_item_id = body.get("order_item_id")
        quantity = body.get("quantity")
        if not order_item_id:
            return {"status": 400, "body": {"error": "order_item_id required"}}
        # Get item details
        item = conn.execute("SELECT * FROM order_items WHERE id=?", [int(order_item_id)]).fetchone()
        if not item:
            return {"status": 404, "body": {"error": "Order item not found"}}
        item = row_to_dict(item) if not isinstance(item, dict) else item
        sku_id = item.get("sku_id")
        alloc_qty = int(quantity) if quantity else item.get("quantity", 0)

        if not sku_id:
            # Try to find sku_id from sku_code
            sku_row = conn.execute("SELECT id FROM skus WHERE code=?", [item.get("sku_code")]).fetchone()
            if sku_row:
                sku_id = sku_row[0] if not isinstance(sku_row, dict) else sku_row["id"]

        if sku_id:
            inv = conn.execute("SELECT * FROM inventory WHERE sku_id=?", [sku_id]).fetchone()
            if inv:
                inv = row_to_dict(inv) if not isinstance(inv, dict) else inv
                available = inv.get("units_on_hand", 0) - inv.get("units_allocated", 0)
                if available < alloc_qty:
                    return {"status": 400, "body": {"error": f"Insufficient stock. Available: {available}, Requested: {alloc_qty}"}}
                # Deduct from inventory
                conn.execute("UPDATE inventory SET units_allocated = units_allocated + ?, units_on_hand = units_on_hand - ?, updated_at=CURRENT_TIMESTAMP WHERE sku_id=?",
                    [alloc_qty, alloc_qty, sku_id])

        # Move item straight to Finished, bypassing production
        conn.execute("UPDATE order_items SET status='F', kanban_status='green_dispatch', produced_quantity=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
            [alloc_qty, int(order_item_id)])

        # Check if all items in order are now F -> update order status
        order_id = item.get("order_id")
        if order_id:
            pending = conn.execute("SELECT COUNT(*) FROM order_items WHERE order_id=? AND status NOT IN ('F','dispatched','delivered')", [order_id]).fetchone()[0]
            if pending == 0:
                conn.execute("UPDATE orders SET status='F' WHERE id=? AND status NOT IN ('dispatched','delivered','collected')", [order_id])

            # Create delivery_log entry if none exists
            existing_dl = conn.execute("SELECT id FROM delivery_log WHERE order_id=?", [order_id]).fetchone()
            if not existing_dl:
                conn.execute("""
                    INSERT INTO delivery_log (order_id, status, delivery_type, notes, created_at)
                    VALUES (?, 'pending', 'delivery', 'Auto-created from inventory allocation', CURRENT_TIMESTAMP)
                """, [order_id])

            update_kanban_statuses(conn, order_id)

        conn.commit()
        return {"status": 200, "body": {"allocated": True, "order_item_id": order_item_id, "quantity": alloc_qty}}

    # ----- GET KANBAN STATUSES for orders -----
    if method == "GET" and path == "/kanban-statuses":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        order_ids = params.get("order_ids", "")
        if order_ids:
            ids = [int(x) for x in order_ids.split(",") if x.strip()]
            placeholders = ",".join("?" * len(ids))
            rows = conn.execute(f"""
                SELECT o.id, o.order_number, o.kanban_status as order_kanban,
                       oi.id as item_id, oi.sku_code, oi.status as item_status, oi.kanban_status as item_kanban
                FROM orders o
                LEFT JOIN order_items oi ON oi.order_id = o.id
                WHERE o.id IN ({placeholders})
                ORDER BY o.id, oi.id
            """, ids).fetchall()
            return {"status": 200, "body": rows_to_list(rows)}
        # Default: return all non-delivered orders
        rows = conn.execute("""
            SELECT o.id, o.order_number, o.kanban_status, o.status
            FROM orders o
            WHERE o.status NOT IN ('delivered','collected')
            ORDER BY o.id
        """).fetchall()
        return {"status": 200, "body": rows_to_list(rows)}

    # ----- DELIVERY ADDRESSES -----
    if method == "GET" and path == "/delivery-addresses":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        where, vals = ["is_active=1"], []
        if params.get("client_id"):
            where.append("client_id=?"); vals.append(int(params["client_id"]))
        rows = conn.execute(f"SELECT * FROM delivery_addresses WHERE {' AND '.join(where)} ORDER BY is_default DESC, id", vals).fetchall()
        return {"status": 200, "body": rows_to_list(rows)}

    if method == "POST" and path == "/delivery-addresses":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
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
            if not current_user:
                return {"status": 401, "body": {"error": "Authentication required"}}
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
            if not current_user:
                return {"status": 401, "body": {"error": "Authentication required"}}
            conn.execute("UPDATE delivery_addresses SET is_active=0 WHERE id=?", [da_id])
            conn.commit()
            return {"status": 200, "body": {"ok": True}}

    # ----- DISPATCH DRAG RESCHEDULE -----
    m = match("/delivery-log/:id/reschedule", path)
    if m and method == "PUT":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        dlid = int(m["id"])
        new_date = body.get("expected_date")
        new_truck = body.get("truck_id")
        new_seq = body.get("load_sequence")
        entry = conn.execute("SELECT * FROM delivery_log WHERE id=?", [dlid]).fetchone()
        if not entry:
            return {"status": 404, "body": {"error": "Delivery log entry not found"}}
        old_date = entry["expected_date"]
        old_truck = entry["truck_id"]
        updates, vals = [], []
        if new_date:
            updates.append("expected_date=?"); vals.append(new_date)
        if new_truck is not None:
            updates.append("truck_id=?"); vals.append(new_truck)
        if new_seq is not None:
            updates.append("load_sequence=?"); vals.append(new_seq)
        if updates:
            vals.append(dlid)
            conn.execute(f"UPDATE delivery_log SET {', '.join(updates)} WHERE id=?", vals)
            conn.commit()
            log_audit(conn, current_user["id"], "reschedule_delivery", "delivery_log", dlid,
                      json.dumps({"old_date": old_date, "old_truck": old_truck}),
                      json.dumps({"new_date": new_date, "new_truck": new_truck}))
        row = conn.execute("SELECT * FROM delivery_log WHERE id=?", [dlid]).fetchone()
        return {"status": 200, "body": row_to_dict(row)}

    # ----- CONTRACTOR ASSIGNMENTS -----
    if method == "GET" and path == "/contractor-assignments":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        where, vals = ["status!='cancelled'"], []
        if params.get("delivery_log_id"):
            where.append("delivery_log_id=?"); vals.append(int(params["delivery_log_id"]))
        if params.get("status"):
            where[0] = "1=1"  # override default filter
            where.append("status=?"); vals.append(params["status"])
        rows = conn.execute(f"SELECT * FROM contractor_assignments WHERE {' AND '.join(where)} ORDER BY created_at DESC", vals).fetchall()
        return {"status": 200, "body": rows_to_list(rows)}

    if method == "POST" and path == "/contractor-assignments":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
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
            if not current_user:
                return {"status": 401, "body": {"error": "Authentication required"}}
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
            if not current_user:
                return {"status": 401, "body": {"error": "Authentication required"}}
            conn.execute("UPDATE contractor_assignments SET status='cancelled', updated_at=CURRENT_TIMESTAMP WHERE id=?", [ca_id])
            conn.commit()
            return {"status": 200, "body": {"ok": True}}

    # ----- CLIENTS -----
    if method == "GET" and path == "/clients":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        is_active = params.get("is_active", "1")
        rows = conn.execute("SELECT * FROM clients WHERE is_active=? ORDER BY company_name", [is_active]).fetchall()
        return {"status": 200, "body": rows_to_list(rows)}

    if method == "POST" and path == "/clients":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        if current_user["role"] not in ("executive", "office"):
            return {"status": 403, "body": {"error": "Only executive/office roles can create clients"}}
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
            if not current_user:
                return {"status": 401, "body": {"error": "Authentication required"}}
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
            if not current_user:
                return {"status": 401, "body": {"error": "Authentication required"}}
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
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
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
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
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

    m = match("/skus/:id", path)
    if m and method == "PUT":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        if current_user["role"] not in ("executive", "office", "planner"):
            return {"status": 403, "body": {"error": "Insufficient role"}}
        sku_id = int(m["id"])
        row = conn.execute("SELECT id FROM skus WHERE id=?", [sku_id]).fetchone()
        if not row:
            return {"status": 404, "body": {"error": "SKU not found"}}
        fields, vals = [], []
        for f in ["code", "name", "drawing_number", "labour_cost", "material_cost", "sell_price", "zone_id", "myob_uid", "is_active"]:
            if f in body:
                val = body[f].upper() if f == "code" else body[f]
                fields.append(f"{f}=?"); vals.append(val)
        if not fields:
            return {"status": 400, "body": {"error": "No updatable fields"}}
        fields.append("updated_at=CURRENT_TIMESTAMP")
        vals.append(sku_id)
        try:
            conn.execute(f"UPDATE skus SET {', '.join(fields)} WHERE id=?", vals)
            conn.commit()
            row = conn.execute("SELECT * FROM skus WHERE id=?", [sku_id]).fetchone()
            return {"status": 200, "body": row_to_dict(row)}
        except Exception as e:
            return {"status": 409, "body": {"error": str(e)}}
    if m and method == "DELETE":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        if current_user["role"] not in ("executive", "office"):
            return {"status": 403, "body": {"error": "Insufficient role"}}
        sku_id = int(m["id"])
        conn.execute("UPDATE skus SET is_active=0 WHERE id=?", [sku_id])
        conn.commit()
        return {"status": 200, "body": {"message": "SKU deactivated"}}

    # ----- STATS -----
    if method == "GET" and path == "/stats/production":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
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
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
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
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
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
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        if current_user.get("role") not in ("executive", "production_manager", "ops_manager"):
            return {"status": 403, "body": {"error": "Insufficient permissions"}}
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
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        if current_user.get("role") not in ("executive", "office", "ops_manager"):
            return {"status": 403, "body": {"error": "Insufficient permissions"}}
        row = conn.execute("SELECT * FROM accounting_config LIMIT 1").fetchone()
        provider = row["provider"] if row else "mock"
        conn.execute("INSERT INTO accounting_sync_log (direction, entity_type, entity_id, status, details) VALUES (?,?,?,?,?)",
            ["outbound", "sync", "all", "success", f"Mock sync triggered for provider: {provider}"])
        conn.execute("UPDATE accounting_config SET last_sync_at=CURRENT_TIMESTAMP WHERE id=1")
        conn.commit()
        return {"status": 200, "body": {"message": "Sync triggered", "provider": provider, "status": "success"}}

    if method == "GET" and path == "/accounting/sync-log":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        limit = safe_int(params.get("limit"), 50)
        rows = conn.execute("SELECT * FROM accounting_sync_log ORDER BY synced_at DESC LIMIT ?", [limit]).fetchall()
        return {"status": 200, "body": rows_to_list(rows)}

    # ----- NOTIFICATIONS -----
    if method == "GET" and path == "/notifications":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        where, vals = ["1=1"], []
        if params.get("order_id"):
            where.append("order_id=?"); vals.append(params["order_id"])
        if params.get("type"):
            where.append("notification_type=?"); vals.append(params["type"])
        limit = safe_int(params.get("limit"), 50)
        rows = conn.execute(f"SELECT * FROM notification_log WHERE {' AND '.join(where)} ORDER BY sent_at DESC LIMIT ?", vals + [limit]).fetchall()
        return {"status": 200, "body": rows_to_list(rows)}

    if method == "POST" and path == "/notifications":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        if current_user.get("role") not in ("executive", "production_manager", "ops_manager"):
            return {"status": 403, "body": {"error": "Insufficient permissions"}}
        for f in ["notification_type", "recipient_email"]:
            if not body.get(f):
                return {"status": 400, "body": {"error": f"Field '{f}' is required"}}
        notif_id = log_and_send_notification(conn, body.get("order_id"), body["notification_type"],
            body["recipient_email"], body.get("subject", ""), body.get("body", ""))
        row = conn.execute("SELECT * FROM notification_log WHERE id=?", [notif_id]).fetchone()
        return {"status": 201, "body": row_to_dict(row)}

    # ----- AUDIT LOG -----
    if method == "GET" and path == "/audit-log":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        where, vals = ["1=1"], []
        if params.get("entity_type"):
            where.append("entity_type=?"); vals.append(params["entity_type"])
        if params.get("user_id"):
            where.append("user_id=?"); vals.append(params["user_id"])
        limit = safe_int(params.get("limit"), 100)
        rows = conn.execute(f"SELECT al.*, u.full_name as user_name FROM audit_log al LEFT JOIN users u ON u.id=al.user_id WHERE {' AND '.join(where)} ORDER BY al.created_at DESC LIMIT ?", vals + [limit]).fetchall()
        return {"status": 200, "body": rows_to_list(rows)}

    # ----- INVENTORY -----
    if method == "GET" and path == "/inventory/on-hand":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        # Returns sku_id -> on_hand count map
        rows = conn.execute(
            "SELECT sku_id, SUM(units_on_hand) as on_hand FROM inventory WHERE sku_id IS NOT NULL GROUP BY sku_id"
        ).fetchall()
        result = {row["sku_id"]: (row["on_hand"] or 0) for row in rows}
        return {"status": 200, "body": result}

    if method == "GET" and path == "/inventory":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        rows = conn.execute("""
            SELECT inv.*, s.code as sku_code, s.name as sku_name, s.zone_id
            FROM inventory inv JOIN skus s ON s.id=inv.sku_id
            ORDER BY s.code
        """).fetchall()
        return {"status": 200, "body": rows_to_list(rows)}

    m = match("/inventory/:sku_id", path)
    if m and method == "PUT":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        if current_user.get("role") not in ("executive", "production_manager", "ops_manager"):
            return {"status": 403, "body": {"error": "Insufficient permissions"}}
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
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
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
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        if current_user.get("role") not in ("executive", "production_manager", "ops_manager"):
            return {"status": 403, "body": {"error": "Insufficient permissions"}}
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
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        if current_user.get("role") not in ("executive", "production_manager", "ops_manager"):
            return {"status": 403, "body": {"error": "Insufficient permissions"}}
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
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        default_rate = conn.execute("SELECT * FROM target_labour_rates WHERE is_default=1 LIMIT 1").fetchone()
        user_rates = rows_to_list(conn.execute(
            "SELECT tlr.*, u.full_name, u.username FROM target_labour_rates tlr LEFT JOIN users u ON u.id=tlr.user_id WHERE tlr.is_default=0 ORDER BY tlr.id"
        ).fetchall())
        return {"status": 200, "body": {
            "default_rate": (row_to_dict(default_rate) or {}).get("rate_per_hour", 55.0) if default_rate else 55.0,
            "user_rates": user_rates
        }}

    if method == "PUT" and path == "/labour-config":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        if current_user.get("role") not in ("executive", "production_manager", "ops_manager"):
            return {"status": 403, "body": {"error": "Insufficient permissions"}}
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
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        if current_user["role"] not in ("executive", "office", "planner", "production_manager"):
            return {"status": 403, "body": {"error": "Insufficient permissions to close production day"}}
        zone_id = body.get("zone_id")
        closed_date = body.get("closed_date")
        if not zone_id or not closed_date:
            return {"status": 400, "body": {"error": "zone_id and closed_date required"}}
        try:
            conn.execute("INSERT INTO close_days (zone_id, closed_date) VALUES (?,?)", [zone_id, closed_date])
            conn.commit()
        except Exception:
            pass  # Already exists — idempotent

        # Auto-push incomplete work orders to next business day
        from datetime import date
        def next_biz_day(start_date_str, skip_zone_id):
            """Find next Mon-Sat that isn't closed for this zone."""
            d = date.fromisoformat(start_date_str) + timedelta(days=1)
            for _ in range(60):
                if d.weekday() < 6:  # Mon=0 ... Sat=5
                    closed_check = conn.execute(
                        "SELECT 1 FROM close_days WHERE zone_id=? AND closed_date=?",
                        [skip_zone_id, d.isoformat()]
                    ).fetchone()
                    if not closed_check:
                        return d.isoformat()
                d += timedelta(days=1)
            return None

        next_date = next_biz_day(closed_date, zone_id)
        pushed_items = []
        if next_date:
            # Find schedule_entries for this zone+date with incomplete items
            incomplete = conn.execute("""
                SELECT se.id, se.order_item_id, oi.status as item_status, o.order_number, se.planned_quantity
                FROM schedule_entries se
                LEFT JOIN order_items oi ON oi.id = se.order_item_id
                LEFT JOIN orders o ON o.id = se.order_id
                WHERE se.zone_id = ? AND se.scheduled_date = ?
                  AND (oi.status IS NULL OR oi.status NOT IN ('F', 'dispatched', 'delivered', 'collected'))
            """, [zone_id, closed_date]).fetchall()
            for row in incomplete:
                conn.execute(
                    "UPDATE schedule_entries SET scheduled_date=? WHERE id=?",
                    [next_date, row["id"]]
                )
                pushed_items.append({
                    "entry_id": row["id"],
                    "order_number": row["order_number"],
                    "qty": row["planned_quantity"]
                })
            if pushed_items:
                conn.commit()

        return {"status": 200, "body": {
            "zone_id": zone_id, "closed_date": closed_date, "closed": True,
            "pushed_count": len(pushed_items),
            "next_date": next_date,
            "pushed_items": pushed_items
        }}

    if method == "DELETE" and path == "/planning/close-day":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
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
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
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
                zone_id, station_id, scheduled_date, eta_date, drawing_number, special_instructions, split_from_item_id, status)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            [orig["order_id"], orig["sku_id"], orig["sku_code"], orig["product_name"],
             new_qty, orig["unit_price"], new_qty * (orig["unit_price"] or 0),
             orig["zone_id"], None, orig["scheduled_date"], orig["eta_date"],
             orig["drawing_number"], orig["special_instructions"], iid, 'T'])
        conn.commit()
        new_item = row_to_dict(conn.execute("SELECT * FROM order_items WHERE id=?", [cur.lastrowid]).fetchone())
        orig_updated = row_to_dict(conn.execute("SELECT * FROM order_items WHERE id=?", [iid]).fetchone())
        return {"status": 201, "body": {"original": orig_updated, "split": new_item}}

    # ----- STOCK COMPLETE -----
    m = match("/orders/:id/stock-complete", path)
    if m and method == "POST":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
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
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        station_id = params.get("station_id")
        scheduled_date = params.get("scheduled_date")
        additional_qty = safe_int(params.get("additional_quantity"), 0)
        zone_id = params.get("zone_id")
        if not station_id or not scheduled_date:
            return {"status": 400, "body": {"error": "station_id and scheduled_date required"}}
        station_id = int(station_id)
        # Get station capacity limit
        cap_row = conn.execute("SELECT max_units_per_day FROM station_capacity WHERE station_id=?", [station_id]).fetchone()
        max_capacity = cap_row[0] if cap_row else 9999
        # Get current total already planned on this station+date (uses planned_station_id — Planning Board column)
        cur_total_row = conn.execute(
            "SELECT COALESCE(SUM(planned_quantity),0) FROM schedule_entries WHERE planned_station_id=? AND scheduled_date=?",
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
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        week_start = params.get("week_start")
        if not week_start:
            # Default to current Monday
            today = datetime.now(timezone.utc)
            days_since_monday = today.weekday()
            monday = today.replace(hour=0, minute=0, second=0, microsecond=0)
            monday = monday.replace(day=monday.day - days_since_monday)
            week_start = monday.strftime("%Y-%m-%d")
        # Parse week start and compute Mon-Sat
        ws = datetime.strptime(week_start, "%Y-%m-%d")
        num_days = min(safe_int(params.get("num_days"), 6), 21)  # Default 6 (Mon-Sat), max 21
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
                   oi.sku_code, oi.product_name, oi.quantity as item_quantity, oi.sku_id, oi.status as item_status, oi.split_from_item_id, oi.cut_list_issued,
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
                m_entries = [e for e in day_entries if e.get("planned_station_id") == mid]
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
        # For null-zone items, exclude if they've been scheduled to ANY zone
        null_zone_scheduled = {row[0] for row in conn.execute(
            "SELECT DISTINCT order_item_id FROM schedule_entries WHERE order_item_id IS NOT NULL AND order_item_id IN (SELECT id FROM order_items WHERE zone_id IS NULL)"
        ).fetchall()}
        intake_raw = rows_to_list(conn.execute("""
            SELECT oi.*, o.order_number, o.status as order_status, o.is_stock_run,
                   o.requested_delivery_date, o.eta_date, c.company_name as client_name
            FROM order_items oi
            JOIN orders o ON o.id=oi.order_id
            LEFT JOIN clients c ON c.id=o.client_id
            WHERE (oi.zone_id=? OR oi.zone_id IS NULL) AND o.status NOT IN ('F','dispatched','delivered','collected')
              AND oi.status NOT IN ('F','dispatched')
        """, [zone_id]).fetchall())
        intake_queue = [
            dict(item, inventory_on_hand=inv_map.get(item.get("sku_id"), 0))
            for item in intake_raw
            if item["id"] not in all_vik_sched and item["id"] not in null_zone_scheduled
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
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        week_start = params.get("week_start")
        if not week_start:
            today = datetime.now(timezone.utc)
            days_since_monday = today.weekday()
            monday = today.replace(hour=0, minute=0, second=0, microsecond=0)
            monday = monday.replace(day=monday.day - days_since_monday)
            week_start = monday.strftime("%Y-%m-%d")
        ws = datetime.strptime(week_start, "%Y-%m-%d")
        num_days = min(safe_int(params.get("num_days"), 6), 21)  # Default 6 (Mon-Sat), max 21
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
                   oi.sku_code, oi.product_name, oi.quantity as item_quantity, oi.sku_id, oi.status as item_status, oi.split_from_item_id, oi.cut_list_issued,
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
                t_entries = [e for e in day_entries if e.get("planned_station_id") == tid]
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
        # For null-zone items, exclude if they've been scheduled to ANY zone
        null_zone_scheduled_hmp = {row[0] for row in conn.execute(
            "SELECT DISTINCT order_item_id FROM schedule_entries WHERE order_item_id IS NOT NULL AND order_item_id IN (SELECT id FROM order_items WHERE zone_id IS NULL)"
        ).fetchall()}
        intake_raw = rows_to_list(conn.execute("""
            SELECT oi.*, o.order_number, o.status as order_status, o.is_stock_run,
                   o.requested_delivery_date, o.eta_date, c.company_name as client_name
            FROM order_items oi
            JOIN orders o ON o.id=oi.order_id
            LEFT JOIN clients c ON c.id=o.client_id
            WHERE (oi.zone_id=? OR oi.zone_id IS NULL) AND o.status NOT IN ('F','dispatched','delivered','collected')
              AND oi.status NOT IN ('F','dispatched')
        """, [zone_id]).fetchall())
        intake_queue = [
            dict(item, inventory_on_hand=inv_map.get(item.get("sku_id"), 0))
            for item in intake_raw
            if item["id"] not in all_hmp_sched and item["id"] not in null_zone_scheduled_hmp
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
            today_p = datetime.now(timezone.utc)
            days_since_mon = today_p.weekday()
            mon = today_p.replace(hour=0, minute=0, second=0, microsecond=0)
            mon = mon.replace(day=mon.day - days_since_mon)
            week_start_p = mon.strftime("%Y-%m-%d")
        ws_p = datetime.strptime(week_start_p, "%Y-%m-%d")
        num_days = min(safe_int(params.get("num_days"), 6), 21)  # Default 6 (Mon-Sat), max 21
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
                   oi.sku_code, oi.product_name, oi.quantity as item_quantity, oi.sku_id, oi.status as item_status, oi.split_from_item_id, oi.cut_list_issued,
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
                se = sorted([e for e in de if e.get("planned_station_id") == sid],
                            key=lambda x: (-int(x.get("priority") or 0), int(x.get("run_order") or 0)))
                st_total = sum(e.get("planned_quantity") or 0 for e in se)
                slots[sid] = {"entries": se, "total": st_total,
                              "over_capacity": st_total > (st.get("max_units_per_day") or 9999)}
            res_days.append({"date": ds, "day_name": day_nms[i], "is_closed": ds in cd_set,
                             "total_planned": tp, "machine_slots": slots})

        all_sched = {r[0] for r in conn.execute(
            "SELECT order_item_id FROM schedule_entries WHERE zone_id=? AND order_item_id IS NOT NULL",
            [zid]).fetchall()}
        # For null-zone items, exclude if they've been scheduled to ANY zone
        null_zone_sched = {r[0] for r in conn.execute(
            "SELECT DISTINCT order_item_id FROM schedule_entries WHERE order_item_id IS NOT NULL AND order_item_id IN (SELECT id FROM order_items WHERE zone_id IS NULL)"
        ).fetchall()}
        iq_raw = rows_to_list(conn.execute("""
            SELECT oi.*, o.order_number, o.status as order_status, o.is_stock_run,
                   o.requested_delivery_date, o.eta_date, c.company_name as client_name
            FROM order_items oi JOIN orders o ON o.id=oi.order_id
            LEFT JOIN clients c ON c.id=o.client_id
            WHERE (oi.zone_id=? OR oi.zone_id IS NULL) AND o.status NOT IN ('F','dispatched','delivered','collected')
              AND oi.status NOT IN ('F','dispatched')
        """, [zid]).fetchall())
        iq = [dict(it, inventory_on_hand=inv_mp.get(it.get("sku_id"), 0))
              for it in iq_raw if it["id"] not in all_sched and it["id"] not in null_zone_sched]
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
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        return _planning_zone("DTL")

    if method == "GET" and path == "/planning/crates":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        return _planning_zone("CRT")

    # ----- DTL BATCH LOG -----
    if method == "POST" and path == "/production/dtl-batch":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        cur = conn.execute("""INSERT INTO production_sessions (order_item_id, station_id, zone_id, target_quantity, produced_quantity, notes, status, end_time)
            VALUES (?,?,?,?,?,?,'completed', CURRENT_TIMESTAMP)""",
            [body.get("order_item_id"), body.get("station_id"), body.get("zone_id"),
             body.get("target_quantity", 0), body.get("produced_quantity", 0), body.get("notes")])
        sid = cur.lastrowid
        conn.execute("INSERT INTO session_workers (session_id, user_id) VALUES (?,?)", [sid, current_user["id"]])
        conn.execute("UPDATE session_workers SET scan_off_time=CURRENT_TIMESTAMP, is_active=0 WHERE session_id=? AND user_id=?", [sid, current_user["id"]])
        conn.commit()
        if body.get("order_item_id"):
            total = conn.execute("SELECT COALESCE(SUM(produced_quantity),0) FROM production_sessions WHERE order_item_id=? AND status='completed'",
                [body["order_item_id"]]).fetchone()[0]
            conn.execute("UPDATE order_items SET produced_quantity=? WHERE id=?", [total, body["order_item_id"]])
            conn.commit()
        return {"status": 201, "body": row_to_dict(conn.execute("SELECT * FROM production_sessions WHERE id=?", [sid]).fetchone())}

    # ----- SHARED WO PROGRESS (cross-station live count) -----
    m = match("/production/shared-progress/:item_id", path)
    if m and method == "GET":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        item_id = int(m["item_id"])
        item = conn.execute("SELECT * FROM order_items WHERE id=?", [item_id]).fetchone()
        if not item:
            return {"status": 404, "body": {"error": "Item not found"}}
        item = row_to_dict(item)
        sessions = rows_to_list(conn.execute("""
            SELECT ps.*, st.name as station_name, z.name as zone_name
            FROM production_sessions ps
            LEFT JOIN stations st ON st.id=ps.station_id
            LEFT JOIN zones z ON z.id=ps.zone_id
            WHERE ps.order_item_id=? AND ps.status IN ('active','paused')
        """, [item_id]).fetchall())
        total = conn.execute("SELECT COALESCE(SUM(produced_quantity),0) FROM production_sessions WHERE order_item_id=?", [item_id]).fetchone()[0]
        return {"status": 200, "body": {
            "order_item_id": item_id,
            "target_quantity": item["quantity"],
            "total_produced": total,
            "remaining": max(0, item["quantity"] - total),
            "active_sessions": sessions,
            "is_complete": total >= item["quantity"]
        }}

    # ----- PRODUCTION LOG SUMMARY -----
    if method == "GET" and path == "/production-log":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        zone_id = params.get("zone_id")
        date_from = params.get("date_from", datetime.now(timezone.utc).strftime("%Y-%m-%d"))
        date_to = params.get("date_to", date_from)
        where, vals = ["se.scheduled_date >= ?", "se.scheduled_date <= ?"], [date_from, date_to]
        if zone_id:
            where.append("se.zone_id=?"); vals.append(int(zone_id))
        rows = conn.execute(f"""
            SELECT se.scheduled_date, se.zone_id, z.name as zone_name, z.code as zone_code,
                   se.station_id, st.name as station_name,
                   oi.sku_code, oi.product_name, oi.produced_quantity, oi.quantity as target_qty,
                   o.order_number, c.company_name as client_name,
                   se.planned_quantity, se.status as entry_status
            FROM schedule_entries se
            LEFT JOIN zones z ON z.id=se.zone_id
            LEFT JOIN stations st ON st.id=se.station_id
            LEFT JOIN order_items oi ON oi.id=se.order_item_id
            LEFT JOIN orders o ON o.id=se.order_id
            LEFT JOIN clients c ON c.id=o.client_id
            WHERE {' AND '.join(where)}
            ORDER BY se.scheduled_date DESC, z.name, st.name
        """, vals).fetchall()
        result = rows_to_list(rows)
        closed = rows_to_list(conn.execute("SELECT * FROM close_days WHERE closed_date >= ? AND closed_date <= ?", [date_from, date_to]).fetchall())
        return {"status": 200, "body": {"entries": result, "closed_days": closed}}

    # ----- DEBUG (secured — exec only) -----
    if method == "GET" and path == "/debug":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        if not current_user or current_user['role'] not in ('executive', 'office'):
            return {"status": 403, "body": {"error": "Executive access required"}}
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
        allowed, remaining = check_rate_limit(conn, f"pin:{pin}:{request.remote_addr}")
        if not allowed:
            return {"status": 429, "body": {"error": "Too many attempts. Try again later."}}
        row = conn.execute("SELECT * FROM users WHERE pin=? AND is_active=1", [pin]).fetchone()
        if not row:
            record_login_attempt(conn, f"pin:{pin}:{request.remote_addr}", False)
            return {"status": 401, "body": {"error": "Invalid PIN"}}
        user = row_to_dict(row)
        token = make_token(user["id"], user["role"])
        record_login_attempt(conn, f"pin:{pin}:{request.remote_addr}", True)
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
        safety_checks = body.get("safety_checklist", [])
        odometer_start = body.get("odometer_start")
        if not truck_id:
            return {"status": 400, "body": {"error": "truck_id required"}}
        # Validate mandatory checklist items
        mandatory_items = conn.execute("SELECT id, item_text FROM safety_checklist_items WHERE is_mandatory=1 AND is_active=1").fetchall()
        mandatory_ids = {r[0] for r in mandatory_items}
        checked_ids = set()
        if isinstance(safety_checks, list):
            for item in safety_checks:
                if isinstance(item, dict) and item.get("checked"):
                    checked_ids.add(item.get("item_id"))
        missing = mandatory_ids - checked_ids
        if missing and mandatory_items:
            return {"status": 400, "body": {"error": "All mandatory safety checks must be completed", "missing_count": len(missing)}}
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
               safety_acknowledged, safety_acknowledged_at, safety_checklist, status, odometer_start)
               VALUES (?,?,?,?,1,?,?,?,?)""",
            [current_user["id"], truck_id, today, now, now,
             json.dumps(safety_checks), "active", odometer_start])
        conn.commit()
        shift_id = cur.lastrowid
        # Create logbook entry
        conn.execute("""INSERT INTO driver_logbook (driver_shift_id, event_type, odometer_reading, location_lat, location_lng, location_description, recorded_at)
            VALUES (?,?,?,?,?,?,?)""",
            [shift_id, "shift_start", odometer_start, body.get("lat"), body.get("lng"), "Depot", now])
        conn.commit()
        shift = row_to_dict(conn.execute("SELECT * FROM driver_shifts WHERE id=?", [shift_id]).fetchone())
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
        except Exception:             total_hours = 0
        odometer_end = body.get("odometer_end")
        total_km = None
        if odometer_end and shift_dict.get("odometer_start"):
            total_km = odometer_end - shift_dict["odometer_start"]
        conn.execute(
            "UPDATE driver_shifts SET clock_off_time=?, status='completed', total_hours=?, odometer_end=?, total_km=? WHERE id=?",
            [now, round(total_hours, 2), odometer_end, total_km, shift_dict["id"]])
        # Create logbook entry
        conn.execute("""INSERT INTO driver_logbook (driver_shift_id, event_type, odometer_reading, location_lat, location_lng, location_description, recorded_at)
            VALUES (?,?,?,?,?,?,?)""",
            [shift_dict["id"], "shift_end", odometer_end, body.get("lat"), body.get("lng"), "Depot", now])
        conn.commit()
        updated = row_to_dict(conn.execute("SELECT * FROM driver_shifts WHERE id=?",
                                           [shift_dict["id"]]).fetchone())
        return {"status": 200, "body": updated}

    # ----- GET ACTIVE SHIFT -----
    if method == "GET" and path == "/driver/shift":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
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
               (delivery_log_id, driver_shift_id, stage, started_at, location_lat, location_lng, stop_number, odometer_start, gps_accuracy)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            [delivery_log_id, shift_id, stage, now,
             body.get("lat"), body.get("lng"), stop_number, body.get("odometer"), body.get("gps_accuracy")])
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
        except Exception:             duration = 0
        odometer_end = body.get("odometer")
        manual_km = body.get("manual_km")
        photo_data = body.get("photo_data")
        notes = body.get("notes")
        conn.execute(
            "UPDATE delivery_run_stages SET ended_at=?, duration_minutes=?, odometer_end=?, manual_km=?, photo_data=?, notes=? WHERE id=?",
            [now, round(duration, 2), odometer_end, manual_km, photo_data, notes, stage_id])
        conn.commit()
        updated = row_to_dict(conn.execute("SELECT * FROM delivery_run_stages WHERE id=?",
                                           [stage_id]).fetchone())
        return {"status": 200, "body": updated}

    # ----- GET STAGES FOR DELIVERY -----
    if method == "GET" and path == "/driver/stages":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
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
        except Exception:             duration = 0
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
        if not dl_id or not shift_id:
            return {"status": 400, "body": {"error": "delivery_log_id and shift_id required"}}
        # Try to get total_km from odometer readings first
        total_km = body.get("total_km", 0)
        if not total_km:
            # Calculate from stage odometer readings
            stages = conn.execute("""
                SELECT odometer_start, odometer_end, manual_km
                FROM delivery_run_stages
                WHERE delivery_log_id=? AND driver_shift_id=?
                ORDER BY started_at
            """, [dl_id, shift_id]).fetchall()
            for s in stages:
                sd_stage = dict(zip(["odometer_start", "odometer_end", "manual_km"], s))
                if sd_stage.get("manual_km"):
                    total_km += sd_stage["manual_km"]
                elif sd_stage.get("odometer_start") and sd_stage.get("odometer_end"):
                    total_km += (sd_stage["odometer_end"] - sd_stage["odometer_start"])
        # Also try from logbook entries
        if not total_km:
            logbook_entries = conn.execute("""
                SELECT odometer_reading FROM driver_logbook
                WHERE driver_shift_id=? AND delivery_log_id=? AND odometer_reading IS NOT NULL
                ORDER BY recorded_at
            """, [shift_id, dl_id]).fetchall()
            if len(logbook_entries) >= 2:
                readings = [r[0] for r in logbook_entries if r[0] is not None]
                if len(readings) >= 2:
                    total_km = max(readings) - min(readings)
        tolls = body.get("tolls", 0)
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
            order_id = dl_order[0]
            conn.execute("UPDATE orders SET status=?, dispatched_at=? WHERE id=?",
                         [final_status, datetime.now(timezone.utc).isoformat(), order_id])
            # Update order items to match delivery status
            conn.execute("UPDATE order_items SET status=? WHERE order_id=? AND status IN ('F', 'dispatched')",
                         [final_status, order_id])
            update_kanban_statuses(conn, order_id)
        conn.commit()
        return {"status": 200, "body": costs}

    # ----- TRUCK FINANCE CONFIG -----
    if method == "GET" and path == "/truck-finance":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
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
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
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
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
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

    # ----- RUNSHEET V2 — grouped by dispatch runs -----
    if method == "GET" and path == "/driver/runsheet-v2":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        truck_id = params.get("truck_id")
        date = params.get("date", datetime.now(timezone.utc).strftime("%Y-%m-%d"))
        if not truck_id:
            return {"status": 400, "body": {"error": "truck_id required"}}
        # Load truck info
        truck_row = conn.execute("SELECT id, name, rego FROM trucks WHERE id=?", [truck_id]).fetchone()
        truck_info = row_to_dict(truck_row) if truck_row else {"id": truck_id, "name": f"Truck {truck_id}", "rego": ""}
        # Load driver info from most recent dispatch_run or current_user
        driver_info = {"id": current_user["id"], "name": current_user.get("full_name", current_user.get("username", "Driver"))}
        # Load all dispatch runs for this truck/date, ordered by run_number
        run_rows = conn.execute("""
            SELECT dr.*, u.full_name as driver_name
            FROM dispatch_runs dr
            LEFT JOIN users u ON u.id = dr.driver_id
            WHERE dr.truck_id=? AND dr.run_date=?
            ORDER BY dr.run_number ASC
        """, [truck_id, date]).fetchall()
        # Load all delivery_log entries for this truck/date with full join
        dl_rows = conn.execute("""
            SELECT dl.id as delivery_log_id, dl.order_id, dl.run_id, dl.load_sequence,
                   dl.status, dl.delivery_type, dl.estimated_minutes,
                   o.order_number, o.delivery_type as order_delivery_type,
                   o.special_instructions,
                   c.company_name as client_name, c.phone as client_phone,
                   da.street_address, da.suburb, da.state, da.postcode,
                   da.estimated_travel_minutes, da.estimated_return_minutes
            FROM delivery_log dl
            LEFT JOIN orders o ON o.id = dl.order_id
            LEFT JOIN clients c ON c.id = o.client_id
            LEFT JOIN delivery_addresses da ON da.client_id = o.client_id AND da.is_default = 1
            WHERE dl.truck_id=? AND dl.expected_date=?
            ORDER BY dl.run_id ASC, dl.load_sequence ASC, dl.id ASC
        """, [truck_id, date]).fetchall()
        dl_list = rows_to_list(dl_rows)
        # Attach items to each stop
        for stop in dl_list:
            items = rows_to_list(conn.execute(
                "SELECT oi.sku_code, oi.product_name, oi.quantity FROM order_items oi WHERE oi.order_id=? ORDER BY oi.id",
                [stop.get("order_id")]).fetchall()) if stop.get("order_id") else []
            stop["items"] = items
            stop["total_qty"] = sum(it.get("quantity", 0) for it in items)
            # Build readable address from component parts
            parts = [stop.get("street_address", ""), stop.get("suburb", ""),
                     stop.get("state", ""), stop.get("postcode", "")]
            stop["client_address"] = ", ".join(p for p in parts if p)
        # Build run groups
        runs = []
        run_stop_map = {}  # run_id -> list of stops
        unassigned = []
        for stop in dl_list:
            rid = stop.get("run_id")
            if rid:
                if rid not in run_stop_map:
                    run_stop_map[rid] = []
                run_stop_map[rid].append(stop)
            else:
                unassigned.append(stop)
        for run_row in rows_to_list(run_rows):
            rid = run_row["id"]
            stops_in_run = run_stop_map.get(rid, [])
            est_total = sum((s.get("estimated_travel_minutes") or s.get("estimated_minutes") or 30) + 30 for s in stops_in_run)
            runs.append({
                "id": rid,
                "run_number": run_row["run_number"],
                "status": run_row.get("status", "planned"),
                "departure_time": run_row.get("departure_time"),
                "driver_name": run_row.get("driver_name") or driver_info["name"],
                "notes": run_row.get("notes"),
                "stops": stops_in_run,
                "total_stops": len(stops_in_run),
                "total_estimated_minutes": est_total,
            })
        return {"status": 200, "body": {
            "date": date,
            "truck": truck_info,
            "driver": driver_info,
            "runs": runs,
            "unassigned_stops": unassigned,
        }}

    # ----- DRIVER: UPDATE STOP SEQUENCE (driver override) -----
    if method == "PUT" and path == "/driver/stop-sequence":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        sequences = body.get("sequences", [])  # [{delivery_log_id, load_sequence}]
        for seq_item in sequences:
            dl_id = seq_item.get("delivery_log_id")
            seq = seq_item.get("load_sequence")
            if dl_id is not None and seq is not None:
                conn.execute("UPDATE delivery_log SET load_sequence=?, updated_at=CURRENT_TIMESTAMP WHERE id=?", [seq, dl_id])
        conn.commit()
        return {"status": 200, "body": {"ok": True, "updated": len(sequences)}}

    # ----- SAFETY CHECKLIST ITEMS -----
    if method == "GET" and path == "/driver/safety-checklist-items":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        rows = conn.execute("SELECT * FROM safety_checklist_items WHERE is_active=1 ORDER BY sort_order").fetchall()
        return {"status": 200, "body": rows_to_list(rows)}

    if method == "POST" and path == "/admin/safety-checklist-items":
        if not current_user or current_user["role"] not in ("executive", "office"):
            return {"status": 403, "body": {"error": "Admin access required"}}
        text = body.get("item_text", "").strip()
        if not text:
            return {"status": 400, "body": {"error": "item_text required"}}
        cur = conn.execute("INSERT INTO safety_checklist_items (item_text, category, is_mandatory, sort_order) VALUES (?,?,?,?)",
            [text, body.get("category", "pre_trip"), body.get("is_mandatory", 1), body.get("sort_order", 99)])
        conn.commit()
        return {"status": 201, "body": row_to_dict(conn.execute("SELECT * FROM safety_checklist_items WHERE id=?", [cur.lastrowid]).fetchone())}

    if method == "DELETE" and path == "/admin/safety-checklist-items":
        if not current_user or current_user["role"] not in ("executive", "office"):
            return {"status": 403, "body": {"error": "Admin access required"}}
        item_id = params.get("id") or body.get("id")
        if not item_id:
            return {"status": 400, "body": {"error": "id required"}}
        conn.execute("UPDATE safety_checklist_items SET is_active=0 WHERE id=?", [item_id])
        conn.commit()
        return {"status": 200, "body": {"ok": True}}

    # ----- DRIVER LOGBOOK -----
    if method == "POST" and path == "/driver/logbook":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        shift_id = body.get("shift_id")
        event_type = body.get("event_type")
        if not shift_id or not event_type:
            return {"status": 400, "body": {"error": "shift_id and event_type required"}}
        now = datetime.now(timezone.utc).isoformat()
        cur = conn.execute("""INSERT INTO driver_logbook
            (driver_shift_id, delivery_log_id, event_type, odometer_reading, manual_km,
             location_lat, location_lng, location_description, notes, recorded_at)
            VALUES (?,?,?,?,?,?,?,?,?,?)""",
            [shift_id, body.get("delivery_log_id"), event_type,
             body.get("odometer_reading"), body.get("manual_km"),
             body.get("lat"), body.get("lng"), body.get("location_description"),
             body.get("notes"), now])
        conn.commit()
        return {"status": 201, "body": row_to_dict(conn.execute("SELECT * FROM driver_logbook WHERE id=?", [cur.lastrowid]).fetchone())}

    if method == "GET" and path == "/driver/logbook":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        shift_id = params.get("shift_id")
        if not shift_id:
            return {"status": 400, "body": {"error": "shift_id required"}}
        rows = conn.execute("SELECT * FROM driver_logbook WHERE driver_shift_id=? ORDER BY recorded_at", [shift_id]).fetchall()
        return {"status": 200, "body": rows_to_list(rows)}

    # ----- DELIVERY PHOTOS -----
    if method == "POST" and path == "/driver/photo":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        dl_id = body.get("delivery_log_id")
        shift_id = body.get("shift_id")
        photo_data = body.get("photo_data")
        photo_type = body.get("photo_type", "pod")
        if not shift_id or not photo_data:
            return {"status": 400, "body": {"error": "shift_id and photo_data required"}}
        now = datetime.now(timezone.utc).isoformat()
        cur = conn.execute("""INSERT INTO delivery_photos
            (delivery_log_id, driver_shift_id, photo_type, photo_data, caption, location_lat, location_lng, taken_at)
            VALUES (?,?,?,?,?,?,?,?)""",
            [dl_id, shift_id, photo_type, photo_data, body.get("caption"),
             body.get("lat"), body.get("lng"), now])
        conn.commit()
        return {"status": 201, "body": {"id": cur.lastrowid, "photo_type": photo_type}}

    if method == "GET" and path == "/driver/photos":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        dl_id = params.get("delivery_log_id")
        shift_id = params.get("shift_id")
        if dl_id:
            rows = conn.execute("SELECT id, delivery_log_id, driver_shift_id, photo_type, caption, location_lat, location_lng, taken_at FROM delivery_photos WHERE delivery_log_id=? ORDER BY taken_at", [dl_id]).fetchall()
        elif shift_id:
            rows = conn.execute("SELECT id, delivery_log_id, driver_shift_id, photo_type, caption, location_lat, location_lng, taken_at FROM delivery_photos WHERE driver_shift_id=? ORDER BY taken_at", [shift_id]).fetchall()
        else:
            return {"status": 400, "body": {"error": "delivery_log_id or shift_id required"}}
        return {"status": 200, "body": rows_to_list(rows)}

    # Get single photo data (base64)
    if method == "GET" and path == "/driver/photo":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        photo_id = params.get("id")
        if not photo_id:
            return {"status": 400, "body": {"error": "id required"}}
        row = conn.execute("SELECT * FROM delivery_photos WHERE id=?", [photo_id]).fetchone()
        if not row:
            return {"status": 404, "body": {"error": "Photo not found"}}
        return {"status": 200, "body": row_to_dict(row)}

    # ----- FATIGUE CONFIG -----
    if method == "GET" and path == "/driver/fatigue-config":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        row = conn.execute("SELECT * FROM driver_fatigue_config WHERE is_active=1 LIMIT 1").fetchone()
        return {"status": 200, "body": row_to_dict(row) if row else {"max_driving_hours_before_break": 5.0, "mandatory_break_minutes": 30, "max_shift_hours": 12.0, "warning_threshold_hours": 11.0}}

    if method == "PUT" and path == "/admin/fatigue-config":
        if not current_user or current_user["role"] not in ("executive", "office"):
            return {"status": 403, "body": {"error": "Admin access required"}}
        fields = ["max_driving_hours_before_break", "mandatory_break_minutes", "max_shift_hours", "warning_threshold_hours"]
        updates = []
        vals = []
        for f in fields:
            if f in body:
                updates.append(f"{f}=?")
                vals.append(body[f])
        if updates:
            row = conn.execute("SELECT id FROM driver_fatigue_config LIMIT 1").fetchone()
            if row:
                vals.append(datetime.now(timezone.utc).isoformat())
                vals.append(row[0])
                conn.execute(f"UPDATE driver_fatigue_config SET {', '.join(updates)}, updated_at=? WHERE id=?", vals)
            else:
                conn.execute("INSERT INTO driver_fatigue_config (max_driving_hours_before_break, mandatory_break_minutes, max_shift_hours, warning_threshold_hours) VALUES (?,?,?,?)",
                    [body.get("max_driving_hours_before_break", 5.0), body.get("mandatory_break_minutes", 30), body.get("max_shift_hours", 12.0), body.get("warning_threshold_hours", 11.0)])
            conn.commit()
        row = conn.execute("SELECT * FROM driver_fatigue_config WHERE is_active=1 LIMIT 1").fetchone()
        return {"status": 200, "body": row_to_dict(row)}

    # ----- FATIGUE CHECK (called by frontend periodically) -----
    if method == "GET" and path == "/driver/fatigue-check":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        shift_id = params.get("shift_id")
        if not shift_id:
            return {"status": 400, "body": {"error": "shift_id required"}}
        shift = conn.execute("SELECT * FROM driver_shifts WHERE id=?", [shift_id]).fetchone()
        if not shift:
            return {"status": 404, "body": {"error": "Shift not found"}}
        sd = row_to_dict(shift)
        config = conn.execute("SELECT * FROM driver_fatigue_config WHERE is_active=1 LIMIT 1").fetchone()
        cfg = row_to_dict(config) if config else {"max_driving_hours_before_break": 5.0, "mandatory_break_minutes": 30, "max_shift_hours": 12.0, "warning_threshold_hours": 11.0}

        # Calculate total shift hours
        try:
            clock_on = datetime.fromisoformat(sd["clock_on_time"].replace("Z", "+00:00"))
            now_dt = datetime.now(timezone.utc)
            shift_hours = (now_dt - clock_on).total_seconds() / 3600
        except Exception:             shift_hours = 0

        # Calculate driving hours since last break
        driving_stages = conn.execute("""
            SELECT COALESCE(SUM(duration_minutes), 0) as total_driving
            FROM delivery_run_stages
            WHERE driver_shift_id=? AND stage IN ('driving_to_customer','driving_return') AND ended_at IS NOT NULL
        """, [shift_id]).fetchone()
        total_driving_mins = driving_stages[0] if driving_stages else 0

        # Check last break
        last_break = conn.execute("""
            SELECT ended_at FROM delivery_run_stages
            WHERE driver_shift_id=? AND stage='break' AND ended_at IS NOT NULL
            ORDER BY ended_at DESC LIMIT 1
        """, [shift_id]).fetchone()

        driving_since_break_mins = total_driving_mins  # simplified — all driving if no break
        if last_break:
            last_break_time = last_break[0]
            driving_after = conn.execute("""
                SELECT COALESCE(SUM(duration_minutes), 0)
                FROM delivery_run_stages
                WHERE driver_shift_id=? AND stage IN ('driving_to_customer','driving_return')
                AND ended_at IS NOT NULL AND started_at > ?
            """, [shift_id, last_break_time]).fetchone()
            driving_since_break_mins = driving_after[0] if driving_after else 0

        driving_since_break_hrs = driving_since_break_mins / 60

        warnings = []
        if driving_since_break_hrs >= cfg["max_driving_hours_before_break"]:
            warnings.append({"type": "mandatory_break", "message": f"You have been driving for {round(driving_since_break_hrs, 1)} hours. A {cfg['mandatory_break_minutes']}-minute break is MANDATORY.", "severity": "critical"})
        elif driving_since_break_hrs >= (cfg["max_driving_hours_before_break"] - 0.5):
            warnings.append({"type": "break_soon", "message": f"Approaching mandatory break threshold ({round(driving_since_break_hrs, 1)}/{cfg['max_driving_hours_before_break']} hrs driving).", "severity": "warning"})

        if shift_hours >= cfg["max_shift_hours"]:
            warnings.append({"type": "max_shift", "message": f"Maximum shift duration ({cfg['max_shift_hours']} hours) reached. You must clock off.", "severity": "critical"})
        elif shift_hours >= cfg["warning_threshold_hours"]:
            warnings.append({"type": "shift_warning", "message": f"Shift duration: {round(shift_hours, 1)} hours. Maximum is {cfg['max_shift_hours']} hours.", "severity": "warning"})

        return {"status": 200, "body": {
            "shift_hours": round(shift_hours, 2),
            "total_driving_minutes": round(total_driving_mins, 2),
            "driving_since_break_minutes": round(driving_since_break_mins, 2),
            "warnings": warnings,
            "config": cfg
        }}

    # ----- TRACKMYRIDE CONFIG -----
    if method == "GET" and path == "/admin/trackmyride-config":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        if not current_user or current_user["role"] not in ("executive", "office"):
            return {"status": 403, "body": {"error": "Admin access required"}}
        row = conn.execute("SELECT * FROM trackmyride_config LIMIT 1").fetchone()
        return {"status": 200, "body": row_to_dict(row) if row else {"is_active": 0}}

    if method == "PUT" and path == "/admin/trackmyride-config":
        if not current_user or current_user["role"] not in ("executive", "office"):
            return {"status": 403, "body": {"error": "Admin access required"}}
        row = conn.execute("SELECT id FROM trackmyride_config LIMIT 1").fetchone()
        if row:
            fields = ["user_key", "api_key", "is_active", "truck_device_mapping", "geofence_radius_m", "auto_stage_enabled", "playback_enabled", "refuel_tracking_enabled"]
            updates = []
            vals = []
            for f in fields:
                if f in body:
                    updates.append(f"{f}=?")
                    vals.append(body[f] if f != "truck_device_mapping" else json.dumps(body[f]) if isinstance(body[f], dict) else body[f])
            if updates:
                vals.append(datetime.now(timezone.utc).isoformat())
                vals.append(row[0])
                conn.execute(f"UPDATE trackmyride_config SET {', '.join(updates)}, updated_at=? WHERE id=?", vals)
        else:
            conn.execute("""INSERT INTO trackmyride_config (user_key, api_key, is_active, truck_device_mapping)
                VALUES (?,?,?,?)""",
                [body.get("user_key"), body.get("api_key"), body.get("is_active", 0),
                 json.dumps(body.get("truck_device_mapping", {})) if isinstance(body.get("truck_device_mapping"), dict) else body.get("truck_device_mapping")])
        conn.commit()
        row = conn.execute("SELECT * FROM trackmyride_config LIMIT 1").fetchone()
        return {"status": 200, "body": row_to_dict(row)}

    # ----- TRACKMYRIDE PROXY — Get live position for a truck -----
    if method == "GET" and path == "/trackmyride/position":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        truck_id = params.get("truck_id")
        if not truck_id:
            return {"status": 400, "body": {"error": "truck_id required"}}
        # Check if TrackMyRide is active
        tmr = conn.execute("SELECT * FROM trackmyride_config WHERE is_active=1 LIMIT 1").fetchone()
        if not tmr:
            return {"status": 200, "body": {"source": "manual", "message": "TrackMyRide not configured. Using manual logbook."}}
        tmr_d = row_to_dict(tmr)
        # Try to proxy to TrackMyRide API
        device_mapping = json.loads(tmr_d.get("truck_device_mapping") or "{}") if isinstance(tmr_d.get("truck_device_mapping"), str) else tmr_d.get("truck_device_mapping") or {}
        device_id = device_mapping.get(str(truck_id))
        if not device_id:
            return {"status": 200, "body": {"source": "manual", "message": "No device mapped for this truck"}}
        # Attempt API call (placeholder — actual TrackMyRide API integration TBD)
        try:
            import urllib.request as urlreq
            api_url = f"https://api.trackmyride.com.au/v1/device/{device_id}/position"
            req = urlreq.Request(api_url, headers={"Authorization": f"Bearer {tmr_d.get('api_key', '')}", "X-User-Key": tmr_d.get("user_key", "")})
            resp = urlreq.urlopen(req, timeout=5)
            data = json.loads(resp.read().decode())
            # Cache event
            conn.execute("""INSERT INTO trackmyride_events (truck_id, device_id, event_type, latitude, longitude, speed, heading, odometer, raw_payload, event_time)
                VALUES (?,?,?,?,?,?,?,?,?,?)""",
                [truck_id, device_id, "position", data.get("lat"), data.get("lng"), data.get("speed"), data.get("heading"), data.get("odometer"), json.dumps(data), data.get("timestamp", datetime.now(timezone.utc).isoformat())])
            conn.commit()
            return {"status": 200, "body": {"source": "trackmyride", "data": data}}
        except Exception as e:
            # Fallback to last cached position or manual
            cached = conn.execute("SELECT * FROM trackmyride_events WHERE truck_id=? ORDER BY received_at DESC LIMIT 1", [truck_id]).fetchone()
            if cached:
                return {"status": 200, "body": {"source": "cached", "data": row_to_dict(cached), "cache_note": "Live API unavailable, showing last known position"}}
            return {"status": 200, "body": {"source": "manual", "message": f"TrackMyRide API unavailable: {str(e)}. Using manual logbook.", "error": str(e)}}

    # ----- TRACKMYRIDE GEOFENCES CRUD -----
    if method == "GET" and path == "/trackmyride/geofences":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        rows = conn.execute("SELECT g.*, c.company_name as client_name FROM trackmyride_geofences g LEFT JOIN clients c ON c.id=g.linked_client_id WHERE g.is_active=1 ORDER BY g.name").fetchall()
        return {"status": 200, "body": rows_to_list(rows)}

    if method == "POST" and path == "/trackmyride/geofences":
        if not current_user or current_user["role"] not in ("executive", "office"):
            return {"status": 403, "body": {"error": "Admin access required"}}
        name = body.get("name", "").strip()
        if not name:
            return {"status": 400, "body": {"error": "name required"}}
        cur = conn.execute("""INSERT INTO trackmyride_geofences (name, type, latitude, longitude, radius_m, polygon_points, linked_client_id, linked_type)
            VALUES (?,?,?,?,?,?,?,?)""",
            [name, body.get("type", "circle"), body.get("latitude"), body.get("longitude"),
             body.get("radius_m", 200), json.dumps(body.get("polygon_points")) if body.get("polygon_points") else None,
             body.get("linked_client_id"), body.get("linked_type", "customer")])
        conn.commit()
        return {"status": 201, "body": row_to_dict(conn.execute("SELECT * FROM trackmyride_geofences WHERE id=?", [cur.lastrowid]).fetchone())}

    # ----- TRACKMYRIDE PLAYBACK (route history for a truck/date) -----
    if method == "GET" and path == "/trackmyride/playback":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        truck_id = params.get("truck_id")
        date = params.get("date", datetime.now(timezone.utc).strftime("%Y-%m-%d"))
        if not truck_id:
            return {"status": 400, "body": {"error": "truck_id required"}}
        # Try live API first, fall back to cached events
        rows = conn.execute("""SELECT * FROM trackmyride_events
            WHERE truck_id=? AND DATE(event_time)=? ORDER BY event_time""",
            [truck_id, date]).fetchall()
        return {"status": 200, "body": rows_to_list(rows)}

    # ----- TRACKMYRIDE REFUEL EVENTS -----
    if method == "GET" and path == "/trackmyride/refuel-events":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        truck_id = params.get("truck_id")
        if not truck_id:
            return {"status": 400, "body": {"error": "truck_id required"}}
        rows = conn.execute("""SELECT * FROM trackmyride_events
            WHERE truck_id=? AND event_type='refuel' ORDER BY event_time DESC LIMIT 50""",
            [truck_id]).fetchall()
        return {"status": 200, "body": rows_to_list(rows)}

    # ----- MANUAL REFUEL ENTRY (logbook mode) -----
    if method == "POST" and path == "/driver/refuel":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        shift_id = body.get("shift_id")
        if not shift_id:
            return {"status": 400, "body": {"error": "shift_id required"}}
        now = datetime.now(timezone.utc).isoformat()
        # Log in logbook
        cur = conn.execute("""INSERT INTO driver_logbook
            (driver_shift_id, event_type, odometer_reading, manual_km, location_lat, location_lng, location_description, notes, recorded_at)
            VALUES (?,?,?,?,?,?,?,?,?)""",
            [shift_id, "refuel", body.get("odometer_reading"), body.get("manual_km"),
             body.get("lat"), body.get("lng"), body.get("location_description", ""),
             body.get("notes", ""), now])
        conn.commit()
        return {"status": 201, "body": row_to_dict(conn.execute("SELECT * FROM driver_logbook WHERE id=?", [cur.lastrowid]).fetchone())}

    # ----- DELIVERY COST BREAKDOWN (enhanced) -----
    if method == "GET" and path == "/driver/cost-breakdown":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        dl_id = params.get("delivery_log_id")
        shift_id = params.get("shift_id")
        if dl_id:
            cost_row = conn.execute("SELECT * FROM delivery_run_costs WHERE delivery_log_id=?", [dl_id]).fetchone()
            if not cost_row:
                return {"status": 200, "body": None}
            cost = row_to_dict(cost_row)
            # Get delivery info for pallet count
            dl = conn.execute("SELECT dl.*, o.order_number FROM delivery_log dl LEFT JOIN orders o ON o.id=dl.order_id WHERE dl.id=?", [dl_id]).fetchone()
            if dl:
                dl_d = row_to_dict(dl)
                order_id = dl_d.get("order_id")
                if order_id:
                    total_qty = conn.execute("SELECT COALESCE(SUM(quantity), 0) FROM order_items WHERE order_id=?", [order_id]).fetchone()[0]
                    cost["total_pallets"] = total_qty
                    cost["cost_per_pallet"] = round(cost["total_cost"] / total_qty, 2) if total_qty else 0
                    cost["order_number"] = dl_d.get("order_number")
            return {"status": 200, "body": cost}
        elif shift_id:
            rows = conn.execute("""
                SELECT drc.*, dl.order_id, o.order_number, c.company_name as client_name
                FROM delivery_run_costs drc
                LEFT JOIN delivery_log dl ON dl.id=drc.delivery_log_id
                LEFT JOIN orders o ON o.id=dl.order_id
                LEFT JOIN clients c ON c.id=o.client_id
                WHERE drc.driver_shift_id=? ORDER BY drc.calculated_at
            """, [shift_id]).fetchall()
            results = []
            total_shift_cost = 0
            for r in rows_to_list(rows):
                if r.get("order_id"):
                    total_qty = conn.execute("SELECT COALESCE(SUM(quantity), 0) FROM order_items WHERE order_id=?", [r["order_id"]]).fetchone()[0]
                    r["total_pallets"] = total_qty
                    r["cost_per_pallet"] = round(r["total_cost"] / total_qty, 2) if total_qty else 0
                total_shift_cost += r.get("total_cost", 0)
                results.append(r)
            return {"status": 200, "body": {"deliveries": results, "total_shift_cost": round(total_shift_cost, 2)}}
        return {"status": 400, "body": {"error": "delivery_log_id or shift_id required"}}

    # ----- OFFLINE SYNC (batch submit queued actions) -----
    if method == "POST" and path == "/driver/sync":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        actions = body.get("actions", [])
        results = []
        for action in actions:
            action_type = action.get("type")
            action_data = action.get("data", {})
            try:
                if action_type == "stage_start":
                    cur = conn.execute("""INSERT INTO delivery_run_stages
                        (delivery_log_id, driver_shift_id, stage, started_at, location_lat, location_lng, stop_number, odometer_start, gps_accuracy)
                        VALUES (?,?,?,?,?,?,?,?,?)""",
                        [action_data.get("delivery_log_id"), action_data.get("shift_id"), action_data.get("stage"),
                         action_data.get("started_at", datetime.now(timezone.utc).isoformat()),
                         action_data.get("lat"), action_data.get("lng"), action_data.get("stop_number", 1),
                         action_data.get("odometer"), action_data.get("gps_accuracy")])
                    conn.commit()
                    results.append({"action_id": action.get("id"), "status": "ok", "server_id": cur.lastrowid})
                elif action_type == "stage_end":
                    stage_id = action_data.get("stage_id")
                    now = action_data.get("ended_at", datetime.now(timezone.utc).isoformat())
                    conn.execute("UPDATE delivery_run_stages SET ended_at=?, duration_minutes=?, odometer_end=?, manual_km=?, photo_data=?, notes=? WHERE id=?",
                        [now, action_data.get("duration_minutes"), action_data.get("odometer"), action_data.get("manual_km"), action_data.get("photo_data"), action_data.get("notes"), stage_id])
                    conn.commit()
                    results.append({"action_id": action.get("id"), "status": "ok"})
                elif action_type == "logbook":
                    cur = conn.execute("""INSERT INTO driver_logbook
                        (driver_shift_id, delivery_log_id, event_type, odometer_reading, manual_km, location_lat, location_lng, location_description, notes, recorded_at)
                        VALUES (?,?,?,?,?,?,?,?,?,?)""",
                        [action_data.get("shift_id"), action_data.get("delivery_log_id"), action_data.get("event_type"),
                         action_data.get("odometer_reading"), action_data.get("manual_km"),
                         action_data.get("lat"), action_data.get("lng"), action_data.get("location_description"),
                         action_data.get("notes"), action_data.get("recorded_at", datetime.now(timezone.utc).isoformat())])
                    conn.commit()
                    results.append({"action_id": action.get("id"), "status": "ok", "server_id": cur.lastrowid})
                elif action_type == "photo":
                    cur = conn.execute("""INSERT INTO delivery_photos
                        (delivery_log_id, driver_shift_id, photo_type, photo_data, caption, location_lat, location_lng, taken_at)
                        VALUES (?,?,?,?,?,?,?,?)""",
                        [action_data.get("delivery_log_id"), action_data.get("shift_id"), action_data.get("photo_type", "pod"),
                         action_data.get("photo_data"), action_data.get("caption"),
                         action_data.get("lat"), action_data.get("lng"), action_data.get("taken_at", datetime.now(timezone.utc).isoformat())])
                    conn.commit()
                    results.append({"action_id": action.get("id"), "status": "ok", "server_id": cur.lastrowid})
                elif action_type == "incident":
                    cur = conn.execute("""INSERT INTO driver_incidents
                        (driver_shift_id, delivery_log_id, incident_type, description, photo_data, location_lat, location_lng, reported_at)
                        VALUES (?,?,?,?,?,?,?,?)""",
                        [action_data.get("shift_id"), action_data.get("delivery_log_id"), action_data.get("incident_type", "other"),
                         action_data.get("description"), action_data.get("photo_data"),
                         action_data.get("lat"), action_data.get("lng"), action_data.get("reported_at", datetime.now(timezone.utc).isoformat())])
                    conn.commit()
                    results.append({"action_id": action.get("id"), "status": "ok", "server_id": cur.lastrowid})
                else:
                    results.append({"action_id": action.get("id"), "status": "error", "error": f"Unknown action type: {action_type}"})
            except Exception as e:
                conn.rollback()
                results.append({"action_id": action.get("id"), "status": "error", "error": str(e)})
        return {"status": 200, "body": {"synced": len([r for r in results if r["status"] == "ok"]), "failed": len([r for r in results if r["status"] == "error"]), "results": results}}

    # ----- EMAIL CONFIG -----
    if method == "GET" and path == "/admin/email-config":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        if not current_user or current_user["role"] not in ("executive", "office"):
            return {"status": 403, "body": {"error": "Admin access required"}}
        row = conn.execute("SELECT * FROM email_config LIMIT 1").fetchone()
        if row:
            cfg = row_to_dict(row)
            if cfg.get("smtp_password"):
                cfg["smtp_password"] = "****"
            return {"status": 200, "body": cfg}
        return {"status": 200, "body": {"is_active": 0}}

    if method == "PUT" and path == "/admin/email-config":
        if not current_user or current_user["role"] not in ("executive", "office"):
            return {"status": 403, "body": {"error": "Admin access required"}}
        row = conn.execute("SELECT id FROM email_config LIMIT 1").fetchone()
        if row:
            fields = ["smtp_host", "smtp_port", "smtp_user", "smtp_password", "smtp_use_tls", "from_name", "from_email", "is_active"]
            updates, vals = [], []
            for f in fields:
                if f in body:
                    updates.append(f"{f}=?")
                    val = _smtp_encrypt(body[f]) if f == "smtp_password" else body[f]
                    vals.append(val)
            if updates:
                vals.append(datetime.now(timezone.utc).isoformat())
                vals.append(row[0])
                conn.execute(f"UPDATE email_config SET {', '.join(updates)}, updated_at=? WHERE id=?", vals)
                conn.commit()
        else:
            conn.execute("INSERT INTO email_config (smtp_host, smtp_port, smtp_user, smtp_password, smtp_use_tls, from_name, from_email, is_active) VALUES (?,?,?,?,?,?,?,?)",
                [body.get("smtp_host",""), body.get("smtp_port",587), body.get("smtp_user",""), _smtp_encrypt(body.get("smtp_password","")),
                 body.get("smtp_use_tls",1), body.get("from_name","Hyne Pallets"), body.get("from_email",""), body.get("is_active",0)])
            conn.commit()
        row = conn.execute("SELECT * FROM email_config LIMIT 1").fetchone()
        cfg = row_to_dict(row)
        if cfg.get("smtp_password"):
            cfg["smtp_password"] = "****"
        return {"status": 200, "body": cfg}

    if method == "POST" and path == "/admin/purge-data":
        if not current_user or current_user["role"] not in ("executive",):
            return {"status": 403, "body": {"error": "Forbidden"}}
        # B-028: Require confirmation token to prevent accidental data wipe
        confirm_token = body.get("confirmation_token", "")
        if confirm_token != "CONFIRM-PURGE-ALL-DATA":
            return {"status": 400, "body": {"error": "Missing or invalid confirmation_token. Must be 'CONFIRM-PURGE-ALL-DATA'"}}

        # B-028: Log purge attempt BEFORE executing (audit_log is in purge list)
        try:
            conn.execute("INSERT INTO audit_log (user_id, action, details) VALUES (?, 'purge_data_initiated', ?)",
                         [current_user["id"], f"Purge initiated by {current_user.get('full_name', 'unknown')}"])
            conn.commit()
        except Exception:
            pass        # Use a dedicated connection with FK checks disabled
        pconn = sqlite3.connect(DB_PATH, timeout=30)
        pconn.execute("PRAGMA foreign_keys = OFF")
        pconn.execute("PRAGMA journal_mode = WAL")
        pconn.execute("PRAGMA busy_timeout = 10000")
        tables_to_purge = [
            # Driver / delivery child tables
            "delivery_photos", "delivery_run_costs", "delivery_run_stages",
            "driver_incidents", "driver_logbook", "driver_shifts",
            "delivery_addresses",
            # Dispatch
            "contractor_assignments", "dispatch_runs", "truck_work_orders", "delivery_log",
            # Production child tables
            "production_log_summary", "production_logs", "session_workers", "production_sessions",
            "pause_logs", "setup_logs",
            # QA
            "qa_defects", "qa_inspections", "qa_audits",
            # Post-production
            "post_production_log",
            # Planning / scheduling
            "schedule_entries", "station_capacity", "close_days",
            # Drawings, inventory, notifications
            "drawing_files", "inventory", "notification_log",
            # Order tables last
            "order_items", "orders",
            # Accounting sync logs (order-related)
            "accounting_sync_log",
            # Login attempts (not config, but transient)
            "login_attempts",
            # Audit log
            "audit_log"
        ]
        purged = []
        for t in tables_to_purge:
            try:
                pconn.execute(f"DELETE FROM {t}")
                pconn.commit()
                purged.append(t)
            except Exception:
                try:
                    pconn.rollback()
                except Exception:
                    pass
        # Record the purge action
        try:
            pconn.execute("INSERT INTO audit_log (user_id, action, entity_type, details) VALUES (?, 'purge_all_data', 'system', ?)",
                [current_user["id"], json.dumps({"tables_cleared": purged})])
            pconn.commit()
        except Exception:
            pass
        pconn.close()
        return {"status": 200, "body": {"success": True, "message": "All production data purged", "tables_cleared": purged}}

    # ----- SEND TEST EMAIL -----
    if method == "POST" and path == "/admin/email-test":
        if not current_user or current_user["role"] not in ("executive", "office"):
            return {"status": 403, "body": {"error": "Admin access required"}}
        to = body.get("to_email", current_user.get("email"))
        if not to:
            return {"status": 400, "body": {"error": "to_email required"}}
        success, err = send_email_smtp(to, "Hyne Pallets — Test Email", "This is a test email from the Hyne Pallets system. If you received this, email is configured correctly.",
            "<div style='font-family:Arial,sans-serif;max-width:500px;margin:auto;padding:24px;'><div style='background:#07324C;color:white;padding:16px;border-radius:8px 8px 0 0;text-align:center;'><h1 style='margin:0;'>Hyne Pallets</h1></div><div style='padding:16px;border:1px solid #e5e7eb;border-top:none;border-radius:0 0 8px 8px;'><p>This is a test email from the Hyne Pallets Manufacturing Management System.</p><p>If you received this, email is configured correctly.</p></div></div>")
        if success:
            conn.execute("UPDATE email_config SET test_email_sent_at=? WHERE id=(SELECT id FROM email_config LIMIT 1)", [datetime.now(timezone.utc).isoformat()])
            conn.commit()
            return {"status": 200, "body": {"ok": True, "message": f"Test email sent to {to}"}}
        return {"status": 500, "body": {"error": f"Failed to send: {err}"}}

    # ----- PRODUCTION ANALYTICS -----
    if method == "GET" and path == "/stats/production-analytics":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        period = params.get("period", "7d")  # 7d, 30d, 90d, all
        days_back = {"7d": 7, "30d": 30, "90d": 90, "all": 3650}.get(period, 7)
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime("%Y-%m-%d")

        # Daily output by zone
        daily_by_zone = rows_to_list(conn.execute("""
            SELECT DATE(ps.start_time) as date, z.code as zone_code, z.name as zone_name,
                   COUNT(ps.id) as sessions, COALESCE(SUM(ps.produced_quantity),0) as units,
                   COALESCE(SUM(ps.produced_quantity * s.sell_price),0) as value
            FROM production_sessions ps
            JOIN zones z ON z.id=ps.zone_id
            LEFT JOIN order_items oi ON oi.id=ps.order_item_id
            LEFT JOIN skus s ON s.id=oi.sku_id
            WHERE DATE(ps.start_time) >= ? AND ps.status='completed'
            GROUP BY DATE(ps.start_time), z.code, z.name
            ORDER BY date DESC, zone_code
        """, [cutoff]).fetchall())

        # Top SKUs produced
        top_skus = rows_to_list(conn.execute("""
            SELECT s.code, s.name, COALESCE(SUM(ps.produced_quantity),0) as total_produced,
                   COUNT(ps.id) as session_count,
                   ROUND(AVG(CASE WHEN ps.end_time IS NOT NULL AND (JULIANDAY(ps.end_time) - JULIANDAY(ps.start_time)) * 24 > 0 THEN ps.produced_quantity / ((JULIANDAY(ps.end_time) - JULIANDAY(ps.start_time)) * 24) ELSE 0 END), 1) as avg_units_per_hour
            FROM production_sessions ps
            JOIN order_items oi ON oi.id=ps.order_item_id
            JOIN skus s ON s.id=oi.sku_id
            WHERE DATE(ps.start_time) >= ? AND ps.status='completed'
            GROUP BY s.code, s.name
            ORDER BY total_produced DESC
            LIMIT 20
        """, [cutoff]).fetchall())

        # Zone summary totals
        zone_summary = rows_to_list(conn.execute("""
            SELECT z.code, z.name,
                   COALESCE(SUM(ps.produced_quantity),0) as total_units,
                   COUNT(ps.id) as total_sessions,
                   COALESCE(SUM(ps.produced_quantity * s.sell_price),0) as total_value,
                   ROUND(AVG(CASE WHEN ps.end_time IS NOT NULL THEN (JULIANDAY(ps.end_time) - JULIANDAY(ps.start_time)) * 24 ELSE NULL END),2) as avg_session_hours
            FROM production_sessions ps
            JOIN zones z ON z.id=ps.zone_id
            LEFT JOIN order_items oi ON oi.id=ps.order_item_id
            LEFT JOIN skus s ON s.id=oi.sku_id
            WHERE DATE(ps.start_time) >= ? AND ps.status='completed'
            GROUP BY z.code, z.name
            ORDER BY total_units DESC
        """, [cutoff]).fetchall())

        # QA pass rate
        qa_stats = conn.execute("""
            SELECT COUNT(*) as total_inspections,
                   SUM(CASE WHEN passed=1 THEN 1 ELSE 0 END) as passed,
                   SUM(CASE WHEN passed=0 THEN 1 ELSE 0 END) as failed,
                   0 as conditional
            FROM qa_inspections
            WHERE DATE(inspected_at) >= ?
        """, [cutoff]).fetchone()
        qa = dict(zip(["total","passed","failed","conditional"], qa_stats)) if qa_stats else {}
        qa["pass_rate"] = round(qa.get("passed",0) / qa["total"] * 100, 1) if qa.get("total",0) > 0 else 0

        return {"status": 200, "body": {
            "period": period,
            "daily_by_zone": daily_by_zone,
            "top_skus": top_skus,
            "zone_summary": zone_summary,
            "qa_stats": qa
        }}

    # ----- DELIVERY ANALYTICS -----
    if method == "GET" and path == "/stats/delivery-analytics":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        period = params.get("period", "30d")
        days_back = {"7d": 7, "30d": 30, "90d": 90, "all": 3650}.get(period, 30)
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime("%Y-%m-%d")

        # Daily delivery counts
        daily_deliveries = rows_to_list(conn.execute("""
            SELECT DATE(actual_date) as date,
                   COUNT(*) as total,
                   SUM(CASE WHEN delivery_type='delivery' THEN 1 ELSE 0 END) as deliveries,
                   SUM(CASE WHEN delivery_type='collection' THEN 1 ELSE 0 END) as collections
            FROM delivery_log
            WHERE actual_date IS NOT NULL AND actual_date >= ?
            GROUP BY DATE(actual_date)
            ORDER BY date DESC
        """, [cutoff]).fetchall())

        # Cost summary per truck
        truck_costs = rows_to_list(conn.execute("""
            SELECT t.name as truck_name, t.id as truck_id,
                   COUNT(drc.id) as deliveries,
                   ROUND(COALESCE(SUM(drc.total_cost),0),2) as total_cost,
                   ROUND(COALESCE(AVG(drc.total_cost),0),2) as avg_cost,
                   ROUND(COALESCE(SUM(drc.total_km),0),1) as total_km,
                   ROUND(COALESCE(SUM(drc.fuel_cost),0),2) as total_fuel,
                   ROUND(COALESCE(SUM(drc.driver_cost),0),2) as total_driver_wages
            FROM trucks t
            LEFT JOIN delivery_run_costs drc ON drc.driver_shift_id IN (
                SELECT ds.id FROM driver_shifts ds WHERE ds.truck_id=t.id AND ds.shift_date >= ?
            )
            WHERE t.is_active=1
            GROUP BY t.id, t.name
            ORDER BY total_cost DESC
        """, [cutoff]).fetchall())

        # On-time delivery rate
        on_time = conn.execute("""
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN actual_date <= expected_date THEN 1 ELSE 0 END) as on_time,
                   SUM(CASE WHEN actual_date > expected_date THEN 1 ELSE 0 END) as late
            FROM delivery_log
            WHERE actual_date IS NOT NULL AND expected_date IS NOT NULL AND actual_date >= ?
        """, [cutoff]).fetchone()
        on_time_data = dict(zip(["total","on_time","late"], on_time)) if on_time else {}
        on_time_data["on_time_rate"] = round(on_time_data.get("on_time",0) / on_time_data["total"] * 100, 1) if on_time_data.get("total",0) > 0 else 0

        # Top clients by delivery count
        top_clients = rows_to_list(conn.execute("""
            SELECT c.company_name, COUNT(dl.id) as delivery_count,
                   ROUND(COALESCE(SUM(drc.total_cost),0),2) as total_cost
            FROM delivery_log dl
            JOIN orders o ON o.id=dl.order_id
            JOIN clients c ON c.id=o.client_id
            LEFT JOIN delivery_run_costs drc ON drc.delivery_log_id=dl.id
            WHERE dl.actual_date IS NOT NULL AND dl.actual_date >= ?
            GROUP BY c.company_name
            ORDER BY delivery_count DESC
            LIMIT 10
        """, [cutoff]).fetchall())

        return {"status": 200, "body": {
            "period": period,
            "daily_deliveries": daily_deliveries,
            "truck_costs": truck_costs,
            "on_time": on_time_data,
            "top_clients": top_clients
        }}

    # ----- BENCHMARKING ANALYTICS -----
    if method == "GET" and path == "/stats/benchmarking":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        period = params.get("period", "30d")
        days_back = {"7d": 7, "30d": 30, "90d": 90, "all": 3650}.get(period, 30)
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime("%Y-%m-%d")

        # Per-driver performance
        driver_perf = rows_to_list(conn.execute("""
            SELECT u.full_name as driver_name, u.id as driver_id,
                   COUNT(DISTINCT ds.id) as shifts,
                   ROUND(COALESCE(AVG(ds.total_hours),0),2) as avg_shift_hours,
                   ROUND(COALESCE(SUM(ds.total_km),0),1) as total_km,
                   COUNT(DISTINCT dl.id) as deliveries_completed
            FROM users u
            JOIN driver_shifts ds ON ds.driver_id=u.id AND ds.shift_date >= ?
            LEFT JOIN delivery_run_costs drc ON drc.driver_shift_id=ds.id
            LEFT JOIN delivery_log dl ON dl.id=drc.delivery_log_id AND dl.status IN ('delivered','collected')
            WHERE u.role IN ('driver','dispatch')
            GROUP BY u.id, u.full_name
            ORDER BY deliveries_completed DESC
        """, [cutoff]).fetchall())

        # Average stage times across all drivers
        stage_averages = rows_to_list(conn.execute("""
            SELECT stage,
                   ROUND(AVG(duration_minutes),1) as avg_minutes,
                   ROUND(MIN(duration_minutes),1) as min_minutes,
                   ROUND(MAX(duration_minutes),1) as max_minutes,
                   COUNT(*) as sample_count
            FROM delivery_run_stages
            WHERE ended_at IS NOT NULL AND duration_minutes > 0 AND duration_minutes < 480
            AND DATE(started_at) >= ?
            GROUP BY stage
            ORDER BY stage
        """, [cutoff]).fetchall())

        # Per-driver stage breakdown
        driver_stage_times = rows_to_list(conn.execute("""
            SELECT u.full_name as driver_name, drs.stage,
                   ROUND(AVG(drs.duration_minutes),1) as avg_minutes,
                   COUNT(*) as count
            FROM delivery_run_stages drs
            JOIN driver_shifts ds ON ds.id=drs.driver_shift_id
            JOIN users u ON u.id=ds.driver_id
            WHERE drs.ended_at IS NOT NULL AND drs.duration_minutes > 0 AND drs.duration_minutes < 480
            AND DATE(drs.started_at) >= ?
            GROUP BY u.full_name, drs.stage
            ORDER BY u.full_name, drs.stage
        """, [cutoff]).fetchall())

        # Per-client average unloading time
        client_unload = rows_to_list(conn.execute("""
            SELECT c.company_name,
                   ROUND(AVG(drs.duration_minutes),1) as avg_unload_minutes,
                   COUNT(*) as sample_count
            FROM delivery_run_stages drs
            JOIN delivery_log dl ON dl.id=drs.delivery_log_id
            JOIN orders o ON o.id=dl.order_id
            JOIN clients c ON c.id=o.client_id
            WHERE drs.stage='being_unloaded' AND drs.ended_at IS NOT NULL
            AND drs.duration_minutes > 0 AND drs.duration_minutes < 480
            AND DATE(drs.started_at) >= ?
            GROUP BY c.company_name
            ORDER BY avg_unload_minutes DESC
            LIMIT 15
        """, [cutoff]).fetchall())

        # Route averages (drive time by client)
        route_times = rows_to_list(conn.execute("""
            SELECT c.company_name,
                   ROUND(AVG(drs.duration_minutes),1) as avg_drive_minutes,
                   COUNT(*) as trip_count
            FROM delivery_run_stages drs
            JOIN delivery_log dl ON dl.id=drs.delivery_log_id
            JOIN orders o ON o.id=dl.order_id
            JOIN clients c ON c.id=o.client_id
            WHERE drs.stage='driving_to_customer' AND drs.ended_at IS NOT NULL
            AND drs.duration_minutes > 0 AND drs.duration_minutes < 480
            AND DATE(drs.started_at) >= ?
            GROUP BY c.company_name
            ORDER BY avg_drive_minutes DESC
            LIMIT 15
        """, [cutoff]).fetchall())

        return {"status": 200, "body": {
            "period": period,
            "driver_performance": driver_perf,
            "stage_averages": stage_averages,
            "driver_stage_times": driver_stage_times,
            "client_unload_times": client_unload,
            "route_times": route_times
        }}

    # ----- ADMIN CASCADE VALIDATION -----
    if method == "GET" and path == "/admin/cascade-check":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        if current_user["role"] not in ("executive", "office"):
            return {"status": 403, "body": {"error": "Admin access required"}}
        # Check for orphaned references
        orphaned_stations = rows_to_list(conn.execute("""
            SELECT s.id, s.name, s.zone_id FROM stations s
            LEFT JOIN zones z ON z.id=s.zone_id
            WHERE z.id IS NULL OR z.is_active=0
        """).fetchall())
        orphaned_schedules = rows_to_list(conn.execute("""
            SELECT se.id, se.scheduled_date, se.station_id FROM schedule_entries se
            LEFT JOIN stations s ON s.id=se.station_id
            WHERE s.id IS NULL OR s.is_active=0
        """).fetchall())
        orphaned_capacity = rows_to_list(conn.execute("""
            SELECT sc.id, sc.station_id FROM station_capacity sc
            LEFT JOIN stations s ON s.id=sc.station_id
            WHERE s.id IS NULL OR s.is_active=0
        """).fetchall())
        return {"status": 200, "body": {
            "orphaned_stations": orphaned_stations,
            "orphaned_schedules": orphaned_schedules,
            "orphaned_capacity": orphaned_capacity,
            "total_issues": len(orphaned_stations) + len(orphaned_schedules) + len(orphaned_capacity)
        }}

    # ----- ADMIN CASCADE FIX -----
    if method == "POST" and path == "/admin/cascade-fix":
        if not current_user or current_user["role"] not in ("executive", "office"):
            return {"status": 403, "body": {"error": "Admin access required"}}
        fixed = 0
        # Deactivate stations with inactive zones
        r = conn.execute("""UPDATE stations SET is_active=0 WHERE zone_id IN (SELECT id FROM zones WHERE is_active=0)""")
        fixed += r.rowcount
        # Delete schedule entries for inactive stations (either assigned or planned)
        r = conn.execute("""DELETE FROM schedule_entries WHERE station_id IN (SELECT id FROM stations WHERE is_active=0) OR planned_station_id IN (SELECT id FROM stations WHERE is_active=0)""")
        fixed += r.rowcount
        # Delete capacity for inactive stations
        r = conn.execute("""DELETE FROM station_capacity WHERE station_id IN (SELECT id FROM stations WHERE is_active=0)""")
        fixed += r.rowcount
        conn.commit()
        return {"status": 200, "body": {"fixed": fixed, "message": f"Cleaned up {fixed} orphaned records"}}


    # ===== TIMBER INVENTORY MODULE (Block 5) =====

    # Role helpers
    def _is_exec(u):
        return u and u.get("role") in ("executive", "office")

    def _is_planner(u):
        return u and u.get("role") in ("planner", "production_manager", "executive", "office")

    def _is_yardsman(u):
        return u and u.get("role") in ("yardsman", "floor_worker", "production_manager", "executive", "office")

    def _is_chainsaw(u):
        return u and u.get("role") in ("chainsaw_operator", "floor_worker", "executive", "office")

    def _cost_fields(u, pack):
        """Strip cost fields unless user is exec/office."""
        if not _is_exec(u):
            pack.pop("cost_per_m3", None)
            pack.pop("pack_cost_total", None)
        return pack

    # ----- SUPPLIERS -----

    if method == "GET" and path == "/timber/suppliers":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        current_user = get_current_user(conn)
        if not current_user:
            return {"status": 401, "body": {"error": "Unauthorized"}}
        status_filter = params.get("status")
        if status_filter == "pending":
            rows = rows_to_list(conn.execute(
                "SELECT * FROM timber_supplier_approvals ORDER BY requested_at DESC"
            ).fetchall())
            return {"status": 200, "body": rows}
        rows = rows_to_list(conn.execute(
            "SELECT * FROM timber_suppliers WHERE is_active=1 ORDER BY name"
        ).fetchall())
        return {"status": 200, "body": rows}

    if method == "POST" and path == "/timber/suppliers":
        current_user = get_current_user(conn)
        if not current_user:
            return {"status": 401, "body": {"error": "Unauthorized"}}
        name = body.get("name", "").strip()
        if not name:
            return {"status": 400, "body": {"error": "name required"}}
        # Yardsman can create pending suppliers; exec creates approved
        if body.get("status") == "pending" or body.get("requested_by") == "yardsman":
            cur = conn.execute(
                """INSERT INTO timber_supplier_approvals
                   (supplier_name, requested_by, status)
                   VALUES (?,?,?)""",
                [name, current_user.get("username", ""), "pending"]
            )
            conn.commit()
            row = row_to_dict(conn.execute(
                "SELECT * FROM timber_supplier_approvals WHERE id=?", [cur.lastrowid]
            ).fetchone())
            return {"status": 201, "body": row}
        if not _is_exec(current_user):
            return {"status": 403, "body": {"error": "Executive role required"}}
        cur = conn.execute(
            """INSERT INTO timber_suppliers
               (name, abn, contact_name, contact_email, contact_phone,
                default_terms, approval_status, created_by)
               VALUES (?,?,?,?,?,?,?,?)""",
            [name, body.get("abn"), body.get("contact_name"),
             body.get("contact_email"), body.get("contact_phone"),
             body.get("default_terms"), "approved",
             current_user.get("username")]
        )
        conn.commit()
        log_audit(conn, current_user["id"], "CREATE_TIMBER_SUPPLIER",
                  "timber_suppliers", cur.lastrowid, None, {"name": name})
        row = row_to_dict(conn.execute(
            "SELECT * FROM timber_suppliers WHERE id=?", [cur.lastrowid]
        ).fetchone())
        return {"status": 201, "body": row}

    m = match("/timber/suppliers/:id", path)
    if m and method == "PUT":
        current_user = get_current_user(conn)
        if not _is_exec(current_user):
            return {"status": 403, "body": {"error": "Executive role required"}}
        sid = int(m["id"])
        existing = row_to_dict(conn.execute(
            "SELECT * FROM timber_suppliers WHERE id=?", [sid]
        ).fetchone())
        if not existing:
            return {"status": 404, "body": {"error": "Supplier not found"}}
        fields = ["name", "abn", "contact_name", "contact_email",
                  "contact_phone", "default_terms", "is_active", "approval_status"]
        updates = {f: body[f] for f in fields if f in body}
        if updates:
            sets = ", ".join(f"{k}=?" for k in updates)
            conn.execute(
                f"UPDATE timber_suppliers SET {sets} WHERE id=?",
                list(updates.values()) + [sid]
            )
            conn.commit()
            log_audit(conn, current_user["id"], "UPDATE_TIMBER_SUPPLIER",
                      "timber_suppliers", sid, existing, updates)
        row = row_to_dict(conn.execute(
            "SELECT * FROM timber_suppliers WHERE id=?", [sid]
        ).fetchone())
        return {"status": 200, "body": row}


    m = match("/timber/suppliers/:id/approve", path)
    if m and method == "POST":
        current_user = get_current_user(conn)
        if not _is_exec(current_user):
            return {"status": 403, "body": {"error": "Executive role required"}}
        aid = int(m["id"])
        approval = row_to_dict(conn.execute(
            "SELECT * FROM timber_supplier_approvals WHERE id=?", [aid]
        ).fetchone())
        if not approval:
            return {"status": 404, "body": {"error": "Approval not found"}}
        conn.execute(
            "UPDATE timber_supplier_approvals SET status='approved', approved_by=?, approved_at=CURRENT_TIMESTAMP WHERE id=?",
            [current_user.get("username"), aid]
        )
        cur2 = conn.execute(
            """INSERT INTO timber_suppliers
               (name, abn, contact_name, contact_email, contact_phone,
                approval_status, created_by)
               VALUES (?,?,?,?,?,'approved',?)""",
            [approval["supplier_name"], approval.get("abn"),
             approval.get("contact_name"), approval.get("contact_email"),
             approval.get("contact_phone"), current_user.get("username")]
        )
        conn.commit()
        log_audit(conn, current_user["id"], "APPROVE_TIMBER_SUPPLIER",
                  "timber_suppliers", cur2.lastrowid, None, approval)
        return {"status": 200, "body": {"message": "Supplier approved and created", "supplier_id": cur2.lastrowid}}

    m = match("/timber/suppliers/:id/reject", path)
    if m and method == "POST":
        current_user = get_current_user(conn)
        if not _is_exec(current_user):
            return {"status": 403, "body": {"error": "Executive role required"}}
        aid = int(m["id"])
        conn.execute(
            "UPDATE timber_supplier_approvals SET status='rejected', approved_by=?, approved_at=CURRENT_TIMESTAMP WHERE id=?",
            [current_user.get("username"), aid]
        )
        conn.commit()
        return {"status": 200, "body": {"message": "Approval rejected"}}

    # ----- SUPPLIER APPROVALS -----

    if method == "POST" and path == "/timber/supplier-approvals":
        current_user = get_current_user(conn)
        if not _is_yardsman(current_user):
            return {"status": 403, "body": {"error": "Yardsman role required"}}
        name = body.get("supplier_name", "").strip()
        if not name:
            return {"status": 400, "body": {"error": "supplier_name required"}}
        cur = conn.execute(
            """INSERT INTO timber_supplier_approvals
               (supplier_name, abn, contact_name, contact_email, contact_phone, requested_by)
               VALUES (?,?,?,?,?,?)""",
            [name, body.get("abn"), body.get("contact_name"),
             body.get("contact_email"), body.get("contact_phone"),
             current_user.get("username", "")]
        )
        conn.commit()
        return {"status": 201, "body": {"id": cur.lastrowid, "message": "Approval request submitted"}}

    if method == "GET" and path == "/timber/supplier-approvals":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        current_user = get_current_user(conn)
        if not _is_exec(current_user):
            return {"status": 403, "body": {"error": "Executive role required"}}
        rows = rows_to_list(conn.execute(
            "SELECT * FROM timber_supplier_approvals ORDER BY requested_at DESC"
        ).fetchall())
        return {"status": 200, "body": {"approvals": rows}}

    m = match("/timber/supplier-approvals/:id/approve", path)
    if m and method == "POST":
        current_user = get_current_user(conn)
        if not _is_exec(current_user):
            return {"status": 403, "body": {"error": "Executive role required"}}
        aid = int(m["id"])
        approval = row_to_dict(conn.execute(
            "SELECT * FROM timber_supplier_approvals WHERE id=?", [aid]
        ).fetchone())
        if not approval:
            return {"status": 404, "body": {"error": "Approval not found"}}
        conn.execute(
            "UPDATE timber_supplier_approvals SET status='approved', approved_by=?, approved_at=CURRENT_TIMESTAMP WHERE id=?",
            [current_user.get("username"), aid]
        )
        # Create supplier record
        cur2 = conn.execute(
            """INSERT INTO timber_suppliers
               (name, abn, contact_name, contact_email, contact_phone,
                approval_status, created_by)
               VALUES (?,?,?,?,?,'approved',?)""",
            [approval["supplier_name"], approval.get("abn"),
             approval.get("contact_name"), approval.get("contact_email"),
             approval.get("contact_phone"), current_user.get("username")]
        )
        conn.commit()
        log_audit(conn, current_user["id"], "APPROVE_TIMBER_SUPPLIER",
                  "timber_suppliers", cur2.lastrowid, None, approval)
        return {"status": 200, "body": {"message": "Supplier approved and created", "supplier_id": cur2.lastrowid}}

    m = match("/timber/supplier-approvals/:id/reject", path)
    if m and method == "POST":
        current_user = get_current_user(conn)
        if not _is_exec(current_user):
            return {"status": 403, "body": {"error": "Executive role required"}}
        aid = int(m["id"])
        conn.execute(
            "UPDATE timber_supplier_approvals SET status='rejected', approved_by=?, approved_at=CURRENT_TIMESTAMP WHERE id=?",
            [current_user.get("username"), aid]
        )
        conn.commit()
        return {"status": 200, "body": {"message": "Approval rejected"}}

    # ----- SPECS -----

    if method == "GET" and path == "/timber/specs":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        current_user = get_current_user(conn)
        if not current_user:
            return {"status": 401, "body": {"error": "Unauthorized"}}
        qry = "SELECT * FROM timber_specs WHERE is_active=1"
        args = []
        tp = params.get("type_prefix")
        if tp:
            qry += " AND type_prefix=?"
            args.append(tp)
        grade = params.get("grade")
        if grade:
            qry += " AND grade_codes LIKE ?"
            args.append(f"%{grade}%")
        qry += " ORDER BY type_prefix, width_mm, thickness_mm, length_mm"
        rows = rows_to_list(conn.execute(qry, args).fetchall())
        return {"status": 200, "body": rows}

    if method == "POST" and path == "/timber/specs":
        current_user = get_current_user(conn)
        if not _is_exec(current_user):
            return {"status": 403, "body": {"error": "Executive role required"}}
        myob_code = body.get("myob_code", "").strip()
        desc = body.get("description", "").strip()
        type_prefix = body.get("type_prefix", "").strip()
        if not desc or not type_prefix:
            return {"status": 400, "body": {"error": "description and type_prefix required"}}
        # Auto-parse if myob_code provided
        parsed = parse_myob_code(myob_code) if myob_code else {}
        cur = conn.execute(
            """INSERT INTO timber_specs
               (myob_code, type_prefix, grade_codes, width_mm, thickness_mm,
                length_mm, suffix_flags, description)
               VALUES (?,?,?,?,?,?,?,?)""",
            [myob_code or None,
             type_prefix or parsed.get("type_prefix"),
             body.get("grade_codes") or parsed.get("grade_codes"),
             body.get("width_mm") or parsed.get("width_mm"),
             body.get("thickness_mm") or parsed.get("thickness_mm"),
             body.get("length_mm") or parsed.get("length_mm"),
             body.get("suffix_flags") or parsed.get("suffix_flags"),
             desc]
        )
        conn.commit()
        log_audit(conn, current_user["id"], "CREATE_TIMBER_SPEC",
                  "timber_specs", cur.lastrowid, None, {"myob_code": myob_code})
        row = row_to_dict(conn.execute(
            "SELECT * FROM timber_specs WHERE id=?", [cur.lastrowid]
        ).fetchone())
        return {"status": 201, "body": row}

    m = match("/timber/specs/:id", path)
    if m and method == "PUT":
        current_user = get_current_user(conn)
        if not _is_exec(current_user):
            return {"status": 403, "body": {"error": "Executive role required"}}
        sid = int(m["id"])
        existing = row_to_dict(conn.execute(
            "SELECT * FROM timber_specs WHERE id=?", [sid]
        ).fetchone())
        if not existing:
            return {"status": 404, "body": {"error": "Spec not found"}}
        fields = ["myob_code", "type_prefix", "grade_codes", "width_mm",
                  "thickness_mm", "length_mm", "suffix_flags", "description", "is_active"]
        updates = {f: body[f] for f in fields if f in body}
        if updates:
            sets = ", ".join(f"{k}=?" for k in updates)
            conn.execute(
                f"UPDATE timber_specs SET {sets} WHERE id=?",
                list(updates.values()) + [sid]
            )
            conn.commit()
        row = row_to_dict(conn.execute(
            "SELECT * FROM timber_specs WHERE id=?", [sid]
        ).fetchone())
        return {"status": 200, "body": row}

    # ----- GRADES -----

    if method == "GET" and path in ("/timber/grades", "/timber/grade-codes"):
        current_user = get_current_user(conn)
        if not current_user:
            return {"status": 401, "body": {"error": "Unauthorized"}}
        rows = rows_to_list(conn.execute(
            "SELECT * FROM timber_grade_codes WHERE is_active=1 ORDER BY code"
        ).fetchall())
        return {"status": 200, "body": rows}

    if method == "POST" and path in ("/timber/grades", "/timber/grade-codes"):
        current_user = get_current_user(conn)
        if not _is_exec(current_user):
            return {"status": 403, "body": {"error": "Executive role required"}}
        code = body.get("code", "").strip().upper()
        full_name = body.get("full_name", "").strip()
        if not code or not full_name:
            return {"status": 400, "body": {"error": "code and full_name required"}}
        cur = conn.execute(
            "INSERT INTO timber_grade_codes (code, full_name, description) VALUES (?,?,?)",
            [code, full_name, body.get("description")]
        )
        conn.commit()
        row = row_to_dict(conn.execute(
            "SELECT * FROM timber_grade_codes WHERE id=?", [cur.lastrowid]
        ).fetchone())
        return {"status": 201, "body": row}

    m = match("/timber/grades/:id", path)
    if m and method == "PUT":
        current_user = get_current_user(conn)
        if not _is_exec(current_user):
            return {"status": 403, "body": {"error": "Executive role required"}}
        gid = int(m["id"])
        fields = ["code", "full_name", "description", "is_active"]
        updates = {f: body[f] for f in fields if f in body}
        if updates:
            sets = ", ".join(f"{k}=?" for k in updates)
            conn.execute(
                f"UPDATE timber_grade_codes SET {sets} WHERE id=?",
                list(updates.values()) + [gid]
            )
            conn.commit()
        row = row_to_dict(conn.execute(
            "SELECT * FROM timber_grade_codes WHERE id=?", [gid]
        ).fetchone())
        return {"status": 200, "body": row}

    # ----- CONFIG -----

    if method == "GET" and path == "/timber/config":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        current_user = get_current_user(conn)
        if not current_user:
            return {"status": 401, "body": {"error": "Unauthorized"}}
        rows = rows_to_list(conn.execute(
            "SELECT * FROM timber_config ORDER BY key"
        ).fetchall())
        cfg = {r["key"]: r["value"] for r in rows}
        return {"status": 200, "body": cfg}

    if method == "PUT" and path == "/timber/config":
        current_user = get_current_user(conn)
        if not _is_exec(current_user):
            return {"status": 403, "body": {"error": "Executive role required"}}
        updates = body.get("updates", {})
        for key, val in updates.items():
            conn.execute(
                "INSERT INTO timber_config (key, value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                [key, str(val)]
            )
        conn.commit()
        rows = rows_to_list(conn.execute("SELECT * FROM timber_config ORDER BY key").fetchall())
        return {"status": 200, "body": {"config": rows}}

    # ----- DELIVERIES -----

    if method == "POST" and path == "/timber/deliveries":
        current_user = get_current_user(conn)
        if not _is_yardsman(current_user):
            return {"status": 403, "body": {"error": "Yardsman role required"}}
        supplier_id = body.get("supplier_id")
        delivery_date = body.get("delivery_date") or datetime.now(timezone.utc).strftime("%Y-%m-%d")
        cur = conn.execute(
            """INSERT INTO timber_deliveries
               (supplier_id, delivery_date, docket_number, docket_photo_path,
                ocr_raw_text, notes, created_by, status)
               VALUES (?,?,?,?,?,?,?,?)""",
            [supplier_id, delivery_date, body.get("docket_number"),
             body.get("docket_photo_path"), body.get("ocr_raw_text"),
             body.get("notes"), current_user.get("username"), "pending"]
        )
        conn.commit()
        did = cur.lastrowid
        log_audit(conn, current_user["id"], "CREATE_TIMBER_DELIVERY",
                  "timber_deliveries", did, None, body)
        row = row_to_dict(conn.execute(
            "SELECT td.*, ts.name as supplier_name FROM timber_deliveries td "
            "LEFT JOIN timber_suppliers ts ON ts.id=td.supplier_id WHERE td.id=?", [did]
        ).fetchone())
        return {"status": 201, "body": row}

    if method == "GET" and path == "/timber/deliveries":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        current_user = get_current_user(conn)
        if not current_user:
            return {"status": 401, "body": {"error": "Unauthorized"}}
        qry = """SELECT td.*, ts.name as supplier_name
                 FROM timber_deliveries td
                 LEFT JOIN timber_suppliers ts ON ts.id=td.supplier_id"""
        args = []
        status_filter = params.get("status")
        if status_filter:
            statuses = [s.strip() for s in status_filter.split(",")]
            placeholders = ",".join("?" for _ in statuses)
            qry += f" WHERE td.status IN ({placeholders})"
            args.extend(statuses)
        qry += " ORDER BY td.created_at DESC"
        rows = rows_to_list(conn.execute(qry, args).fetchall())
        return {"status": 200, "body": rows}

    m = match("/timber/deliveries/:id", path)
    if m and method == "GET":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        current_user = get_current_user(conn)
        if not current_user:
            return {"status": 401, "body": {"error": "Unauthorized"}}
        did = int(m["id"])
        delivery = row_to_dict(conn.execute(
            "SELECT td.*, ts.name as supplier_name FROM timber_deliveries td "
            "LEFT JOIN timber_suppliers ts ON ts.id=td.supplier_id WHERE td.id=?", [did]
        ).fetchone())
        if not delivery:
            return {"status": 404, "body": {"error": "Delivery not found"}}
        items = rows_to_list(conn.execute(
            """SELECT tdi.*, tsp.description as spec_description, tsp.myob_code
               FROM timber_delivery_items tdi
               LEFT JOIN timber_specs tsp ON tsp.id=tdi.spec_id
               WHERE tdi.delivery_id=? ORDER BY tdi.id""",
            [did]
        ).fetchall())
        delivery["items"] = items
        return {"status": 200, "body": delivery}

    m = match("/timber/deliveries/:id", path)
    if m and method == "PUT":
        current_user = get_current_user(conn)
        if not _is_yardsman(current_user):
            return {"status": 403, "body": {"error": "Yardsman role required"}}
        did = int(m["id"])
        fields = ["supplier_id", "delivery_date", "docket_number",
                  "docket_photo_path", "ocr_raw_text", "status", "notes"]
        updates = {f: body[f] for f in fields if f in body}
        if updates:
            sets = ", ".join(f"{k}=?" for k in updates)
            conn.execute(
                f"UPDATE timber_deliveries SET {sets} WHERE id=?",
                list(updates.values()) + [did]
            )
            conn.commit()
        row = row_to_dict(conn.execute(
            "SELECT * FROM timber_deliveries WHERE id=?", [did]
        ).fetchone())
        return {"status": 200, "body": row}

    m = match("/timber/deliveries/:id/complete", path)
    if m and method == "POST":
        current_user = get_current_user(conn)
        if not _is_yardsman(current_user):
            return {"status": 403, "body": {"error": "Yardsman role required"}}
        did = int(m["id"])
        conn.execute(
            "UPDATE timber_deliveries SET status='completed', completed_at=CURRENT_TIMESTAMP WHERE id=?",
            [did]
        )
        conn.commit()
        log_audit(conn, current_user["id"], "COMPLETE_TIMBER_DELIVERY",
                  "timber_deliveries", did, None, {"status": "completed"})
        return {"status": 200, "body": {"message": "Delivery completed"}}


    m = match("/timber/deliveries/:id/items", path)
    if m and method == "GET":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        current_user = get_current_user(conn)
        if not current_user:
            return {"status": 401, "body": {"error": "Unauthorized"}}
        did = int(m["id"])
        items = rows_to_list(conn.execute(
            """SELECT tdi.*, tsp.description as spec_description, tsp.myob_code
               FROM timber_delivery_items tdi
               LEFT JOIN timber_specs tsp ON tsp.id=tdi.spec_id
               WHERE tdi.delivery_id=? ORDER BY tdi.id""",
            [did]
        ).fetchall())
        return {"status": 200, "body": items}

    # ----- DELIVERY ITEMS -----

    if method == "POST" and path == "/timber/delivery-items":
        current_user = get_current_user(conn)
        if not _is_yardsman(current_user):
            return {"status": 403, "body": {"error": "Yardsman role required"}}
        delivery_id = body.get("delivery_id")
        if not delivery_id:
            return {"status": 400, "body": {"error": "delivery_id required"}}
        cur = conn.execute(
            """INSERT INTO timber_delivery_items
               (delivery_id, spec_id, description, expected_packs,
                pcs_per_pack, cost_per_m3, total_amount, lineal_metres_per_pack)
               VALUES (?,?,?,?,?,?,?,?)""",
            [delivery_id, body.get("spec_id"), body.get("description"),
             body.get("expected_packs", 0), body.get("pcs_per_pack"),
             body.get("cost_per_m3"), body.get("total_amount"),
             body.get("lineal_metres_per_pack")]
        )
        conn.commit()
        row = row_to_dict(conn.execute(
            "SELECT * FROM timber_delivery_items WHERE id=?", [cur.lastrowid]
        ).fetchone())
        return {"status": 201, "body": row}

    m = match("/timber/delivery-items/:id", path)
    if m and method == "PUT":
        current_user = get_current_user(conn)
        if not _is_yardsman(current_user):
            return {"status": 403, "body": {"error": "Yardsman role required"}}
        iid = int(m["id"])
        fields = ["spec_id", "description", "expected_packs", "assigned_packs",
                  "pcs_per_pack", "cost_per_m3", "total_amount",
                  "lineal_metres_per_pack", "status"]
        updates = {f: body[f] for f in fields if f in body}
        if updates:
            sets = ", ".join(f"{k}=?" for k in updates)
            conn.execute(
                f"UPDATE timber_delivery_items SET {sets} WHERE id=?",
                list(updates.values()) + [iid]
            )
            conn.commit()
        row = row_to_dict(conn.execute(
            "SELECT * FROM timber_delivery_items WHERE id=?", [iid]
        ).fetchone())
        return {"status": 200, "body": row}

    m = match("/timber/delivery-items/:id", path)
    if m and method == "DELETE":
        current_user = get_current_user(conn)
        if not _is_exec(current_user):
            return {"status": 403, "body": {"error": "Executive role required"}}
        iid = int(m["id"])
        conn.execute("DELETE FROM timber_delivery_items WHERE id=?", [iid])
        conn.commit()
        return {"status": 200, "body": {"message": "Delivery item deleted"}}

    # ----- PACKS (CREATE) -----

    if method == "POST" and path == "/timber/packs":
        current_user = get_current_user(conn)
        if not _is_yardsman(current_user):
            return {"status": 403, "body": {"error": "Yardsman role required"}}
        spec_id = body.get("spec_id")
        supplier_id = body.get("supplier_id")
        m3_volume = body.get("m3_volume")
        if not spec_id or not supplier_id or m3_volume is None:
            return {"status": 400, "body": {"error": "spec_id, supplier_id, m3_volume required"}}
        # Auto-generate QR code
        qr = body.get("qr_code")
        if not qr:
            prefix_row = conn.execute(
                "SELECT value FROM timber_config WHERE key='qr_prefix'"
            ).fetchone()
            seq_row = conn.execute(
                "SELECT value FROM timber_config WHERE key='qr_sequence_start'"
            ).fetchone()
            prefix = prefix_row[0] if prefix_row else "EP-"
            seq = int(seq_row[0]) if seq_row else 1
            qr = f"{prefix}{seq:05d}"
            conn.execute(
                "UPDATE timber_config SET value=? WHERE key='qr_sequence_start'",
                [str(seq + 1)]
            )
        received_date = body.get("received_date") or datetime.now(timezone.utc).strftime("%Y-%m-%d")
        pack_cost = None
        cost_per_m3 = body.get("cost_per_m3")
        if cost_per_m3:
            pack_cost = round(float(cost_per_m3) * float(m3_volume), 2)
        cur = conn.execute(
            """INSERT INTO timber_packs
               (qr_code, delivery_item_id, spec_id, supplier_id,
                received_date, received_by, pcs_per_pack, m3_volume,
                lineal_metres, cost_per_m3, pack_cost_total, status,
                pack_type, is_test_data)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            [qr, body.get("delivery_item_id"), spec_id, supplier_id,
             received_date, current_user.get("username"),
             body.get("pcs_per_pack"), float(m3_volume),
             body.get("lineal_metres"), cost_per_m3, pack_cost,
             body.get("status", "inventory"),
             body.get("pack_type", "full"),
             body.get("is_test_data", 0)]
        )
        conn.commit()
        pid = cur.lastrowid
        log_audit(conn, current_user["id"], "CREATE_TIMBER_PACK",
                  "timber_packs", pid, None, {"qr_code": qr})
        row = row_to_dict(conn.execute(
            "SELECT * FROM timber_packs WHERE id=?", [pid]
        ).fetchone())
        _cost_fields(current_user, row)
        return {"status": 201, "body": row}


    if method == "GET" and path == "/timber/packs":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        current_user = get_current_user(conn)
        if not current_user:
            return {"status": 401, "body": {"error": "Unauthorized"}}
        qry = """SELECT tp.*, ts.description as spec_description, ts.myob_code,
                        ts.type_prefix, ts.width_mm, ts.thickness_mm, ts.length_mm,
                        tsu.name as supplier_name
                 FROM timber_packs tp
                 LEFT JOIN timber_specs ts ON ts.id=tp.spec_id
                 LEFT JOIN timber_suppliers tsu ON tsu.id=tp.supplier_id
                 WHERE 1=1"""
        args = []
        if params.get("supplier_id"):
            qry += " AND tp.supplier_id=?"
            args.append(params["supplier_id"])
        if params.get("spec_id"):
            qry += " AND tp.spec_id=?"
            args.append(params["spec_id"])
        status_f = params.get("status", "inventory")
        qry += " AND tp.status=?"
        args.append(status_f)
        qry += " ORDER BY tp.received_date ASC, tp.id ASC"
        rows = rows_to_list(conn.execute(qry, args).fetchall())
        show_cost = _is_exec(current_user)
        if not show_cost:
            for r in rows:
                r.pop("cost_per_m3", None)
                r.pop("pack_cost_total", None)
        return {"status": 200, "body": rows}

    # ----- INVENTORY -----

    if method == "GET" and path == "/timber/inventory":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        current_user = get_current_user(conn)
        if not current_user:
            return {"status": 401, "body": {"error": "Unauthorized"}}
        qry = """SELECT tp.*, ts.description as spec_description, ts.myob_code,
                        ts.type_prefix, ts.width_mm, ts.thickness_mm, ts.length_mm,
                        tsu.name as supplier_name
                 FROM timber_packs tp
                 LEFT JOIN timber_specs ts ON ts.id=tp.spec_id
                 LEFT JOIN timber_suppliers tsu ON tsu.id=tp.supplier_id
                 WHERE 1=1"""
        args = []
        if params.get("supplier_id"):
            qry += " AND tp.supplier_id=?"
            args.append(params["supplier_id"])
        if params.get("spec_id"):
            qry += " AND tp.spec_id=?"
            args.append(params["spec_id"])
        if params.get("type_prefix"):
            qry += " AND ts.type_prefix=?"
            args.append(params["type_prefix"])
        status_f = params.get("status", "inventory")
        qry += " AND tp.status=?"
        args.append(status_f)
        if params.get("search"):
            qry += " AND (tp.qr_code LIKE ? OR ts.myob_code LIKE ? OR ts.description LIKE ?)"
            s = f"%{params['search']}%"
            args.extend([s, s, s])
        qry += " ORDER BY tp.received_date ASC, tp.id ASC"
        rows = rows_to_list(conn.execute(qry, args).fetchall())
        show_cost = _is_exec(current_user)
        result = []
        for r in rows:
            if not show_cost:
                r.pop("cost_per_m3", None)
                r.pop("pack_cost_total", None)
            result.append(r)
        return {"status": 200, "body": result}


    if method == "GET" and path == "/timber/summary":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        current_user = get_current_user(conn)
        if not current_user:
            return {"status": 401, "body": {"error": "Unauthorized"}}
        summary_row = conn.execute(
            """SELECT COUNT(*) as pack_count,
                      COALESCE(SUM(m3_volume),0) as total_m3,
                      COALESCE(SUM(pack_cost_total),0) as total_value
               FROM timber_packs WHERE status='inventory'"""
        ).fetchone()
        summary = dict(summary_row)
        if not _is_exec(current_user):
            summary.pop("total_value", None)
        by_type = rows_to_list(conn.execute(
            """SELECT ts.type_prefix,
                      COUNT(tp.id) as pack_count,
                      COALESCE(SUM(tp.m3_volume),0) as total_m3
               FROM timber_packs tp
               LEFT JOIN timber_specs ts ON ts.id=tp.spec_id
               WHERE tp.status='inventory'
               GROUP BY ts.type_prefix ORDER BY ts.type_prefix"""
        ).fetchall())
        summary["by_type"] = by_type
        return {"status": 200, "body": summary}

    if method == "GET" and path == "/timber/inventory/summary":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        current_user = get_current_user(conn)
        if not current_user:
            return {"status": 401, "body": {"error": "Unauthorized"}}
        summary_row = conn.execute(
            """SELECT COUNT(*) as pack_count,
                      COALESCE(SUM(m3_volume),0) as total_m3,
                      COALESCE(SUM(pack_cost_total),0) as total_value
               FROM timber_packs WHERE status='inventory'"""
        ).fetchone()
        summary = dict(summary_row)
        if not _is_exec(current_user):
            summary.pop("total_value", None)
        # By type
        by_type = rows_to_list(conn.execute(
            """SELECT ts.type_prefix,
                      COUNT(tp.id) as pack_count,
                      COALESCE(SUM(tp.m3_volume),0) as total_m3
               FROM timber_packs tp
               LEFT JOIN timber_specs ts ON ts.id=tp.spec_id
               WHERE tp.status='inventory'
               GROUP BY ts.type_prefix ORDER BY ts.type_prefix"""
        ).fetchall())
        return {"status": 200, "body": {"summary": summary, "by_type": by_type}}

    m = match("/timber/packs/:qr", path)
    if m and method == "GET":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        current_user = get_current_user(conn)
        if not current_user:
            return {"status": 401, "body": {"error": "Unauthorized"}}
        qr = m["qr"]
        row = row_to_dict(conn.execute(
            """SELECT tp.*, ts.description as spec_description, ts.myob_code,
                      ts.type_prefix, ts.width_mm, ts.thickness_mm, ts.length_mm,
                      tsu.name as supplier_name
               FROM timber_packs tp
               LEFT JOIN timber_specs ts ON ts.id=tp.spec_id
               LEFT JOIN timber_suppliers tsu ON tsu.id=tp.supplier_id
               WHERE tp.qr_code=?""",
            [qr]
        ).fetchone())
        if not row:
            return {"status": 404, "body": {"error": "Pack not found"}}
        _cost_fields(current_user, row)
        return {"status": 200, "body": row}

    # ----- CONSUMPTION -----

    m = match("/timber/packs/:qr/consume", path)
    if m and method == "POST":
        current_user = get_current_user(conn)
        if not _is_chainsaw(current_user):
            return {"status": 403, "body": {"error": "Chainsaw or floor worker role required"}}
        qr = m["qr"]
        pack = row_to_dict(conn.execute(
            "SELECT * FROM timber_packs WHERE qr_code=?", [qr]
        ).fetchone())
        if not pack:
            return {"status": 404, "body": {"error": "Pack not found"}}
        if pack["status"] != "inventory":
            return {"status": 409, "body": {"error": f"Pack is not in inventory (status: {pack['status']})"}}
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "UPDATE timber_packs SET status='consumed', consumed_at=?, consumed_by=? WHERE id=?",
            [now, current_user.get("username"), pack["id"]]
        )
        cur_c = conn.execute(
            """INSERT INTO timber_consumption
               (pack_id, consumed_by, consumed_by_user_id, destination, destination_zone, notes)
               VALUES (?,?,?,?,?,?)""",
            [pack["id"], current_user.get("username"), current_user.get("id"),
             body.get("destination"), body.get("destination_zone"), body.get("notes")]
        )
        conn.commit()
        log_audit(conn, current_user["id"], "CONSUME_TIMBER_PACK",
                  "timber_packs", pack["id"], {"status": "inventory"}, {"status": "consumed"})
        # FIFO advisory check
        fifo_warning = None
        try:
            threshold_row = conn.execute(
                "SELECT value FROM timber_config WHERE key='fifo_threshold_days'"
            ).fetchone()
            threshold_days = int(threshold_row[0]) if threshold_row else 14
            oldest = conn.execute(
                """SELECT MIN(received_date) FROM timber_packs
                   WHERE spec_id=? AND status='inventory'""",
                [pack["spec_id"]]
            ).fetchone()
            if oldest and oldest[0]:
                from datetime import date as _date
                oldest_date = datetime.strptime(oldest[0], "%Y-%m-%d").date()
                consumed_date = datetime.strptime(pack["received_date"], "%Y-%m-%d").date() if pack.get("received_date") else _date.today()
                diff = (consumed_date - oldest_date).days
                if diff > threshold_days:
                    fifo_warning = f"FIFO advisory: older stock exists ({diff} days). Consume older packs first."
        except Exception:
            pass
        # Low stock check
        low_stock = _check_low_stock(conn, pack["spec_id"])
        return {"status": 200, "body": {
            "message": "Pack consumed",
            "consumption_id": cur_c.lastrowid,
            "fifo_warning": fifo_warning,
            "low_stock_alerts": low_stock,
        }}

    if method == "POST" and path == "/timber/packs/bulk-consume":
        current_user = get_current_user(conn)
        if not _is_chainsaw(current_user):
            return {"status": 403, "body": {"error": "Chainsaw or floor worker role required"}}
        qr_codes = body.get("qr_codes", [])
        if len(qr_codes) > 5:
            return {"status": 400, "body": {"error": f"Maximum 5 QR codes per bulk consume (received {len(qr_codes)})"}}
        results = []
        for qr in qr_codes:
            pack = row_to_dict(conn.execute(
                "SELECT * FROM timber_packs WHERE qr_code=?", [qr]
            ).fetchone())
            if not pack:
                results.append({"qr_code": qr, "success": False, "error": "Pack not found"})
                continue
            if pack["status"] != "inventory":
                results.append({"qr_code": qr, "success": False, "error": f"Not in inventory (status: {pack['status']})"})
                continue
            now = datetime.now(timezone.utc).isoformat()
            conn.execute(
                "UPDATE timber_packs SET status='consumed', consumed_at=?, consumed_by=? WHERE id=?",
                [now, current_user.get("username"), pack["id"]]
            )
            conn.execute(
                "INSERT INTO timber_consumption (pack_id, consumed_by, consumed_by_user_id) VALUES (?,?,?)",
                [pack["id"], current_user.get("username"), current_user.get("id")]
            )
            results.append({"qr_code": qr, "success": True})
        conn.commit()
        return {"status": 200, "body": {"results": results}}

    m = match("/timber/packs/:qr/yardsman-consume", path)
    if m and method == "POST":
        current_user = get_current_user(conn)
        if not _is_yardsman(current_user):
            return {"status": 403, "body": {"error": "Yardsman role required"}}
        qr = m["qr"]
        pack = row_to_dict(conn.execute(
            "SELECT * FROM timber_packs WHERE qr_code=?", [qr]
        ).fetchone())
        if not pack:
            return {"status": 404, "body": {"error": "Pack not found"}}
        if pack["status"] not in ("inventory",):
            return {"status": 409, "body": {"error": f"Pack not in inventory (status: {pack['status']})"}}
        destination = body.get("destination", "production")
        destination_zone = body.get("destination_zone")
        now = datetime.now(timezone.utc).isoformat()
        if destination == "chainsaw":
            conn.execute(
                "UPDATE timber_packs SET status='allocated_chainsaw', assigned_worker=?, destination_zone=? WHERE id=?",
                [current_user.get("username"), destination_zone, pack["id"]]
            )
            conn.execute(
                "INSERT INTO timber_chainsaw_allocations (pack_id, allocated_by) VALUES (?,?)",
                [pack["id"], current_user.get("username")]
            )
            conn.commit()
            log_audit(conn, current_user["id"], "ALLOCATE_CHAINSAW",
                      "timber_packs", pack["id"], {"status": "inventory"}, {"status": "allocated_chainsaw"})
            return {"status": 200, "body": {"message": "Pack allocated to chainsaw"}}
        else:
            # Direct consumption to production
            conn.execute(
                "UPDATE timber_packs SET status='consumed', consumed_at=?, consumed_by=?, destination_zone=? WHERE id=?",
                [now, current_user.get("username"), destination_zone, pack["id"]]
            )
            conn.execute(
                """INSERT INTO timber_consumption
                   (pack_id, consumed_by, consumed_by_user_id, destination, destination_zone, notes)
                   VALUES (?,?,?,?,?,?)""",
                [pack["id"], current_user.get("username"), current_user.get("id"),
                 "production", destination_zone, body.get("notes")]
            )
            conn.commit()
            log_audit(conn, current_user["id"], "YARDSMAN_CONSUME",
                      "timber_packs", pack["id"], {"status": "inventory"}, {"status": "consumed"})
            low_stock = _check_low_stock(conn, pack["spec_id"])
            return {"status": 200, "body": {"message": "Pack consumed to production", "low_stock_alerts": low_stock}}

    m = match("/timber/packs/:qr/undo-consume", path)
    if m and method == "POST":
        current_user = get_current_user(conn)
        if not current_user:
            return {"status": 401, "body": {"error": "Unauthorized"}}
        # Yardsman, chainsaw_operator, floor_worker can undo their own; exec can undo any
        allowed_roles = ("executive", "office", "yardsman", "floor_worker",
                         "chainsaw_operator", "production_manager")
        if current_user.get("role") not in allowed_roles:
            return {"status": 403, "body": {"error": "Insufficient role to undo consumption"}}
        qr = m["qr"]
        pack = row_to_dict(conn.execute(
            "SELECT * FROM timber_packs WHERE qr_code=?", [qr]
        ).fetchone())
        if not pack:
            return {"status": 404, "body": {"error": "Pack not found"}}
        if pack["status"] not in ("consumed", "allocated_chainsaw"):
            return {"status": 409, "body": {"error": f"Pack cannot be undone (status: {pack['status']})"}}
        # Non-exec: can only undo their own consumption
        if not _is_exec(current_user):
            own = conn.execute(
                "SELECT id FROM timber_consumption WHERE pack_id=? AND consumed_by=? ORDER BY consumed_at DESC LIMIT 1",
                [pack["id"], current_user.get("username")]
            ).fetchone()
            if not own:
                own_alloc = conn.execute(
                    "SELECT id FROM timber_chainsaw_allocations WHERE pack_id=? AND allocated_by=? ORDER BY allocated_at DESC LIMIT 1",
                    [pack["id"], current_user.get("username")]
                ).fetchone()
                if not own_alloc:
                    return {"status": 403, "body": {"error": "You can only undo your own consumption"}}
        # Get last consumption record
        last_consume = row_to_dict(conn.execute(
            "SELECT * FROM timber_consumption WHERE pack_id=? ORDER BY consumed_at DESC LIMIT 1",
            [pack["id"]]
        ).fetchone())
        orig_id = last_consume["id"] if last_consume else None
        # Restore pack
        conn.execute(
            "UPDATE timber_packs SET status='inventory', consumed_at=NULL, consumed_by=NULL, destination_zone=NULL WHERE id=?",
            [pack["id"]]
        )
        # Log undo
        conn.execute(
            """INSERT INTO timber_consumption_undo
               (pack_id, original_consumption_id, undone_by, undone_by_user_id, reason)
               VALUES (?,?,?,?,?)""",
            [pack["id"], orig_id, current_user.get("username"),
             current_user.get("id"), body.get("reason")]
        )
        conn.commit()
        log_audit(conn, current_user["id"], "UNDO_TIMBER_CONSUME",
                  "timber_packs", pack["id"],
                  {"status": pack["status"]}, {"status": "inventory"})
        return {"status": 200, "body": {"message": "Consumption undone", "pack_id": pack["id"]}}

    # ----- FINANCE -----

    if method == "POST" and path == "/timber/cost-imports":
        current_user = get_current_user(conn)
        if not _is_exec(current_user):
            return {"status": 403, "body": {"error": "Executive role required"}}
        file_name = body.get("file_name", "import.csv")
        period_month = body.get("period_month")
        period_year = body.get("period_year")
        rows_data = body.get("rows", [])
        cur_i = conn.execute(
            """INSERT INTO timber_cost_imports
               (imported_by, file_name, period_month, period_year,
                status, total_rows, matched_rows)
               VALUES (?,?,?,?,'processing',?,0)""",
            [current_user.get("username"), file_name,
             period_month, period_year, len(rows_data)]
        )
        conn.commit()
        import_id = cur_i.lastrowid
        matched = 0
        for row_d in rows_data:
            myob_code = row_d.get("myob_code") or row_d.get("Item/Acct", "")
            mapped_pack = conn.execute(
                """SELECT tp.id FROM timber_packs tp
                   JOIN timber_specs ts ON ts.id=tp.spec_id
                   WHERE ts.myob_code=? AND tp.status='inventory'
                   ORDER BY tp.received_date ASC LIMIT 1""",
                [myob_code]
            ).fetchone()
            mapped_id = mapped_pack[0] if mapped_pack else None
            if mapped_id:
                matched += 1
            conn.execute(
                """INSERT INTO timber_cost_import_items
                   (import_id, myob_code, supplier_name, date, quantity,
                    description, amount, tax, status_field, mapped_pack_id)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                [import_id, myob_code, row_d.get("supplier_name"),
                 row_d.get("date") or row_d.get("Date"),
                 row_d.get("quantity") or row_d.get("Quantity"),
                 row_d.get("description") or row_d.get("Description"),
                 row_d.get("amount") or row_d.get("Amount"),
                 row_d.get("tax") or row_d.get("Tax"),
                 row_d.get("status") or row_d.get("Status"),
                 mapped_id]
            )
        conn.execute(
            "UPDATE timber_cost_imports SET status='complete', matched_rows=? WHERE id=?",
            [matched, import_id]
        )
        conn.commit()
        return {"status": 201, "body": {
            "import_id": import_id,
            "total_rows": len(rows_data),
            "matched_rows": matched,
        }}

    if method == "GET" and path == "/timber/cost-imports":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        current_user = get_current_user(conn)
        if not _is_exec(current_user):
            return {"status": 403, "body": {"error": "Executive role required"}}
        rows = rows_to_list(conn.execute(
            "SELECT * FROM timber_cost_imports ORDER BY import_date DESC"
        ).fetchall())
        return {"status": 200, "body": rows}

    if method == "GET" and path == "/timber/valuation":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        current_user = get_current_user(conn)
        if not _is_exec(current_user):
            return {"status": 403, "body": {"error": "Executive role required"}}
        row = conn.execute(
            """SELECT COUNT(*) as pack_count,
                      COALESCE(SUM(m3_volume),0) as total_m3,
                      COALESCE(SUM(pack_cost_total),0) as total_value
               FROM timber_packs WHERE status='inventory'"""
        ).fetchone()
        by_spec = rows_to_list(conn.execute(
            """SELECT ts.myob_code, ts.description, ts.type_prefix,
                      COUNT(tp.id) as pack_count,
                      COALESCE(SUM(tp.m3_volume),0) as total_m3,
                      COALESCE(SUM(tp.pack_cost_total),0) as total_value,
                      COALESCE(AVG(tp.cost_per_m3),0) as avg_cost_per_m3
               FROM timber_packs tp
               LEFT JOIN timber_specs ts ON ts.id=tp.spec_id
               WHERE tp.status='inventory'
               GROUP BY tp.spec_id ORDER BY total_value DESC"""
        ).fetchall())
        return {"status": 200, "body": {
            "summary": dict(row),
            "by_spec": by_spec,
        }}

    # ----- STOCKTAKES -----

    if method == "POST" and path == "/timber/stocktakes":
        current_user = get_current_user(conn)
        if not _is_exec(current_user):
            return {"status": 403, "body": {"error": "Executive role required"}}
        stocktake_date = body.get("stocktake_date") or datetime.now(timezone.utc).strftime("%Y-%m-%d")
        cur = conn.execute(
            "INSERT INTO timber_stocktakes (stocktake_date, conducted_by) VALUES (?,?)",
            [stocktake_date, current_user.get("username")]
        )
        conn.commit()
        return {"status": 201, "body": {"stocktake_id": cur.lastrowid}}

    m = match("/timber/stocktakes/:id", path)
    if m and method == "GET":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        current_user = get_current_user(conn)
        if not current_user:
            return {"status": 401, "body": {"error": "Unauthorized"}}
        stid = int(m["id"])
        st = row_to_dict(conn.execute(
            "SELECT * FROM timber_stocktakes WHERE id=?", [stid]
        ).fetchone())
        if not st:
            return {"status": 404, "body": {"error": "Stocktake not found"}}
        counts = rows_to_list(conn.execute(
            """SELECT tsc.*, ts.description as spec_description, ts.myob_code,
                      tsu.name as supplier_name
               FROM timber_stocktake_counts tsc
               LEFT JOIN timber_specs ts ON ts.id=tsc.spec_id
               LEFT JOIN timber_suppliers tsu ON tsu.id=tsc.supplier_id
               WHERE tsc.stocktake_id=? ORDER BY tsc.id""",
            [stid]
        ).fetchall())
        st["counts"] = counts
        return {"status": 200, "body": {"stocktake": st}}

    m = match("/timber/stocktakes/:id/sheet", path)
    if m and method == "GET":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        current_user = get_current_user(conn)
        if not current_user:
            return {"status": 401, "body": {"error": "Unauthorized"}}
        stid = int(m["id"])
        # Generate sheet: current inventory grouped by spec
        sheet_rows = rows_to_list(conn.execute(
            """SELECT ts.myob_code, ts.description, ts.type_prefix,
                      ts.width_mm, ts.thickness_mm, ts.length_mm,
                      COUNT(tp.id) as system_packs,
                      COALESCE(SUM(tp.m3_volume),0) as system_m3
               FROM timber_packs tp
               LEFT JOIN timber_specs ts ON ts.id=tp.spec_id
               WHERE tp.status='inventory'
               GROUP BY tp.spec_id
               ORDER BY ts.type_prefix, ts.width_mm, ts.thickness_mm"""
        ).fetchall())
        return {"status": 200, "body": {"stocktake_id": stid, "sheet": sheet_rows}}

    m = match("/timber/stocktakes/:id/counts", path)
    if m and method == "POST":
        current_user = get_current_user(conn)
        if not _is_exec(current_user):
            return {"status": 403, "body": {"error": "Executive role required"}}
        stid = int(m["id"])
        counts_data = body.get("counts", [])
        for count in counts_data:
            spec_id = count.get("spec_id")
            supplier_id = count.get("supplier_id")
            system_packs = conn.execute(
                "SELECT COUNT(*) FROM timber_packs WHERE spec_id=? AND status='inventory'",
                [spec_id]
            ).fetchone()[0]
            system_m3 = conn.execute(
                "SELECT COALESCE(SUM(m3_volume),0) FROM timber_packs WHERE spec_id=? AND status='inventory'",
                [spec_id]
            ).fetchone()[0]
            phys_packs = count.get("physical_packs", 0)
            phys_m3 = count.get("physical_m3", 0)
            conn.execute(
                """INSERT INTO timber_stocktake_counts
                   (stocktake_id, spec_id, supplier_id, system_packs, system_m3,
                    physical_packs, physical_m3, variance_packs, variance_m3, notes)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                [stid, spec_id, supplier_id, system_packs, system_m3,
                 phys_packs, phys_m3,
                 phys_packs - system_packs,
                 round(phys_m3 - system_m3, 4),
                 count.get("notes")]
            )
        conn.commit()
        return {"status": 200, "body": {"message": "Counts recorded"}}

    m = match("/timber/stocktakes/:id/complete", path)
    if m and method == "POST":
        current_user = get_current_user(conn)
        if not _is_exec(current_user):
            return {"status": 403, "body": {"error": "Executive role required"}}
        stid = int(m["id"])
        conn.execute(
            "UPDATE timber_stocktakes SET status='completed', completed_at=CURRENT_TIMESTAMP WHERE id=?",
            [stid]
        )
        conn.commit()
        return {"status": 200, "body": {"message": "Stocktake completed"}}

    # ----- REPORTS -----

    if method == "GET" and path == "/timber/reports/valuation":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        current_user = get_current_user(conn)
        if not _is_exec(current_user):
            return {"status": 403, "body": {"error": "Executive role required"}}
        rows = rows_to_list(conn.execute(
            """SELECT ts.myob_code, ts.description, ts.type_prefix,
                      COUNT(tp.id) as pack_count,
                      COALESCE(SUM(tp.m3_volume),0) as total_m3,
                      COALESCE(SUM(tp.pack_cost_total),0) as total_value,
                      COALESCE(AVG(tp.cost_per_m3),0) as avg_cost_m3
               FROM timber_packs tp
               LEFT JOIN timber_specs ts ON ts.id=tp.spec_id
               WHERE tp.status='inventory'
               GROUP BY tp.spec_id ORDER BY total_value DESC"""
        ).fetchall())
        total = conn.execute(
            "SELECT COALESCE(SUM(pack_cost_total),0) FROM timber_packs WHERE status='inventory'"
        ).fetchone()[0]
        return {"status": 200, "body": {"rows": rows, "total_value": total}}

    if method == "GET" and path == "/timber/reports/purchases":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        current_user = get_current_user(conn)
        if not _is_exec(current_user):
            return {"status": 403, "body": {"error": "Executive role required"}}
        qry = """SELECT td.delivery_date, td.docket_number, tsu.name as supplier_name,
                        tdi.description, tdi.expected_packs, tdi.cost_per_m3, tdi.total_amount
                 FROM timber_delivery_items tdi
                 JOIN timber_deliveries td ON td.id=tdi.delivery_id
                 LEFT JOIN timber_suppliers tsu ON tsu.id=td.supplier_id WHERE 1=1"""
        args = []
        if params.get("supplier_id"):
            qry += " AND td.supplier_id=?"
            args.append(params["supplier_id"])
        if params.get("date_from"):
            qry += " AND td.delivery_date>=?"
            args.append(params["date_from"])
        if params.get("date_to"):
            qry += " AND td.delivery_date<=?"
            args.append(params["date_to"])
        qry += " ORDER BY td.delivery_date DESC"
        rows = rows_to_list(conn.execute(qry, args).fetchall())
        return {"status": 200, "body": {"rows": rows}}

    if method == "GET" and path == "/timber/reports/consumption":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        current_user = get_current_user(conn)
        if not _is_planner(current_user):
            return {"status": 403, "body": {"error": "Planner role required"}}
        qry = """SELECT tc.consumed_at, tc.consumed_by, tc.destination, tc.destination_zone,
                        ts.myob_code, ts.description as spec_description,
                        tp.qr_code, tp.m3_volume, tsu.name as supplier_name
                 FROM timber_consumption tc
                 JOIN timber_packs tp ON tp.id=tc.pack_id
                 LEFT JOIN timber_specs ts ON ts.id=tp.spec_id
                 LEFT JOIN timber_suppliers tsu ON tsu.id=tp.supplier_id WHERE 1=1"""
        args = []
        if params.get("spec_id"):
            qry += " AND tp.spec_id=?"
            args.append(params["spec_id"])
        if params.get("operator"):
            qry += " AND tc.consumed_by=?"
            args.append(params["operator"])
        if params.get("zone"):
            qry += " AND tc.destination_zone=?"
            args.append(params["zone"])
        if params.get("date_from"):
            qry += " AND date(tc.consumed_at)>=?"
            args.append(params["date_from"])
        if params.get("date_to"):
            qry += " AND date(tc.consumed_at)<=?"
            args.append(params["date_to"])
        qry += " ORDER BY tc.consumed_at DESC"
        rows = rows_to_list(conn.execute(qry, args).fetchall())
        return {"status": 200, "body": {"rows": rows}}

    if method == "GET" and path == "/timber/reports/supplier-analysis":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        current_user = get_current_user(conn)
        if not _is_exec(current_user):
            return {"status": 403, "body": {"error": "Executive role required"}}
        rows = rows_to_list(conn.execute(
            """SELECT tsu.name as supplier_name,
                      COUNT(tp.id) as total_packs,
                      COALESCE(SUM(tp.m3_volume),0) as total_m3,
                      COALESCE(SUM(tp.pack_cost_total),0) as total_cost,
                      COALESCE(AVG(tp.cost_per_m3),0) as avg_cost_m3,
                      SUM(CASE WHEN tp.status='inventory' THEN 1 ELSE 0 END) as packs_in_inventory,
                      SUM(CASE WHEN tp.status='consumed' THEN 1 ELSE 0 END) as packs_consumed
               FROM timber_packs tp
               JOIN timber_suppliers tsu ON tsu.id=tp.supplier_id
               GROUP BY tp.supplier_id ORDER BY total_m3 DESC"""
        ).fetchall())
        return {"status": 200, "body": {"rows": rows}}

    if method == "GET" and path == "/timber/reports/fifo-compliance":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        current_user = get_current_user(conn)
        if not _is_planner(current_user):
            return {"status": 403, "body": {"error": "Planner role required"}}
        threshold_row = conn.execute(
            "SELECT value FROM timber_config WHERE key='fifo_threshold_days'"
        ).fetchone()
        threshold_days = int(threshold_row[0]) if threshold_row else 14
        # Find specs where oldest inventory pack age > threshold
        old_packs = rows_to_list(conn.execute(
            """SELECT ts.myob_code, ts.description, tp.qr_code,
                      tp.received_date, tp.supplier_id, tsu.name as supplier_name,
                      tp.m3_volume,
                      julianday('now') - julianday(tp.received_date) as age_days
               FROM timber_packs tp
               LEFT JOIN timber_specs ts ON ts.id=tp.spec_id
               LEFT JOIN timber_suppliers tsu ON tsu.id=tp.supplier_id
               WHERE tp.status='inventory'
                 AND julianday('now') - julianday(tp.received_date) > ?
               ORDER BY age_days DESC""",
            [threshold_days]
        ).fetchall())
        return {"status": 200, "body": {
            "threshold_days": threshold_days,
            "overdue_packs": old_packs,
            "count": len(old_packs),
        }}

    if method == "GET" and path in ("/timber/reports/undo-log", "/timber/undo-log"):
        current_user = get_current_user(conn)
        if not _is_exec(current_user):
            return {"status": 403, "body": {"error": "Executive role required"}}
        rows = rows_to_list(conn.execute(
            """SELECT tcu.*, tp.qr_code, ts.description as spec_description
               FROM timber_consumption_undo tcu
               JOIN timber_packs tp ON tp.id=tcu.pack_id
               LEFT JOIN timber_specs ts ON ts.id=tp.spec_id
               ORDER BY tcu.undone_at DESC"""
        ).fetchall())
        return {"status": 200, "body": rows}

    if method == "GET" and path == "/timber/reports/export/myob":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        current_user = get_current_user(conn)
        if not _is_exec(current_user):
            return {"status": 403, "body": {"error": "Executive role required"}}
        rows = rows_to_list(conn.execute(
            """SELECT tc.consumed_at as Date, tp.spec_id,
                      ts.myob_code as 'Item/Acct', ts.description as Description,
                      tp.m3_volume as Quantity, tp.pack_cost_total as Amount,
                      tc.consumed_by as 'Consumed By',
                      tsu.name as 'Supplier'
               FROM timber_consumption tc
               JOIN timber_packs tp ON tp.id=tc.pack_id
               LEFT JOIN timber_specs ts ON ts.id=tp.spec_id
               LEFT JOIN timber_suppliers tsu ON tsu.id=tp.supplier_id
               ORDER BY tc.consumed_at DESC"""
        ).fetchall())
        return {"status": 200, "body": {"rows": rows, "format": "myob_purchase_detail"}}

    # ----- LOW STOCK ALERTS -----

    if method == "GET" and path in ("/timber/low-stock-alerts", "/timber/stock-alerts"):
        current_user = get_current_user(conn)
        if not current_user:
            return {"status": 401, "body": {"error": "Unauthorized"}}
        rows = rows_to_list(conn.execute(
            """SELECT la.*, ts.description as spec_description, ts.myob_code
               FROM timber_low_stock_alerts la
               LEFT JOIN timber_specs ts ON ts.id=la.spec_id
               ORDER BY la.id"""
        ).fetchall())
        return {"status": 200, "body": rows}

    if method == "POST" and path in ("/timber/low-stock-alerts", "/timber/stock-alerts"):
        current_user = get_current_user(conn)
        if not _is_exec(current_user):
            return {"status": 403, "body": {"error": "Executive role required"}}
        spec_id = body.get("spec_id")
        threshold = body.get("threshold_value")
        if not spec_id or threshold is None:
            return {"status": 400, "body": {"error": "spec_id and threshold_value required"}}
        cur = conn.execute(
            "INSERT INTO timber_low_stock_alerts (spec_id, threshold_value, threshold_unit) VALUES (?,?,?)",
            [spec_id, threshold, body.get("threshold_unit", "m3")]
        )
        conn.commit()
        return {"status": 201, "body": {"id": cur.lastrowid}}

    m = match("/timber/low-stock-alerts/:id", path)
    if m and method == "PUT":
        current_user = get_current_user(conn)
        if not _is_exec(current_user):
            return {"status": 403, "body": {"error": "Executive role required"}}
        lid = int(m["id"])
        fields = ["spec_id", "threshold_value", "threshold_unit", "is_active"]
        updates = {f: body[f] for f in fields if f in body}
        if updates:
            sets = ", ".join(f"{k}=?" for k in updates)
            conn.execute(
                f"UPDATE timber_low_stock_alerts SET {sets} WHERE id=?",
                list(updates.values()) + [lid]
            )
            conn.commit()
        return {"status": 200, "body": {"message": "Alert updated"}}

    m = match("/timber/low-stock-alerts/:id", path)
    if m and method == "DELETE":
        current_user = get_current_user(conn)
        if not _is_exec(current_user):
            return {"status": 403, "body": {"error": "Executive role required"}}
        lid = int(m["id"])
        conn.execute("DELETE FROM timber_low_stock_alerts WHERE id=?", [lid])
        conn.commit()
        return {"status": 200, "body": {"message": "Alert deleted"}}

    if method == "GET" and path == "/timber/alert-recipients":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        current_user = get_current_user(conn)
        if not current_user:
            return {"status": 401, "body": {"error": "Unauthorized"}}
        rows = rows_to_list(conn.execute(
            "SELECT * FROM timber_alert_recipients ORDER BY id"
        ).fetchall())
        return {"status": 200, "body": {"recipients": rows}}

    if method == "POST" and path == "/timber/alert-recipients":
        current_user = get_current_user(conn)
        if not _is_exec(current_user):
            return {"status": 403, "body": {"error": "Executive role required"}}
        email = body.get("email", "").strip()
        if not email:
            return {"status": 400, "body": {"error": "email required"}}
        cur = conn.execute(
            "INSERT INTO timber_alert_recipients (email, name) VALUES (?,?)",
            [email, body.get("name")]
        )
        conn.commit()
        return {"status": 201, "body": {"id": cur.lastrowid}}

    m = match("/timber/alert-recipients/:id", path)
    if m and method == "PUT":
        current_user = get_current_user(conn)
        if not _is_exec(current_user):
            return {"status": 403, "body": {"error": "Executive role required"}}
        rid = int(m["id"])
        fields = ["email", "name", "is_active"]
        updates = {f: body[f] for f in fields if f in body}
        if updates:
            sets = ", ".join(f"{k}=?" for k in updates)
            conn.execute(
                f"UPDATE timber_alert_recipients SET {sets} WHERE id=?",
                list(updates.values()) + [rid]
            )
            conn.commit()
        return {"status": 200, "body": {"message": "Recipient updated"}}

    m = match("/timber/alert-recipients/:id", path)
    if m and method == "DELETE":
        current_user = get_current_user(conn)
        if not _is_exec(current_user):
            return {"status": 403, "body": {"error": "Executive role required"}}
        rid = int(m["id"])
        conn.execute("DELETE FROM timber_alert_recipients WHERE id=?", [rid])
        conn.commit()
        return {"status": 200, "body": {"message": "Recipient deleted"}}

    # ----- ADMIN -----

    if method == "DELETE" and path == "/timber/test-data":
        current_user = get_current_user(conn)
        if not _is_exec(current_user):
            return {"status": 403, "body": {"error": "Executive role required"}}
        conn.execute("DELETE FROM timber_consumption WHERE pack_id IN (SELECT id FROM timber_packs WHERE is_test_data=1)")
        conn.execute("DELETE FROM timber_chainsaw_allocations WHERE pack_id IN (SELECT id FROM timber_packs WHERE is_test_data=1)")
        conn.execute("DELETE FROM timber_consumption_undo WHERE pack_id IN (SELECT id FROM timber_packs WHERE is_test_data=1)")
        r = conn.execute("DELETE FROM timber_packs WHERE is_test_data=1")
        conn.commit()
        deleted = r.rowcount
        log_audit(conn, current_user["id"], "DELETE_TIMBER_TEST_DATA",
                  "timber_packs", None, None, {"deleted": deleted})
        return {"status": 200, "body": {"message": f"Deleted {deleted} test packs and related records"}}


    # ----- KANBAN SUMMARY -----
    if method == "GET" and path == "/ops/kanban-summary":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        # Count order items by kanban stage
        # T = Pending Stock (RED), C = In Docking (AMBER), R = Ready/In Production (AMBER),
        # P = Picked/QA (GREEN Planning), F = Fulfilled
        # Dispatch statuses from delivery_log (joined by order_id)

        # Get order items with their current status and order details
        items = rows_to_list(conn.execute("""
            SELECT oi.id, oi.order_id, oi.sku_code, oi.quantity, oi.status as item_status,
                   oi.docking_completed_at,
                   o.order_number, c.company_name as client_name, o.status as order_status,
                   o.delivery_type, o.created_at as order_date,
                   COALESCE(dl.status, '') as dispatch_status,
                   COALESCE(o.dispatched_at, '') as dispatched_at,
                   COALESCE(dl.actual_date, '') as delivered_date
            FROM order_items oi
            JOIN orders o ON o.id = oi.order_id
            LEFT JOIN clients c ON c.id = o.client_id
            LEFT JOIN delivery_log dl ON dl.order_id = o.id
            WHERE o.status NOT IN ('cancelled','archived')
            ORDER BY o.created_at DESC
        """))

        # Build kanban groups
        groups = {
            "pending_stock": {"label": "Pending Stock", "color": "#dc2626", "items": []},
            "in_docking": {"label": "In Docking", "color": "#f59e0b", "items": []},
            "in_production": {"label": "In Production", "color": "#f59e0b", "items": []},
            "ready_planning": {"label": "Ready / Planning", "color": "#22c55e", "items": []},
            "ready_to_dispatch": {"label": "Ready to Dispatch", "color": "#16a34a", "items": []},
            "dispatched": {"label": "Dispatched", "color": "#2563eb", "items": []},
            "delivered": {"label": "Delivered", "color": "#dc2626", "items": []}
        }

        for item in items:
            entry = {
                "id": item["id"],
                "order_id": item["order_id"],
                "order_number": item["order_number"],
                "client_name": item["client_name"],
                "sku_code": item["sku_code"],
                "quantity": item["quantity"],
                "item_status": item["item_status"],
                "delivery_type": item["delivery_type"]
            }

            ds = item.get("dispatch_status", "")
            ist = item.get("item_status", "T")

            if ds in ("delivered", "collected") or item.get("delivered_date"):
                groups["delivered"]["items"].append(entry)
            elif ds in ("loaded", "in_transit") or item.get("dispatched_at"):
                groups["dispatched"]["items"].append(entry)
            elif ist == "F":
                groups["ready_to_dispatch"]["items"].append(entry)
            elif ist == "P":
                groups["ready_planning"]["items"].append(entry)
            elif ist == "R":
                groups["in_production"]["items"].append(entry)
            elif ist == "C":
                groups["in_docking"]["items"].append(entry)
            else:  # T
                groups["pending_stock"]["items"].append(entry)

        # Add counts
        for k, g in groups.items():
            g["count"] = len(g["items"])
            # Limit items returned for performance (show top 20 per group)
            g["items"] = g["items"][:20]

        return {"status": 200, "body": groups}

    # ----- OPS MANAGER DASHBOARD -----
    if method == "GET" and path == "/ops/dashboard":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        # Note: datetime, timezone, timedelta already imported at module level (line 17)
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        # Fetch labour rate from config (default $55/hr)
        lc_row = conn.execute("SELECT rate_per_hour FROM target_labour_rates WHERE is_default=1 LIMIT 1").fetchone()
        LABOUR_RATE = float(lc_row["rate_per_hour"]) if lc_row and lc_row["rate_per_hour"] else 55.0

        # ---- KPIs ----
        active_workers = conn.execute(
            "SELECT COUNT(DISTINCT sw.user_id) FROM session_workers sw "
            "JOIN production_sessions ps ON ps.id=sw.session_id "
            "WHERE ps.status='active' AND sw.is_active=1"
        ).fetchone()[0]

        total_pallets_today = conn.execute(
            "SELECT COALESCE(SUM(produced_quantity),0) FROM production_sessions "
            "WHERE status='completed' AND DATE(end_time)=?", [today]
        ).fetchone()[0]
        # Also add in-progress (partial) production
        total_pallets_today += conn.execute(
            "SELECT COALESCE(SUM(produced_quantity),0) FROM production_sessions "
            "WHERE status IN ('active','paused') AND DATE(start_time)=?", [today]
        ).fetchone()[0]

        # Avg efficiency: actual produced / target across sessions started today
        eff_rows = conn.execute(
            "SELECT produced_quantity, target_quantity FROM production_sessions "
            "WHERE DATE(start_time)=? AND target_quantity > 0", [today]
        ).fetchall()
        if eff_rows:
            avg_eff = sum(r[0] / r[1] for r in eff_rows) / len(eff_rows)
        else:
            avg_eff = 0.0

        # Labour cost today: sum all active session durations * $55/hr
        active_mins = conn.execute(
            "SELECT COALESCE(SUM((strftime('%s','now') - strftime('%s', start_time)) / 60.0), 0) "
            "FROM production_sessions WHERE status='active' AND DATE(start_time)=?", [today]
        ).fetchone()[0]
        completed_mins = conn.execute(
            "SELECT COALESCE(SUM((strftime('%s', end_time) - strftime('%s', start_time)) / 60.0), 0) "
            "FROM production_sessions WHERE status='completed' AND DATE(start_time)=?", [today]
        ).fetchone()[0]
        total_worker_minutes = active_mins + completed_mins
        # Multiply by active_workers for active sessions to get worker-minutes
        # Simpler: count session-minutes and multiply by average concurrent workers
        labour_cost_today = round((total_worker_minutes / 60.0) * LABOUR_RATE, 2)

        jobs_completed = conn.execute(
            "SELECT COUNT(*) FROM production_sessions "
            "WHERE status='completed' AND DATE(end_time)=?", [today]
        ).fetchone()[0]

        # ---- Production Rate ----
        # Current rate: pallets produced in last 60 minutes
        recent_pallets = conn.execute(
            "SELECT COALESCE(SUM(quantity_change),0) FROM production_logs "
            "WHERE logged_at >= datetime('now','-60 minutes') AND quantity_change > 0"
        ).fetchone()[0]
        # Add currently active session partial counts
        current_per_hour = float(recent_pallets)

        # Target: sum of target_quantity / shift_hours for active sessions
        target_rows = conn.execute(
            "SELECT COALESCE(SUM(target_quantity),0) FROM production_sessions "
            "WHERE status='active' AND DATE(start_time)=?"
        , [today]).fetchone()[0]
        # Assume 8hr shift; target per hour = target / 8
        target_per_hour = round(float(target_rows) / 8.0, 1) if target_rows else 50.0
        if target_per_hour == 0:
            target_per_hour = 50.0
        prod_pct = round((current_per_hour / target_per_hour) * 100, 1) if target_per_hour else 0

        # ---- Production Today Table (per SKU) ----
        sku_rows = conn.execute("""
            SELECT
                oi.sku_code,
                COALESCE(s.sell_price, 0) as sell_price,
                COALESCE(SUM(ps.produced_quantity), 0) as pallets,
                COALESCE(SUM(
                    (strftime('%s', COALESCE(ps.end_time, 'now')) - strftime('%s', ps.start_time)) / 60.0
                    - COALESCE((
                        SELECT SUM(COALESCE(pl2.duration_minutes, 0))
                        FROM pause_logs pl2 WHERE pl2.session_id = ps.id
                    ), 0)
                ), 0) as net_minutes
            FROM production_sessions ps
            LEFT JOIN order_items oi ON oi.id = ps.order_item_id
            LEFT JOIN skus s ON s.id = oi.sku_id
            WHERE DATE(ps.start_time) = ? AND oi.sku_code IS NOT NULL
            GROUP BY oi.sku_code, s.sell_price
            ORDER BY pallets DESC
        """, [today]).fetchall()

        # Get allocated minutes from schedule_entries for today
        alloc_rows = conn.execute("""
            SELECT oi.sku_code,
                   COALESCE(SUM(se.planned_quantity), 0) as alloc_qty
            FROM schedule_entries se
            JOIN order_items oi ON oi.id = se.order_item_id
            WHERE se.scheduled_date = ?
            GROUP BY oi.sku_code
        """, [today]).fetchall()
        alloc_map = {r[0]: r[1] for r in alloc_rows}

        production_today = []
        total_pallets = 0
        total_net_mins = 0
        total_alloc_mins = 0
        total_labour = 0.0
        total_value = 0.0
        for r in sku_rows:
            sku_code = r[0] or "Unknown"
            sell_price = float(r[1] or 0)
            pallets = int(r[2] or 0)
            net_minutes = round(float(r[3] or 0), 1)
            # Allocated minutes: assume each pallet takes ~net_mins/pallets ratio vs alloc qty
            alloc_qty = alloc_map.get(sku_code, 0)
            # Use pallets-per-minute rate to estimate allocated minutes
            rate = pallets / net_minutes if net_minutes > 0 else 0
            alloc_minutes = round(alloc_qty / rate, 1) if rate > 0 and alloc_qty > 0 else 0
            variance = round(alloc_minutes - net_minutes, 1)
            if variance > 5:
                on_target = "behind"
            elif variance < -5:
                on_target = "ahead"
            else:
                on_target = "on_target"
            labour_cost = round((net_minutes / 60.0) * LABOUR_RATE, 2)
            value = round(pallets * sell_price, 2)
            production_today.append({
                "sku": sku_code,
                "pallets": pallets,
                "net_minutes": net_minutes,
                "allocated_minutes": alloc_minutes,
                "variance": variance,
                "on_target": on_target,
                "labour_cost": labour_cost,
                "value": value
            })
            total_pallets += pallets
            total_net_mins += net_minutes
            total_alloc_mins += alloc_minutes
            total_labour += labour_cost
            total_value += value

        # ---- Zone Summary ----
        zone_rows = conn.execute("""
            SELECT z.name as zone,
                   COUNT(DISTINCT CASE WHEN ps.status='active' THEN sw.user_id END) as active_workers,
                   COALESCE(SUM(ps.produced_quantity),0) as pallets,
                   COALESCE(AVG(CASE WHEN ps.target_quantity > 0 THEN CAST(ps.produced_quantity AS REAL)/ps.target_quantity END), 0) as efficiency
            FROM zones z
            LEFT JOIN production_sessions ps ON ps.zone_id = z.id AND DATE(ps.start_time) = ?
            LEFT JOIN session_workers sw ON sw.session_id = ps.id AND sw.is_active = 1
            WHERE z.is_active = 1
            GROUP BY z.id, z.name
            ORDER BY z.name
        """, [today]).fetchall()
        zone_summary = []
        for zr in zone_rows:
            zone_summary.append({
                "zone": zr[0],
                "active_workers": int(zr[1] or 0),
                "pallets": int(zr[2] or 0),
                "efficiency": round(float(zr[3] or 0), 2)
            })

        # ---- Alerts ----
        alerts = []
        # Workers with very long sessions (>10hrs)
        long_sessions = conn.execute("""
            SELECT u.full_name, z.name as zone,
                   round((strftime('%s','now') - strftime('%s', ps.start_time))/3600.0, 1) as hours
            FROM production_sessions ps
            JOIN session_workers sw ON sw.session_id = ps.id AND sw.is_active = 1
            JOIN users u ON u.id = sw.user_id
            JOIN zones z ON z.id = ps.zone_id
            WHERE ps.status = 'active' AND (strftime('%s','now') - strftime('%s', ps.start_time)) > 36000
        """).fetchall()
        for ls in long_sessions:
            alerts.append({"type": "warning", "message": f"{ls[0]} has been active for {ls[2]}h in {ls[1]}"})

        # Low efficiency zones
        for zs in zone_summary:
            if zs["efficiency"] > 0 and zs["efficiency"] < 0.6:
                alerts.append({"type": "danger", "message": f"{zs['zone']} zone below 60% efficiency ({round(zs['efficiency']*100)}%)"})

        return {"status": 200, "body": {
            "kpis": {
                "active_workers": active_workers,
                "total_pallets_today": total_pallets_today,
                "avg_efficiency": round(avg_eff, 2),
                "labour_cost_today": labour_cost_today,
                "jobs_completed_today": jobs_completed
            },
            "production_rate": {
                "current_per_hour": round(current_per_hour, 1),
                "target_per_hour": target_per_hour,
                "percentage": prod_pct
            },
            "production_today": production_today,
            "production_totals": {
                "pallets": total_pallets,
                "net_minutes": round(total_net_mins, 1),
                "allocated_minutes": round(total_alloc_mins, 1),
                "labour_cost": round(total_labour, 2),
                "value": round(total_value, 2)
            },
            "zone_summary": zone_summary,
            "alerts": alerts,
            "generated_at": datetime.now(timezone.utc).isoformat()
        }}

    # ----- ALLOCATE INVENTORY FOR FULL ORDER -----
    m = match("/orders/:id/allocate-inventory", path)
    if m and method == "POST":
        if not current_user:
            return {"status": 401, "body": {"error": "Authentication required"}}
        order_id = int(m["id"])
        order = conn.execute("SELECT * FROM orders WHERE id=?", [order_id]).fetchone()
        if not order:
            return {"status": 404, "body": {"error": "Order not found"}}
        items = conn.execute(
            "SELECT * FROM order_items WHERE order_id=? AND status NOT IN ('F','dispatched','delivered')",
            [order_id]
        ).fetchall()
        if not items:
            return {"status": 400, "body": {"error": "No pending items on this order"}}
        allocated = []
        errors = []
        for item in items:
            item_d = row_to_dict(item)
            sku_id = item_d.get("sku_id")
            qty = item_d.get("quantity", 0)
            if not sku_id:
                sku_row = conn.execute("SELECT id FROM skus WHERE code=?", [item_d.get("sku_code")]).fetchone()
                if sku_row:
                    sku_id = sku_row[0] if not isinstance(sku_row, dict) else sku_row["id"]
            if sku_id:
                inv = conn.execute("SELECT * FROM inventory WHERE sku_id=?", [sku_id]).fetchone()
                if inv:
                    inv_d = row_to_dict(inv)
                    available = inv_d.get("units_on_hand", 0) - inv_d.get("units_allocated", 0)
                    if available < qty:
                        errors.append(f"SKU {item_d.get('sku_code')}: need {qty}, have {available}")
                        continue
                    conn.execute(
                        "UPDATE inventory SET units_on_hand=units_on_hand-?, units_allocated=units_allocated+?, updated_at=CURRENT_TIMESTAMP WHERE sku_id=?",
                        [qty, qty, sku_id]
                    )
            conn.execute(
                "UPDATE order_items SET status='F', kanban_status='green_dispatch', produced_quantity=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                [qty, item_d["id"]]
            )
            allocated.append(item_d["id"])

        if allocated:
            # Check if all items now F
            pending = conn.execute(
                "SELECT COUNT(*) FROM order_items WHERE order_id=? AND status NOT IN ('F','dispatched','delivered')",
                [order_id]
            ).fetchone()[0]
            if pending == 0:
                conn.execute(
                    "UPDATE orders SET status='F', kanban_status='green_dispatch' WHERE id=? AND status NOT IN ('dispatched','delivered','collected')",
                    [order_id]
                )
            # Auto-create delivery_log entry if none exists
            existing_dl = conn.execute("SELECT id FROM delivery_log WHERE order_id=?", [order_id]).fetchone()
            if not existing_dl:
                conn.execute(
                    "INSERT INTO delivery_log (order_id, status, delivery_type, notes, created_at) VALUES (?, 'pending', 'delivery', 'Auto-created from inventory allocation', CURRENT_TIMESTAMP)",
                    [order_id]
                )
            update_kanban_statuses(conn, order_id)
            conn.commit()
            log_audit(conn, current_user["id"], "allocate_inventory", "orders", order_id, None,
                      {"allocated_items": allocated, "errors": errors})

        return {"status": 200, "body": {
            "allocated_items": len(allocated),
            "errors": errors,
            "order_id": order_id
        }}


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
