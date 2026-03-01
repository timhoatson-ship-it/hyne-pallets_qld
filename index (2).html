# Hyne Pallets — Driver Mobile Web App: LLM Build Brief

> **Purpose**: This document is a comprehensive prompt brief for another LLM-powered coding agent to build a **driver-facing mobile web application** that integrates with the existing Hyne Pallets Production Management System. The agent should treat this as the single source of truth for scope, architecture, integration points, and business rules.

---

## 1. PROJECT CONTEXT

### 1.1 Business Overview
Hyne Pallets is a timber pallet manufacturer based in Queensland, Australia. They operate a full production-to-dispatch pipeline across 4 manufacturing zones (Viking, Handmade, DTL, Crates). The core management system — already built and deployed — covers order intake, production planning, station allocation, floor production tracking, QA inspection, and dispatch planning for **7 trucks** (6 internal drivers + 1 contractor slot).

### 1.2 Existing System Stack
| Component | Technology |
|-----------|-----------|
| Backend | Python Flask, single `server.py` file (~3,446 lines) |
| Database | SQLite (34+ tables) |
| Frontend | Inline React (Babel JSX transform, no build step), Tailwind CSS CDN |
| Auth | JWT token-based (email/password for office, PIN for floor workers) |
| Hosting | Railway (auto-deploy from GitHub) |
| Process | `gunicorn server:app --bind 0.0.0.0:$PORT --workers 2` |

### 1.3 Corporate Branding
- **Primary Navy**: `#07324C`
- **Accent Red**: `#ED1C24`
- **Logo**: Available at hynepallets.com.au
- **Font sizing**: All text should be two sizes larger than typical defaults throughout the app

### 1.4 Source Files Provided
The following files are bundled with this brief. You MUST read and understand them before building:

| File | Description | Lines |
|------|-------------|-------|
| `server.py` | Complete backend — DB schema, seed data, all API endpoints, auth middleware | ~3,446 |
| `static/index.html` | Complete frontend — React app with all pages/components | ~5,256 |
| `static/style.css` | Tailwind overrides and custom styles | ~278 |

---

## 2. WHAT YOU ARE BUILDING

A **mobile-first web application** (not a native app) for Hyne Pallets truck drivers. This app runs on the driver's phone/tablet and integrates with the existing backend API. It is a separate frontend that calls the same `server.py` backend.

### 2.1 Core Purpose
- Allow drivers to **clock on/off** for shifts
- Show the driver their **allocated load** (deliveries assigned to their truck for the day)
- Provide **run time estimates** based on delivery addresses and distances
- Track **time stages** throughout each delivery run for benchmarking and costing
- Capture **finance/costing data** per truck (R&M, wages, rego, insurance)
- Integrate with **TrackMyRide.com.au** for GPS tracking (if API allows)

### 2.2 Users
- 6 internal drivers: Leeroy (Truck 1), Usef (Truck 2), Ronny (Truck 3), Ben (Truck 4), Marcus (Truck 5), Besher (Truck 6)
- Contractor drivers on Truck 7 (variable)
- Dispatch manager (read-only oversight view)

---

## 3. AUTHENTICATION & ASSET SELECTION

### 3.1 Login Flow
1. Driver opens the app on their phone
2. Logs in via **PIN** (same as existing floor worker PIN auth) or **email/password**
3. On successful auth, the driver must have role = `dispatch` or a new `driver` role (see section 10.1)

### 3.2 Shift Clock-In
After login, before seeing any delivery data:

1. **Safety Modal** — A mandatory safety acknowledgement modal appears:
   - "I confirm I have completed my pre-trip vehicle inspection and am fit for duty"
   - Checkbox for each: Vehicle walkaround complete, Load restraints checked, PPE worn, Fit for duty declaration
   - Driver must tick all checkboxes and tap "Confirm & Start Shift"
   - This is logged with timestamp

2. **Asset Selection** — Driver selects which truck they are driving today:
   - Dropdown/list showing all active trucks from the `trucks` table
   - Internal drivers see their default truck pre-selected (matched by `driver_name`)
   - Contractor sees Truck 7 pre-selected
   - Selection is locked for the shift once confirmed (can be changed by dispatch manager only)

3. **Shift Record Created** — A shift record is created in the database:
   - `driver_id`, `truck_id`, `clock_on_time`, `date`, `safety_acknowledged`

---

## 4. MAIN APP STRUCTURE (Post Clock-In)

### 4.1 Tab Layout
Two primary tabs at the bottom of the screen:

| Tab | Content |
|-----|---------|
| **Current Load** | The active delivery run — what's on the truck right now |
| **Upcoming Runs** | Queue of remaining deliveries for the day |

### 4.2 Current Load Tab
Shows all deliveries currently loaded on this truck for the active run:

- **Header**: Truck name, driver name, today's date, shift start time
- **Delivery Cards** (ordered by `load_sequence`):
  - Client company name
  - Delivery address (street, suburb, state, postcode)
  - Order number(s) being delivered
  - SKU codes / pallet codes / part numbers for each order item
  - Quantity per item
  - Estimated travel time (from `delivery_addresses.estimated_travel_minutes`)
  - Estimated KM (if available from TrackMyRide or address database)
  - Delivery type badge: "Delivery" or "Collection"
  - Status indicator: Waiting → Loaded → In Transit → Delivered

- **Run Summary Bar**:
  - Total stops: X
  - Total estimated time: X hrs Y mins
  - Total pallets: X units

### 4.3 Upcoming Runs Tab
Shows deliveries assigned to this truck for later in the day (not yet loaded):
- Same card format as Current Load but greyed out
- Tap to expand for details
- Shows scheduled load time

---

## 5. TIME CLOCK & DELIVERY STAGES

This is the core workflow feature. Each delivery run passes through timed stages. The driver taps through stages sequentially, and each stage records start/end time for benchmarking.

### 5.1 Stage Pipeline

```
DEPOT (Loading) → DRIVING (To Customer) → CUSTOMER SITE → DRIVING (Return to Depot)
```

Each of these macro stages has sub-stages:

#### At Depot (Start of Run)
| Stage | Description |
|-------|-------------|
| `waiting_to_load` | Driver is at depot waiting for forklift to load truck |
| `being_loaded` | Truck is actively being loaded by yard team |
| `tie_down` | Driver is securing/restraining the load |

#### Driving to Customer
| Stage | Description |
|-------|-------------|
| `driving_to_customer` | Truck is in transit to delivery address |
| `break` | Driver takes a break (can be triggered at any driving point) |

#### At Customer Site
| Stage | Description |
|-------|-------------|
| `waiting_at_customer` | Arrived at customer, waiting to be unloaded |
| `being_unloaded` | Customer is actively unloading the truck |

#### Driving Return
| Stage | Description |
|-------|-------------|
| `driving_return` | Returning to depot (or to next customer if multi-drop) |
| `break` | Break during return leg |

#### For Collection Runs (Reverse)
| Stage | Description |
|-------|-------------|
| `driving_to_customer` | Drive to customer for collection pickup |
| `waiting_at_customer` | Waiting for customer to prepare goods |
| `being_loaded_at_customer` | Customer loading goods onto truck |
| `tie_down` | Securing collected goods |
| `driving_return` | Return to depot with collected goods |
| `being_unloaded_at_depot` | Unloading collected goods at depot |

### 5.2 Stage UI Behaviour

- **Big prominent timer** at the top of Current Load tab showing current stage name + elapsed time
- **Next Action Button**: Large, thumb-friendly button at bottom:
  - Displays the next stage name: e.g. "Start Loading" → "Finished Loading — Tie Down" → "Start Driving"
  - Tap advances to next stage, stops the previous timer, starts the new timer
  - **Auto-prompt**: When a stage ends (driver taps), a confirmation dialog asks: "Loading complete. Start Tie Down?" — driver confirms or can select Break instead
- **Break Button**: Always visible, pauses the current stage and starts a break timer. On break end, returns to the interrupted stage.
- **Stage History**: Scrollable list below the timer showing completed stages with durations:
  ```
  ✓ Waiting to Load    — 12 min
  ✓ Being Loaded       — 28 min
  ✓ Tie Down           — 8 min
  ● Driving to Customer — 0:34:12 (active)
  ```

### 5.3 Multi-Drop Runs
If the truck has multiple delivery stops in one run:
- After unloading at Customer A, the driver is prompted: "Drive to next stop (Customer B)?" or "Return to Depot?"
- The stage pipeline repeats for each stop
- Each stop's stages are tracked independently

### 5.4 Time Data Captured Per Stage
For every stage transition, record:
```json
{
  "delivery_log_id": 123,
  "stage": "being_loaded",
  "started_at": "2026-02-28T06:15:00Z",
  "ended_at": "2026-02-28T06:43:00Z",
  "duration_minutes": 28,
  "driver_id": 5,
  "truck_id": 1,
  "location_lat": -27.4698,
  "location_lng": 153.0251,
  "notes": ""
}
```

---

## 6. RUN TIME ESTIMATES & ROUTING

### 6.1 Existing Address Database
The system already has a `delivery_addresses` table with:
- `street_address`, `suburb`, `state`, `postcode`
- `estimated_travel_minutes` (manually entered by dispatch)
- `estimated_return_minutes`
- `client_id` (linked to client)

### 6.2 Distance/Time Calculation
For the driver app, provide time/distance estimates using:

1. **Primary**: Use the `estimated_travel_minutes` from `delivery_addresses` table (dispatch-entered data)
2. **Enhanced (if TrackMyRide available)**: Pull real-time location and calculate ETA dynamically
3. **Fallback**: Use a simple km-based formula from depot postcode to destination postcode
   - Depot location: Hyne Pallets, Maryborough QLD 4650 (approximate coords: -25.5333, 152.7000)

### 6.3 Run Sheet View
A summarised run sheet for the day showing:
- Stop order (1, 2, 3...)
- Client name + address
- Estimated arrival time (cumulative from shift start)
- Estimated time at each stop
- Total run duration estimate

---

## 7. TRACKMYRIDE.COM.AU INTEGRATION

### 7.1 API Overview
TrackMyRide is an Australian GPS fleet tracking provider. They offer a REST API at:

**Base URL**: `https://app.trackmyride.com.au/v2/php/api.php`

**Authentication**: API key pair — `user_key` + `api_key` passed as query parameters or form-data on every request.

**Response format**: XML by default; append `&json=1` to get JSON responses.

### 7.2 Available API Modules

| Module | Key Endpoints | Relevance to Driver App |
|--------|--------------|------------------------|
| **Devices** | `?module=devices&action=get` — Get latest vehicle location/trackpoint data | HIGH — Real-time truck position |
| **Devices** | `?module=devices&action=playback` — Historic tracking data over time period | HIGH — Trip history, actual routes |
| **Devices** | `?module=devices&action=set_driver` — Assign driver to vehicle | MEDIUM — Auto-assign on shift clock-in |
| **Drivers** | `?module=drivers&action=get` — List all drivers | MEDIUM — Sync driver roster |
| **Drivers** | `?module=drivers&action=save` — Create/update driver | MEDIUM — Auto-provision new drivers |
| **Alerts** | `?module=alerts&action=get` — Latest 100 vehicle alerts | LOW — Safety alerts (speeding, geofence breach) |
| **Alerts** | `?module=alerts&action=criterias` — All configured alerts | LOW — View alert rules |
| **Zones** | `?module=zones&action=get` — All geofences (GeoJSON) | MEDIUM — Detect depot arrival/departure |
| **Reports** | `report{n}.php` — Journey, Tax, Utilisation reports | MEDIUM — KM summaries, journey history |
| **Aux** | `?module=aux&action=set_journey_business` — Mark trip business/private | LOW — Tax logbook |
| **Aux** | `?module=aux&action=add_refuel_record` — Log refuelling | MEDIUM — Fuel cost tracking |
| **Subaccounts** | `?module=subaccounts&action=get` — Fleet management sub-accounts | LOW — Multi-account fleet setup |
| **User** | `?module=user&action=get` — Account info | LOW — API validation |

### 7.3 Integration Strategy

**Phase 1 — Read Only (MVP)**:
- On shift clock-in, call `devices&action=get` to confirm the selected truck's GPS device is online
- Display real-time truck position on a mini-map (Google Maps / Leaflet) within the app
- Periodically poll (every 60s) for updated position during driving stages
- Use device data to auto-detect: arrived at customer (geofence), departed depot, returned to depot

**Phase 2 — Active Integration**:
- Auto-assign driver to device via `devices&action=set_driver` on shift start
- Use `devices&action=playback` to pull actual trip data after run completion for benchmarking
- Log refuelling events via `aux&action=add_refuel_record`
- Use zone geofences to auto-trigger stage transitions (e.g., entering customer geofence → auto-prompt "Arrived at customer")

**Phase 3 — Reporting**:
- Pull journey reports for KM tracking and fuel analysis
- Cross-reference actual drive times vs. estimated times for continuous improvement

### 7.4 Implementation Notes
- TrackMyRide API keys will be stored in the `accounting_config` table or a new `integrations_config` table
- API calls should be proxied through the Flask backend (never expose API keys to the mobile frontend)
- Implement a `/api/trackmyride/...` proxy endpoint pattern in server.py
- Handle API rate limits gracefully — cache device positions for 30 seconds minimum
- If TrackMyRide is not configured/available, the app must still work fully with manual stage transitions

---

## 8. COSTING MODEL INTEGRATION

### 8.1 Purpose
Every delivery run has a cost. The driver app feeds time data into a costing model that management uses for per-delivery and per-truck profitability analysis.

### 8.2 Cost Components per Truck

| Cost Category | Source | Per |
|--------------|--------|-----|
| **Driver Wages** | Hourly rate × time on shift | Per shift |
| **Fuel** | TrackMyRide fuel data or manual entry | Per run / per KM |
| **R&M (Repairs & Maintenance)** | Manually entered by fleet manager | Per truck per period |
| **Registration (Rego)** | Annual cost ÷ operating days | Per truck per day |
| **Insurance** | Annual cost ÷ operating days | Per truck per day |
| **Tolls** | Manual entry by driver | Per run |
| **Tyres** | Cost per KM allocation | Per KM |

### 8.3 Finance Fields on Truck Record (Admin Config)
Extend the truck management in Admin to include:

```
truck_finance_config:
  truck_id (FK trucks)
  driver_hourly_rate REAL         -- e.g. $38.50/hr
  fuel_cost_per_litre REAL        -- e.g. $1.85/L
  avg_fuel_consumption_per_100km REAL -- e.g. 32L/100km
  annual_rego_cost REAL           -- e.g. $4,200
  annual_insurance_cost REAL      -- e.g. $8,500
  rm_budget_monthly REAL          -- e.g. $2,000/month
  tyre_cost_per_km REAL           -- e.g. $0.04/km
  operating_days_per_year INTEGER -- e.g. 260
  notes TEXT
```

### 8.4 Cost Calculation per Run
After a run is completed:
```
driver_cost = total_shift_hours × driver_hourly_rate
fuel_cost = (total_km / 100) × avg_fuel_consumption_per_100km × fuel_cost_per_litre
rego_cost = annual_rego_cost / operating_days_per_year
insurance_cost = annual_insurance_cost / operating_days_per_year
rm_allocation = rm_budget_monthly / (operating_days_per_year / 12)
tyre_cost = total_km × tyre_cost_per_km
TOTAL_RUN_COST = driver_cost + fuel_cost + rego_cost + insurance_cost + rm_allocation + tyre_cost + tolls
```

### 8.5 Cost per Delivery
For multi-drop runs, allocate proportionally:
```
delivery_cost = TOTAL_RUN_COST × (delivery_km / total_run_km)
```
or by time:
```
delivery_cost = TOTAL_RUN_COST × (delivery_time / total_run_time)
```

---

## 9. DATABASE SCHEMA — EXISTING TABLES (REFERENCE)

### 9.1 Tables Relevant to the Driver App

#### trucks
```sql
CREATE TABLE trucks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    rego TEXT NOT NULL UNIQUE,
    driver_name TEXT,
    truck_type TEXT DEFAULT 'internal' CHECK(truck_type IN ('internal','contractor')),
    capacity_notes TEXT,
    is_active INTEGER DEFAULT 1
);
```
**Seed data**: 7 trucks — Truck 1-6 (internal: Leeroy, Usef, Ronny, Ben, Marcus, Besher), Truck 7 (contractor)

#### delivery_log
```sql
CREATE TABLE delivery_log (
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
```

#### delivery_addresses
```sql
CREATE TABLE delivery_addresses (
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
```

#### truck_work_orders
```sql
CREATE TABLE truck_work_orders (
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
```

#### truck_capacity_config
```sql
CREATE TABLE truck_capacity_config (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    truck_id INTEGER NOT NULL REFERENCES trucks(id),
    day_of_week INTEGER,                -- 0=Monday ... 6=Sunday
    capacity_minutes INTEGER NOT NULL DEFAULT 480,
    overtime_minutes INTEGER DEFAULT 120,
    notes TEXT,
    UNIQUE(truck_id, day_of_week)
);
```

#### contractor_assignments
```sql
CREATE TABLE contractor_assignments (
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
```

#### users (relevant fields)
```sql
CREATE TABLE users (
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
```

#### orders (relevant fields)
```sql
CREATE TABLE orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_number TEXT NOT NULL UNIQUE,
    client_id INTEGER REFERENCES clients(id),
    status TEXT NOT NULL DEFAULT 'T' CHECK(status IN ('T','C','R','P','F','dispatched','delivered','collected')),
    delivery_type TEXT DEFAULT 'delivery' CHECK(delivery_type IN ('delivery','collection')),
    truck_id INTEGER REFERENCES trucks(id),
    dispatch_date TEXT,
    dispatched_at TIMESTAMP,
    requested_delivery_date TEXT,
    ...
);
```

#### order_items (relevant fields)
```sql
CREATE TABLE order_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id INTEGER NOT NULL REFERENCES orders(id),
    sku_id INTEGER REFERENCES skus(id),
    sku_code TEXT,
    product_name TEXT,
    quantity INTEGER NOT NULL,
    produced_quantity INTEGER DEFAULT 0,
    status TEXT DEFAULT 'T' CHECK(status IN ('T','C','R','P','F','dispatched')),
    ...
);
```

---

## 10. NEW DATABASE TABLES REQUIRED

### 10.1 Add 'driver' Role
Extend the users role CHECK constraint:
```sql
-- ALTER the CHECK to include 'driver':
role TEXT NOT NULL CHECK(role IN ('executive','office','planner','production_manager','floor_worker','qa_lead','dispatch','yard','driver'))
```

### 10.2 driver_shifts
```sql
CREATE TABLE driver_shifts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    driver_id INTEGER NOT NULL REFERENCES users(id),
    truck_id INTEGER NOT NULL REFERENCES trucks(id),
    shift_date TEXT NOT NULL,
    clock_on_time TIMESTAMP NOT NULL,
    clock_off_time TIMESTAMP,
    safety_acknowledged INTEGER DEFAULT 0,
    safety_acknowledged_at TIMESTAMP,
    total_hours REAL,
    total_km REAL,
    status TEXT DEFAULT 'active' CHECK(status IN ('active','completed','abandoned')),
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 10.3 delivery_run_stages
```sql
CREATE TABLE delivery_run_stages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    delivery_log_id INTEGER NOT NULL REFERENCES delivery_log(id),
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
```

### 10.4 truck_finance_config
```sql
CREATE TABLE truck_finance_config (
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
    notes TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 10.5 delivery_run_costs
```sql
CREATE TABLE delivery_run_costs (
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
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 10.6 trackmyride_config
```sql
CREATE TABLE trackmyride_config (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_key TEXT,
    api_key TEXT,
    is_active INTEGER DEFAULT 0,
    truck_device_mapping TEXT,  -- JSON: {"truck_id": "device_id", ...}
    last_sync_at TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

---

## 11. NEW API ENDPOINTS REQUIRED

All endpoints follow the existing pattern: single catch-all route `/api/<path:route>`, dispatched by `method + path` inside `api_handler()`.

### 11.1 Driver Auth & Shift

| Method | Path | Description |
|--------|------|-------------|
| POST | `/driver/clock-on` | Clock on: `{truck_id, safety_checks: {...}}` → creates `driver_shifts` record |
| POST | `/driver/clock-off` | Clock off: calculates total hours, total km, sets status='completed' |
| GET | `/driver/shift` | Get current active shift for authenticated driver |
| GET | `/driver/shift-history` | Get past shifts for the driver (with date range filter) |

### 11.2 Delivery Load

| Method | Path | Description |
|--------|------|-------------|
| GET | `/driver/load?truck_id=X&date=Y` | Get all delivery_log entries for this truck on this date, joined with order details, items, client, address |
| GET | `/driver/upcoming?truck_id=X&date=Y` | Get deliveries not yet loaded (status='pending') |
| GET | `/driver/runsheet?truck_id=X&date=Y` | Complete run sheet with stop order, ETAs, cumulative times |

### 11.3 Stage Tracking

| Method | Path | Description |
|--------|------|-------------|
| POST | `/driver/stage/start` | Start a new stage: `{delivery_log_id, stage, location_lat, location_lng}` |
| POST | `/driver/stage/end` | End current stage: `{stage_id}` — auto-calculates duration |
| GET | `/driver/stages?delivery_log_id=X` | Get all stages for a delivery run |
| POST | `/driver/break/start` | Start break (pauses current stage) |
| POST | `/driver/break/end` | End break (resumes previous stage) |

### 11.4 Delivery Status Updates

| Method | Path | Description |
|--------|------|-------------|
| PUT | `/driver/delivery/status` | Update delivery status: `{delivery_log_id, status}` (loaded, in_transit, delivered, collected) |
| POST | `/driver/delivery/complete` | Mark delivery complete — triggers cost calculation |
| POST | `/driver/delivery/photo` | Upload proof-of-delivery photo (base64) |

### 11.5 TrackMyRide Proxy

| Method | Path | Description |
|--------|------|-------------|
| GET | `/trackmyride/devices` | Proxy to `devices&action=get` — returns truck positions |
| GET | `/trackmyride/playback?device_id=X&from=Y&to=Z` | Proxy to `devices&action=playback` |
| POST | `/trackmyride/set-driver` | Proxy to `devices&action=set_driver` |
| GET | `/trackmyride/zones` | Proxy to `zones&action=get` — geofences |
| POST | `/trackmyride/refuel` | Proxy to `aux&action=add_refuel_record` |

### 11.6 Finance / Costing

| Method | Path | Description |
|--------|------|-------------|
| GET | `/truck-finance?truck_id=X` | Get finance config for a truck |
| PUT | `/truck-finance` | Update finance config (admin only) |
| GET | `/delivery-costs?delivery_log_id=X` | Get calculated costs for a delivery |
| GET | `/truck-costs?truck_id=X&from=Y&to=Z` | Get aggregated costs for a truck over a date range |
| POST | `/delivery-costs/calculate` | Trigger cost calculation for a completed delivery |

---

## 12. EXISTING API ENDPOINTS (REFERENCE)

These endpoints already exist in `server.py` and the driver app should call them where appropriate:

### Auth
| Method | Path | Description |
|--------|------|-------------|
| POST | `/auth/login` | Email/password login → JWT token |
| POST | `/auth/pin-login` | PIN login → JWT token |
| GET | `/auth/me` | Get current user from token |

### Dispatch (Read by Driver App)
| Method | Path | Description |
|--------|------|-------------|
| GET | `/dispatch` | All dispatched orders |
| GET | `/dispatch-planning` | Full dispatch planning board data |
| GET | `/dispatch-runsheet` | Run sheet for a truck on a date |
| GET | `/trucks` | All trucks |
| GET | `/truck-work-orders` | Truck asset work orders |
| GET | `/truck-capacity` | Truck capacity config |
| GET | `/delivery-addresses` | All delivery addresses with ETAs |
| GET | `/delivery-log` | Delivery log entries |

### Orders (Read-only for Driver)
| Method | Path | Description |
|--------|------|-------------|
| GET | `/orders` | All orders with items |
| GET | `/clients` | All clients |
| GET | `/skus` | All SKUs |

---

## 13. UI/UX REQUIREMENTS

### 13.1 Mobile-First Design
- Designed for **portrait orientation** on phone screens (375px–430px width)
- Large, thumb-friendly buttons (minimum 48px tap targets)
- Bottom tab navigation (Current Load / Upcoming Runs)
- Pull-to-refresh on all data views
- Works offline for stage tracking (sync when connection restored)

### 13.2 Stage Timer UI
- **Full-width timer display** at top: current stage name + big digital clock (MM:SS or HH:MM:SS)
- Color-coded by stage type:
  - Waiting stages: Amber/Yellow
  - Loading/unloading stages: Blue
  - Driving stages: Green
  - Break: Grey
- **Pulsing dot** animation when timer is active

### 13.3 Next Action Button
- Large pill-shaped button fixed at bottom of screen
- Shows the next logical action: "Start Loading" → "Loading Complete" → "Start Driving" etc.
- Navy background (`#07324C`) with white text
- **Red** (`#ED1C24`) for urgent actions (e.g., "Return to Depot — Overdue")
- Disabled state when no action available

### 13.4 Delivery Cards
- White card with subtle shadow
- Left colour stripe indicating status (grey=pending, blue=loaded, green=delivered)
- Client name in bold
- Address in lighter text
- Order number + SKU codes prominently displayed
- Quantity badge (e.g., "200 units")
- Estimated time badge

### 13.5 Headers & Navigation
- Top header: Hyne Pallets logo (small) + driver name + truck name
- Current time display
- Hamburger menu for: Run Sheet view, Shift Summary, Clock Off, Settings

### 13.6 Branding
- Use `#07324C` (navy) for headers, primary buttons, active tabs
- Use `#ED1C24` (red) for alerts, overdue indicators, urgent badges
- White/light grey backgrounds
- Two font sizes larger than default throughout

---

## 14. SAFETY & COMPLIANCE

### 14.1 Pre-Trip Checklist (on Clock-In)
Mandatory before any work:
- Vehicle walkaround inspection complete
- Load restraint equipment checked
- PPE (hi-vis, steel caps) worn
- Fit for duty (no fatigue, no impairment)
- First aid kit present
- Fire extinguisher present
- All checkbox items must be ticked
- Timestamp and driver ID recorded

### 14.2 Fatigue Management
- After 5 hours of continuous driving stages, prompt a mandatory break
- After 12 hours total shift time, warn that shift should end
- These thresholds should be configurable in admin

### 14.3 Incident Reporting
- "Report Incident" button accessible from hamburger menu
- Form: incident type (vehicle damage, load damage, near miss, injury, other), description, photo upload
- Creates a record linked to the current shift and delivery

---

## 15. BENCHMARKING & ANALYTICS DATA

### 15.1 Stage Time Benchmarks
Over time, the system builds a database of stage durations that management uses for:
- Average loading time per truck
- Average drive time per route (address)
- Average unloading time per customer
- Break frequency and duration
- Time waiting (non-productive) vs. time driving/loading (productive)

### 15.2 Data Available for Management Dashboard (future)
The driver app generates data that feeds into a future management reporting dashboard:
- Cost per delivery
- Cost per pallet delivered
- Truck utilisation rate (driving time / shift time)
- Driver productivity comparison
- Route efficiency (actual vs. estimated times)
- Fuel efficiency per truck
- Customer wait time (how long drivers wait at customer sites)

---

## 16. TECHNICAL BUILD INSTRUCTIONS

### 16.1 Architecture Decision
Build this as a **separate HTML file** (`static/driver.html`) in the same project, sharing the same `server.py` backend. This keeps the driver app lightweight and independently deployable while reusing the auth and data APIs.

Alternatively, if the agent prefers, build as a completely separate project with its own backend that calls the main system's API — but the single-project approach is recommended for simplicity.

### 16.2 Tech Stack (Must Match Existing)
- **Frontend**: React (via CDN Babel transform, no build step) + Tailwind CSS CDN
- **Backend**: Extend `server.py` with new endpoint handlers in the same `api_handler()` function
- **Auth**: Same JWT pattern — `Authorization: Bearer <token>` header
- **API Helper**: Same `api()` function pattern from `index.html`

### 16.3 Offline Support
- Use Service Worker for caching the app shell
- IndexedDB for storing stage transitions while offline
- Sync queue that pushes cached data to server when connection restores
- Visual indicator when offline (banner at top)

### 16.4 GPS/Location Access
- Request geolocation permission on shift clock-in
- Capture location at each stage transition
- Use `navigator.geolocation.getCurrentPosition()` for each stage start/end
- If permission denied, continue without location data (don't block workflow)

### 16.5 PWA Features
- Add `manifest.json` for "Add to Home Screen" capability
- App icon using Hyne Pallets branding
- Splash screen with logo
- Lock orientation to portrait

---

## 17. MIGRATION STRATEGY

### 17.1 Database Migrations
Add new tables via the existing `run_migrations()` function in `server.py`:
```python
def run_migrations():
    # ... existing migrations ...
    
    # Driver app tables
    c.execute("""CREATE TABLE IF NOT EXISTS driver_shifts (...)""")
    c.execute("""CREATE TABLE IF NOT EXISTS delivery_run_stages (...)""")
    c.execute("""CREATE TABLE IF NOT EXISTS truck_finance_config (...)""")
    c.execute("""CREATE TABLE IF NOT EXISTS delivery_run_costs (...)""")
    c.execute("""CREATE TABLE IF NOT EXISTS trackmyride_config (...)""")
```

### 17.2 Seed Data
- Seed `truck_finance_config` with default values for all 7 trucks
- Seed `trackmyride_config` with `is_active=0` (unconfigured by default)
- Add driver users with PIN auth:
  ```
  Leeroy → PIN: 111111, role: driver
  Usef → PIN: 222222, role: driver
  Ronny → PIN: 333333, role: driver
  Ben → PIN: 444444, role: driver
  Marcus → PIN: 555555, role: driver
  Besher → PIN: 666666, role: driver
  ```

### 17.3 Role Extension
The existing `users` table CHECK constraint on `role` needs to be extended. Since SQLite doesn't support ALTER CHECK, handle this in the migration by:
1. Creating a new column or using a migration approach that recreates the table
2. Or simply removing the CHECK constraint via table rebuild in the migration function

---

## 18. TESTING CHECKLIST

Before considering the driver app complete, verify:

- [ ] Driver can log in via PIN
- [ ] Safety checklist modal appears and blocks until completed
- [ ] Truck selection works and pre-selects default
- [ ] Shift record is created on clock-in
- [ ] Current Load tab shows correct deliveries for the selected truck/date
- [ ] Stage timer starts and stops correctly
- [ ] All stage transitions record correct timestamps and durations
- [ ] Break functionality pauses and resumes correctly
- [ ] Multi-drop runs prompt for next stop correctly
- [ ] Upcoming Runs tab shows pending deliveries
- [ ] Run sheet view calculates cumulative ETAs
- [ ] Delivery status updates reflect in the main dispatch board
- [ ] Cost calculation runs correctly on delivery completion
- [ ] Finance config is editable from admin
- [ ] TrackMyRide proxy endpoints work (when configured)
- [ ] Offline stage tracking syncs correctly on reconnection
- [ ] Clock-off calculates total hours and summarises the shift
- [ ] Data appears correctly in existing dispatch planning board
- [ ] Fatigue management alerts trigger at correct thresholds
- [ ] SKU codes / pallet codes / part numbers show on all delivery cards

---

## 19. IMPORTANT BUSINESS RULES

1. **Pallet Code / Part Number must show everywhere**: Work orders, delivery cards, and all views must display the SKU code prominently. This is a universal rule across the entire system.

2. **Week starts on Monday**: All date-based calculations (day_of_week = 0 is Monday).

3. **Docking is a mandatory gate**: WOs must be at status 'C' or beyond before they appear on the dispatch board. The driver app only sees orders that have been dispatched.

4. **Item-level pipeline independence**: Each `order_item` moves through T→C→R→P→F independently. The driver sees items at 'F' (finished) or 'dispatched' status.

5. **QA is the gate that releases dispatch from yard**: Items don't leave the yard without QA clearance.

6. **$55/hr loaded labour rate**: Default production costing rate (separate from driver wage rates).

7. **Contractor (Truck 7)**: When a contractor is assigned, their details come from `contractor_assignments` table. The driver app should support contractor login with temporary credentials.

---

## 20. SUMMARY

Build a mobile-first web app for truck drivers at Hyne Pallets that:
1. Handles shift clock-in with safety checklist and truck selection
2. Shows the day's delivery load with full order/SKU/address details
3. Tracks every stage of every delivery with a prominent timer UI
4. Automatically prompts the next action when a stage completes
5. Captures GPS location at each stage transition
6. Calculates run costs using the truck finance configuration
7. Integrates with TrackMyRide.com.au for real-time GPS (when available)
8. Works offline with sync-when-connected stage tracking
9. Feeds benchmarking data for management analytics

The app extends the existing `server.py` backend and lives alongside the existing management frontend. It must use the same tech stack (React CDN, Tailwind, Flask, SQLite, JWT auth) and follow the existing patterns in the codebase.

---

*End of Brief — Generated 2026-02-28*
