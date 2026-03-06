# Hyne Pallets QLD — EPC v2.3

Timber inventory, production planning, and dispatch management system for Hyne Pallets Queensland.

## Stack
- **Backend**: Python / Flask / SQLite
- **Frontend**: React (CDN) / Single-page apps
- **Deploy**: Railway (auto-deploy from GitHub)

## Apps
| Path | Purpose |
|------|---------|
| `/` | Office app — orders, planning, ops dashboard, dispatch |
| `/floor` | Floor tablet — production scanning and job tracking |
| `/driver` | Driver mobile — delivery runs and proof of delivery |
| `/chainsaw` | Chainsaw station — docking and crosscut operations |
| `/receiving` | Receiving — inbound timber and stock receipts |

## Environment Variables
| Variable | Required | Default |
|----------|----------|---------|
| `PORT` | No | `8080` |
| `JWT_SECRET` | Recommended | `hyne_pallets_secret_2026_CHANGE_ME` |

## Deploy to Railway
1. Connect this repo to a Railway project
2. Set `JWT_SECRET` environment variable
3. Railway auto-detects `Procfile` and deploys

## Default Login
- **Office**: tim@hynepallets.com.au / admin123 (executive)
- **Floor**: bob.floor1 / PIN: 123456

## Version
- v2.3.0 — Kanban pipeline, docking flow, docking log, audit hardening
