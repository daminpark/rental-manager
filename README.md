# Rental Manager

Property management system for 193 & 195 Vauxhall Bridge Road. Manages Z-Wave lock codes for 22 Yale Keyless Connected locks across two houses based on HostTools calendar bookings.

## Features

- **Automatic code management**: Generates 4-digit codes from guest phone numbers and sets them on locks at check-in, clears at check-out
- **22 Z-Wave locks** across 2 houses (11 each)
- **19 calendars** from HostTools (9 per house + 1 shared for both houses)
- **Uniform slot allocation**: Same slot numbers mean the same calendar across all locks
- **Manual time overrides**: Adjust code activation times in advance for early check-in/late checkout
- **Master and emergency codes**: Set across all locks with one action
- **Retry logic**: Automatically retries failed code syncs with exponential backoff
- **Web dashboard**: Monitor and control all locks from a single interface

## Quick Start

1. Copy `.env.example` to `.env` and configure your Home Assistant connections:
   ```bash
   cp .env.example .env
   ```

2. Edit `.env` with your Home Assistant URLs and long-lived access tokens

3. Run with Docker Compose:
   ```bash
   docker-compose up -d
   ```

4. Open http://localhost:8000 in your browser

## Architecture

```
Rental Manager Server
├── Web Dashboard (/)
├── REST API (/api)
├── Core Logic
│   ├── iCal fetcher (polls HostTools every 2 min)
│   ├── Code generator (last 4 digits of phone)
│   ├── Slot allocator (uniform across locks)
│   └── Retry state machine
└── Home Assistant Communication (2 instances)
```

## Slot Allocation

Every lock uses the same slot numbers for the same calendars:

| Slot | Calendar |
|------|----------|
| 1 | Master code |
| 2-3 | Room 1 |
| 4-5 | Room 2 |
| 6-7 | Room 3 |
| 8-9 | Room 4 |
| 10-11 | Room 5 |
| 12-13 | Room 6 |
| 14-15 | Suite A |
| 16-17 | Suite B |
| 18-19 | Whole home (shared with 193195vbr) |
| 20 | Emergency code |

## Default Timing

| Lock Type | Code Activates | Code Deactivates |
|-----------|---------------|------------------|
| Room | 12:00 | 11:00 (next day) |
| Bathroom | 15:00 | 11:00 (next day) |
| Kitchen | 15:00 | 11:00 (next day) |
| Front door | 11:00 | 14:00 |
| Storage | 01:00 | 23:59 |

## API Endpoints

- `GET /api/health` - Health check
- `GET /api/locks` - List all locks
- `POST /api/locks/{id}/action` - Lock/unlock
- `POST /api/codes/master` - Set master code
- `POST /api/codes/emergency` - Set emergency code
- `GET /api/bookings` - List bookings
- `POST /api/bookings/{id}/time-override` - Set time override
- `GET /api/calendars` - List calendars
- `PUT /api/calendars/{id}/url` - Update calendar URL
- `POST /api/calendars/refresh` - Force calendar refresh

## Development

```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # or .venv\Scripts\activate on Windows

# Install dependencies
pip install -e ".[dev]"

# Run in development mode
RENTAL_DEBUG=true python -m rental_manager.main
```

## Configuration

Environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| RENTAL_DATABASE_URL | sqlite+aiosqlite:///./rental_manager.db | Database connection |
| RENTAL_HOST | 0.0.0.0 | Server host |
| RENTAL_PORT | 8000 | Server port |
| RENTAL_DEBUG | false | Enable debug mode |
| RENTAL_CALENDAR_POLL_INTERVAL | 120 | Seconds between calendar polls |
| RENTAL_CODE_SYNC_TIMEOUT_SECONDS | 120 | Timeout before retry |
| RENTAL_CODE_SYNC_MAX_RETRIES | 3 | Max retry attempts |
| RENTAL_HA_195_URL | | Home Assistant 195 URL |
| RENTAL_HA_195_TOKEN | | Home Assistant 195 token |
| RENTAL_HA_193_URL | | Home Assistant 193 URL |
| RENTAL_HA_193_TOKEN | | Home Assistant 193 token |
