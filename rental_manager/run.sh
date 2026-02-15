#!/usr/bin/with-contenv bashio

# Read options from HA add-on config
export RENTAL_HOUSE_CODE="$(bashio::config 'house_code')"
export RENTAL_CALENDAR_POLL_INTERVAL="$(bashio::config 'calendar_poll_interval')"
export RENTAL_CODE_SYNC_TIMEOUT_SECONDS="$(bashio::config 'code_sync_timeout_seconds')"
export RENTAL_CODE_SYNC_MAX_RETRIES="$(bashio::config 'code_sync_max_retries')"

# HA Supervisor API — auto-injected by Supervisor
export RENTAL_HA_URL="http://supervisor/core/api"
export RENTAL_HA_TOKEN="${SUPERVISOR_TOKEN}"

# Database in persistent /data directory
export RENTAL_DATABASE_URL="sqlite+aiosqlite:///data/rental_manager.db"

bashio::log.info "Starting Rental Manager for house ${RENTAL_HOUSE_CODE}..."

exec python -m rental_manager.main
