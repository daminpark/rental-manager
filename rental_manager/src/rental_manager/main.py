"""Main application entry point."""

import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from pydantic import BaseModel
from starlette.middleware.base import BaseHTTPMiddleware

from rental_manager.api.routes import router as api_router, set_manager
from rental_manager.config import settings
from rental_manager.core.manager import RentalManager
from rental_manager.db.database import init_db

# Get the web directory path
WEB_DIR = Path(__file__).parent / "web"
STATIC_DIR = WEB_DIR / "static"
TEMPLATES_DIR = WEB_DIR / "templates"

# Configure logging
logging.basicConfig(
    level=logging.DEBUG if settings.debug else logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Global manager instance
manager: RentalManager | None = None


class IngressMiddleware(BaseHTTPMiddleware):
    """Middleware to handle HA ingress path rewriting.

    When running behind HA ingress, the X-Ingress-Path header contains
    the base path (e.g., /api/hassio_ingress/abc123). We strip this
    prefix from the request path so our routes match correctly.
    """

    async def dispatch(self, request: Request, call_next):
        ingress_path = request.headers.get("X-Ingress-Path", "")
        if ingress_path and request.url.path.startswith(ingress_path):
            # Strip the ingress prefix
            new_path = request.url.path[len(ingress_path):] or "/"
            request.scope["path"] = new_path
        return await call_next(request)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    global manager

    logger.info("Starting rental manager application for house %s...", settings.house_code)

    # Initialize database
    await init_db()

    # Create and initialize manager
    manager = RentalManager(settings)
    await manager.initialize()
    set_manager(manager)

    # Start the manager
    await manager.start()

    logger.info("Application started successfully")

    yield

    # Shutdown
    logger.info("Shutting down rental manager application...")
    if manager:
        await manager.stop()
    logger.info("Application shutdown complete")


app = FastAPI(
    title="Rental Manager",
    description=f"Lock code manager for {settings.house_code} Vauxhall Bridge Road",
    version="0.1.0",
    lifespan=lifespan,
)

# Add ingress middleware (must be before other middleware)
app.add_middleware(IngressMiddleware)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount API routes
app.include_router(api_router, prefix="/api")

# Mount static files
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# Dashboard endpoint
@app.get("/", response_class=HTMLResponse)
async def dashboard():
    """Serve the dashboard."""
    index_path = TEMPLATES_DIR / "index.html"
    return FileResponse(index_path)


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard_redirect():
    """Serve the dashboard."""
    index_path = TEMPLATES_DIR / "index.html"
    return FileResponse(index_path)


class LockEventPayload(BaseModel):
    """Payload from HA automation for lock events."""

    entity_id: str
    code_slot: Optional[int] = None
    event_type: Optional[str] = None
    event_label: Optional[str] = None
    timestamp: Optional[str] = None


@app.post("/webhooks/lock-event")
async def webhook_lock_event(payload: LockEventPayload):
    """Receive lock unlock events from HA automation.

    This endpoint is mounted outside /api so HA automations can
    POST to it without needing the ingress path.
    """
    if not manager:
        return JSONResponse(status_code=503, content={"error": "Manager not initialized"})

    # Parse timestamp if provided
    ts = None
    if payload.timestamp:
        try:
            ts = datetime.fromisoformat(payload.timestamp.replace("Z", "+00:00"))
        except ValueError:
            pass

    # Determine method from event label
    method = "unknown"
    label = (payload.event_label or "").lower()
    if "keypad" in label:
        method = "keypad"
    elif "manual" in label or "thumb" in label:
        method = "manual"
    elif "auto" in label:
        method = "auto_lock"
    elif "rf" in label or "remote" in label:
        method = "rf"

    result = await manager.record_unlock_event(
        entity_id=payload.entity_id,
        code_slot=payload.code_slot,
        method=method,
        timestamp=ts,
        raw_details=json.dumps(payload.model_dump(), default=str),
    )
    return result


def main():
    """Run the application."""
    import uvicorn

    uvicorn.run(
        "rental_manager.main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.debug,
    )


if __name__ == "__main__":
    main()
