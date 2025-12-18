"""
Refactored main application file for DiyurCalc.
Uses modular structure with separate route handlers.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from config import config
from logic import (
    calculate_person_monthly_totals,
)
from utils import human_date
from utils import calculate_accruals, format_currency
from routes.home import home
from routes.guide import simple_summary_view, guide_view
from routes.admin import manage_payment_codes, update_payment_codes
from routes.summary import general_summary
from routes.export import (
    export_gesher,
    export_gesher_person,
    export_gesher_preview,
    export_excel,
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# FastAPI app setup
app = FastAPI(title="ניהול משמרות בענן")
templates = Jinja2Templates(directory=str(config.TEMPLATES_DIR))
templates.env.filters["human_date"] = human_date
templates.env.filters["format_currency"] = format_currency

@app.get("/debug/filters")
def debug_filters():
    """Debug endpoint to check if filters are registered."""
    return {
        "format_currency_registered": "format_currency" in templates.env.filters,
        "human_date_registered": "human_date" in templates.env.filters,
        "available_filters": list(templates.env.filters.keys())
    }

# Route registrations
@app.get("/health")
def health_check():
    return {"status": "ok"}


@app.get("/", response_class=HTMLResponse)
def home_route(request: Request, month: int | None = None, year: int | None = None, q: str | None = None):
    """Home page route."""
    return home(request, month, year, q)


@app.get("/guide/{person_id}/simple", response_class=HTMLResponse)
def simple_summary_route(request: Request, person_id: int, month: int | None = None, year: int | None = None):
    """Simple summary view for a guide."""
    return simple_summary_view(request, person_id, month, year)


@app.get("/guide/{person_id}", response_class=HTMLResponse)
def guide_route(request: Request, person_id: int, month: int | None = None, year: int | None = None):
    """Detailed guide view."""
    return guide_view(request, person_id, month, year)


@app.get("/admin/payment-codes", response_class=HTMLResponse)
def manage_payment_codes_route(request: Request):
    """Payment codes management page."""
    return manage_payment_codes(request)


@app.post("/admin/payment-codes/update")
async def update_payment_codes_route(request: Request):
    """Update payment codes."""
    return await update_payment_codes(request)


@app.get("/summary", response_class=HTMLResponse)
def general_summary_route(request: Request, year: int = None, month: int = None):
    """General monthly summary."""
    return general_summary(request, year, month)


@app.get("/export/gesher")
def export_gesher_route(year: int, month: int, company: str = None, filter_name: str = None, encoding: str = "ascii"):
    """Export Gesher file by company."""
    return export_gesher(year, month, company, filter_name, encoding)


@app.get("/export/gesher/person/{person_id}")
def export_gesher_person_route(person_id: int, year: int, month: int, encoding: str = "ascii"):
    """Export Gesher file for individual person."""
    return export_gesher_person(person_id, year, month, encoding)


@app.get("/export/gesher/preview")
def export_gesher_preview_route(request: Request, year: int = None, month: int = None, show_zero: str = None):
    """Gesher export preview."""
    return export_gesher_preview(request, year, month, show_zero)


@app.get("/export/excel")
def export_excel_route(year: int = None, month: int = None):
    """Export monthly summary to Excel."""
    return export_excel(year, month)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app:app",
        host=config.HOST,
        port=config.PORT,
        reload=config.DEBUG
    )
