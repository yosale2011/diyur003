"""
Admin routes for DiyurCalc application.
Contains administrative functionality like payment codes management.
"""
from __future__ import annotations

from fastapi import Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from config import config
from database import get_conn
from logic import get_payment_codes
from utils import human_date, format_currency

templates = Jinja2Templates(directory=str(config.TEMPLATES_DIR))
templates.env.filters["human_date"] = human_date
templates.env.filters["format_currency"] = format_currency
templates.env.globals["app_version"] = config.VERSION


def manage_payment_codes(request: Request) -> HTMLResponse:
    """Display payment codes management page."""
    with get_conn() as conn:
        codes = get_payment_codes(conn.conn)
    return templates.TemplateResponse("payment_codes.html", {"request": request, "codes": codes})


async def update_payment_codes(request: Request) -> RedirectResponse:
    """Update payment codes from form submission."""
    form_data = await request.form()

    # Parse form data manually to gather updates by ID
    ids = set()
    for key in form_data:
        if key.startswith("display_name_"):
            ids.add(key.split("_")[-1])

    with get_conn() as conn:
        for code_id in ids:
            display_name = form_data.get(f"display_name_{code_id}")
            merav_code = form_data.get(f"merav_code_{code_id}")
            display_order = form_data.get(f"display_order_{code_id}")
            icon = form_data.get(f"icon_{code_id}", "")

            if display_name:
                conn.execute("""
                    UPDATE payment_codes
                    SET display_name = %s, merav_code = %s, display_order = %s, icon = %s
                    WHERE id = %s
                """, (display_name, merav_code, display_order, icon, code_id))
        conn.commit()

    return RedirectResponse(url="/admin/payment-codes", status_code=303)