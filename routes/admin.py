"""
Admin routes for DiyurCalc application.
Contains administrative functionality like payment codes management.
"""
from __future__ import annotations

import logging

from fastapi import Request
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from config import config
from database import get_conn
from logic import get_payment_codes
from utils import human_date, format_currency
from db_sync import sync_database, check_demo_database_status

logger = logging.getLogger(__name__)

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
    try:
        form_data = await request.form()

        # Parse form data manually to gather updates by ID
        ids = set()
        for key in form_data:
            if key.startswith("display_name_"):
                ids.add(key.split("_")[-1])

        with get_conn() as conn:
            for code_id in ids:
                # Get form values, handling None/empty cases
                display_name = form_data.get(f"display_name_{code_id}")
                merav_code = form_data.get(f"merav_code_{code_id}")
                display_order_raw = form_data.get(f"display_order_{code_id}")

                # Convert display_order to integer or None
                display_order = None
                if display_order_raw:
                    try:
                        display_order = int(display_order_raw)
                    except (ValueError, TypeError):
                        display_order = None

                # Ensure string values are not None
                display_name = display_name or ""
                merav_code = merav_code or ""

                # Only update if we have a display_name
                if display_name:
                    conn.execute("""
                        UPDATE payment_codes
                        SET display_name = %s, merav_code = %s, display_order = %s
                        WHERE id = %s
                    """, (display_name, merav_code, display_order, code_id))
            conn.commit()

        return RedirectResponse(url="/admin/payment-codes", status_code=303)
    except Exception as e:
        # Log the error and re-raise for FastAPI to handle
        logger.error(f"Error updating payment codes: {e}", exc_info=True)
        raise


def demo_sync_page(request: Request) -> HTMLResponse:
    """Display demo database sync page."""
    status = check_demo_database_status()
    return templates.TemplateResponse("demo_sync.html", {
        "request": request,
        "demo_status": status
    })


async def sync_demo_database(request: Request):
    """Sync demo database with production data using Server-Sent Events for progress."""
    from fastapi.responses import StreamingResponse
    import json

    async def generate_progress():
        progress_data = {"current": 0, "total": 0, "message": ""}

        def progress_callback(step, total, message):
            progress_data["current"] = step
            progress_data["total"] = total
            progress_data["message"] = message

        # Send initial message
        yield f"data: {json.dumps({'type': 'start', 'message': 'מתחיל סנכרון...'})}\n\n"

        try:
            # Run sync with progress callback
            import threading
            result_holder = [None]
            error_holder = [None]

            def run_sync():
                try:
                    result_holder[0] = sync_database(progress_callback)
                except Exception as e:
                    error_holder[0] = e

            sync_thread = threading.Thread(target=run_sync)
            sync_thread.start()

            import asyncio
            last_step = -1
            while sync_thread.is_alive():
                if progress_data["current"] != last_step:
                    last_step = progress_data["current"]
                    yield f"data: {json.dumps({'type': 'progress', 'current': progress_data['current'], 'total': progress_data['total'], 'message': progress_data['message']})}\n\n"
                await asyncio.sleep(0.1)

            sync_thread.join()

            if error_holder[0]:
                raise error_holder[0]

            result = result_holder[0]

            if result["success"]:
                tables = result["tables_synced"]
                rows = result["total_rows"]
                msg = f"הסנכרון הושלם בהצלחה! {tables} טבלאות, {rows} שורות"
                data = {"type": "complete", "success": True, "message": msg, "details": result}
                yield f"data: {json.dumps(data)}\n\n"
            else:
                data = {"type": "complete", "success": False, "message": "הסנכרון הושלם עם שגיאות", "details": result}
                yield f"data: {json.dumps(data)}\n\n"

        except Exception as e:
            logger.error(f"Error syncing demo database: {e}", exc_info=True)
            yield f"data: {json.dumps({'type': 'error', 'message': f'שגיאה בסנכרון: {str(e)}'})}\n\n"

    return StreamingResponse(
        generate_progress(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        }
    )


def demo_sync_status(request: Request) -> JSONResponse:
    """Get demo database status."""
    status = check_demo_database_status()
    return JSONResponse(status)