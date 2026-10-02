"""
Shopify -> TCS auto-fulfillment service.

Flow:
  1. Shopify sends a webhook the instant a new order is created (orders/create).
     We verify it's genuinely from Shopify (HMAC check), then book a TCS
     shipment for it and mark it as fulfilled in Shopify.
  2. Because a webhook can be missed (server briefly down, Shopify's retry
     window expires, etc.), a periodic catch-up job independently asks
     Shopify "which orders are still unfulfilled?" and processes any that
     slipped through - so downtime never silently loses an order.
  3. A tiny local JSON file tracks which order IDs we've already booked,
     so the catch-up job (and any duplicate webhook delivery, which Shopify
     explicitly warns can happen) never double-books a shipment.

TCS booking itself is stubbed out in book_tcs_shipment() - fill that in once
real TCS API credentials/docs are available. Until then it logs what it
would have sent, so the rest of the pipeline (webhook, catch-up, dedup,
marking fulfilled) can be fully tested end-to-end right now.
"""
import hashlib
import hmac
import base64
import json
import logging
import os
import threading
import time
from pathlib import Path

import requests
from flask import Flask, request, abort

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("tcs-fulfillment")

app = Flask(__name__)

# ---------------------------------------------------------------- config
# e.g. yourstore.myshopify.com - tolerate a pasted URL (scheme / trailing slash)
SHOPIFY_STORE_DOMAIN = (
    os.environ["SHOPIFY_STORE_DOMAIN"].strip()
    .removeprefix("https://").removeprefix("http://").strip("/")
)
# Dev Dashboard apps: client id/secret are exchanged for a 24h access token.
SHOPIFY_CLIENT_ID = os.environ.get("SHOPIFY_CLIENT_ID", "")
SHOPIFY_CLIENT_SECRET = os.environ.get("SHOPIFY_CLIENT_SECRET", "")
# Optional: a legacy static token (shpat_...). If set, it is used instead.
SHOPIFY_ADMIN_TOKEN = os.environ.get("SHOPIFY_ADMIN_API_TOKEN", "")
# Webhooks from an app are signed with the app's client secret.
SHOPIFY_WEBHOOK_SECRET = os.environ.get("SHOPIFY_WEBHOOK_SECRET") or SHOPIFY_CLIENT_SECRET
SHOPIFY_API_VERSION = os.environ.get("SHOPIFY_API_VERSION", "2026-07")
# Public URL of this service (e.g. https://tcs-fulfillment.onrender.com).
# When set, the orders/create webhook is registered automatically on startup.
APP_BASE_URL = os.environ.get("APP_BASE_URL", "").rstrip("/")

TCS_API_USER = os.environ.get("TCS_API_USER", "")
TCS_API_KEY = os.environ.get("TCS_API_KEY", "")
TCS_API_URL = os.environ.get("TCS_API_URL", "")  # set once TCS gives you their endpoint

# Off by default: simulated TCS bookings are logged but never fulfil real orders.
# Set to "true" only to test the full pipeline on a test order (and use your own email).
ALLOW_SIMULATED_FULFILLMENT = os.environ.get("ALLOW_SIMULATED_FULFILLMENT", "").lower() == "true"

CATCH_UP_INTERVAL_SECONDS =int(os.environ.get("CATCH_UP_INTERVAL_SECONDS", "900"))  # 15 min
PROCESSED_ORDERS_FILE = Path(os.environ.get("PROCESSED_ORDERS_FILE", "processed_orders.json"))

SHOPIFY_BASE = f"https://{SHOPIFY_STORE_DOMAIN}/admin/api/{SHOPIFY_API_VERSION}"

_token_cache = {"token": "", "expires_at": 0.0}
_token_lock = threading.Lock()


def get_access_token() -> str:
    """Static token if configured, otherwise a client-credentials token that
    is cached and refreshed shortly before its 24h expiry."""
    if SHOPIFY_ADMIN_TOKEN:
        return SHOPIFY_ADMIN_TOKEN
    with _token_lock:
        if _token_cache["token"] and time.time() < _token_cache["expires_at"] - 300:
            return _token_cache["token"]
        resp = requests.post(
            f"https://{SHOPIFY_STORE_DOMAIN}/admin/oauth/access_token",
            data={
                "grant_type": "client_credentials",
                "client_id": SHOPIFY_CLIENT_ID,
                "client_secret": SHOPIFY_CLIENT_SECRET,
            },
            timeout=(10, 30),
        )
        resp.raise_for_status()
        data = resp.json()
        _token_cache["token"] = data["access_token"]
        _token_cache["expires_at"] = time.time() + int(data.get("expires_in", 86399))
        log.info("Fetched a new Shopify access token")
        return _token_cache["token"]


def shopify_headers() -> dict:
    return {
        "X-Shopify-Access-Token": get_access_token(),
        "Content-Type": "application/json",
    }


def ensure_webhook_registered() -> None:
    """Create the orders/create webhook pointing at this service if missing."""
    if not APP_BASE_URL:
        log.info("APP_BASE_URL not set - skipping automatic webhook registration")
        return
    address = f"{APP_BASE_URL}/webhooks/orders-create"
    existing = requests.get(
        f"{SHOPIFY_BASE}/webhooks.json", headers=shopify_headers(),
        params={"topic": "orders/create"}, timeout=(10, 30),
    )
    existing.raise_for_status()
    if any(w.get("address") == address for w in existing.json().get("webhooks", [])):
        log.info("Webhook already registered at %s", address)
        return
    resp = requests.post(
        f"{SHOPIFY_BASE}/webhooks.json", headers=shopify_headers(),
        json={"webhook": {"topic": "orders/create", "address": address, "format": "json"}},
        timeout=(10, 30),
    )
    resp.raise_for_status()
    log.info("Registered orders/create webhook at %s", address)

# Counters only (no order or customer data) so /status is safe to expose.
status = {
    "commit": os.environ.get("RENDER_GIT_COMMIT", "")[:7],
    "started_at": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
    "stage": "catch-up thread not started yet",
    "webhooks_received": 0,
    "webhooks_rejected_bad_hmac": 0,
    "last_webhook_at": None,
    "last_catch_up_at": None,
    "last_catch_up_found": None,
    "last_error": None,
    "orders_booked_simulated_dry_run": 0,
    "orders_fulfilled": 0,
    "order_failures": 0,
    "last_order_customer_fields_present": None,
    "dry_run": not ALLOW_SIMULATED_FULFILLMENT,
    "tcs_connected": bool(TCS_API_URL),
}


def _now() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())


_lock = threading.Lock()  # guards processed-orders file from concurrent webhook + catch-up writes


# ---------------------------------------------------------- processed-order tracking
def _load_processed() -> set:
    if not PROCESSED_ORDERS_FILE.exists():
        return set()
    try:
        return set(json.loads(PROCESSED_ORDERS_FILE.read_text()))
    except Exception:
        log.exception("Could not read %s - treating as empty", PROCESSED_ORDERS_FILE)
        return set()


def _save_processed(order_ids: set) -> None:
    PROCESSED_ORDERS_FILE.write_text(json.dumps(sorted(order_ids)))


def already_processed(order_id) -> bool:
    with _lock:
        return str(order_id) in _load_processed()


def mark_processed(order_id) -> None:
    with _lock:
        ids = _load_processed()
        ids.add(str(order_id))
        _save_processed(ids)


# ---------------------------------------------------------------- Shopify helpers
def verify_shopify_webhook(raw_body: bytes, hmac_header: str) -> bool:
    digest = hmac.new(SHOPIFY_WEBHOOK_SECRET.encode(), raw_body, hashlib.sha256).digest()
    computed = base64.b64encode(digest).decode()
    # constant-time compare - a plain == here would leak timing info about the secret
    return hmac.compare_digest(computed, hmac_header or "")


def fetch_unfulfilled_orders(days_back: int = 3) -> list:
    """Ask Shopify directly for recent unfulfilled orders - this is what makes
    recovery after downtime independent of whether Shopify's webhook retry
    window already expired."""
    since = time.strftime("%Y-%m-%dT%H:%M:%S%z", time.gmtime(time.time() - days_back * 86400))
    resp = requests.get(
        f"{SHOPIFY_BASE}/orders.json",
        headers=shopify_headers(),
        params={
            "status": "open",
            "fulfillment_status": "unfulfilled",
            "created_at_min": since,
            "limit": 250,
        },
        timeout=(10, 30),
    )
    resp.raise_for_status()
    return resp.json().get("orders", [])


def mark_order_fulfilled(order_id, tracking_number: str, tracking_url: str = "") -> None:
    # Shopify's fulfillment API needs the fulfillment order id, not the order id directly.
    fo_resp = requests.get(
        f"{SHOPIFY_BASE}/orders/{order_id}/fulfillment_orders.json",
        headers=shopify_headers(), timeout=(10, 30),
    )
    fo_resp.raise_for_status()
    fulfillment_orders = fo_resp.json().get("fulfillment_orders", [])
    if not fulfillment_orders:
        log.warning("Order %s has no fulfillment orders (already fulfilled/cancelled?)", order_id)
        return

    payload = {
        "fulfillment": {
            "line_items_by_fulfillment_order": [
                {"fulfillment_order_id": fo["id"]} for fo in fulfillment_orders
            ],
            "tracking_info": {
                "company": "TCS",
                "number": tracking_number,
                "url": tracking_url,
            },
            "notify_customer": True,
        }
    }
    resp = requests.post(
        f"{SHOPIFY_BASE}/fulfillments.json",
        headers=shopify_headers(), json=payload, timeout=(10, 30),
    )
    resp.raise_for_status()
    log.info("Order %s marked fulfilled in Shopify (tracking %s)", order_id, tracking_number)


# ---------------------------------------------------------------- TCS booking (STUB)
def book_tcs_shipment(order: dict) -> dict:
    """
    Books a TCS shipment for one Shopify order and returns
    {"tracking_number": ..., "tracking_url": ...}.

    STUB: replace the body of this function with the real TCS API call once
    credentials/docs are available. Shape below follows TCS's typical
    consignment-booking fields (consignee name/address/phone, COD amount,
    pieces, weight) - adjust field names to match their actual API once you
    have the docs; the rest of the pipeline doesn't care how this function
    gets its result.
    """
    shipping = order.get("shipping_address") or {}
    consignee_name = f"{shipping.get('first_name','')} {shipping.get('last_name','')}".strip()
    cod_amount = order.get("total_price") if order.get("financial_status") != "paid" else "0"

    if not TCS_API_URL:
        # No real endpoint configured yet - log what WOULD be sent and return a
        # fake tracking number so the rest of the flow (marking fulfilled,
        # dedup) can be exercised end-to-end before TCS access exists.
        fake_tracking = f"FAKE-{order['id']}"
        log.warning(
            "TCS_API_URL not set - SIMULATING booking for order %s: consignee=%s, "
            "address=%s, phone=%s, COD=%s, pieces=%s -> fake tracking %s",
            order["id"], consignee_name, shipping.get("address1"), shipping.get("phone"),
            cod_amount, len(order.get("line_items", [])), fake_tracking,
        )
        return {"tracking_number": fake_tracking, "tracking_url": "", "simulated": True}

    payload = {
        "consignee_name": consignee_name,
        "consignee_address": f"{shipping.get('address1','')} {shipping.get('address2','')}".strip(),
        "consignee_city": shipping.get("city", ""),
        "consignee_phone": shipping.get("phone", ""),
        "cod_amount": cod_amount,
        "pieces": len(order.get("line_items", [])) or 1,
        "reference": order.get("name", str(order["id"])),
        # TODO: add origin/shipper fields TCS requires, weight, service type etc.
        # once their API docs are in hand.
    }
    resp = requests.post(
        TCS_API_URL,
        auth=(TCS_API_USER, TCS_API_KEY),
        json=payload,
        timeout=(10, 30),
    )
    resp.raise_for_status()
    data = resp.json()
    # TODO: adjust these field names to match TCS's actual response shape.
    return {"tracking_number": data["tracking_number"], "tracking_url": data.get("tracking_url", "")}


# ---------------------------------------------------------------- core processing
def process_order(order: dict) -> None:
    order_id = order["id"]
    if already_processed(order_id):
        log.info("Order %s already processed, skipping", order_id)
        return
    if order.get("fulfillment_status") == "fulfilled":
        mark_processed(order_id)
        return

    log.info("Processing new order %s (%s)", order_id, order.get("name"))
    shipping = order.get("shipping_address") or {}
    status["last_order_customer_fields_present"] = bool(
        shipping.get("first_name") and shipping.get("address1") and shipping.get("phone")
    )
    try:
        booking = book_tcs_shipment(order)
        if booking.get("simulated") and not ALLOW_SIMULATED_FULFILLMENT:
            # Safety: without real TCS access, never fulfil a real order with a
            # fake tracking number (the customer would be emailed it).
            log.warning(
                "DRY RUN: order %s (%s) NOT fulfilled in Shopify - TCS is not connected yet",
                order_id, order.get("name"),
            )
            status["orders_booked_simulated_dry_run"] += 1
            mark_processed(order_id)
            return
        mark_order_fulfilled(order_id, booking["tracking_number"], booking.get("tracking_url", ""))
        status["orders_fulfilled"] += 1
        mark_processed(order_id)
    except Exception as e:
        status["order_failures"] += 1
        status["last_error"] = f"{_now()} order failure: {type(e).__name__}"
        log.exception("Failed to process order %s - will retry on next catch-up pass", order_id)
        # deliberately NOT marked processed, so the catch-up loop retries it


# ---------------------------------------------------------------- webhook endpoint
@app.route("/webhooks/orders-create", methods=["POST"])
def orders_create_webhook():
    raw_body = request.get_data()
    hmac_header = request.headers.get("X-Shopify-Hmac-Sha256", "")
    if not verify_shopify_webhook(raw_body, hmac_header):
        status["webhooks_rejected_bad_hmac"] += 1
        status["last_webhook_at"] = _now()
        log.warning("Rejected webhook with invalid HMAC signature")
        abort(401)

    status["webhooks_received"] += 1
    status["last_webhook_at"] = _now()
    order = json.loads(raw_body)
    # Respond fast; Shopify expects a quick 200 or it treats it as a failed
    # delivery and queues a retry even though we did receive it.
    threading.Thread(target=process_order, args=(order,), daemon=True).start()
    return "", 200


@app.route("/", methods=["GET"])
def home():
    return "TCS auto-fulfillment service is running.", 200


@app.route("/status", methods=["GET"])
def status_page():
    return dict(status, now=_now()), 200


@app.route("/health", methods=["GET"])
def health():
    return {"status": "ok"}, 200


# ---------------------------------------------------------------- catch-up loop
def catch_up_loop():
    webhook_ok = False
    status["stage"] = "catch-up thread started"
    while True:
        if not webhook_ok:
            try:
                status["stage"] = "registering webhook"
                ensure_webhook_registered()
                webhook_ok = True
            except Exception as e:
                status["last_error"] = f"{_now()} webhook registration: {type(e).__name__}: {str(e)[:150]}"
                log.exception("Webhook registration failed - will retry next pass")
        try:
            status["stage"] = "fetching orders"
            orders = fetch_unfulfilled_orders()
            status["last_catch_up_at"] = _now()
            status["last_catch_up_found"] = len(orders)
            log.info("Catch-up pass: %d unfulfilled order(s) found", len(orders))
            for order in orders:
                process_order(order)
        except Exception as e:
            status["last_catch_up_at"] = _now()
            status["last_error"] = f"{_now()} catch-up: {type(e).__name__}: {str(e)[:150]}"
            log.exception("Catch-up pass failed")
        status["stage"] = "sleeping until next pass"
        time.sleep(CATCH_UP_INTERVAL_SECONDS)


threading.Thread(target=catch_up_loop, daemon=True).start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
