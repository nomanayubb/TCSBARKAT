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
SHOPIFY_STORE_DOMAIN = os.environ["SHOPIFY_STORE_DOMAIN"]          # e.g. yourstore.myshopify.com
SHOPIFY_ADMIN_TOKEN = os.environ["SHOPIFY_ADMIN_API_TOKEN"]        # shpat_...
SHOPIFY_WEBHOOK_SECRET = os.environ["SHOPIFY_WEBHOOK_SECRET"]      # from the webhook's config
SHOPIFY_API_VERSION = os.environ.get("SHOPIFY_API_VERSION", "2024-10")

TCS_API_USER = os.environ.get("TCS_API_USER", "")
TCS_API_KEY = os.environ.get("TCS_API_KEY", "")
TCS_API_URL = os.environ.get("TCS_API_URL", "")  # set once TCS gives you their endpoint

CATCH_UP_INTERVAL_SECONDS = int(os.environ.get("CATCH_UP_INTERVAL_SECONDS", "900"))  # 15 min
PROCESSED_ORDERS_FILE = Path(os.environ.get("PROCESSED_ORDERS_FILE", "processed_orders.json"))

SHOPIFY_BASE = f"https://{SHOPIFY_STORE_DOMAIN}/admin/api/{SHOPIFY_API_VERSION}"
SHOPIFY_HEADERS = {
    "X-Shopify-Access-Token": SHOPIFY_ADMIN_TOKEN,
    "Content-Type": "application/json",
}

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
        headers=SHOPIFY_HEADERS,
        params={
            "status": "open",
            "fulfillment_status": "unfulfilled",
            "created_at_min": since,
            "limit": 250,
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json().get("orders", [])


def mark_order_fulfilled(order_id, tracking_number: str, tracking_url: str = "") -> None:
    # Shopify's fulfillment API needs the fulfillment order id, not the order id directly.
    fo_resp = requests.get(
        f"{SHOPIFY_BASE}/orders/{order_id}/fulfillment_orders.json",
        headers=SHOPIFY_HEADERS, timeout=30,
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
        headers=SHOPIFY_HEADERS, json=payload, timeout=30,
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
        return {"tracking_number": fake_tracking, "tracking_url": ""}

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
        timeout=30,
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
    try:
        booking = book_tcs_shipment(order)
        mark_order_fulfilled(order_id, booking["tracking_number"], booking.get("tracking_url", ""))
        mark_processed(order_id)
    except Exception:
        log.exception("Failed to process order %s - will retry on next catch-up pass", order_id)
        # deliberately NOT marked processed, so the catch-up loop retries it


# ---------------------------------------------------------------- webhook endpoint
@app.route("/webhooks/orders-create", methods=["POST"])
def orders_create_webhook():
    raw_body = request.get_data()
    hmac_header = request.headers.get("X-Shopify-Hmac-Sha256", "")
    if not verify_shopify_webhook(raw_body, hmac_header):
        log.warning("Rejected webhook with invalid HMAC signature")
        abort(401)

    order = json.loads(raw_body)
    # Respond fast; Shopify expects a quick 200 or it treats it as a failed
    # delivery and queues a retry even though we did receive it.
    threading.Thread(target=process_order, args=(order,), daemon=True).start()
    return "", 200


@app.route("/health", methods=["GET"])
def health():
    return {"status": "ok"}, 200


# ---------------------------------------------------------------- catch-up loop
def catch_up_loop():
    while True:
        try:
            orders = fetch_unfulfilled_orders()
            log.info("Catch-up pass: %d unfulfilled order(s) found", len(orders))
            for order in orders:
                process_order(order)
        except Exception:
            log.exception("Catch-up pass failed")
        time.sleep(CATCH_UP_INTERVAL_SECONDS)


threading.Thread(target=catch_up_loop, daemon=True).start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
