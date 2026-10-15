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
import html
import base64
import json
import logging
import os
import re
import threading
import time
import urllib.parse
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests
from flask import Flask, request, abort, jsonify

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

# TCS COD API (see COD-API-UserManual.pdf). Sandbox unless TCS_ENV=production.
TCS_ENV = os.environ.get("TCS_ENV", "sandbox").strip().lower()
TCS_CLIENT_ID = os.environ.get("TCS_CLIENT_ID", "")
TCS_CLIENT_SECRET = os.environ.get("TCS_CLIENT_SECRET", "")
TCS_USERNAME = os.environ.get("TCS_USERNAME", "")
TCS_PASSWORD = os.environ.get("TCS_PASSWORD", "")
TCS_ACCOUNT_NO = os.environ.get("TCS_ACCOUNT_NO", "")
# Customer number for the report API; defaults to the account number.
TCS_CUSTOMER_NO = os.environ.get("TCS_CUSTOMER_NO", "")
TCS_COST_CENTER_CODE = os.environ.get("TCS_COST_CENTER_CODE", "")
TCS_SERVICE_CODE = os.environ.get("TCS_SERVICE_CODE", "O")
TCS_SHIPPER_NAME = os.environ.get("TCS_SHIPPER_NAME", "")
TCS_SHIPPER_ADDRESS = os.environ.get("TCS_SHIPPER_ADDRESS", "")
TCS_SHIPPER_CITY = os.environ.get("TCS_SHIPPER_CITY", "")
TCS_SHIPPER_CITY_CODE = os.environ.get("TCS_SHIPPER_CITY_CODE", "")
TCS_SHIPPER_ZIP = os.environ.get("TCS_SHIPPER_ZIP", "")
TCS_SHIPPER_MOBILE = os.environ.get("TCS_SHIPPER_MOBILE", "")
# TCS marks middlename as mandatory; Shopify has none, so a placeholder is sent.
TCS_CONSIGNEE_MIDDLENAME = os.environ.get("TCS_CONSIGNEE_MIDDLENAME", "N/A")
# A ready-made long-lived bearer token (e.g. generated in Envio) replaces the
# client id/secret login. Username/password are only needed for the extra e-com token.
TCS_BEARER_TOKEN = os.environ.get("TCS_BEARER_TOKEN", "")
TCS_HAS_LOGIN = bool(TCS_BEARER_TOKEN or (TCS_CLIENT_ID and TCS_CLIENT_SECRET))
# Setting NAMES only (never values) - shown on /status so missing ones are obvious.
TCS_MISSING = (
    ([] if TCS_HAS_LOGIN else ["TCS_BEARER_TOKEN"])
    + [name for name, value in [
        ("TCS_ACCOUNT_NO", TCS_ACCOUNT_NO), ("TCS_COST_CENTER_CODE", TCS_COST_CENTER_CODE),
        ("TCS_SHIPPER_NAME", TCS_SHIPPER_NAME), ("TCS_SHIPPER_ADDRESS", TCS_SHIPPER_ADDRESS),
        ("TCS_SHIPPER_CITY", TCS_SHIPPER_CITY), ("TCS_SHIPPER_MOBILE", TCS_SHIPPER_MOBILE),
    ] if not value]
)
TCS_CONFIGURED = not TCS_MISSING  # needed for booking

# Password for the /orders page (it shows customer details). Page is disabled if unset.
ORDERS_PAGE_KEY = os.environ.get("ORDERS_PAGE_KEY", "")

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
    "tcs_connected": TCS_CONFIGURED,
    "tcs_missing_settings": TCS_MISSING,
    "tcs_env": TCS_ENV,
}


def _now() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())


status["recent_orders"] = []  # newest first: order number + outcome only, no customer data


def _record(order: dict, outcome: str) -> None:
    status["recent_orders"].insert(0, {
        "order": order.get("name") or str(order.get("id")),
        "at": _now(),
        "outcome": outcome,
    })
    del status["recent_orders"][15:]


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


# ---------------------------------------------------------------- TCS booking
# Implements TCS's "COD API User Guide v1.0": Authorization -> E-COM Authentication
# -> Booking Create. Sandbox (devconnect) is the default; production (ociconnect)
# only when TCS_ENV=production.
LIVE_TCS_ENVS = ("production", "connect")  # "connect" = connect.tcscourier.com, the host named in Envio tokens


def _tcs_base(env=None) -> str:
    env = env or TCS_ENV
    if env == "connect":
        return "https://connect.tcscourier.com"
    return ("https://ociconnect.tcscourier.com" if env == "production"
            else "https://devconnect.tcscourier.com")


_tcs_tokens_by_env = {}
_blank_tokens = lambda: {"bearer": "", "bearer_exp": 0.0, "ecom": "", "ecom_exp": 0.0}
_tcs_lock = threading.Lock()


def _expiry_ts(value, fallback_seconds=3000) -> float:
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except Exception:
        return time.time() + fallback_seconds


def _tcs_get_tokens(env=None) -> tuple:
    """Returns (bearer_token, ecom_access_token), refreshing either when near expiry."""
    env = env or TCS_ENV
    with _tcs_lock:
        _tcs_tokens = _tcs_tokens_by_env.setdefault(env, _blank_tokens())
        now = time.time()
        if TCS_BEARER_TOKEN:
            _tcs_tokens["bearer"] = TCS_BEARER_TOKEN
            _tcs_tokens["bearer_exp"] = float("inf")
        if not (_tcs_tokens["bearer"] and now < _tcs_tokens["bearer_exp"] - 300):
            r = requests.get(
                f"{_tcs_base(env)}/auth/api/auth",
                params={"clientid": TCS_CLIENT_ID, "clientsecret": TCS_CLIENT_SECRET},
                timeout=(10, 30),
            )
            r.raise_for_status()
            res = (r.json() or {}).get("result") or {}
            if not res.get("accessToken"):
                raise RuntimeError("TCS authorization returned no token (check client id/secret)")
            _tcs_tokens["bearer"] = res["accessToken"]
            _tcs_tokens["bearer_exp"] = _expiry_ts(res.get("expiry"))
            log.info("Fetched a TCS bearer token (%s)", TCS_ENV)
        if not (TCS_USERNAME and TCS_PASSWORD):
            # No e-com login configured: the bearer token doubles as the body token.
            _tcs_tokens["ecom"] = _tcs_tokens["bearer"]
            _tcs_tokens["ecom_exp"] = float("inf")
        if not (_tcs_tokens["ecom"] and now < _tcs_tokens["ecom_exp"] - 300):
            r = requests.get(
                f"{_tcs_base(env)}/ecom/api/authentication/token",
                headers={"Authorization": f"Bearer {_tcs_tokens['bearer']}"},
                params={"username": TCS_USERNAME, "password": TCS_PASSWORD},
                timeout=(10, 30),
            )
            if r.status_code >= 400:  # never raise_for_status: its text contains the password URL
                raise RuntimeError(f"TCS e-com login failed ({r.status_code}): {_tcs_reject_reason(r)}")
            data = r.json() or {}
            if not data.get("accesstoken"):
                raise RuntimeError("TCS e-com authentication returned no token (check username/password)")
            _tcs_tokens["ecom"] = urllib.parse.unquote(data["accesstoken"])  # TCS sends it URL-encoded
            _tcs_tokens["ecom_exp"] = _expiry_ts(data.get("expiry"))
            log.info("Fetched a TCS e-com access token (%s)", TCS_ENV)
        return _tcs_tokens["bearer"], _tcs_tokens["ecom"]


def _pk_mobile(raw) -> str:
    """Normalise to TCS's required 11-digit 03xxxxxxxxx format."""
    digits = re.sub(r"\D", "", str(raw or ""))
    if digits.startswith("0092"):
        digits = digits[4:]
    elif digits.startswith("92") and len(digits) == 12:
        digits = digits[2:]
    if len(digits) == 10 and digits.startswith("3"):
        digits = "0" + digits
    if not (len(digits) == 11 and digits.startswith("03")):
        raise ValueError("customer phone number is not a valid Pakistani mobile (needs 03xxxxxxxxx)")
    return digits


def _tcs_reject_reason(resp) -> str:
    """TCS's own words for a 401 (they differ: bearer vs access token) - no secrets in them."""
    try:
        body = resp.json() or {}
        return str(body.get("message") or body.get("code") or body.get("status") or "")[:160]
    except Exception:
        return (getattr(resp, "text", "") or "")[:160]


def _pad3(text: str) -> str:
    text = (text or "").strip()
    return text if len(text) >= 3 else text.ljust(3, ".")


def _build_tcs_payload(order: dict, ecom_token: str) -> dict:
    ship = order.get("shipping_address") or order.get("billing_address") or {}
    first = (ship.get("first_name") or "").strip()
    last = (ship.get("last_name") or "").strip()
    if len(first) < 3:
        first = f"{first} {last}".strip()
        last = ""
    lines = order.get("line_items", []) or []
    grams = sum((li.get("grams") or 0) * (li.get("quantity") or 1) for li in lines)
    weight = max(0.5, round(grams / 1000, 2))
    pieces = max(1, sum((li.get("quantity") or 1) for li in lines))

    paid = order.get("financial_status") == "paid"
    cod = 0 if paid else int(round(float(order.get("total_price") or 0)))
    if cod > 250000:
        raise ValueError("COD amount exceeds TCS limit of 250000")

    skus = []
    for li in lines:
        skus.append({
            "description": _pad3(str(li.get("title") or "Item"))[:50],
            "quantity": max(1, li.get("quantity") or 1),
            "weight": max(0.5, round((li.get("grams") or 0) / 1000, 2)),
            "uom": "KG",
            "unitprice": min(250000, max(1, int(round(float(li.get("price") or 1))))),
            "declaredvalue": None,
            "insuredvalue": None,
        })
    content = _pad3(", ".join(str(li.get("title") or "") for li in lines))[:50]

    addr1 = _pad3(str(ship.get("address1") or ""))[:120]
    consignee = {
        "firstname": _pad3(first)[:50],
        "middlename": TCS_CONSIGNEE_MIDDLENAME,
        "address1": addr1,
        "countrycode": "PK",
        "countryname": "Pakistan",
        "cityname": _pad3(str(ship.get("city") or ""))[:50],
        "mobile": _pk_mobile(ship.get("phone") or order.get("phone")),
    }
    if len(last) >= 3:
        consignee["lastname"] = last[:50]
    if ship.get("address2"):
        consignee["address2"] = _pad3(str(ship["address2"]))[:120]
    if ship.get("zip"):
        consignee["zip"] = str(ship["zip"])[:6]
    if order.get("email"):
        consignee["email"] = str(order["email"])[:50]

    pkt = timezone(timedelta(hours=5))
    return {
        "accesstoken": ecom_token,
        "shipperinfo": {
            "tcsaccount": TCS_ACCOUNT_NO,
            "shippername": TCS_SHIPPER_NAME,
            "address1": TCS_SHIPPER_ADDRESS,
            "zip": TCS_SHIPPER_ZIP,
            "countrycode": "PK",
            "countryname": "Pakistan",
            "citycode": TCS_SHIPPER_CITY_CODE,
            "cityname": TCS_SHIPPER_CITY,
            "mobile": TCS_SHIPPER_MOBILE,
        },
        "consigneeinfo": consignee,
        "shipmentinfo": {
            "costcentercode": TCS_COST_CENTER_CODE,
            "referenceno": str(order.get("name") or order.get("id"))[:50],
            "contentdesc": content,
            "servicecode": TCS_SERVICE_CODE,
            "shipmentdate": datetime.now(pkt).strftime("%d/%m/%Y %H:%M:%S"),
            "currency": "PKR",
            "codamount": cod,
            "weightinkg": weight,
            "pieces": pieces,
            "fragile": False,
            "remarks": f"Shopify order {order.get('name')}",
            "skus": skus,
        },
    }


def book_tcs_shipment(order: dict, env=None) -> dict:
    """Books a TCS shipment for one Shopify order -> {"tracking_number", "tracking_url", ...}.
    Without full TCS settings it only simulates (logged, never touches Shopify)."""
    if not TCS_CONFIGURED:
        fake_tracking = f"FAKE-{order['id']}"
        log.warning("TCS settings incomplete - SIMULATING booking for order %s", order["id"])
        return {"tracking_number": fake_tracking, "tracking_url": "", "simulated": True}

    bearer, ecom = _tcs_get_tokens(env)
    payload = _build_tcs_payload(order, ecom)
    resp = requests.post(
        f"{_tcs_base()}/ecom/api/booking/create",
        headers={"Authorization": f"Bearer {bearer}"},
        json=payload,
        timeout=(10, 30),
    )
    try:
        data = resp.json()
    except ValueError:
        data = {}
    if resp.status_code == 401:
        with _tcs_lock:  # force fresh tokens on the next attempt
            _tcs_tokens_by_env.pop(env or TCS_ENV, None)
        raise RuntimeError(f"TCS rejected our token (401): {_tcs_reject_reason(resp)}")
    cn = data.get("consignmentNo")
    if not (data.get("status") is True and cn):
        errors = "; ".join(
            str(e.get("errorname", "")) for e in (data.get("error") or []) if isinstance(e, dict)
        )
        raise RuntimeError(
            f"TCS booking rejected: {data.get('message') or resp.status_code} {errors}".strip()[:250]
        )
    log.info("TCS booking created for order %s -> CN %s (%s)", order.get("name"), cn, TCS_ENV)
    return {"tracking_number": str(cn), "tracking_url": "", "sandbox": TCS_ENV not in LIVE_TCS_ENVS}


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
        if (booking.get("simulated") or booking.get("sandbox")) and not ALLOW_SIMULATED_FULFILLMENT:
            # Safety: never fulfil a real order with a fake/test tracking number
            # (the customer would be emailed it).
            kind = "simulated" if booking.get("simulated") else "SANDBOX test booking"
            log.warning("DRY RUN: order %s (%s) %s, NOT fulfilled in Shopify",
                        order_id, order.get("name"), kind)
            status["orders_booked_simulated_dry_run"] += 1
            _record(order, f"dry run: {kind} (CN {booking['tracking_number']}), NOT fulfilled in Shopify")
            mark_processed(order_id)
            return
        mark_order_fulfilled(order_id, booking["tracking_number"], booking.get("tracking_url", ""))
        status["orders_fulfilled"] += 1
        _record(order, f"fulfilled, tracking {booking['tracking_number']}")
        mark_processed(order_id)
    except Exception as e:
        status["order_failures"] += 1
        _record(order, f"FAILED: {type(e).__name__}: {str(e)[:150]} (will retry)")
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


DISPLAY_UTC_OFFSET_HOURS = float(os.environ.get("DISPLAY_UTC_OFFSET_HOURS", "5"))  # Pakistan = UTC+5
DISPLAY_TZ_LABEL = os.environ.get("DISPLAY_TZ_LABEL", "PKT")


def _local_time(iso_string: str) -> str:
    try:
        dt = datetime.fromisoformat(str(iso_string))
        tz = timezone(timedelta(hours=DISPLAY_UTC_OFFSET_HOURS))
        return dt.astimezone(tz).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return str(iso_string)[:16].replace("T", " ")


@app.route("/orders", methods=["GET"])
def orders_page():
    """Readable list of the latest Shopify orders. Contains customer details,
    so it is locked behind ORDERS_PAGE_KEY (open /orders?key=YOUR_KEY)."""
    if not ORDERS_PAGE_KEY:
        return "Set ORDERS_PAGE_KEY in the Render environment to enable this page.", 404
    if not hmac.compare_digest(request.args.get("key", ""), ORDERS_PAGE_KEY):
        abort(401)

    try:
        resp = requests.get(
            f"{SHOPIFY_BASE}/orders.json", headers=shopify_headers(),
            params={"status": "any", "limit": 50}, timeout=(10, 30),
        )
        resp.raise_for_status()
        orders = resp.json().get("orders", [])
    except Exception as e:
        return f"Could not load orders from Shopify: {type(e).__name__}", 502

    esc = html.escape
    rows = []
    for o in orders:
        ship = o.get("shipping_address") or {}
        name = f"{ship.get('first_name') or ''} {ship.get('last_name') or ''}".strip()
        address = ", ".join(p for p in [
            ship.get("address1"), ship.get("address2"), ship.get("city"),
            ship.get("province"), ship.get("zip"), ship.get("country"),
        ] if p)
        items = "<br>".join(
            f"{esc(str(li.get('title', '')))} &times; {li.get('quantity', 1)}"
            for li in o.get("line_items", [])
        )
        rows.append(
            "<tr>"
            f"<td><b>{esc(str(o.get('name', '')))}</b></td>"
            f"<td>{esc(_local_time(o.get('created_at', '')))}</td>"
            f"<td>{esc(name)}</td>"
            f"<td>{esc(ship.get('phone') or o.get('phone') or '')}</td>"
            f"<td>{esc(address)}</td>"
            f"<td>{items}</td>"
            f"<td>{esc(str(o.get('total_price', '')))} {esc(str(o.get('currency', '')))}</td>"
            f"<td>{esc(str(o.get('financial_status', '')))}</td>"
            f"<td>{esc(str(o.get('fulfillment_status') or 'unfulfilled'))}</td>"
            "</tr>"
        )
    page = (
        "<!doctype html><meta charset='utf-8'>"
        "<meta name='viewport' content='width=device-width,initial-scale=1'>"
        "<meta http-equiv='refresh' content='30'>"
        "<title>Orders</title>"
        "<style>body{font-family:Arial,sans-serif;margin:16px}"
        "table{border-collapse:collapse;width:100%}"
        "th,td{border:1px solid #ccc;padding:6px 8px;text-align:left;vertical-align:top;font-size:14px}"
        "th{background:#f0f0f0}.w{overflow-x:auto}</style>"
        f"<h2>Latest {len(orders)} Shopify orders "
        f"<small style='font-weight:normal;color:#666'>(updated "
        f"{esc(_local_time(datetime.now(timezone.utc).isoformat()))} {esc(DISPLAY_TZ_LABEL)}, "
        "refreshes every 30 seconds)</small></h2>"
        f"<div class='w'><table><tr><th>Order</th><th>Placed ({esc(DISPLAY_TZ_LABEL)})</th><th>Customer</th>"
        "<th>Phone</th><th>Address</th><th>Items</th><th>Total</th><th>Payment</th>"
        "<th>Fulfilment</th></tr>" + "".join(rows) + "</table></div>"
    )
    return page, 200


# ---------------------------------------------------------------- TCS parcel report
# Source: TCS "Payment Detail" API (all parcels in a date range with booking date,
# status code, COD paid and charges) + "Delivery Status List" for status names.
STATUS_LABELS = {"OK": "Delivered", "RO": "Returned to origin", "SC": "Awaiting receiver collection"}
_labels_cache = {"at": 0.0, "map": {}}


def _tcs_status_labels(env=None) -> dict:
    if time.time() - _labels_cache["at"] > 3600:
        try:
            bearer, _ = _tcs_get_tokens(env)
            r = requests.get(
                f"{_tcs_base(env)}/ecom/setup/deliverystatuslist",
                headers={"Authorization": f"Bearer {bearer}"}, timeout=(10, 30),
            )
            r.raise_for_status()
            _labels_cache["map"] = {
                str(d["statuscode"]): str(d.get("description") or d["statuscode"])
                for d in ((r.json() or {}).get("detail") or []) if d.get("statuscode")
            }
        except Exception:
            log.exception("Could not load TCS status names (showing raw codes)")
        _labels_cache["at"] = time.time()  # don't retry on every page view
    return {**_labels_cache["map"], **STATUS_LABELS}


def _fetch_tcs_parcels(d_from: date, d_to: date, env=None) -> list:
    """All parcels booked between the two dates, fetched in 7-day slices."""
    bearer, ecom = _tcs_get_tokens(env)
    if not (TCS_USERNAME and TCS_PASSWORD):
        raise RuntimeError("Add TCS_USERNAME and TCS_PASSWORD (your TCS/Envio API login) in Render - the report needs an E-COM access token")
    parcels = {}
    cur = d_from
    while cur <= d_to:
        end = min(cur + timedelta(days=6), d_to)
        r = requests.get(
            f"{_tcs_base(env)}/ecom/api/Payment/detail",
            headers={"Authorization": f"Bearer {bearer}"},
            params={  # TCS reads GET parameters from the query string
                "accesstoken": ecom,
                "customerno": TCS_CUSTOMER_NO or TCS_ACCOUNT_NO,
                "fromdate": cur.isoformat(),
                "todate": end.isoformat(),
            },
            timeout=(10, 60),
        )
        if r.status_code == 401:
            with _tcs_lock:
                _tcs_tokens_by_env.pop(env or TCS_ENV, None)
            raise RuntimeError(f"TCS rejected the token (401): {_tcs_reject_reason(r)}")
        r.raise_for_status()
        for row in ((r.json() or {}).get("detail") or []):
            cn = str(row.get("cn by courier") or "")
            if cn:
                parcels[cn] = row
        cur = end + timedelta(days=1)
    return list(parcels.values())


TCS_REPORT_ENV = os.environ.get("TCS_REPORT_ENV", "").strip().lower()
_report_env = {"ok": None}


def _report_envs() -> list:
    """Reading a report is harmless, so unlike booking it may use live data.
    With a ready-made bearer token we try production, then sandbox, and remember
    which one TCS accepts."""
    if TCS_REPORT_ENV:
        return [TCS_REPORT_ENV]
    if TCS_BEARER_TOKEN:
        return [_report_env["ok"]] if _report_env["ok"] else ["connect", "production", "sandbox"]
    return [TCS_ENV]


def _fetch_parcels_any_env(d_from: date, d_to: date):
    last, errors = None, []
    for env in _report_envs():
        try:
            rows = _fetch_tcs_parcels(d_from, d_to, env)
            _report_env["ok"] = env
            return rows, env
        except (RuntimeError, requests.HTTPError) as e:
            last = e
            errors.append(f"{env}: {str(e).split('?')[0][:200]}")
    raise RuntimeError(" | ".join(errors))


def _parse_dmy(text):
    try:
        return datetime.strptime(str(text), "%d/%m/%Y").date()
    except Exception:
        return None


def _load_cns(extra: str = "") -> list:
    """CN numbers to report on: cns.txt in the repo plus any given in the URL (&cn=1,2)."""
    out = []
    try:
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "cns.txt")) as f:
            out += [ln.strip() for ln in f if ln.strip() and not ln.startswith("#")]
    except OSError:
        pass
    out += re.findall(r"\d{9,14}", extra or "")
    return list(dict.fromkeys(out))


_track_cache = {}


def _track_one(cn: str):
    hit = _track_cache.get(cn)
    if hit and time.time() - hit[0] < 300:
        return hit[1]
    row = _track_one_uncached(cn)
    _track_cache[cn] = (time.time(), row)
    return row


def _track_one_uncached(cn: str):
    try:
        r = requests.get(
            f"{_tcs_base('connect')}/tracking/api/Tracking/GetDynamicTrackDetail",
            headers={"Authorization": f"Bearer {TCS_BEARER_TOKEN}"},
            params={"consignee": cn}, timeout=(10, 30))
        d = r.json() or {}
        info = (d.get("shipmentinfo") or [None])[0]
        if not info:
            return None
        if cn.startswith("#") and str(info.get("referenceno") or "").strip() != cn:
            return None  # TCS returned a parcel with a different reference - not ours
        summ = str(d.get("shipmentsummary") or "")
        booked = datetime.strptime(info.get("bookingdate"), "%b %d, %Y").strftime("%d/%m/%Y")
        delivered = re.search(r"Delivered On:\s*\S+\s+(\w+ \d+, \d{4})", summ)
        dinfo = d.get("deliveryinfo") or []
        cps = d.get("checkpoints") or []
        codes = [str(x.get("code") or "") for x in dinfo]
        first = codes[0] if codes else ""
        cp0 = cps[0] if cps else {}
        cp_text = re.sub(r"\s*\(For Additional.*$", "", str(cp0.get("status") or "")).strip()
        detail = str(dinfo[0].get("status") or "") if dinfo else cp_text
        if first == "OK" or re.search(r"Current Status:\s*DELIVERED", summ, re.I):
            state, label = "delivered", "Delivered"
        elif first == "RO" or "returned to origin" in cp_text.lower():
            state, label = "returned", "Returned to origin"
        elif "RD" in codes:
            state, label = "rejected", "Refused by receiver"
        else:
            state, label = "in_progress", "In Transit"
        return {"cn by courier": info.get("consignmentno") or cn, "booking date": booked,
                "order no": cn if cn.startswith("#") else "",
                "cn status": label, "state": state, "detail": detail or cp_text,
                "reason": "refused by receiver" if "RD" in codes else "",
                "last_event": cp_text, "last_time": str(cp0.get("datetime") or ""),
                "delivery date": delivered.group(1) if delivered else None,
                "city": info.get("destination"), "delivery charges": 0, "amount paid": 0}
    except Exception:
        log.exception("Tracking failed for %s", cn)
        return None


_order_products = {}  # order name -> "Product x 2 + Other x 1", filled whenever orders are read


def _shopify_orders_since(since_iso: str, fields: str, max_orders: int = 1500) -> list:
    """All orders created since a moment, following Shopify's pages (250 each)."""
    out, url = [], f"{SHOPIFY_BASE}/orders.json"
    params = {"status": "any", "limit": 250, "created_at_min": since_iso, "fields": fields}
    while url and len(out) < max_orders:
        r = requests.get(url, headers=shopify_headers(), params=params, timeout=(10, 30))
        r.raise_for_status()
        out += r.json().get("orders", [])
        url = r.links.get("next", {}).get("url")
        params = None  # the next-page link already carries them
    for o in out:
        if o.get("name") and o.get("line_items") is not None:
            _order_products[o["name"]] = " + ".join(
                f"{li.get('title')} x {li.get('quantity')}" for li in o["line_items"])
    return out


def _fetch_by_shopify_orders(d_from: date, d_to: date) -> tuple:
    """TCS tracking accepts our order reference (#000...), so every Shopify order
    that was booked with TCS is found without knowing its CN."""
    from concurrent.futures import ThreadPoolExecutor
    since = (datetime.combine(d_from, datetime.min.time()) - timedelta(days=14)).isoformat() + "Z"
    orders = _shopify_orders_since(
        since, "name,line_items,created_at,fulfillment_status,cancelled_at,total_price")
    orders = [o for o in orders if o.get("name")]
    with ThreadPoolExecutor(8) as ex:
        tracked = list(ex.map(_track_one, [o["name"] for o in orders]))
    rows = [x for x in tracked if x]
    unbooked = []
    for o, t in zip(orders, tracked):
        created = str(o.get("created_at") or "")[:10]
        if not t and not o.get("cancelled_at") and d_from.isoformat() <= created <= d_to.isoformat():
            unbooked.append({"order": o["name"], "created": created,
                             "fulfillment": o.get("fulfillment_status") or "unfulfilled",
                             "product": _order_products.get(o["name"], ""),
                             "total": o.get("total_price")})
    rows = [x for x in rows if (b := _parse_dmy(x["booking date"])) and d_from <= b <= d_to]
    return rows, sorted(unbooked, key=lambda u: u["order"], reverse=True)


def _fetch_via_tracking(d_from: date, d_to: date, extra: str = "") -> list:
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(6) as ex:
        rows = [r for r in ex.map(_track_one, _load_cns(extra)) if r]
    return [r for r in rows if (b := _parse_dmy(r["booking date"])) and d_from <= b <= d_to]


def _build_report(d_from: date, d_to: date, extra: str = "") -> dict:
    try:
        rows, used_env = _fetch_parcels_any_env(d_from, d_to)
    except Exception as e:
        log.warning("Payment Detail unavailable (%s) - using CN tracking", str(e)[:80])
        rows, used_env = _fetch_via_tracking(d_from, d_to, extra), "connect (tracking)"
    unbooked = []
    try:  # add parcels the ledger doesn't list yet, found through the Shopify order numbers
        have = {str(x.get("cn by courier")) for x in rows}
        extra, unbooked = _fetch_by_shopify_orders(d_from, d_to)
        rows = rows + [x for x in extra if str(x["cn by courier"]) not in have]
    except Exception:
        log.exception("Shopify order lookup for the report failed")
    if rows and "tracking" not in used_env:  # live tracking status is more complete than the ledger code
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(6) as ex:
            live = list(ex.map(_track_one, [str(r.get("cn by courier")) for r in rows]))
        for r, t in zip(rows, live):
            if t:
                r["cn status"] = t["cn status"]
                r["delivery date"] = t["delivery date"] or r.get("delivery date")
    labels = _tcs_status_labels("connect")
    status_totals = Counter()
    by_date = {}
    parcels = []
    charges = paid = 0.0
    for r in rows:
        code = str(r.get("cn status") or "?")
        label = labels.get(code, code)
        booked = _parse_dmy(r.get("booking date"))
        key = booked.isoformat() if booked else "unknown"
        status_totals[label] += 1
        day = by_date.setdefault(key, {"total": 0, "statuses": Counter(), "charges": 0.0})
        day["total"] += 1
        day["statuses"][label] += 1
        day["charges"] += float(r.get("delivery charges") or 0)
        charges += float(r.get("delivery charges") or 0)
        paid += float(r.get("amount paid") or 0)
        parcels.append({
            "cn": r.get("cn by courier"), "booked": key, "delivered_on": r.get("delivery date"),
            "status": label, "code": code, "city": r.get("city"), "order": r.get("order no"),
            "product": _order_products.get(str(r.get("order no") or ""), ""),
            "weight": r.get("parcel weight"), "amount_paid": r.get("amount paid"),
            "charges": r.get("delivery charges"),
        })
    parcels.sort(key=lambda p: (p["booked"], str(p["cn"])), reverse=True)
    return {
        "from": d_from.isoformat(), "to": d_to.isoformat(), "env": used_env,
        "total": len(rows), "delivery_charges": round(charges, 2), "amount_paid": round(paid, 2),
        "status_totals": dict(status_totals.most_common()),
        "by_date": {
            k: {"total": v["total"], "statuses": dict(v["statuses"]), "charges": round(v["charges"], 2)}
            for k, v in sorted(by_date.items(), reverse=True)
        },
        "parcels": parcels,
        "unbooked": unbooked,
    }


def _report_html(rep: dict, key: str) -> str:
    esc = html.escape
    total = rep["total"] or 1
    cols = list(rep["status_totals"].keys())
    head = "".join(f"<th>{esc(c)}</th>" for c in cols)
    date_rows = "".join(
        f"<tr><td><b>{esc(d)}</b></td><td><b>{v['total']}</b></td>"
        + "".join(f"<td>{v['statuses'].get(c, 0) or ''}</td>" for c in cols)
        + f"<td>{v['charges']:.0f}</td></tr>"
        for d, v in rep["by_date"].items()
    )
    status_rows = "".join(
        f"<tr><td>{esc(s)}</td><td>{n}</td><td>{n * 100 / total:.1f}%</td></tr>"
        for s, n in rep["status_totals"].items()
    )
    parcel_rows = "".join(
        "<tr>" + "".join(f"<td>{esc(str(p[k]) if p[k] is not None else '')}</td>" for k in
                         ("cn", "booked", "delivered_on", "status", "city", "order", "product",
                          "weight", "amount_paid", "charges")) + "</tr>"
        for p in rep["parcels"]
    )
    return (
        "<!doctype html><meta charset='utf-8'>"
        "<meta name='viewport' content='width=device-width,initial-scale=1'>"
        "<title>TCS report</title>"
        "<style>body{font-family:Arial,sans-serif;margin:16px}"
        "table{border-collapse:collapse;margin-bottom:24px}"
        "th,td{border:1px solid #ccc;padding:6px 10px;text-align:left;font-size:14px}"
        "th{background:#f0f0f0}.w{overflow-x:auto}.c{display:inline-block;margin:0 14px 10px 0;"
        "padding:10px 16px;background:#f5f5f5;border-radius:6px}</style>"
        f"<h2>TCS parcels {esc(rep['from'])} to {esc(rep['to'])} "
        f"<small style='font-weight:normal;color:#666'>({esc(rep['env'])} data)</small></h2>"
        f"<form method='get'><input type='hidden' name='key' value='{esc(key)}'>"
        f"From <input type='date' name='from' value='{esc(rep['from'])}'> "
        f"to <input type='date' name='to' value='{esc(rep['to'])}'> <button>Show</button></form><br>"
        f"<div class='c'><b>{rep['total']}</b><br>parcels booked</div>"
        + "".join(f"<div class='c'><b>{n}</b><br>{esc(s)}</div>" for s, n in rep["status_totals"].items())
        + f"<div class='c'><b>{rep['amount_paid']:.0f}</b><br>COD paid</div>"
        f"<div class='c'><b>{rep['delivery_charges']:.0f}</b><br>delivery charges</div>"
        "<h3>Parcels booked per date</h3><div class='w'><table>"
        f"<tr><th>Booking date</th><th>Total</th>{head}<th>Charges</th></tr>{date_rows}</table></div>"
        "<h3>Status summary</h3><div class='w'><table>"
        f"<tr><th>Status</th><th>Parcels</th><th>Share</th></tr>{status_rows}</table></div>"
        f"<h3>All booked parcels ({len(rep['parcels'])})</h3><div class='w'><table>"
        "<tr><th>CN</th><th>Booked</th><th>Delivered on</th><th>Status</th><th>City</th>"
        "<th>Order</th><th>Product</th><th>Kg</th><th>COD paid</th><th>Charges</th></tr>"
        f"{parcel_rows}</table></div>"
        f"<h3>Not booked at TCS ({len(rep.get('unbooked', []))})</h3><div class='w'><table>"
        "<tr><th>Order</th><th>Placed</th><th>Shopify status</th><th>Product</th><th>Total</th></tr>"
        + "".join("<tr>" + "".join(f"<td>{esc(str(u[k] if u[k] is not None else ''))}</td>" for k in
                                   ("order", "created", "fulfillment", "product", "total")) + "</tr>"
                  for u in rep.get("unbooked", []))
        + "</table></div>"
    )


@app.route("/report", methods=["GET"])
@app.route("/report.json", methods=["GET"])
def report():
    """Parcels per booking date and every status, straight from TCS.
    /report?key=KEY&from=YYYY-MM-DD&to=YYYY-MM-DD   (add .json for the raw data)."""
    if not ORDERS_PAGE_KEY:
        return "Set ORDERS_PAGE_KEY in the Render environment to enable this page.", 404
    if not hmac.compare_digest(request.args.get("key", ""), ORDERS_PAGE_KEY):
        abort(401)
    # The report only needs a TCS login and the customer/account number - not the
    # shipper or cost-center settings that booking needs.
    report_missing = (
        ([] if TCS_HAS_LOGIN else ["TCS_BEARER_TOKEN"])
        + ([] if (TCS_CUSTOMER_NO or TCS_ACCOUNT_NO) else ["TCS_ACCOUNT_NO"])
    )
    if report_missing:
        return ("Add these in Render -> Environment to enable the report: "
                + ", ".join(report_missing)), 503

    today = datetime.now(timezone(timedelta(hours=DISPLAY_UTC_OFFSET_HOURS))).date()
    try:
        d_to = date.fromisoformat(request.args.get("to") or today.isoformat())
        d_from = date.fromisoformat(request.args.get("from") or (d_to - timedelta(days=29)).isoformat())
    except ValueError:
        return "Dates must look like 2026-10-31.", 400
    if d_from > d_to:
        d_from, d_to = d_to, d_from
    if (d_to - d_from).days > 92:
        d_from = d_to - timedelta(days=92)  # keep the number of TCS calls reasonable

    try:
        rep = _build_report(d_from, d_to, request.args.get('cn', ''))
    except Exception as e:
        log.exception("Report failed")
        return f"Could not load the report from TCS: {type(e).__name__}: {str(e)[:200]}", 502
    if request.path.endswith(".json"):
        return jsonify(rep), 200
    return _report_html(rep, request.args.get("key", "")), 200


@app.route("/track", methods=["GET"])
def track_page():
    """Type a CN or an order number (like #00011063) and see the full TCS tracking."""
    if not ORDERS_PAGE_KEY:
        return "Set ORDERS_PAGE_KEY in the Render environment to enable this page.", 404
    key = request.args.get("key", "")
    if not hmac.compare_digest(key, ORDERS_PAGE_KEY):
        abort(401)
    esc = html.escape
    q = request.args.get("q", "").strip()
    out = ""
    if q:
        try:
            r = requests.get(
                f"{_tcs_base('connect')}/tracking/api/Tracking/GetDynamicTrackDetail",
                headers={"Authorization": f"Bearer {TCS_BEARER_TOKEN}"},
                params={"consignee": q}, timeout=(10, 30))
            d = r.json() or {}
        except Exception as e:
            d = {}
            out = f"<p>Could not reach TCS ({esc(type(e).__name__)}).</p>"
        info = (d.get("shipmentinfo") or [None])[0]
        if info:
            warn = ""
            if q.startswith("#") and str(info.get("referenceno") or "").strip() != q:
                warn = "<p style='color:#b00'>Warning: this parcel has a different reference than you typed.</p>"
            elif q.isdigit() and len(q) < 11:
                warn = "<p style='color:#b00'>Short numbers can match another customer's parcel - use the full CN or #00011063.</p>"
            rows = "".join(
                f"<tr><th>{esc(label)}</th><td>{esc(str(info.get(k) or ''))}</td></tr>"
                for label, k in (("CN", "consignmentno"), ("Order ref", "referenceno"),
                                 ("Booked", "bookingdate"), ("From", "origin"),
                                 ("To", "destination"), ("Consignee", "consignee")))
            items = ""
            ref = str(info.get("referenceno") or "").strip()
            if ref.startswith("#"):
                try:  # product names are not in TCS tracking - read them from the Shopify order
                    sr = requests.get(f"{SHOPIFY_BASE}/orders.json", headers=shopify_headers(),
                                      params={"status": "any", "name": ref, "fields": "name,line_items"},
                                      timeout=(10, 30))
                    for o in sr.json().get("orders", []):
                        if o.get("name") == ref:
                            items = "".join(f"<li>{esc(str(li.get('title') or ''))}"
                                            f"{' - ' + esc(str(li['variant_title'])) if li.get('variant_title') else ''}"
                                            f" x {li.get('quantity')}</li>" for li in o.get("line_items", []))
                except Exception:
                    log.exception("Could not load products for %s", ref)
            if items:
                rows += f"<tr><th>Products</th><td><ul style='margin:0;padding-left:18px'>{items}</ul></td></tr>"
            summ = esc(str(d.get("shipmentsummary") or "")).replace(chr(10), "<br>")
            steps = "".join(
                f"<tr><td>{esc(str(c.get('datetime') or ''))}</td><td>{esc(str(c.get('status') or ''))}</td>"
                f"<td>{esc(str(c.get('recievedby') or ''))}</td></tr>"
                for c in (d.get("checkpoints") or []))
            out = (warn + f"<table>{rows}</table><p><b>{summ}</b></p>"
                   "<h3>History</h3><table><tr><th>When</th><th>Status</th><th>Place / person</th></tr>"
                   f"{steps}</table>")
        elif not out:
            out = "<p>No parcel found for that number.</p>"
    return (
        "<!doctype html><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'>"
        "<title>Track a parcel</title>"
        "<style>body{font-family:system-ui,sans-serif;margin:16px;max-width:760px}"
        "input[type=text]{padding:8px;width:min(340px,70vw);font-size:16px}button{padding:8px 14px;font-size:16px}"
        "table{border-collapse:collapse;margin:8px 0}th,td{border:1px solid #ccc;padding:5px 10px;text-align:left}</style>"
        "<h2>Track a parcel</h2>"
        f"<form><input type='hidden' name='key' value='{esc(key)}'>"
        f"<input type='text' name='q' value='{esc(q)}' placeholder='CN or order no, e.g. #00011063' autofocus> "
        "<button>Track</button></form>" + out
    ), 200



# ------------------------------------------------ live product report
PRODUCTS_DAYS = int(os.environ.get("PRODUCTS_REPORT_DAYS", "30"))
PRODUCTS_REFRESH_SECONDS = int(os.environ.get("PRODUCTS_REFRESH_SECONDS", "60"))
_products = {"data": None, "at": 0.0, "error": "", "started": False}


def _classify(status_text: str) -> str:
    t = (status_text or "").lower()
    if "return" in t or "rto" in t:
        return "returned"
    if any(w in t for w in ("refus", "reject", "cancel", "undeliver", "not accept")):
        return "rejected"
    if "deliver" in t and "out for" not in t:
        return "delivered"
    return "in_progress"


def _build_products() -> dict:
    from concurrent.futures import ThreadPoolExecutor
    since = (datetime.utcnow() - timedelta(days=PRODUCTS_DAYS)).isoformat() + "Z"
    r = requests.get(f"{SHOPIFY_BASE}/orders.json", headers=shopify_headers(),
                     params={"status": "any", "limit": 250, "created_at_min": since,
                             "fields": "name,created_at,line_items,cancelled_at"},
                     timeout=(10, 30))
    r.raise_for_status()
    orders = [o for o in r.json().get("orders", []) if o.get("name") and not o.get("cancelled_at")]
    with ThreadPoolExecutor(8) as ex:
        tracked = list(ex.map(_track_one_uncached, [o["name"] for o in orders]))
    prods = {}
    totals = {"delivered": 0, "in_progress": 0, "returned": 0, "rejected": 0, "not_booked": 0}
    for o, t in zip(orders, tracked):
        state = t["state"] if t else "not_booked"
        totals[state] += 1
        for li in o.get("line_items", []):
            title = str(li.get("title") or "?")
            qty = int(li.get("quantity") or 1)
            p = prods.setdefault(title, {"product": title, "units": 0, "delivered": 0, "in_progress": 0,
                                         "returned": 0, "rejected": 0, "not_booked": 0,
                                         "cities": {}, "parcels": []})
            p["units"] += qty
            p[state] += qty
            if t:
                city = t.get("city") or "?"
                p["cities"][city] = p["cities"].get(city, 0) + qty
            p["parcels"].append({
                "order": o["name"], "cn": t["cn by courier"] if t else "", "qty": qty,
                "city": (t or {}).get("city") or "",
                "status": (t["cn status"] + (" - " + t["detail"] if t["detail"] and t["detail"] != t["cn status"] else "")) if t else "Not found at TCS",
                "state": state, "booked": (t or {}).get("booking date") or "",
                "last_event": (t or {}).get("last_event") or "",
                "last_time": (t or {}).get("last_time") or "",
            })
    out = sorted(prods.values(), key=lambda p: -p["units"])
    for p in out:
        p["cities"] = dict(sorted(p["cities"].items(), key=lambda kv: -kv[1]))
        p["parcels"].sort(key=lambda x: x["order"], reverse=True)
    return {"products": out, "totals": totals, "orders": len(orders), "days": PRODUCTS_DAYS}


def _products_refresh_loop():
    while True:
        try:
            _products["data"] = _build_products()
            _products["at"] = time.time()
            _products["error"] = ""
        except Exception as e:
            log.exception("Product report refresh failed")
            _products["error"] = f"{type(e).__name__}: {str(e).split('?')[0][:150]}"
        time.sleep(PRODUCTS_REFRESH_SECONDS)


def _ensure_products_thread():
    with _tcs_lock:
        if _products["started"]:
            return
        _products["started"] = True
    threading.Thread(target=_products_refresh_loop, daemon=True, name="products").start()


PRODUCTS_PAGE = """<!doctype html><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'>
<title>Product report</title>
<style>
body{font-family:system-ui,sans-serif;margin:16px;color:#222}
h2{margin:0 0 4px}.sub{color:#666;font-size:13px;margin-bottom:12px}
.cards{display:flex;flex-wrap:wrap;gap:8px;margin:10px 0}
.card{border:1px solid #ddd;border-radius:8px;padding:8px 14px;min-width:96px}
.card b{display:block;font-size:22px}
.d{color:#12803c}.p{color:#b26a00}.r{color:#b3261e}.x{color:#7a1fa2}.n{color:#666}
table{border-collapse:collapse;width:100%;margin:6px 0}th,td{border:1px solid #ddd;padding:5px 8px;text-align:left;font-size:14px}
th{background:#f5f5f5}td.num{text-align:right}
details{margin:2px 0}summary{cursor:pointer}
@media (prefers-color-scheme:dark){body{background:#111;color:#eee}.card,th,td{border-color:#444}th{background:#222}.sub{color:#aaa}}
</style>
<h2>Product report</h2>
<div class='sub' id='sub'>Loading... first load takes about 10 seconds</div>
<div class='cards' id='cards'></div>
<div id='list'></div>
<script>
var KEY = new URLSearchParams(location.search).get('key') || '';
var opened = {}, last = null, lastAt = 0;
function esc(s){return String(s==null?'':s).replace(/[&<>"]/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]})}
function card(l,v,c){return "<div class='card'><span class='"+c+"'>"+l+"</span><b class='"+c+"'>"+v+"</b></div>"}
function render(){
  if(!last) return;
  var t=last.totals, h='';
  h+=card('Orders',last.orders,'n')+card('Delivered',t.delivered,'d')+card('In progress',t.in_progress,'p')+card('Returned',t.returned,'r')+card('Rejected',t.rejected,'x')+card('Not at TCS',t.not_booked,'n');
  document.getElementById('cards').innerHTML=h;
  var rows='<table><tr><th>Product</th><th>Units</th><th>Delivered</th><th>In progress</th><th>Returned</th><th>Rejected</th><th>Not at TCS</th><th>Going to</th></tr>';
  last.products.forEach(function(p,i){
    var keys=Object.keys(p.cities);
    var cities=keys.slice(0,6).map(function(c){return esc(c)+' ('+p.cities[c]+')'}).join(', ')+(keys.length>6?' ...':'');
    rows+="<tr><td><details data-i='"+i+"'"+(opened[p.product]?' open':'')+"><summary><b>"+esc(p.product)+"</b></summary>";
    rows+="<table><tr><th>Order</th><th>CN</th><th>Qty</th><th>City</th><th>Status</th><th>Last event</th></tr>";
    p.parcels.forEach(function(x){rows+="<tr><td>"+esc(x.order)+"</td><td>"+esc(x.cn)+"</td><td class='num'>"+x.qty+"</td><td>"+esc(x.city)+"</td><td>"+esc(x.status)+"</td><td>"+esc(x.last_event)+" "+esc(x.last_time)+"</td></tr>"});
    rows+="</table></details></td><td class='num'>"+p.units+"</td><td class='num d'>"+p.delivered+"</td><td class='num p'>"+p.in_progress+"</td><td class='num r'>"+p.returned+"</td><td class='num x'>"+p.rejected+"</td><td class='num n'>"+p.not_booked+"</td><td>"+cities+"</td></tr>";
  });
  document.getElementById('list').innerHTML=rows+'</table>';
  document.querySelectorAll('details').forEach(function(d){d.addEventListener('toggle',function(){var p=last.products[+d.getAttribute('data-i')];if(p)opened[p.product]=d.open})});
}
function load(){
  fetch('/products.json?key='+encodeURIComponent(KEY),{cache:'no-store'}).then(function(r){return r.json()}).then(function(d){
    if(d.products){
      var changed=!last||JSON.stringify(d.products)!==JSON.stringify(last.products)||JSON.stringify(d.totals)!==JSON.stringify(last.totals);
      last=d;lastAt=Date.now();if(changed)render();
      document.getElementById('sub').textContent='Last '+d.days+' days. Page checked '+new Date().toLocaleTimeString()+'. TCS data is '+d.age+'s old (refreshed every '+d.refresh+'s)'+(d.error?'. Last refresh problem: '+d.error:'');
    } else if(d.error){document.getElementById('sub').textContent='Error: '+d.error}
  }).catch(function(){document.getElementById('sub').textContent='Connection problem, retrying...'});
}
load();setInterval(load,1000);
</script>
"""


@app.route("/products", methods=["GET"])
def products_page():
    if not ORDERS_PAGE_KEY:
        return "Set ORDERS_PAGE_KEY in the Render environment to enable this page.", 404
    if not hmac.compare_digest(request.args.get("key", ""), ORDERS_PAGE_KEY):
        abort(401)
    _ensure_products_thread()
    return PRODUCTS_PAGE, 200


@app.route("/products.json", methods=["GET"])
def products_json():
    if not ORDERS_PAGE_KEY:
        return "Set ORDERS_PAGE_KEY in the Render environment to enable this page.", 404
    if not hmac.compare_digest(request.args.get("key", ""), ORDERS_PAGE_KEY):
        abort(401)
    _ensure_products_thread()
    data = _products["data"]
    if not data:
        return jsonify({"error": _products["error"], "loading": True}), 200
    return jsonify(dict(data, age=int(time.time() - _products["at"]),
                        refresh=PRODUCTS_REFRESH_SECONDS, error=_products["error"])), 200


@app.route("/status", methods=["GET"])
def status_page():
    return dict(status, now=_now()), 200


@app.route("/health", methods=["GET"])
def health():
    return {"status": "ok"}, 200


# ---------------------------------------------------------------- catch-up loop
_loop_gen = {"n": 0}
_beat = {"t": time.time()}
_start_lock = threading.Lock()
_started = {"done": False}


def _heartbeat() -> None:
    _beat["t"] = time.time()


def catch_up_loop(gen):
    webhook_ok = False
    status["stage"] = "catch-up thread started"
    while gen == _loop_gen["n"]:
        _heartbeat()
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
                _heartbeat()
                process_order(order)
        except Exception as e:
            status["last_catch_up_at"] = _now()
            status["last_error"] = f"{_now()} catch-up: {type(e).__name__}: {str(e)[:150]}"
            log.exception("Catch-up pass failed")
        status["stage"] = "sleeping until next pass"
        for _ in range(max(1, CATCH_UP_INTERVAL_SECONDS // 15)):
            if gen != _loop_gen["n"]:
                return
            _heartbeat()
            time.sleep(15)


def start_catch_up_thread() -> None:
    _loop_gen["n"] += 1  # retires any older (possibly stuck) loop thread
    _heartbeat()
    threading.Thread(target=catch_up_loop, args=(_loop_gen["n"],), daemon=True).start()


def watchdog() -> None:
    """Restart the catch-up loop if it stops making progress (e.g. a hung network call)."""
    while True:
        time.sleep(30)
        if time.time() - _beat["t"] > 150:
            status["watchdog_restarts"] = status.get("watchdog_restarts", 0) + 1
            status["last_error"] = f"{_now()} watchdog: loop stalled at '{status['stage']}', restarted"
            log.warning("Catch-up loop stalled at '%s' - restarting it", status["stage"])
            start_catch_up_thread()


@app.before_request
def _start_background_work_once():
    # Started on the first request, after the app is fully loaded - not at import time.
    if not _started["done"]:
        with _start_lock:
            if not _started["done"]:
                _started["done"] = True
                start_catch_up_thread()
                threading.Thread(target=watchdog, daemon=True).start()


if __name__ == "__main__":
    _start_background_work_once()
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
