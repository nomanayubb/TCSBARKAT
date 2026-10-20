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
    out += list(_webhook_cns)
    return list(dict.fromkeys(out))


_track_cache = {}


def _track_one(cn: str):
    hit = _track_cache.get(cn)
    if hit and time.time() - hit[0] < (86400 if (hit[1] or {}).get("state") in ("delivered", "returned") else 15):
        return hit[1]
    row = _track_one_uncached(cn)
    _track_cache[cn] = (time.time(), row)
    return row


_REASONS = {"RD": "REFUSED DELIVERY", "BA": "BAD ADDRESS", "NP": "NO SUCH CONSIGNEE",
            "OH": "ON HOLD", "CN": "CONSIGNEE NOT AVAILABLE", "SC": "AWAITING COLLECTION"}


def _fail_reason(entry: dict) -> str:
    code = str(entry.get("code") or "")
    return _REASONS.get(code) or str(entry.get("status") or "").upper()


def _dmy_from_tcs(text) -> str:
    """'Tuesday Oct 13, 2026 16:22' -> '13-10-2026'."""
    m = re.search(r"([A-Za-z]{3}) (\d{1,2}), (\d{4})", str(text or ""))
    if not m:
        return ""
    try:
        return datetime.strptime(" ".join(m.groups()), "%b %d %Y").strftime("%d-%m-%Y")
    except ValueError:
        return ""


_order_totals = {}  # order name -> Shopify total (the COD value)
_order_items = {}   # order name -> [(product title, quantity)]


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
        elif first == "RS" or "RS" in codes:
            state, label = "returned", "Return to shipper"
        elif first == "RO" or "returned to origin" in cp_text.lower():
            state, label = "returning", "Returning to origin"
        elif "RD" in codes:
            state, label = "rejected", "Refused by receiver"
        else:
            state, label = "in_progress", "In Transit"
        tries = [x for x in dinfo if str(x.get("code") or "") not in ("RO", "RS")]
        counted = [x for x in tries if str(x.get("code") or "") != "OH"]
        rs = [x for x in dinfo if str(x.get("code") or "") == "RS"]
        return {"cn by courier": info.get("consignmentno") or cn, "booking date": booked,
                "order no": cn if cn.startswith("#") else str(info.get("referenceno") or ""),
                "cn status": label, "state": state, "detail": detail or cp_text,
                "reason": _fail_reason(tries[0]) if tries else "",
                "attempts": len(counted),
                "first_attempt": _dmy_from_tcs(tries[-1].get("datetime")) if tries else "",
                "last_attempt": _dmy_from_tcs(tries[0].get("datetime")) if tries else "",
                "returned_on": _dmy_from_tcs(rs[0].get("datetime")) if rs else "",
                "consignee": str(info.get("consignee") or ""),
                "last_event": cp_text, "last_time": str(cp0.get("datetime") or ""),
                "delivery date": delivered.group(1) if delivered else None,
                "city": info.get("destination"), "origin": info.get("origin"), "delivery charges": 0, "amount paid": 0}
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
        if o.get("name") and o.get("total_price") is not None:
            _order_totals[o["name"]] = o["total_price"]
        if o.get("name") and o.get("line_items") is not None:
            _order_items[o["name"]] = [(str(li.get("title")), int(li.get("quantity") or 1)) for li in o["line_items"]]
    return out


_loadsheet_cache = {}


def _loadsheet_cns(d_from: date, d_to: date) -> list:
    """Every CN TCS has on your load sheets (pickup lists) between two dates - this finds all booked parcels
    whatever their reference is (#barkat..., NA, ...). The call accepts about 7 days at a time."""
    today = datetime.now(timezone(timedelta(hours=DISPLAY_UTC_OFFSET_HOURS))).date()
    d_to = min(d_to, today)
    out, cur = [], d_from
    while cur <= d_to:
        end = min(cur + timedelta(days=6), d_to)
        key = (cur.isoformat(), end.isoformat())
        hit = _loadsheet_cache.get(key)
        if hit and time.time() - hit[0] < 60:
            out += hit[1]
        else:
            try:
                bearer, ecom = _tcs_get_tokens("connect")
                r = requests.get(f"{_tcs_base('connect')}/ecom/api/report/loadsheetlogs",
                                 headers={"Authorization": f"Bearer {bearer}"},
                                 params={"accesstoken": ecom, "customerno": TCS_CUSTOMER_NO or TCS_ACCOUNT_NO,
                                         "fromdate": key[0], "todate": key[1]}, timeout=(10, 60))
                body = r.json() if r.ok else {}
                cns = [str(x.get("consignmentno")) for sheet in (body.get("detail") or [])
                       for x in (sheet.get("loadsheetdetails") or []) if x.get("consignmentno")]
                _loadsheet_cache[key] = (time.time(), cns)
                out += cns
            except Exception:
                log.exception("Load sheet lookup failed for %s", key)
        cur = end + timedelta(days=1)
    return list(dict.fromkeys(out))


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
    # TCS finds a parcel by reference only when the reference is numeric (#00011051),
    # not for names like #barkat1090 - those are found through their CN (cns.txt).
    have_cn = {str(x["cn by courier"]) for x in rows}
    with ThreadPoolExecutor(8) as ex:
        wanted = list(dict.fromkeys(_load_cns() + _loadsheet_cns(d_from - timedelta(days=2), d_to)))
        cn_rows = [x for x in ex.map(_track_one, [c for c in wanted if c not in have_cn]) if x]
    shop_names = {o["name"] for o in orders}
    for x in cn_rows:  # parcel whose reference is not one of our Shopify orders
        x["non_shopify"] = str(x.get("order no") or "") not in shop_names
    rows += cn_rows
    by_cn_ref = {str(x.get("order no") or "") for x in cn_rows}
    unbooked = []
    for o, t in zip(orders, tracked):
        created = str(o.get("created_at") or "")[:10]
        if not t and o["name"] not in by_cn_ref and not o.get("cancelled_at") and d_from.isoformat() <= created <= d_to.isoformat():
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
                for k in ("cn status", "state", "reason", "attempts", "first_attempt", "last_attempt",
                          "returned_on", "consignee", "last_event", "last_time"):
                    r[k] = t.get(k)
                r["delivery date"] = t["delivery date"] or r.get("delivery date")
                r["order no"] = r.get("order no") or t.get("order no")
    labels = _tcs_status_labels("connect")
    status_totals = Counter()
    by_date = {}
    parcels = []
    payments = []
    non_shopify = []
    charges = paid = 0.0
    for r in rows:
        if r.get("non_shopify"):
            non_shopify.append({
                "cn": r.get("cn by courier"), "booked": (_parse_dmy(r.get("booking date")) or date.min).isoformat(),
                "status": r.get("cn status"), "detail": r.get("detail") or "", "city": r.get("city"),
                "reference": r.get("order no") or "", "last_event": r.get("last_event") or "",
                "last_time": r.get("last_time") or ""})
            continue
        code = str(r.get("cn status") or "?")
        label = labels.get(code, code)
        booked = _parse_dmy(r.get("booking date"))
        key = booked.isoformat() if booked else "unknown"
        if r.get("payment status") is not None:  # this parcel is in TCS's payment ledger
            f = lambda k: float(r.get(k) or 0)
            fuel, stax, osa = f("fuel surcharge"), f("sales tax"), f("osa charges")
            ship = round(f("delivery charges") - fuel + 0.01)  # TCS rounds the shipping charge to a whole rupee
            gst, itw, addl = f("whgst"), f("whit"), f("addwhit")
            cod = f("amount paid")
            payments.append({
                "cn": r.get("cn by courier"), "order": r.get("order no") or "", "booked": key,
                "consignee": r.get("consignee") or "", "city": r.get("city"), "weight": r.get("parcel weight"),
                "status": label, "state": r.get("state") or "",
                "pay_status": "Paid" if r.get("payment status") == "Y" else "Pending",
                "pay_date": r.get("payment date") or "", "cod": cod, "shipping": ship, "fuel": fuel,
                "osa": osa, "stax": stax, "gst_wh": gst, "it_wh": itw, "addl": addl,
                "net": round(cod - ship - fuel - stax - osa - gst - itw - addl, 2)})
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
            "state": r.get("state") or "", "reason": r.get("reason") or "",
            "attempts": r.get("attempts"), "first_attempt": r.get("first_attempt") or "",
            "last_attempt": r.get("last_attempt") or "", "returned_on": r.get("returned_on") or "",
            "consignee": r.get("consignee") or "",
            "last_event": r.get("last_event") or "", "last_time": r.get("last_time") or "",
            "cod": _order_totals.get(str(r.get("order no") or ""), ""),
        })
    parcels.sort(key=lambda p: (p["booked"], str(p["cn"])), reverse=True)
    by_cn = {str(p["cn"]): p for p in payments}
    for p in parcels:  # every payment figure next to its parcel (blank when TCS has not listed it yet)
        q = by_cn.get(str(p["cn"]))
        p["pay_status"] = q["pay_status"] if q else "Not listed yet"
        for k in ("pay_date", "cod", "shipping", "fuel", "stax", "gst_wh", "it_wh", "net"):
            p[k] = q[k] if q else ""
    if payments:  # delivered parcels TCS has not put in its payment list yet: charges are the usual ones (estimate)
        std = {k: Counter(p[k] for p in payments).most_common(1)[0][0] for k in ("shipping", "fuel", "stax")}
        for p in parcels:
            if p["state"] == "delivered" and str(p["cn"]) not in by_cn:
                payments.append({
                    "cn": p["cn"], "order": p["order"] or "", "booked": p["booked"], "consignee": p.get("consignee") or "",
                    "city": p["city"], "weight": p.get("weight") or 0.5, "status": p["status"], "state": "delivered",
                    "pay_status": "Not listed yet", "pay_date": "", "cod": 0.0, "shipping": std["shipping"],
                    "fuel": std["fuel"], "osa": 0.0, "stax": std["stax"], "gst_wh": 0.0, "it_wh": 0.0, "addl": 0.0,
                    "net": round(-(std["shipping"] + std["fuel"] + std["stax"]), 2)})
        payments.sort(key=lambda p: (p["booked"], str(p["cn"])), reverse=True)
    pay_summary = _pay_summary(payments)
    return {
        "from": d_from.isoformat(), "to": d_to.isoformat(), "env": used_env,
        "total": len(rows) - len(non_shopify), "delivery_charges": round(charges, 2), "amount_paid": round(paid, 2),
        "status_totals": dict(status_totals.most_common()),
        "by_date": {
            k: {"total": v["total"], "statuses": dict(v["statuses"]), "charges": round(v["charges"], 2)}
            for k, v in sorted(by_date.items(), reverse=True)
        },
        "parcels": parcels,
        "returns": [p for p in parcels if p["state"] in ("returned", "returning")],
        "payments": payments,
        "pay_summary": pay_summary,
        "unbooked": unbooked,
        "non_shopify": non_shopify,
    }


REPORT_EYE_JS = '<style>\nbutton.eye{border:1px solid #888;background:#fff;border-radius:4px;cursor:pointer;padding:1px 8px}\nbutton.eye.on{background:#222;color:#fff}\ntr.det>td{background:#fafafa;padding:10px 16px}\ntr.det table{margin:6px 0}\n@media (prefers-color-scheme:dark){button.eye{background:#222;color:#eee}tr.det>td{background:#1b1b1b}}\n</style>\n<script>\n(function(){\nvar KEY=new URLSearchParams(location.search).get(\'key\')||\'\';\nfunction esc(s){return String(s==null?\'\':s).replace(/[&<>"]/g,function(c){return {\'&\':\'&amp;\',\'<\':\'&lt;\',\'>\':\'&gt;\',\'"\':\'&quot;\'}[c]})}\nfunction show(d){\n  if(!d||!d.ok) return \'<i>\'+esc((d&&d.error)||\'No tracking data from TCS yet\')+\'</i>\';\n  var h=\'<b>\'+esc(d.cn)+\'</b> &nbsp; \'+esc(d.reference||\'\')+\' &nbsp; \'+esc(d.origin||\'\')+\' &rarr; \'+esc(d.destination||\'\');\n  if(d.products) h+=\'<br>Products: \'+esc(d.products);\n  if(d.summary) h+=\'<br><b>\'+esc(d.summary)+\'</b>\';\n  if(d.delivery&&d.delivery.length){\n    h+=\'<table><tr><th>Delivery info</th><th>Station</th><th>When</th><th>Received by</th></tr>\';\n    d.delivery.forEach(function(x){h+=\'<tr><td>\'+esc(x.status)+\'</td><td>\'+esc(x.station)+\'</td><td>\'+esc(x.datetime)+\'</td><td>\'+esc(x.received_by)+\'</td></tr>\'});\n    h+=\'</table>\';\n  }\n  h+=\'<table><tr><th>When</th><th>Step</th><th>Place / person</th></tr>\';\n  (d.steps||[]).forEach(function(x){h+=\'<tr><td>\'+esc(x.datetime)+\'</td><td>\'+esc(x.status)+\'</td><td>\'+esc(x.by)+\'</td></tr>\'});\n  h+=\'</table><small>Updated \'+new Date().toLocaleTimeString()+\' &middot; refreshes every 10 seconds while open</small>\';\n  return h;\n}\ndocument.addEventListener(\'click\',function(e){\n  var b=e.target.closest(\'button.eye\'); if(!b) return;\n  var tr=b.closest(\'tr\');\n  if(tr._det){clearInterval(tr._iv);tr._det.remove();tr._det=null;b.classList.remove(\'on\');return}\n  var det=document.createElement(\'tr\'); det.className=\'det\';\n  var td=document.createElement(\'td\'); td.colSpan=tr.cells.length; td.textContent=\'Loading...\';\n  det.appendChild(td); tr.after(det); tr._det=det; b.classList.add(\'on\');\n  function load(){\n    fetch(\'/track.json?key=\'+encodeURIComponent(KEY)+\'&q=\'+encodeURIComponent(b.getAttribute(\'data-cn\')),{cache:\'no-store\'})\n      .then(function(r){return r.json()}).then(function(d){td.innerHTML=show(d)})\n      .catch(function(){td.textContent=\'Connection problem, retrying...\'});\n  }\n  load();\n  tr._iv=setInterval(function(){ if(!document.body.contains(det)){clearInterval(tr._iv);return} load(); },10000);\n});\n})();\n</script>\n'


REPORT_GLASSES_JS = '<style>\nbutton.gl{margin-left:6px;border:1px solid #888;background:#fff;border-radius:4px;cursor:pointer;padding:0 6px}\nbutton.gl.on{background:#222;color:#fff}\ntr.gdet>td{background:#f6f9ff;padding:10px 16px}\ntr.gdet table{margin:6px 0}\ntr.gdet th{width:220px}\n@media (prefers-color-scheme:dark){button.gl{background:#222;color:#eee}tr.gdet>td{background:#18202b}}\n</style>\n<script>\n(function(){\nvar KEY=new URLSearchParams(location.search).get(\'key\')||\'\';\nfunction esc(s){return String(s==null?\'\':s).replace(/[&<>"]/g,function(c){return {\'&\':\'&amp;\',\'<\':\'&lt;\',\'>\':\'&gt;\',\'"\':\'&quot;\'}[c]})}\ndocument.addEventListener(\'click\',function(e){\n  var b=e.target.closest(\'button.gl\'); if(!b) return;\n  var tr=b.closest(\'tr\');\n  if(tr._gdet){clearInterval(tr._giv);tr._gdet.remove();tr._gdet=null;b.classList.remove(\'on\');return}\n  var det=document.createElement(\'tr\'); det.className=\'det gdet\';\n  var td=document.createElement(\'td\'); td.colSpan=tr.cells.length; td.textContent=\'Loading...\';\n  det.appendChild(td); (tr._det||tr).after(det); tr._gdet=det; b.classList.add(\'on\');\n  var sr=tr.cells[0]?tr.cells[0].textContent.trim():\'\';\n  function load(){\n    fetch(\'/return-detail.json?key=\'+encodeURIComponent(KEY)+\'&cn=\'+encodeURIComponent(b.getAttribute(\'data-cn\'))+\'&sr=\'+encodeURIComponent(sr),{cache:\'no-store\'})\n      .then(function(r){return r.json()}).then(function(d){\n        if(!d.ok){td.textContent=d.error||\'No data\';return}\n        var h=\'<table>\';\n        d.fields.forEach(function(f){h+=\'<tr><th>\'+esc(f[0])+\'</th><td>\'+esc(f[1])+\'</td></tr>\'});\n        td.innerHTML=h+\'</table><small>Updated \'+new Date().toLocaleTimeString()+\' &middot; refreshes every 10 seconds while open</small>\';\n      }).catch(function(){td.textContent=\'Connection problem, retrying...\'});\n  }\n  load();\n  tr._giv=setInterval(function(){ if(!document.body.contains(det)){clearInterval(tr._giv);return} load(); },10000);\n});\n})();\n</script>\n'


REPORT_LIVE_JS = '<script>\n(function(){\nvar KEY=new URLSearchParams(location.search).get(\'key\')||\'\';\nvar tb=document.getElementById(\'rettbl\'); if(!tb) return;\nvar sumEl=document.getElementById(\'retsum\'), last=\'\';\nfunction esc(s){return String(s==null?\'\':s).replace(/[&<>"]/g,function(c){return {\'&\':\'&amp;\',\'<\':\'&lt;\',\'>\':\'&gt;\',\'"\':\'&quot;\'}[c]})}\nfunction label(th){return th.textContent.replace(/[▲▼]/g,\'\').trim()}\nfunction cell(name,p,n){\n  switch(name){\n    case \'#\': return n;\n    case \'Booked\': return esc(p.booked);\n    case \'Order\': return esc(p.order);\n    case \'CN\': return esc(p.cn);\n    case \'Consignee\': return esc(p.consignee);\n    case \'Destination\': return esc(p.city);\n    case \'Failed delivery reason\': return esc(p.reason);\n    case \'Attempts\': return esc(p.attempts);\n    case \'First attempt\': return esc(p.first_attempt);\n    case \'Last attempt\': return esc(p.last_attempt);\n    case \'Returned on\': return esc(p.returned_on);\n    case \'Status\': return esc(p.status+((p.last_event&&p.state!==\'delivered\'&&p.state!==\'returned\'&&p.state!==\'returning\'&&p.last_event.toLowerCase()!==String(p.status).toLowerCase()&&p.last_event.toLowerCase().indexOf(\'returned to origin\')<0)?\' (\'+p.last_event+\')\':\'\'))+(p.state===\'returned\'?"<button class=\'gl\' title=\'Return details\' data-cn=\'"+esc(p.cn)+"\'>&#128083;</button>":\'\');\n    case \'COD value\': return esc(p.cod);\n    case \'Product\': return esc(p.product);\n    case \'Track\': return "<button class=\'eye\' title=\'Tracking history\' data-cn=\'"+esc(p.cn)+"\'>&#128065;</button>";\n  }\n  return \'\';\n}\nfunction render(d){\n  var openGl={},openEye={};\n  Array.prototype.forEach.call(tb.rows,function(r){\n    var g=r.querySelector(\'button.gl.on\'),e=r.querySelector(\'button.eye.on\');\n    if(g)openGl[g.getAttribute(\'data-cn\')]=1; if(e)openEye[e.getAttribute(\'data-cn\')]=1;\n  });\n  var head=tb.rows[0], names=Array.prototype.map.call(head.cells,label);\n  var body=tb.tBodies[0]||tb;\n  while(tb.rows.length>1) tb.deleteRow(1);\n  var counts={}; d.returns.forEach(function(p){counts[p.booked]=(counts[p.booked]||0)+1});\n  var cur=null,n=0,k=1;\n  d.returns.forEach(function(p){\n    if(p.booked!==cur){\n      cur=p.booked;n=0;\n      var g=document.createElement(\'tr\'); g.className=\'grp\'; g.setAttribute(\'data-day\',cur);\n      g.innerHTML="<td colspan=\'"+names.length+"\' style=\'background:#eef;font-weight:bold\'>"+esc(cur)+" - "+counts[cur]+" parcels</td>";\n      g._ord=k++; body.appendChild(g);\n    }\n    n++;\n    var r=document.createElement(\'tr\'); r.setAttribute(\'data-day\',cur);\n    r.innerHTML=names.map(function(nm){return \'<td>\'+cell(nm,p,n)+\'</td>\'}).join(\'\');\n    r._ord=k++; body.appendChild(r);\n  });\n  if(window.__reportApply) window.__reportApply(tb);\n  tb.querySelectorAll(\'button.gl\').forEach(function(b){if(openGl[b.getAttribute(\'data-cn\')])b.click()});\n  tb.querySelectorAll(\'button.eye\').forEach(function(b){if(openEye[b.getAttribute(\'data-cn\')])b.click()});\n  if(sumEl){\n    var reasons=Object.keys(d.reasons).map(function(r){return esc(r)+\': \'+d.reasons[r]}).join(\' | \')||\'none yet\';\n    sumEl.innerHTML=\'Back with you: <b>\'+d.final+\'</b> (COD value \'+Math.round(d.cod).toLocaleString()+\') &middot; still returning: <b>\'+d.returning+\'</b><br>Reasons: \'+reasons+\'<br><small>Live &middot; TCS checked \'+d.age+\'s ago &middot; page refreshed \'+new Date().toLocaleTimeString()+\'</small>\';\n  }\n}\nfunction poll(){var sec=tb.closest(\'.sec\');if(document.hidden||(sec&&sec.style.display===\'none\'))return;\n  fetch(\'/returns.json?key=\'+encodeURIComponent(KEY)+\'&from=\'+encodeURIComponent((document.querySelector(\'input[name=from]\')||{}).value||\'\')+\'&to=\'+encodeURIComponent((document.querySelector(\'input[name=to]\')||{}).value||\'\'),{cache:\'no-store\'}).then(function(r){return r.json()}).then(function(d){\n    if(!d.returns) return;\n    var sig=JSON.stringify(d.returns)+d.final+d.returning;\n    if(sig!==last){last=sig;render(d)} else if(sumEl){\n      sumEl.querySelector(\'small\')&&(sumEl.querySelector(\'small\').innerHTML=\'Live &middot; TCS checked \'+d.age+\'s ago &middot; page refreshed \'+new Date().toLocaleTimeString());\n    }\n  }).catch(function(){});\n}\npoll(); setInterval(poll,1000);\n})();\n</script>\n'


REPORT_PARCELS_JS = '<script>\n(function(){\nvar KEY=new URLSearchParams(location.search).get(\'key\')||\'\';\nvar tb=document.getElementById(\'partbl\'); if(!tb) return;\nvar last=\'\';\nfunction esc(s){return String(s==null?\'\':s).replace(/[&<>"]/g,function(c){return {\'&\':\'&amp;\',\'<\':\'&lt;\',\'>\':\'&gt;\',\'"\':\'&quot;\'}[c]})}\nfunction num(v){return v==null||v===\'\'?\'\':Number(v).toLocaleString(undefined,{maximumFractionDigits:2})}\nfunction label(th){return th.textContent.replace(/[▲▼]/g,\'\').trim()}\nfunction cell(name,p,n){\n  switch(name){\n    case \'#\': return n;\n    case \'CN\': return esc(p.cn);\n    case \'Booked\': return esc(p.booked);\n    case \'Delivered on\': return esc(p.delivered_on);\n    case \'Status\': return esc(p.status+((p.last_event&&p.state!==\'delivered\'&&p.state!==\'returned\'&&p.state!==\'returning\'&&p.last_event.toLowerCase()!==String(p.status).toLowerCase()&&p.last_event.toLowerCase().indexOf(\'returned to origin\')<0)?\' (\'+p.last_event+\')\':\'\'))+(p.state===\'returned\'?"<button class=\'gl\' title=\'Return details\' data-cn=\'"+esc(p.cn)+"\'>&#128083;</button>":\'\');\n    case \'Current step\': return esc(p.last_event);\n    case \'Step time\': return esc(p.last_time);\n    case \'City\': return esc(p.city);\n    case \'Order\': return esc(p.order);\n    case \'Product\': return esc(p.product);\n    case \'Kg\': return esc(p.weight);\n    case \'Payment\': return esc(p.pay_status);\n    case \'Paid on\': return esc(p.pay_date);\n    case \'COD amount\': return num(p.cod);\n    case \'Shipping CHG\': return num(p.shipping);\n    case \'Fuel\': return num(p.fuel);\n    case \'Sales tax\': return num(p.stax);\n    case \'GST WH 2%\': return num(p.gst_wh);\n    case \'IT WH 2%\': return num(p.it_wh);\n    case \'Net payable\': return num(p.net);\n    case \'Track\': return "<button class=\'eye\' title=\'Tracking history\' data-cn=\'"+esc(p.cn)+"\'>&#128065;</button>";\n  }\n  return \'\';\n}\nfunction render(d){\n  var openGl={},openEye={};\n  Array.prototype.forEach.call(tb.rows,function(r){\n    var g=r.querySelector(\'button.gl.on\'),e=r.querySelector(\'button.eye.on\');\n    if(g)openGl[g.getAttribute(\'data-cn\')]=1; if(e)openEye[e.getAttribute(\'data-cn\')]=1;\n  });\n  var names=Array.prototype.map.call(tb.rows[0].cells,label), body=tb.tBodies[0]||tb;\n  while(tb.rows.length>1) tb.deleteRow(1);\n  var counts={}; d.parcels.forEach(function(p){counts[p.booked]=(counts[p.booked]||0)+1});\n  var cur=null,n=0,k=1;\n  d.parcels.forEach(function(p){\n    if(p.booked!==cur){\n      cur=p.booked;n=0;\n      var g=document.createElement(\'tr\'); g.className=\'grp\'; g.setAttribute(\'data-day\',cur);\n      g.innerHTML="<td colspan=\'"+names.length+"\' style=\'background:#eef;font-weight:bold\'>"+esc(cur)+" - "+counts[cur]+" parcels</td>";\n      g._ord=k++; body.appendChild(g);\n    }\n    n++;\n    var r=document.createElement(\'tr\'); r.setAttribute(\'data-day\',cur);\n    r.innerHTML=names.map(function(nm){return \'<td>\'+cell(nm,p,n)+\'</td>\'}).join(\'\');\n    r._ord=k++; body.appendChild(r);\n  });\n  if(window.__reportApply) window.__reportApply(tb);\n  tb.querySelectorAll(\'button.gl\').forEach(function(b){if(openGl[b.getAttribute(\'data-cn\')])b.click()});\n  tb.querySelectorAll(\'button.eye\').forEach(function(b){if(openEye[b.getAttribute(\'data-cn\')])b.click()});\n}\nfunction poll(){var sec=tb.closest(\'.sec\');if(document.hidden||(sec&&sec.style.display===\'none\'))return;\n  fetch(\'/parcels.json?key=\'+encodeURIComponent(KEY)+\'&from=\'+encodeURIComponent((document.querySelector(\'input[name=from]\')||{}).value||\'\')+\'&to=\'+encodeURIComponent((document.querySelector(\'input[name=to]\')||{}).value||\'\'),{cache:\'no-store\'}).then(function(r){return r.json()}).then(function(d){\n    if(!d.parcels) return;\n    var sig=JSON.stringify(d.parcels);\n    if(sig!==last){ if(last!==\'\' || true){render(d)} last=sig; }\n  }).catch(function(){});\n}\npoll(); setInterval(poll,1000);\n})();\n</script>\n'


REPORT_SUM_JS = '<script>\n(function(){\nvar KEY=new URLSearchParams(location.search).get(\'key\')||\'\';\nvar tb=document.getElementById(\'sumtbl\'); if(!tb) return;\nvar last=\'\';\nfunction esc(s){return String(s==null?\'\':s).replace(/[&<>"]/g,function(c){return {\'&\':\'&amp;\',\'<\':\'&lt;\',\'>\':\'&gt;\',\'"\':\'&quot;\'}[c]})}\nfunction label(th){return th.textContent.replace(/[▲▼]/g,\'\').trim()}\nfunction num(v){return v==null||v===\'\'?\'\':Number(v).toLocaleString(undefined,{maximumFractionDigits:2})}\nfunction cell(name,p,n){\n  switch(name){\n    case \'#\': return n;\n    case \'Payment\': return \'<b>\'+esc(p.group)+\'</b>\';\n    case \'Parcels\': return esc(p.parcels);\n    case \'Orders\': return esc(p.orders);\n    case \'Products\': return esc(p.products);\n    case \'COD\': return num(p.cod);\n    case \'Shipping CHG\': return num(p.shipping);\n    case \'Fuel\': return num(p.fuel);\n    case \'Sales tax\': return num(p.stax);\n    case \'GST WH 2%\': return num(p.gst_wh);\n    case \'IT WH 2%\': return num(p.it_wh);\n    case \'Total deducted\': return num(p.deductions);\n    case \'Net to receive\': return \'<b>\'+num(p.net)+\'</b>\';\n    case \'What it is\': return esc(p.note);\n  }\n  return \'\';\n}\nfunction render(d){\n  var names=Array.prototype.map.call(tb.rows[0].cells,label), body=tb.tBodies[0]||tb;\n  while(tb.rows.length>1) tb.deleteRow(1);\n  d.summary.forEach(function(p,i){\n    var r=document.createElement(\'tr\'); if(p.kind===\'total\') r.className=\'tot\';\n    r.innerHTML=names.map(function(nm){return \'<td>\'+cell(nm,p,i+1)+\'</td>\'}).join(\'\');\n    r._ord=i; body.appendChild(r);\n  });\n  if(window.__reportApply) window.__reportApply(tb);\n}\nfunction poll(){\n  var sec=tb.closest(\'.sec\'); if(document.hidden||(sec&&sec.style.display===\'none\')) return;\n  fetch(\'/payments.json?key=\'+encodeURIComponent(KEY)+\'&from=\'+encodeURIComponent((document.querySelector(\'input[name=from]\')||{}).value||\'\')+\'&to=\'+encodeURIComponent((document.querySelector(\'input[name=to]\')||{}).value||\'\'),{cache:\'no-store\'})\n    .then(function(r){return r.json()}).then(function(d){\n      if(!d.summary) return;\n      var sig=JSON.stringify(d.summary);\n      if(sig!==last){last=sig;render(d)}\n    }).catch(function(){});\n}\npoll(); setInterval(poll,1000);\n})();\n</script>\n'


REPORT_PAY_JS = '<script>\n(function(){\nvar KEY=new URLSearchParams(location.search).get(\'key\')||\'\';\nvar tb=document.getElementById(\'paytbl\'); if(!tb) return;\nvar sumEl=document.getElementById(\'paysum\'), last=\'\';\nfunction esc(s){return String(s==null?\'\':s).replace(/[&<>"]/g,function(c){return {\'&\':\'&amp;\',\'<\':\'&lt;\',\'>\':\'&gt;\',\'"\':\'&quot;\'}[c]})}\nfunction label(th){return th.textContent.replace(/[▲▼]/g,\'\').trim()}\nfunction num(v){return v==null||v===\'\'?\'\':Number(v).toLocaleString(undefined,{maximumFractionDigits:2})}\nfunction cell(name,p,n){\n  switch(name){\n    case \'#\': return n;\n    case \'CN\': return esc(p.cn);\n    case \'Order\': return esc(p.order);\n    case \'Consignee\': return esc(p.consignee);\n    case \'Destination\': return esc(p.city);\n    case \'Kg\': return esc(p.weight);\n    case \'Status\': return esc(p.status);\n    case \'Payment\': return esc(p.pay_status);\n    case \'Paid on\': return esc(p.pay_date);\n    case \'COD amount\': return num(p.cod);\n    case \'Shipping CHG\': return num(p.shipping);\n    case \'Fuel\': return num(p.fuel);\n    case \'OSA\': return num(p.osa);\n    case \'Sales tax\': return num(p.stax);\n    case \'GST WH 2%\': return num(p.gst_wh);\n    case \'IT WH 2%\': return num(p.it_wh);\n    case \'Addl IT WH\': return num(p.addl);\n    case \'Net payable\': return \'<b>\'+num(p.net)+\'</b>\';\n    case \'Track\': return "<button class=\'eye\' title=\'Tracking history\' data-cn=\'"+esc(p.cn)+"\'>&#128065;</button>";\n  }\n  return \'\';\n}\nfunction render(d){\n  var open={};\n  Array.prototype.forEach.call(tb.rows,function(r){var e=r.querySelector(\'button.eye.on\');if(e)open[e.getAttribute(\'data-cn\')]=1});\n  var names=Array.prototype.map.call(tb.rows[0].cells,label), body=tb.tBodies[0]||tb;\n  while(tb.rows.length>1) tb.deleteRow(1);\n  var counts={}; d.payments.forEach(function(p){counts[p.booked]=(counts[p.booked]||0)+1});\n  var cur=null,n=0,k=1;\n  d.payments.forEach(function(p){\n    if(p.booked!==cur){\n      cur=p.booked;n=0;\n      var g=document.createElement(\'tr\'); g.className=\'grp\'; g.setAttribute(\'data-day\',cur);\n      g.innerHTML="<td colspan=\'"+names.length+"\' style=\'background:#eef;font-weight:bold\'>"+esc(cur)+" - "+counts[cur]+" parcels</td>";\n      g._ord=k++; body.appendChild(g);\n    }\n    n++;\n    var r=document.createElement(\'tr\'); r.setAttribute(\'data-day\',cur);\n    r.innerHTML=names.map(function(nm){return \'<td>\'+cell(nm,p,n)+\'</td>\'}).join(\'\');\n    r._ord=k++; body.appendChild(r);\n  });\n  if(window.__reportApply) window.__reportApply(tb);\n  tb.querySelectorAll(\'button.eye\').forEach(function(b){if(open[b.getAttribute(\'data-cn\')])b.click()});\n  if(sumEl){var t=d.totals; sumEl.innerHTML=\'COD <b>\'+num(t.cod)+\'</b> &middot; Shipping <b>\'+num(t.shipping)+\'</b> &middot; Fuel <b>\'+num(t.fuel)+\'</b> &middot; Sales tax <b>\'+num(t.stax)+\'</b> &middot; GST WH <b>\'+num(t.gst_wh)+\'</b> &middot; IT WH <b>\'+num(t.it_wh)+\'</b> &middot; Net payable <b>\'+num(t.net)+\'</b><br><small>Paid parcels: \'+t.paid+\' &middot; pending: \'+t.pending+\' &middot; updated \'+new Date().toLocaleTimeString()+\'</small>\'}\n}\nfunction poll(){\n  var sec=tb.closest(\'.sec\'); if(document.hidden||(sec&&sec.style.display===\'none\')) return;\n  fetch(\'/payments.json?key=\'+encodeURIComponent(KEY)+\'&from=\'+encodeURIComponent((document.querySelector(\'input[name=from]\')||{}).value||\'\')+\'&to=\'+encodeURIComponent((document.querySelector(\'input[name=to]\')||{}).value||\'\'),{cache:\'no-store\'})\n    .then(function(r){return r.json()}).then(function(d){\n      if(!d.payments) return;\n      var sig=JSON.stringify(d.payments);\n      if(sig!==last){last=sig;render(d)}\n    }).catch(function(){});\n}\npoll(); setInterval(poll,1000);\n})();\n</script>\n'


REPORT_CTL_JS = '<style>\n#tablebar{margin:10px 0;padding:8px 12px;background:#f3f3f3;border-radius:6px}\n#tablebar label{margin-right:14px;white-space:nowrap;cursor:pointer}\n.pager{margin:4px 0 6px;font-size:14px}\n.pager button{padding:2px 10px;margin:0 3px;cursor:pointer}\n.pager select{padding:2px 4px}\n@media (prefers-color-scheme:dark){#tablebar{background:#222}}\n</style>\n<script>\n(function(){\nvar secs=Array.prototype.slice.call(document.querySelectorAll(\'.sec\'));\nvar TITLES={\'sec-date\':\'Per date\',\'sec-status\':\'Status summary\',\'sec-returns\':\'Return analysis\',\'sec-paysum\':\'TCS payout\',\'sec-payments\':\'Payments\',\'sec-parcels\':\'All parcels\',\'sec-nonshopify\':\'Non-Shopify\',\'sec-unbooked\':\'Not booked\'};\nvar saved={}; try{saved=JSON.parse(localStorage.getItem(\'rtables\')||\'{}\')}catch(e){}\nvar bar=document.createElement(\'div\'); bar.id=\'tablebar\';\nbar.innerHTML=\'<b>Tables shown:</b> \';\nsecs.forEach(function(sec){\n  var on=saved[sec.id]!==false;\n  var lb=document.createElement(\'label\'); var cb=document.createElement(\'input\'); cb.type=\'checkbox\'; cb.checked=on;\n  lb.appendChild(cb); lb.appendChild(document.createTextNode(\' \'+(TITLES[sec.id]||sec.id)));\n  bar.appendChild(lb); sec.style.display=on?\'\':\'none\';\n  cb.addEventListener(\'change\',function(){\n    sec.style.display=cb.checked?\'\':\'none\'; saved[sec.id]=cb.checked;\n    try{localStorage.setItem(\'rtables\',JSON.stringify(saved))}catch(e){}\n  });\n});\nvar mb=document.getElementById(\'modebar\'); if(mb) mb.parentNode.insertBefore(bar,mb); else document.body.insertBefore(bar,document.body.firstChild);\n\nvar SIZES=[10,25,50,100,300,500,0];\nvar size=300; try{size=parseInt(localStorage.getItem(\'rsize2\')||\'300\',10)}catch(e){}\nfunction isData(r){return !r.classList.contains(\'grp\')&&!r.classList.contains(\'tot\')&&!r.classList.contains(\'det\')}\nfunction paginate(tb){\n  if(!tb._pager) return;\n  var rows=Array.prototype.slice.call(tb.rows,1);\n  var total=rows.filter(isData).length;\n  var sz=tb._size, pages=sz?Math.max(1,Math.ceil(total/sz)):1;\n  if(tb._page>=pages) tb._page=pages-1; if(tb._page<0) tb._page=0;\n  var start=sz?tb._page*sz:0, end=sz?start+sz:total+1, idx=0, lastVisible=true;\n  var grpRow=null, grpHasVisible=false;\n  function flushGrp(){ if(grpRow) grpRow.style.display=grpHasVisible?\'\':\'none\' }\n  rows.forEach(function(r){\n    if(r.classList.contains(\'grp\')){\n      flushGrp(); grpHasVisible=false;\n      if(r._hid){grpRow=null;return}\n      grpRow=r;\n    } else if(isData(r)){\n      var vis=idx>=start&&idx<end; idx++;\n      r.style.display=vis?\'\':\'none\'; lastVisible=vis; if(vis) grpHasVisible=true;\n    } else if(r.classList.contains(\'det\')){\n      r.style.display=lastVisible?\'\':\'none\';\n    }\n  });\n  flushGrp();\n  var p=tb._pager;\n  p.querySelector(\'.pinfo\').textContent=total?(\'Showing \'+(Math.min(start+1,total))+\' to \'+Math.min(end,total)+\' of \'+total+\' entries - page \'+(tb._page+1)+\' of \'+pages):\'No entries\';\n  p.querySelector(\'.pprev\').disabled=tb._page<=0; p.querySelector(\'.pnext\').disabled=tb._page>=pages-1;\n}\nwindow.__paginate=paginate;\ndocument.querySelectorAll(\'.sec .w table\').forEach(function(tb){\n  var wrap=tb.parentElement; if(wrap.parentElement.querySelector(\'.pager\')) return;\n  var p=document.createElement(\'div\'); p.className=\'pager\';\n  p.innerHTML=\'Show <select class="psize">\'+SIZES.map(function(s){return \'<option value="\'+s+\'"\'+(s===size?\' selected\':\'\')+\'>\'+(s||\'All\')+\'</option>\'}).join(\'\')+\'</select> entries \'+\n    \'<button class="pprev">&laquo; Prev</button><button class="pnext">Next &raquo;</button> <span class="pinfo"></span>\';\n  wrap.parentNode.insertBefore(p,wrap);\n  tb._pager=p; tb._size=size; tb._page=0;\n  p.querySelector(\'.psize\').addEventListener(\'change\',function(e){\n    size=parseInt(e.target.value,10); try{localStorage.setItem(\'rsize2\',String(size))}catch(x){}\n    document.querySelectorAll(\'.sec .w table\').forEach(function(t){ if(t._pager){t._size=size;t._page=0;t._pager.querySelector(\'.psize\').value=String(size);paginate(t)} });\n  });\n  p.querySelector(\'.pprev\').addEventListener(\'click\',function(){tb._page--;paginate(tb)});\n  p.querySelector(\'.pnext\').addEventListener(\'click\',function(){tb._page++;paginate(tb)});\n  paginate(tb);\n});\n})();\n</script>\n'


REPORT_SORT_JS = '<style>th{cursor:pointer;user-select:none;white-space:nowrap}th.over{outline:2px dashed #888}th .ar{font-size:10px;color:#666}\n#modebar button{padding:5px 12px;border:1px solid #888;background:#fff;cursor:pointer}#modebar button.on{background:#222;color:#fff}</style>\n<script>\n(function(){\nvar mode=\'date\'; try{mode=localStorage.getItem(\'rmode\')||\'date\'}catch(e){}\nfunction val(td){var t=td.textContent.trim();var n=parseFloat(t.replace(/,/g,\'\'));return (t!==\'\'&&!isNaN(n)&&/^[\\d.,\\-\\s]+$/.test(t))?[0,n]:[1,t.toLowerCase()]}\nfunction cmp(i,d){return function(a,b){var x=val(a.cells[i]),y=val(b.cells[i]);var c=x[0]-y[0]||(x[1]<y[1]?-1:x[1]>y[1]?1:0);return d===\'asc\'?c:-c}}\nfunction numCol(head){var nc=-1;Array.prototype.forEach.call(head.cells,function(h,k){if(h.textContent.replace(/[▲▼\\s]/g,\'\')===\'#\')nc=k});return nc}\nfunction apply(tb){try{apply0(tb)}finally{if(window.__paginate)window.__paginate(tb)}}function apply0(tb){\n  var head=tb.rows[0], st=tb._sort, body=tb.tBodies[0]||tb;\n  var all=Array.prototype.slice.call(tb.rows,1);\n  var data=all.filter(function(r){return !r.classList.contains(\'grp\')&&!r.classList.contains(\'tot\')&&!r.classList.contains(\'det\')});\n  var tots=all.filter(function(r){return r.classList.contains(\'tot\')});\n  var grps=all.filter(function(r){return r.classList.contains(\'grp\')});\n  var nc=numCol(head);\n  function order(rows){ if(st&&st.th.parentNode===head){rows=rows.slice().sort(cmp(st.th.cellIndex,st.d))}else{rows=rows.slice().sort(function(a,b){return a._ord-b._ord})} return rows}\n  if(mode===\'date\'&&grps.length){\n    grps.forEach(function(g){g.style.display=\'\';g._hid=false;body.appendChild(g);\n      var rows=order(data.filter(function(r){return r.getAttribute(\'data-day\')===g.getAttribute(\'data-day\')}));\n      rows.forEach(function(r,k){body.appendChild(r);if(r._det)body.appendChild(r._det);if(r._gdet)body.appendChild(r._gdet);if(nc>=0)r.cells[nc].textContent=k+1})});\n  }else{\n    grps.forEach(function(g){g.style.display=\'none\';g._hid=true});\n    order(data).forEach(function(r,k){body.appendChild(r);if(r._det)body.appendChild(r._det);if(r._gdet)body.appendChild(r._gdet);if(nc>=0)r.cells[nc].textContent=k+1});\n  }\n  tots.forEach(function(r){body.appendChild(r)});\n  Array.prototype.forEach.call(head.cells,function(h){h.innerHTML=h.textContent.replace(/[▲▼\\s]+$/,\'\')});\n  if(st&&st.th.parentNode===head)st.th.innerHTML=st.th.textContent+\' <span class="ar">\'+(st.d===\'asc\'?\'▲\':\'▼\')+\'</span>\';\n}\nvar tables=Array.prototype.slice.call(document.querySelectorAll(\'.w table\'));window.__reportApply=apply;\ntables.forEach(function(tb){\n  var head=tb.rows[0]; if(!head) return;\n  Array.prototype.forEach.call(tb.rows,function(r,k){r._ord=k});\n  Array.prototype.forEach.call(head.cells,function(th){th.draggable=true});\n  head.addEventListener(\'click\',function(e){\n    var th=e.target.closest(\'th\'); if(!th) return;\n    var d=(tb._sort&&tb._sort.th===th&&tb._sort.d===\'asc\')?\'desc\':\'asc\';\n    tb._sort={th:th,d:d}; apply(tb);\n  });\n  var from=null;\n  head.addEventListener(\'dragstart\',function(e){var th=e.target.closest(\'th\');if(th){from=th.cellIndex;e.dataTransfer.setData(\'text\',\'c\')}});\n  head.addEventListener(\'dragover\',function(e){var th=e.target.closest(\'th\');if(th){e.preventDefault();th.classList.add(\'over\')}});\n  head.addEventListener(\'dragleave\',function(e){var th=e.target.closest(\'th\');if(th)th.classList.remove(\'over\')});\n  head.addEventListener(\'drop\',function(e){\n    e.preventDefault(); var th=e.target.closest(\'th\'); if(!th||from===null) return; th.classList.remove(\'over\');\n    var to=th.cellIndex; if(to===from) return;\n    Array.prototype.forEach.call(tb.rows,function(r){\n      if(r.cells.length<=Math.max(from,to)) return;\n      var c=r.cells[from]; var ref=r.cells[to]; r.insertBefore(c, to>from?ref.nextSibling:ref);\n    });\n    from=null;\n  });\n});\nfunction setMode(m){\n  mode=m; try{localStorage.setItem(\'rmode\',m)}catch(e){}\n  document.querySelectorAll(\'#modebar button\').forEach(function(b){b.classList.toggle(\'on\',b.getAttribute(\'data-m\')===m)});\n  tables.forEach(apply);\n}\ndocument.querySelectorAll(\'#modebar button\').forEach(function(b){b.addEventListener(\'click\',function(){setMode(b.getAttribute(\'data-m\'))})});\nsetMode(mode);\n})();\n</script>\n'


def _status_text(it: dict) -> str:
    """'In Transit (Address Information Needed)': the newest TCS step in brackets after the status."""
    st, ev = str(it.get("status") or ""), str(it.get("last_event") or "")
    if ev and it.get("state") not in ("delivered", "returned", "returning") and ev.lower() != st.lower():
        return f"{st} ({ev})"
    return st


def _grouped_rows(items: list, date_key: str, keys: tuple, esc, ncols: int, eye_key: str = "") -> str:
    """Table rows grouped by date: a heading row with the day's count, numbering restarts each day."""
    out, cur, n, groups = [], None, 0, {}
    for it in items:
        groups[str(it.get(date_key) or "unknown")] = groups.get(str(it.get(date_key) or "unknown"), 0) + 1
    for it in items:
        day = str(it.get(date_key) or "unknown")
        if day != cur:
            cur, n = day, 0
            out.append(f"<tr class='grp' data-day='{esc(day)}'><td colspan='{ncols}' style='background:#eef;font-weight:bold'>"
                       f"{esc(day)} - {groups[day]} parcels</td></tr>")
        n += 1
        out.append(f"<tr data-day='{esc(day)}'><td>{n}</td>" + "".join(
            (f"<td>{esc(_status_text(it) if k == 'status' else (str(it[k]) if it.get(k) is not None else ''))}"
             + (f"<button class='gl' title='Return details' data-cn='{esc(str(it.get('cn') or ''))}'>&#128083;</button>"
                if k == "status" and it.get("state") == "returned" else "") + "</td>")
            for k in keys)
            + (f"<td><button class='eye' title='Tracking history' data-cn='{esc(str(it.get(eye_key) or ''))}'>&#128065;</button></td>"
               if eye_key else "") + "</tr>")
    return "".join(out)


def _products_text(orders: list) -> str:
    cnt = Counter()
    for o in orders:
        for title, q in _order_items.get(o, []):
            cnt[title] += q
    return "; ".join(f"{t} x {q}" for t, q in cnt.most_common())


def _pay_summary(payments: list) -> list:
    """What TCS has paid, what it still owes, and what it keeps for charges - all from TCS's own figures.
    Only the 'awaiting' row uses an estimate (Shopify total) and says so."""
    def row(group, kind, items, note, cod=None, **over):
        r = {"group": group, "kind": kind, "parcels": len(items),
             "orders": ", ".join(p["order"] for p in items[:6]) + (" ..." if len(items) > 6 else ""),
             "products": _products_text([p["order"] for p in items]),
             "cod": round(sum(p["cod"] for p in items), 2) if cod is None else round(cod, 2),
             "shipping": round(sum(p["shipping"] for p in items), 2), "fuel": round(sum(p["fuel"] for p in items), 2),
             "stax": round(sum(p["stax"] for p in items), 2), "gst_wh": round(sum(p["gst_wh"] for p in items), 2),
             "it_wh": round(sum(p["it_wh"] for p in items), 2), "note": note}
        r.update(over)
        r["deductions"] = round(r["shipping"] + r["fuel"] + r["stax"] + r["gst_wh"] + r["it_wh"], 2)
        r["net"] = round(r["cod"] - r["deductions"], 2)
        return r

    out = []
    paid = [p for p in payments if p["pay_status"] == "Paid"]
    days = sorted({p["pay_date"][:10] for p in paid}, key=lambda d: d[6:10] + d[3:5] + d[:2])
    for d in days:
        items = [p for p in paid if p["pay_date"][:10] == d]
        out.append(row(f"Paid on {d}", "paid", items,
                       "TCS paid this. COD collected minus shipping, fuel, sales tax and the 2% GST and IT withholding (TCS figures)."))
    delivered_unpaid = [p for p in payments if p["pay_status"] != "Paid" and p["state"] == "delivered"]
    if delivered_unpaid:
        exp = 0.0
        for p in delivered_unpaid:
            try:
                exp += int(float(_order_totals.get(p["order"], 0)))  # TCS collects whole rupees (1269.88 -> 1269), as on the paid parcels
            except (TypeError, ValueError):
                pass
        r = row("Delivered, awaiting payment", "awaiting", delivered_unpaid,
                "ESTIMATE: COD = your Shopify order total. TCS has delivered these but not paid yet (some are not even "
                "in its payment list yet, so their charges are the usual 150 + fuel + tax); withholding "
                "shown as 2% + 2% of that COD, as TCS did on the paid parcels.", cod=exp)
        r["gst_wh"] = round(exp * 0.02, 2)
        r["it_wh"] = round(exp * 0.02, 2)
        r["deductions"] = round(r["shipping"] + r["fuel"] + r["stax"] + r["gst_wh"] + r["it_wh"], 2)
        r["net"] = round(r["cod"] - r["deductions"], 2)
        out.append(r)
    back = [p for p in payments if p["pay_status"] == "Pending" and p["state"] in ("returned", "returning", "rejected")]
    if back:
        out.append(row("Returned / refused (charges only)", "charges", back,
                       "No COD comes. TCS still bills the shipping, fuel and sales tax for these parcels and shows them as a "
                       "negative net payable in its statement, so they reduce what TCS pays you. Return charges are not refunded."))
    moving = [p for p in payments if p["pay_status"] == "Pending" and p["state"] not in ("delivered", "returned", "returning", "rejected")]
    if moving:
        out.append(row("In transit (charges so far)", "charges", moving,
                       "Not delivered yet. TCS already shows the shipping, fuel and sales tax as a negative net payable. "
                       "If the parcel is delivered, the COD is added; if it comes back, the charge stays."))
    try:  # TCS billing-week invoices and money that really reached your bank (typed in received.json)
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "received.json"), encoding="utf-8") as f:
            ledger = json.load(f)
    except (OSError, ValueError):
        ledger = {}
    inv, got = ledger.get("invoices", []), ledger.get("received", [])
    for v in inv:
        out.append({"group": f"TCS INVOICE {v['week']}", "kind": "invoice", "parcels": "", "orders": "", "products": "",
                    "cod": "", "shipping": "", "fuel": "", "stax": "", "gst_wh": v["wht_st"], "it_wh": v["wht_it"],
                    "deductions": "", "net": float(v["paid"]),
                    "note": f"TCS's own invoice: payable {v['payable']:,}, paid {v['paid']:,} on {v['paid_date']} "
                            f"(invoice date {v['invoice_date']}); withholding IT {v['wht_it']} + ST {v['wht_st']}."})
    for g in got:
        out.append({"group": f"RECEIVED {g['at']}", "kind": "received", "parcels": "", "orders": "", "products": "",
                    "cod": "", "shipping": "", "fuel": "", "stax": "", "gst_wh": "", "it_wh": "", "deductions": "",
                    "net": round(float(g["amount"]), 2), "note": g.get("note", "")})
    if got and inv:
        rec = round(sum(float(g["amount"]) for g in got), 2)
        paid_inv = round(sum(float(v["paid"]) for v in inv), 2)
        out.append({"group": "BANK minus INVOICES", "kind": "total", "parcels": "", "orders": "", "products": "",
                    "cod": "", "shipping": "", "fuel": "", "stax": "", "gst_wh": "", "it_wh": "", "deductions": "",
                    "net": round(rec - paid_inv, 2),
                    "note": f"Bank {rec:,.2f} vs invoices paid {paid_inv:,.2f}. Week 08-14 Oct matches exactly; the first "
                            "bank payment (15 Oct) is higher than the 01-07 Oct invoice - ask TCS what the extra covers."})
    if out:
        def total(group, kind_rows, note):
            t = {k: round(sum(r[k] for r in kind_rows), 2) for k in ("cod", "shipping", "fuel", "stax", "gst_wh", "it_wh", "deductions", "net")}
            return {"group": group, "kind": "total", "parcels": sum(r["parcels"] for r in kind_rows),
                    "orders": "", "products": "", "note": note, **t}
        paid_rows = [r for r in out if r["kind"] == "paid"]
        open_rows = [r for r in out if r["kind"] in ("awaiting", "charges")]
        if paid_rows:
            out.append(total("PARCEL-LEVEL PAID rows (not the invoice)", paid_rows, "Per-parcel figures from TCS's ledger. TCS bills by week and rounds the withholding, so they do not add up to the invoice amounts."))
        if open_rows:
            out.append(total("STILL OPEN (rough guess, not reliable)", open_rows,
                             "Per-parcel arithmetic only. TCS bills by week and its invoices differ from the per-parcel sums "
                             "(week 08-14 Oct: parcel figures give 6,523.56, the invoice says 6,396). Use the TCS INVOICE rows as the truth."))
    return out


def _pay_totals(payments: list) -> dict:
    t = {k: round(sum(p[k] for p in payments), 2) for k in ("cod", "shipping", "fuel", "stax", "gst_wh", "it_wh", "net")}
    t["paid"] = sum(1 for p in payments if p["pay_status"] == "Paid")
    t["pending"] = len(payments) - t["paid"]
    return t


def _paysum_html(rep: dict, esc) -> str:
    rows = rep.get("pay_summary", [])
    heads = ("Payment", "Parcels", "Orders", "Products", "COD", "Shipping CHG", "Fuel", "Sales tax", "GST WH 2%", "IT WH 2%",
             "Total deducted", "Net to receive", "What it is")
    keys = ("group", "parcels", "orders", "products", "cod", "shipping", "fuel", "stax", "gst_wh", "it_wh", "deductions", "net", "note")
    body = "".join(
        "<tr" + (" class='tot'" if r["kind"] == "total" else "") + f"><td>{i}</td>"
        + "".join(f"<td>{esc(str(r.get(k, '')))}</td>" for k in keys) + "</tr>"
        for i, r in enumerate(rows, 1))
    return (
        "<div class='sec' id='sec-paysum'><h3>What TCS pays you and what it deducts</h3><div class='w'><table id='sumtbl'>"
        "<tr><th>#</th>" + "".join(f"<th>{h}</th>" for h in heads) + "</tr>" + body + "</table></div></div>"
    )


def _payments_html(rep: dict, esc) -> str:
    pays = rep.get("payments", [])
    t = _pay_totals(pays)
    keys = ("cn", "order", "consignee", "city", "weight", "status", "pay_status", "pay_date", "cod", "shipping",
            "fuel", "osa", "stax", "gst_wh", "it_wh", "addl", "net")
    heads = ("CN", "Order", "Consignee", "Destination", "Kg", "Status", "Payment", "Paid on", "COD amount",
             "Shipping CHG", "Fuel", "OSA", "Sales tax", "GST WH 2%", "IT WH 2%", "Addl IT WH", "Net payable")
    return (
        f"<div class='sec' id='sec-payments'><h3>Payment details ({len(pays)})</h3>"
        f"<p id='paysum'>COD <b>{t['cod']:,.2f}</b> &middot; Shipping <b>{t['shipping']:,.2f}</b> &middot; "
        f"Fuel <b>{t['fuel']:,.2f}</b> &middot; Sales tax <b>{t['stax']:,.2f}</b> &middot; GST WH <b>{t['gst_wh']:,.2f}</b> "
        f"&middot; IT WH <b>{t['it_wh']:,.2f}</b> &middot; Net payable <b>{t['net']:,.2f}</b></p><div class='w'><table id='paytbl'>"
        "<tr><th>#</th>" + "".join(f"<th>{h}</th>" for h in heads) + "<th>Track</th></tr>"
        + _grouped_rows(pays, "booked", keys, esc, len(keys) + 2, eye_key="cn")
        + "</table></div></div>"
    )


def _returns_html(rep: dict, esc) -> str:
    rets = rep.get("returns", [])
    final = [p for p in rets if p["state"] == "returned"]
    by_reason = Counter(p["reason"] or "UNKNOWN" for p in final)
    cod = sum(float(p["cod"]) for p in final if str(p.get("cod") or "").replace(".", "", 1).isdigit())
    summary = (" | ".join(f"{esc(r)}: {n}" for r, n in by_reason.most_common()) or "none yet")
    keys = ("booked", "order", "cn", "consignee", "city", "reason", "attempts", "first_attempt",
            "last_attempt", "returned_on", "status", "cod", "product")
    heads = ("Booked", "Order", "CN", "Consignee", "Destination", "Failed delivery reason", "Attempts",
             "First attempt", "Last attempt", "Returned on", "Status", "COD value", "Product")
    return (
        f"<div class='sec' id='sec-returns'><h3>Return Shipment Analysis ({len(rets)})</h3>"
        f"<p id='retsum'>Back with you: <b>{len(final)}</b> (COD value {cod:,.0f}) &middot; still returning: "
        f"<b>{len(rets) - len(final)}</b><br>Reasons: {summary}</p><div class='w'><table id='rettbl'>"
        "<tr><th>#</th>" + "".join(f"<th>{h}</th>" for h in heads) + "<th>Track</th></tr>"
        + _grouped_rows(rets, "booked", keys, esc, len(keys) + 2, eye_key="cn")
        + "</table></div></div>"
    )


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
    parcel_rows = _grouped_rows(rep["parcels"], "booked",
                                ("cn", "booked", "delivered_on", "status", "last_time", "city", "order",
                                 "product", "weight", "pay_status", "pay_date", "cod", "shipping", "fuel", "stax", "gst_wh", "it_wh",
                                 "net"), esc, 20, eye_key="cn")
    date_rows += (f"<tr class='tot'><td><b>Total</b></td><td><b>{rep['total']}</b></td>"
                  + "".join(f"<td><b>{rep['status_totals'][c]}</b></td>" for c in cols) + "<td></td></tr>")
    status_rows += f"<tr class='tot'><td><b>Total</b></td><td><b>{rep['total']}</b></td><td></td></tr>"
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
        "<div id='modebar'><b>View:</b> <button data-m='date'>By date (1 to N for each day)</button> "
        "<button data-m='all'>All together (1 to N overall)</button></div>"
        "<div class='sec' id='sec-date'><h3>Parcels booked per date</h3><div class='w'><table>"
        f"<tr><th>Booking date</th><th>Total</th>{head}<th>Charges</th></tr>{date_rows}</table></div></div>"
        "<div class='sec' id='sec-status'><h3>Status summary</h3><div class='w'><table>"
        f"<tr><th>Status</th><th>Parcels</th><th>Share</th></tr>{status_rows}</table></div></div>"
        + _paysum_html(rep, esc) + _payments_html(rep, esc) + _returns_html(rep, esc) +
        f"<div class='sec' id='sec-parcels'><h3>All booked parcels ({len(rep['parcels'])})</h3><div class='w'><table id='partbl'>"
        "<tr><th>#</th><th>CN</th><th>Booked</th><th>Delivered on</th><th>Status</th><th>Step time</th><th>City</th>"
        "<th>Order</th><th>Product</th><th>Kg</th><th>Payment</th><th>Paid on</th><th>COD amount</th>"
        "<th>Shipping CHG</th><th>Fuel</th><th>Sales tax</th><th>GST WH 2%</th><th>IT WH 2%</th><th>Net payable</th><th>Track</th></tr>"
        f"{parcel_rows}</table></div></div>"
        f"<div class='sec' id='sec-nonshopify'><h3>Non-Shopify orders ({len(rep.get('non_shopify', []))})</h3><div class='w'><table>"
        "<tr><th>#</th><th>CN</th><th>Booked</th><th>Status</th><th>City</th><th>Reference</th><th>Last event</th><th>Track</th></tr>"
        + _grouped_rows(rep.get("non_shopify", []), "booked",
                        ("cn", "booked", "status", "city", "reference", "last_event"), esc, 8, eye_key="cn")
        + "</table></div></div>"
        f"<div class='sec' id='sec-unbooked'><h3>Not booked at TCS ({len(rep.get('unbooked', []))})</h3><div class='w'><table>"
        "<tr><th>#</th><th>Order</th><th>Placed</th><th>Shopify status</th><th>Product</th><th>Total</th></tr>"
        + _grouped_rows(rep.get("unbooked", []), "created",
                        ("order", "created", "fulfillment", "product", "total"), esc, 6)
        + "</table></div></div>"
        + REPORT_SORT_JS + REPORT_CTL_JS + REPORT_EYE_JS + REPORT_GLASSES_JS + REPORT_LIVE_JS + REPORT_PARCELS_JS + REPORT_PAY_JS + REPORT_SUM_JS
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

    _ensure_live_thread()
    try:
        rep = _build_report(d_from, d_to, request.args.get('cn', ''))
    except Exception as e:
        log.exception("Report failed")
        return f"Could not load the report from TCS: {type(e).__name__}: {str(e)[:200]}", 502
    if request.path.endswith(".json"):
        return jsonify(rep), 200
    return _report_html(rep, request.args.get("key", "")), 200


_track_json_cache = {}


_return_detail_cache = {}


# ------------------------------------------------ live returns (refreshed in the background)
LIVE_REFRESH_SECONDS = int(os.environ.get("LIVE_REFRESH_SECONDS", "20"))
LIVE_DAYS = int(os.environ.get("LIVE_REPORT_DAYS", "60"))
_live = {"rep": None, "at": 0.0, "error": "", "started": False, "last_hit": 0.0}


def _live_loop():
    while True:
        if time.time() - _live["last_hit"] > 120 and _live["rep"] is not None:
            time.sleep(5)  # nobody is looking at the report: do not call TCS
            continue
        try:
            today = datetime.now(timezone(timedelta(hours=DISPLAY_UTC_OFFSET_HOURS))).date()
            _live["rep"] = _build_report(today - timedelta(days=LIVE_DAYS), today)
            _live["at"] = time.time()
            _live["error"] = ""
        except Exception as e:
            log.exception("Live returns refresh failed")
            _live["error"] = f"{type(e).__name__}: {str(e).split('?')[0][:150]}"
        time.sleep(LIVE_REFRESH_SECONDS)


def _ensure_live_thread():
    _live["last_hit"] = time.time()
    with _tcs_lock:
        if _live["started"]:
            return
        _live["started"] = True
    threading.Thread(target=_live_loop, daemon=True, name="live-returns").start()


def _in_range(booked: str, args) -> bool:
    f, t = args.get("from", ""), args.get("to", "")
    return (not f or booked >= f) and (not t or booked <= t)


@app.route("/payment-raw.json", methods=["GET"])
def payment_raw():
    """Exactly what TCS's Payment Detail call returns (all fields), for a date range of up to 31 days."""
    if not ORDERS_PAGE_KEY or not hmac.compare_digest(request.args.get("key", ""), ORDERS_PAGE_KEY):
        abort(401)
    try:
        d_to = date.fromisoformat(request.args.get("to") or datetime.now().date().isoformat())
        d_from = date.fromisoformat(request.args.get("from") or (d_to - timedelta(days=6)).isoformat())
        rows, env = _fetch_parcels_any_env(d_from, d_to)
    except Exception as e:
        return jsonify({"ok": False, "error": f"{type(e).__name__}: {str(e).split('?')[0][:200]}"}), 502
    keys = sorted({k for r in rows for k in r})
    return jsonify({"ok": True, "env": env, "rows": len(rows), "fields": keys, "sample": rows[:int(request.args.get("n", "3"))]}), 200


_EXPLORE = {  # read-only report calls from TCS's own swagger file (https://connect.tcscourier.com/ecom/v1/swagger.json)
    "loadsheetlogs": "/ecom/api/report/loadsheetlogs",
    "shipmentweight": "/ecom/api/report/shipmentweight",
    "cnbulkinquiry": "/ecom/api/inquiry/cnbulkinquiry",
    "paymentinvoice": "/ecom/api/booking/paymentinvoice",
    "paymentstatus": "/ecom/api/Payment/status",
    "pendingtickets": "/ecom/api/shipperadvice/pendingtickets",
    "costcenter": "/ecom/api/inquiry/costcenterinquiry",
}


@app.route("/tcs-explore.json", methods=["GET"])
def tcs_explore():
    """Look at what a TCS report call returns (keys + first rows). Read-only; allow-listed calls only."""
    if not ORDERS_PAGE_KEY or not hmac.compare_digest(request.args.get("key", ""), ORDERS_PAGE_KEY):
        abort(401)
    ep = request.args.get("ep", "")
    if ep not in _EXPLORE:
        return jsonify({"ok": False, "error": "ep must be one of " + ", ".join(_EXPLORE)}), 400
    try:
        bearer, ecom = _tcs_get_tokens("connect")
        params = {"accesstoken": ecom}
        if ep == "paymentinvoice":
            params["invoicenumber"] = request.args.get("invoice", "")
        elif ep == "paymentstatus":
            params.update(customerno=request.args.get("customerno") or TCS_CUSTOMER_NO or TCS_ACCOUNT_NO,
                          cnno=request.args.get("cn", ""))
        elif ep in ("pendingtickets", "costcenter"):
            params["customerno"] = request.args.get("customerno") or TCS_CUSTOMER_NO or TCS_ACCOUNT_NO
        else:
            params.update(customerno=request.args.get("customerno") or TCS_CUSTOMER_NO or TCS_ACCOUNT_NO,
                          fromdate=request.args.get("from", ""), todate=request.args.get("to", ""))
            if ep == "shipmentweight":
                params["page"] = request.args.get("page", "1")
            if request.args.get("costcenter"):
                params["costcenterno"] = request.args["costcenter"]
        r = requests.get(f"{_tcs_base('connect')}{_EXPLORE[ep]}", headers={"Authorization": f"Bearer {bearer}"},
                         params=params, timeout=(10, 60))
        try:
            body = r.json()
        except ValueError:
            body = {"text": r.text[:500]}
    except Exception as e:
        return jsonify({"ok": False, "error": f"{type(e).__name__}: {str(e).split('?')[0][:200]}"}), 502

    def shrink(x, depth=0):
        if isinstance(x, list):
            return {"count": len(x), "first": [shrink(i, depth + 1) for i in x[:int(request.args.get('n', '3'))]]}
        if isinstance(x, dict):
            return {k: shrink(v, depth + 1) for k, v in x.items()}
        return x
    return jsonify({"ok": r.ok, "status": r.status_code, "body": shrink(body)}), 200


@app.route("/parcels.json", methods=["GET"])
def parcels_json():
    if not ORDERS_PAGE_KEY or not hmac.compare_digest(request.args.get("key", ""), ORDERS_PAGE_KEY):
        abort(401)
    _ensure_live_thread()
    rep = _live["rep"]
    if not rep:
        return jsonify({"loading": True, "error": _live["error"]}), 200
    keep = ("cn", "booked", "delivered_on", "status", "state", "last_event", "last_time", "city", "order",
            "product", "weight", "pay_status", "pay_date", "cod", "shipping", "fuel", "stax", "gst_wh", "it_wh", "net")
    return jsonify({"parcels": [{k: p.get(k) for k in keep} for p in rep["parcels"]
                                if _in_range(p["booked"], request.args)],
                    "age": int(time.time() - _live["at"])}), 200


@app.route("/payments.json", methods=["GET"])
def payments_json():
    if not ORDERS_PAGE_KEY or not hmac.compare_digest(request.args.get("key", ""), ORDERS_PAGE_KEY):
        abort(401)
    _ensure_live_thread()
    rep = _live["rep"]
    if not rep:
        return jsonify({"loading": True, "error": _live["error"]}), 200
    pays = [p for p in rep.get("payments", []) if _in_range(p["booked"], request.args)]
    return jsonify({"payments": pays, "totals": _pay_totals(pays),
                    "summary": _pay_summary(pays)}), 200


@app.route("/returns.json", methods=["GET"])
def returns_json():
    if not ORDERS_PAGE_KEY or not hmac.compare_digest(request.args.get("key", ""), ORDERS_PAGE_KEY):
        abort(401)
    _ensure_live_thread()
    rep = _live["rep"]
    if not rep:
        return jsonify({"loading": True, "error": _live["error"]}), 200
    rets = [p for p in rep.get("returns", []) if _in_range(p["booked"], request.args)]
    final = [p for p in rets if p["state"] == "returned"]
    cod = 0.0
    for p in final:
        try:
            cod += float(p.get("cod") or 0)
        except (TypeError, ValueError):
            pass
    keep = ("booked", "order", "cn", "consignee", "city", "reason", "attempts", "first_attempt",
            "last_attempt", "returned_on", "status", "state", "cod", "product", "last_event")
    return jsonify({
        "returns": [{k: p.get(k) for k in keep} for p in rets],
        "final": len(final), "returning": len(rets) - len(final), "cod": cod,
        "reasons": dict(Counter(p["reason"] or "UNKNOWN" for p in final).most_common()),
        "age": int(time.time() - _live["at"]), "error": _live["error"],
    }), 200


@app.route("/return-detail.json", methods=["GET"])
def return_detail():
    """The 21 columns of the portal's Return Shipment Analysis for one returned parcel."""
    if not ORDERS_PAGE_KEY or not hmac.compare_digest(request.args.get("key", ""), ORDERS_PAGE_KEY):
        abort(401)
    cn = re.sub(r"\D", "", request.args.get("cn", ""))[:14]
    sr = request.args.get("sr", "")[:6]
    if not cn:
        return jsonify({"ok": False, "error": "No CN"}), 400
    hit = _return_detail_cache.get(cn)
    if hit and time.time() - hit[0] < 8:
        t, order = hit[1]
    else:
        t = _track_one_uncached(cn)
        order = {}
        if t and str(t.get("order no") or "").startswith("#"):
            try:
                r = requests.get(f"{SHOPIFY_BASE}/orders.json", headers=shopify_headers(),
                                 params={"status": "any", "name": t["order no"],
                                         "fields": "name,total_price,email,phone,shipping_address"},
                                 timeout=(10, 30))
                order = next((o for o in r.json().get("orders", []) if o.get("name") == t["order no"]), {})
            except Exception:
                log.exception("Shopify lookup failed for return detail")
        _return_detail_cache[cn] = (time.time(), (t, order))
    if not t:
        return jsonify({"ok": False, "error": "No tracking data from TCS yet"}), 200
    ship = order.get("shipping_address") or {}
    addr = ", ".join(x for x in (ship.get("address1"), ship.get("address2"), ship.get("city")) if x)
    try:
        cod = f"{int(float(order.get('total_price'))):,}"
    except (TypeError, ValueError):
        cod = ""
    booked = _parse_dmy(t["booking date"])
    final = t.get("state") == "returned"
    fields = [
        ("Sr#", sr), ("Booking Date", booked.strftime("%d-%m-%Y") if booked else ""),
        ("COD Value", cod), ("Cash Received", ""), ("Consignee Address", addr),
        ("Consignee Contact #", order.get("phone") or ship.get("phone") or ""),
        ("Consignee Email", order.get("email") or ""), ("Consignee Name", t.get("consignee") or ""),
        ("Consignment #", t["cn by courier"]), ("Delivery Date", t.get("returned_on") or ""),
        ("Delivery Status", "Returned" if final else "Returning"),
        ("Destination", t.get("city") or ""), ("Failed Delivery Reason", t.get("reason") or ""),
        ("First Attempt Date", t.get("first_attempt") or ""), ("Last Attempt Date", t.get("last_attempt") or ""),
        ("No of Attempts", t.get("attempts") if t.get("attempts") is not None else ""),
        ("Order Reference #", t.get("order no") or ""), ("Origin", t.get("origin") or ""),
        ("Status Code", "RS" if final else "RO"),
        ("Status Description", "RETURN TO SHIPPER" if final else "RETURNING TO ORIGIN"),
        ("Weight", "0.5"),
    ]
    return jsonify({"ok": True, "fields": [[k, str(v)] for k, v in fields]}), 200


@app.route("/track.json", methods=["GET"])
def track_json():
    """Full TCS tracking for one CN or order reference, as JSON (used by the eye button)."""
    if not ORDERS_PAGE_KEY or not hmac.compare_digest(request.args.get("key", ""), ORDERS_PAGE_KEY):
        abort(401)
    q = request.args.get("q", "").strip()[:60]
    if not q:
        return jsonify({"ok": False, "error": "No number given"}), 400
    hit = _track_json_cache.get(q)
    if hit and time.time() - hit[0] < 8:
        return jsonify(hit[1]), 200
    try:
        r = requests.get(f"{_tcs_base('connect')}/tracking/api/Tracking/GetDynamicTrackDetail",
                         headers={"Authorization": f"Bearer {TCS_BEARER_TOKEN}"},
                         params={"consignee": q}, timeout=(10, 30))
        d = r.json() or {}
    except Exception as e:
        return jsonify({"ok": False, "error": f"Could not reach TCS ({type(e).__name__})"}), 502
    info = (d.get("shipmentinfo") or [None])[0]
    if not info:
        out = {"ok": False, "error": "No tracking data from TCS yet"}
    else:
        ref = str(info.get("referenceno") or "")
        summary = str(d.get("shipmentsummary") or "").split("\n")
        out = {
            "ok": True, "cn": info.get("consignmentno"), "reference": ref,
            "origin": info.get("origin"), "destination": info.get("destination"),
            "products": _order_products.get(ref, ""),
            "summary": " ".join(x.strip() for x in summary[:2] if x.strip())[:160],
            "delivery": [{"status": x.get("status"), "station": x.get("station"), "datetime": x.get("datetime"),
                          "received_by": x.get("recievedby") or ""} for x in (d.get("deliveryinfo") or [])],
            "steps": [{"datetime": c.get("datetime"), "by": c.get("recievedby") or "",
                       "status": re.sub(r"\s*\(For Additional.*$", "", str(c.get("status") or ""))}
                      for c in (d.get("checkpoints") or [])],
        }
    _track_json_cache[q] = (time.time(), out)
    return jsonify(out), 200


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
        state = ("returned" if t["state"] == "returning" else t["state"]) if t else "not_booked"
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
  var rows='<table><tr><th>#</th><th>Product</th><th>Units</th><th>Delivered</th><th>In progress</th><th>Returned</th><th>Rejected</th><th>Not at TCS</th><th>Going to</th></tr>';
  last.products.forEach(function(p,i){
    var keys=Object.keys(p.cities);
    var cities=keys.slice(0,6).map(function(c){return esc(c)+' ('+p.cities[c]+')'}).join(', ')+(keys.length>6?' ...':'');
    rows+="<tr><td>"+(i+1)+"</td><td><details data-i='"+i+"'"+(opened[p.product]?' open':'')+"><summary><b>"+esc(p.product)+"</b></summary>";
    rows+="<table><tr><th>#</th><th>Order</th><th>CN</th><th>Qty</th><th>City</th><th>Status</th><th>Last event</th></tr>";
    p.parcels.forEach(function(x,j){rows+="<tr><td>"+(j+1)+"</td><td>"+esc(x.order)+"</td><td>"+esc(x.cn)+"</td><td class='num'>"+x.qty+"</td><td>"+esc(x.city)+"</td><td>"+esc(x.status)+"</td><td>"+esc(x.last_event)+" "+esc(x.last_time)+"</td></tr>"});
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


# ------------------------------------------------ TCS/Envio webhook receiver
_webhook_cns = {}   # CN -> time received (kept in memory; also re-learned from every new event)
_webhook_log = []   # last payloads (phone numbers masked), shown on /webhook-log


def _mask(text: str) -> str:
    return re.sub(r"(?<!\d)(?:\+?92|0)?3\d{9}(?!\d)", "<phone>", text)


@app.route("/webhooks/tcs", methods=["GET", "POST"])
def tcs_webhook():
    """Envio/TCS calls this when a parcel is booked or its status changes.
    URL to give Envio: /webhooks/tcs?key=ORDERS_PAGE_KEY"""
    if not ORDERS_PAGE_KEY or not hmac.compare_digest(request.args.get("key", ""), ORDERS_PAGE_KEY):
        abort(401)
    raw = request.get_data(as_text=True)[:20000]
    found = set(re.findall(r"(?<!\d)(\d{12})(?!\d)", raw))
    for cn in found:
        _webhook_cns.setdefault(cn, _now())
    _webhook_log.append({"at": _now(), "method": request.method, "cns": sorted(found),
                         "content_type": request.content_type, "body": _mask(raw)[:3000]})
    del _webhook_log[:-30]
    return {"ok": True, "cns": len(found)}, 200


@app.route("/webhook-log", methods=["GET"])
def webhook_log_page():
    if not ORDERS_PAGE_KEY or not hmac.compare_digest(request.args.get("key", ""), ORDERS_PAGE_KEY):
        abort(401)
    return jsonify({"known_cns": len(_webhook_cns), "events": _webhook_log[::-1]}), 200


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
