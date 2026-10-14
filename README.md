# Shopify → TCS Auto-Booking

Catches every new Shopify order, books a TCS shipment through TCS's COD API,
then marks the order fulfilled with the tracking number. Runs on Render's free
tier so it works while your own computer is off.

Live service: https://tcsbarkat.onrender.com — repo: `nomanayubb/TCSBARKAT`.

## How it works

- **Webhook** `/webhooks/orders-create` — Shopify calls it the moment an order
  is placed (HMAC-verified with the app's client secret).
- **Catch-up loop** — every 15 min asks Shopify for unfulfilled orders from the
  last 3 days and processes anything missed. A watchdog restarts the loop if it
  ever stalls. The loop starts on the first request, not at import.
- **TCS booking** (`book_tcs_shipment`) — follows `COD-API-UserManual.pdf`:
  Authorization (bearer) → optional E-COM authentication → `booking/create`.
  Phone numbers are normalised to `03xxxxxxxxx`, COD is 0 for paid orders,
  weight defaults to 0.5 kg minimum.
- **Dry-run safeguard** — simulated or **sandbox** bookings are logged but the
  order is **not** fulfilled in Shopify (customers would be emailed a test
  number). Only a production booking fulfils the order.

## Endpoints

| Path | Purpose |
|---|---|
| `/health` | liveness (used by the keep-alive job) |
| `/status` | counters, last errors, recent orders + outcome (no customer data) |
| `/report?key=ORDERS_PAGE_KEY&from=YYYY-MM-DD&to=YYYY-MM-DD` | parcels booked per date and every TCS status (delivered / returned / in transit...), pulled from TCS's Payment Detail API; add `.json` (`/report.json`) for raw data. Default: last 30 days, max 92 |
| `/orders?key=ORDERS_PAGE_KEY` | readable table of the latest 50 Shopify orders, Pakistan time, refreshes every 30 s |

## Environment variables (Render → Environment)

Shopify: `SHOPIFY_STORE_DOMAIN` (e.g. `60nibp-51.myshopify.com`, no `https://`),
`SHOPIFY_CLIENT_ID`, `SHOPIFY_CLIENT_SECRET`, `APP_BASE_URL`
(`https://tcsbarkat.onrender.com`, enables automatic webhook registration).
Optional: `SHOPIFY_API_VERSION`, `CATCH_UP_INTERVAL_SECONDS`.

Orders page: `ORDERS_PAGE_KEY` (long random password; page is disabled if unset),
`DISPLAY_UTC_OFFSET_HOURS` (default 5), `DISPLAY_TZ_LABEL` (default PKT).

TCS: `TCS_BEARER_TOKEN` (or `TCS_CLIENT_ID` + `TCS_CLIENT_SECRET`),
`TCS_ACCOUNT_NO`, `TCS_COST_CENTER_CODE`, `TCS_SHIPPER_NAME`,
`TCS_SHIPPER_ADDRESS`, `TCS_SHIPPER_CITY`, `TCS_SHIPPER_MOBILE`.
Optional: `TCS_CUSTOMER_NO` (report API customer number; defaults to `TCS_ACCOUNT_NO`), `TCS_USERNAME` + `TCS_PASSWORD` (E-COM token), `TCS_SERVICE_CODE`
(default `O`), `TCS_SHIPPER_CITY_CODE`, `TCS_SHIPPER_ZIP`,
`TCS_CONSIGNEE_MIDDLENAME` (default `N/A`), `TCS_ENV`.

`TCS_REPORT_ENV` (optional, `production` or `sandbox`): the read-only report tries production then sandbox when a bearer token is used and remembers which one TCS accepts; set this to force one.

**`TCS_ENV` defaults to sandbox.** Setting `TCS_ENV=production` makes the
service book *real* shipments and fulfil orders — including every unfulfilled
order from the last 3 days. Cancel test orders in Shopify first.
`ALLOW_SIMULATED_FULFILLMENT=true` overrides the safeguard (testing only).

## Shopify app setup (Dev Dashboard)

Create an app, release a version with scopes `read_orders`, `read_fulfillments`,
`write_fulfillments`, `read_merchant_managed_fulfillment_orders`,
`write_merchant_managed_fulfillment_orders`, install it on the store (same
Shopify organization), and copy Client ID/secret from the app's Settings. The
service exchanges them for a 24 h token automatically.

## Known limits

- Render free tier: ephemeral disk (`processed_orders.json` resets on restart —
  fulfilment status in Shopify is the source of truth), and it sleeps without
  traffic; `.github/workflows/keepalive.yml` pings `/health` every 5 min.
- Tracking URL is left blank (not given in the TCS manual); Shopify still shows
  the CN number.
- Unverified until tested against TCS: using the bearer token as the booking
  body `accesstoken` when no E-COM username/password is set; city names must
  match TCS's city list.
