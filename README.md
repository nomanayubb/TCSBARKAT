# Shopify → TCS Auto-Fulfillment

Automatically books a TCS courier shipment for every new Shopify order and
marks it fulfilled with the tracking number. Runs as a tiny always-on web
service (free on Render) so it works even when your own computer is off.

## How it works

- **Webhook** (`/webhooks/orders-create`): Shopify calls this the instant a
  new order is placed. We verify the request is genuinely from Shopify
  (HMAC signature check), then book the TCS shipment and mark the order
  fulfilled.
- **Catch-up loop** (runs every 15 min in the background): independently
  asks Shopify "what's still unfulfilled?" and processes anything the
  webhook might have missed (e.g. a brief outage). This means even if the
  service was down for hours, it self-heals the moment it's back up —
  nothing is silently lost.
- **Dedup file** (`processed_orders.json`): stops the same order from being
  booked with TCS twice if a webhook is delivered more than once (Shopify
  explicitly warns this can happen) or the catch-up loop and a webhook
  overlap.

## What's stubbed and needs filling in

`book_tcs_shipment()` in `app.py` is a stub. Until `TCS_API_URL` is set, it
*simulates* a booking (logs what it would have sent, returns a fake
tracking number) so you can test the entire pipeline — webhook receipt,
catch-up recovery, marking Shopify fulfilled — before TCS API access
exists. Once you have TCS's real API docs, fill in that function's request
shape (field names are guessed/typical placeholders right now) and set the
three `TCS_*` environment variables below.

## Setup

### 1. Shopify custom app
Already covered in chat — you need `SHOPIFY_STORE_DOMAIN` and
`SHOPIFY_ADMIN_API_TOKEN` (the `shpat_...` token) from a custom app with
`read_orders`, `read_fulfillments`, `write_fulfillments` scopes.

### 2. Deploy to Render
1. Push this folder to a GitHub repo.
2. In Render: **New → Web Service**, connect the repo.
3. Runtime: Python 3. Render will auto-detect `Procfile` and
   `requirements.txt`.
4. Add environment variables (Render dashboard → Environment):
   - `SHOPIFY_STORE_DOMAIN`
   - `SHOPIFY_ADMIN_API_TOKEN`
   - `SHOPIFY_WEBHOOK_SECRET` (see step 3 below — you'll get this when
     creating the webhook)
   - `TCS_API_URL`, `TCS_API_USER`, `TCS_API_KEY` (leave blank until you
     have real TCS access — the stub handles that gracefully)
5. Deploy. Render gives you a public URL like
   `https://your-service.onrender.com`.

### 3. Register the Shopify webhook
In Shopify admin → **Settings → Notifications** (or via the Admin API —
ask if you want this automated instead of manual), create a webhook:
- Event: **Order creation**
- Format: JSON
- URL: `https://your-service.onrender.com/webhooks/orders-create`

Shopify shows you a **webhook signing secret** when you create it via the
API (if created through the UI, it uses the app's API secret — ask if you
want this walked through more precisely for your exact Shopify plan).
That value goes into `SHOPIFY_WEBHOOK_SECRET`.

## Known limitation (free Render tier)

Render's free web service has **ephemeral disk** — `processed_orders.json`
resets on every redeploy/restart. This doesn't cause missed orders (the
catch-up loop only asks Shopify for orders still marked *unfulfilled*
there, which is the real source of truth), but in a narrow race window
right around a restart it could theoretically re-attempt a TCS booking for
an order that was being processed at that exact moment. Low-impact, but
worth knowing. If this matters for your volume, Render's paid tier adds a
persistent disk, or we switch the dedup store to a free hosted database
(e.g. a free-tier Postgres) later.

## Local testing

```bash
pip install -r requirements.txt
export SHOPIFY_STORE_DOMAIN=yourstore.myshopify.com
export SHOPIFY_ADMIN_API_TOKEN=shpat_...
export SHOPIFY_WEBHOOK_SECRET=...
python app.py
```

Use `ngrok http 5000` to get a public URL for testing the real Shopify
webhook against your local machine before deploying to Render.
