"""Extra tools for the TCS report, kept apart from app.py so nothing earlier can be disturbed.

Every tool is a small function that reads data the service already holds (live parcels, the Envio portal
statement, webhook events) and returns a table, a text or bars. The /tools page lists them, renders them
generically and refreshes the open one every 15 seconds. All routes need the page key.

Add a tool: write a function with @tool("id", "Group", "Title") that returns T(...), TEXT(...). Nothing else.
"""
import csv
import hmac
import io
import json
import os
import re
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone

from flask import Response, abort, jsonify, request

A = None            # the app module, set by register()
TOOLS = []          # [{"id","group","title","fn","param"}]


def tool(tid, group, title, param=""):
    def deco(fn):
        TOOLS.append({"id": tid, "group": group, "title": title, "fn": fn, "param": param})
        return fn
    return deco


def T(columns, rows, note="", bar=None):
    return {"type": "table", "columns": columns, "rows": rows, "note": note, "bar": bar}


def TEXT(text, note=""):
    return {"type": "text", "text": text, "note": note}


# ------------------------------------------------------------------ data access
def today():
    return datetime.now(timezone(timedelta(hours=A.DISPLAY_UTC_OFFSET_HOURS))).date()


def rep():
    return A._live.get("rep") or {}


def parcels():
    return rep().get("parcels", [])


def lines():
    return A._envio_data.get("lines") or []


def ledger():
    return A._envio_data.get("ledger") or []


def events():
    return list(A._webhook_events)


def num(x):
    return A._num(x)


def pdate(x):
    return A._parse_any_date(x) if x else None


def line_date(x):
    """Booking date of a portal line ('10/5/2026 12:00:00 AM')."""
    try:
        return datetime.strptime(str(x).split(" ")[0], "%m/%d/%Y").date()
    except ValueError:
        return None


def fee_of(l):
    return round(num(l.get("CODAmount")) - num(l.get("GSTWH")) - num(l.get("ITWH")) - num(l.get("BalPayable")), 2) \
        if num(l.get("ShippingChg")) > 0 else 0.0


def weeks_desc():
    return sorted({l["_week"] for l in lines()}, key=A._wk, reverse=True)


def m2(x):
    return f"{x:,.2f}"


def days_since(d):
    return (today() - d).days if d else None


def state_group(p):
    st = p.get("state")
    return "delivered" if st == "delivered" else "returned" if st in ("returned", "returning", "rejected") else "transit"


# ================================================================== PARCELS
@tool("status_mix", "Parcels", "Status mix")
def status_mix():
    c = Counter(p.get("status") or "?" for p in parcels())
    return T(["Status", "Parcels", "Share %"], [[k, n, round(100 * n / max(1, len(parcels())), 1)] for k, n in c.most_common()], bar=1)


@tool("aging", "Parcels", "Aging of parcels not delivered")
def aging():
    buckets = [("0-2 days", 0, 2), ("3-5 days", 3, 5), ("6-10 days", 6, 10), ("11+ days", 11, 10 ** 6)]
    rows = []
    for name, lo, hi in buckets:
        n = sum(1 for p in parcels() if state_group(p) == "transit" and pdate(p.get("booked")) and lo <= days_since(pdate(p.get("booked"))) <= hi)
        rows.append([name, n])
    return T(["Days since booking", "Parcels in transit"], rows, bar=1)


@tool("longest_transit", "Parcels", "Longest in transit (top 20)")
def longest_transit():
    rows = []
    for p in parcels():
        b = pdate(p.get("booked"))
        if state_group(p) == "transit" and b:
            rows.append([days_since(b), p.get("order"), p["cn"], p.get("city"), p.get("last_event")])
    rows.sort(key=lambda r: -r[0])
    return T(["Days", "Order", "CN", "City", "Last step"], rows[:20])


@tool("bookings_day", "Parcels", "Bookings per day")
def bookings_day():
    c = Counter(p.get("booked") for p in parcels())
    return T(["Booking date", "Parcels"], [[k, c[k]] for k in sorted(c, reverse=True)], bar=1)


@tool("deliveries_day", "Parcels", "Deliveries per day")
def deliveries_day():
    c = Counter(str(pdate(p.get("delivered_on"))) for p in parcels() if p.get("state") == "delivered" and pdate(p.get("delivered_on")))
    return T(["Delivery date", "Parcels"], [[k, c[k]] for k in sorted(c, reverse=True)], bar=1)


@tool("delivery_hist", "Parcels", "Delivery time (days from booking)")
def delivery_hist():
    c = Counter()
    for p in parcels():
        b, d = pdate(p.get("booked")), pdate(p.get("delivered_on"))
        if p.get("state") == "delivered" and b and d and d >= b:
            c[(d - b).days] += 1
    return T(["Days", "Parcels"], [[k, c[k]] for k in sorted(c)], bar=1)


def _city_days():
    per = defaultdict(list)
    for p in parcels():
        b, d = pdate(p.get("booked")), pdate(p.get("delivered_on"))
        if p.get("state") == "delivered" and b and d and d >= b:
            per[p.get("city") or "?"].append((d - b).days)
    return {c: (round(sum(v) / len(v), 1), len(v)) for c, v in per.items() if len(v) >= 2}


@tool("slow_cities", "Parcels", "Slowest cities (average days)")
def slow_cities():
    return T(["City", "Average days", "Delivered parcels"], [[c, a, n] for c, (a, n) in sorted(_city_days().items(), key=lambda kv: -kv[1][0])],
             note="Cities with at least 2 delivered parcels.", bar=1)


@tool("fast_cities", "Parcels", "Fastest cities (average days)")
def fast_cities():
    return T(["City", "Average days", "Delivered parcels"], [[c, a, n] for c, (a, n) in sorted(_city_days().items(), key=lambda kv: kv[1][0])],
             note="Cities with at least 2 delivered parcels.", bar=1)


@tool("city_returns", "Parcels", "Return rate by city")
def city_returns():
    g = defaultdict(lambda: [0, 0])
    for p in parcels():
        s = state_group(p)
        if s != "transit":
            g[p.get("city") or "?"][0 if s == "delivered" else 1] += 1
    rows = [[c, d + r, r, round(100 * r / (d + r), 1)] for c, (d, r) in g.items() if d + r >= 3]
    return T(["City", "Finished parcels", "Returned / refused", "Return rate %"], sorted(rows, key=lambda r: -r[3]),
             note="Cities with at least 3 finished parcels.", bar=3)


@tool("weekday", "Parcels", "Bookings by weekday")
def weekday():
    names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    c = Counter(pdate(p.get("booked")).weekday() for p in parcels() if pdate(p.get("booked")))
    return T(["Weekday", "Parcels"], [[names[i], c[i]] for i in range(7)], bar=1)


@tool("unbooked_age", "Parcels", "Shopify orders waiting to be booked")
def unbooked_age():
    rows = []
    for u in rep().get("unbooked", []):
        d = pdate(u.get("created"))
        rows.append([days_since(d) if d else "", u.get("order"), u.get("created"), u.get("fulfillment"), u.get("product"), u.get("total")])
    rows.sort(key=lambda r: -(r[0] if r[0] != "" else -1))
    return T(["Days waiting", "Order", "Placed", "Shopify status", "Product", "Total"], rows)


@tool("dup_refs", "Parcels", "Orders with more than one CN")
def dup_refs():
    g = defaultdict(set)
    for l in lines():
        if l.get("CustomerReference"):
            g[l["CustomerReference"]].add(str(l.get("ConsignmentNumber")))
    rows = [[k, len(v), ", ".join(sorted(v))] for k, v in g.items() if len(v) > 1]
    return T(["Order", "CNs", "Consignment numbers"], rows, note="Same order reference booked twice: check for a duplicate parcel.")


@tool("product_matrix", "Parcels", "Products by status")
def product_matrix():
    g = defaultdict(Counter)
    for p in parcels():
        for title, _q in A._order_items.get(p.get("order") or "", []):
            g[title[:48]][state_group(p)] += 1
    rows = [[k, v["delivered"], v["transit"], v["returned"], sum(v.values())] for k, v in g.items()]
    return T(["Product", "Delivered", "In transit", "Returned / refused", "Orders"], sorted(rows, key=lambda r: -r[4]), bar=4)


@tool("lookup", "Parcels", "Look up one parcel (CN or order)", param="CN or order, e.g. #00011051")
def lookup():
    q = (request.args.get("q") or "").strip()
    if not q:
        return TEXT("Type a CN or an order number above and press Enter.")
    ps = [p for p in parcels() if q.lower() in (str(p["cn"]) + " " + str(p.get("order") or "")).lower()][:5]
    if not ps:
        return TEXT("No parcel found for that number.")
    rows = []
    for p in ps:
        ls = [l for l in lines() if str(l.get("ConsignmentNumber")) == str(p["cn"])]
        evs = [e for e in events() if e["cn"] == str(p["cn"])]
        rows.append([p["cn"], p.get("order"), p.get("status"), p.get("last_event"), p.get("last_time"), p.get("city"),
                     ", ".join(f"{l['_week'][:6]} {l.get('DeliveryStatus')} {l.get('BalPayable')}" for l in ls) or "not in a weekly report yet",
                     len(evs)])
    return T(["CN", "Order", "Status", "Last step", "When", "City", "Statement lines", "Webhook events"], rows)


# ================================================================== MONEY (portal)
@tool("week_money", "Money", "Weekly money summary")
def week_money():
    inv = {v["BILLING_WEEK"]: v for v in ledger()}
    rows = []
    for w in weeks_desc():
        ls = [l for l in lines() if l["_week"] == w]
        cod = sum(num(l.get("CODAmount")) for l in ls)
        fee = sum(fee_of(l) for l in ls)
        wht = sum(num(l.get("GSTWH")) + num(l.get("ITWH")) for l in ls)
        net = sum(num(l.get("BalPayable")) for l in ls)
        v = inv.get(w, {})
        rows.append([w, len(ls), m2(cod), m2(fee), m2(wht), m2(net), v.get("AMOUNTPAYABLE", "no invoice"), v.get("PAYMENTDATE", "")])
    return T(["Billing week", "Lines", "COD", "Fees cut", "Withholding", "Sum of line nets", "Invoice payable", "Paid on"], rows)


@tool("gap_tracker", "Money", "Invoice vs statement lines (gap)")
def gap_tracker():
    inv = {v["BILLING_WEEK"]: v for v in ledger()}
    rows = []
    for w in weeks_desc():
        if w in inv:
            net = round(sum(num(l.get("BalPayable")) for l in lines() if l["_week"] == w), 2)
            pay = num(inv[w].get("AMOUNTPAYABLE"))
            rows.append([w, m2(net), m2(pay), m2(net - pay)])
    return T(["Billing week", "Sum of statement lines", "TCS invoice payable", "Difference"], rows,
             note="TCS's own two reports disagree by this much. Ask TCS what the difference is.")


@tool("pay_delay", "Money", "Invoice to payment delay")
def pay_delay():
    rows = []
    for v in sorted(ledger(), key=lambda v: A._wk(v["BILLING_WEEK"]), reverse=True):
        try:
            a = datetime.strptime(v["INVOICEDATE"], "%d-%b-%Y").date()
            b = datetime.strptime(v["PAYMENTDATE"], "%d-%b-%Y").date()
            rows.append([v["BILLING_WEEK"], v["INVOICEDATE"], v["PAYMENTDATE"], (b - a).days])
        except (ValueError, KeyError, TypeError):
            rows.append([v.get("BILLING_WEEK"), v.get("INVOICEDATE"), v.get("PAYMENTDATE") or "not paid yet", ""])
    return T(["Billing week", "Invoice date", "Paid on", "Days to pay"], rows)


@tool("tax_summary", "Money", "Withholding tax summary")
def tax_summary():
    rows, ti, ts = [], 0.0, 0.0
    for w in weeks_desc():
        i = sum(num(l.get("ITWH")) for l in lines() if l["_week"] == w)
        s = sum(num(l.get("GSTWH")) for l in lines() if l["_week"] == w)
        ti, ts = ti + i, ts + s
        rows.append([w, m2(i), m2(s), m2(i + s)])
    rows.append(["TOTAL", m2(ti), m2(ts), m2(ti + ts)])
    return T(["Billing week", "IT WH 2%", "ST/GST WH 2%", "Total"], rows, note="From the line-by-line statement (TCS rounds on the invoice).")


@tool("return_loss", "Money", "Fees paid on returned or refused parcels")
def return_loss():
    back = {str(p["cn"]) for p in parcels() if state_group(p) == "returned"}
    rows, tot = [], 0.0
    for w in weeks_desc():
        ls = [l for l in lines() if l["_week"] == w and str(l.get("ConsignmentNumber")) in back and num(l.get("ShippingChg")) > 0]
        f = sum(fee_of(l) for l in ls)
        tot += f
        rows.append([w, len(ls), m2(f)])
    rows.append(["TOTAL", sum(r[1] for r in rows), m2(tot)])
    return T(["Billing week", "Returned parcels charged", "Fees lost"], rows,
             note="Fee TCS cut for parcels that came back: you don't get it refunded.")


@tool("avg_fee", "Money", "Average fee per parcel (rate check)")
def avg_fee():
    rows = []
    for w in weeks_desc():
        fs = [fee_of(l) for l in lines() if l["_week"] == w and num(l.get("ShippingChg")) > 0]
        if fs:
            rows.append([w, len(fs), m2(sum(fs) / len(fs)), m2(min(fs)), m2(max(fs))])
    return T(["Billing week", "Fee lines", "Average fee", "Lowest", "Highest"], rows,
             note="If TCS changes a rate, the average moves here first.")


@tool("cod_waiting", "Money", "COD waiting for TCS to pay")
def cod_waiting():
    paid = {str(l.get("ConsignmentNumber")) for l in lines() if num(l.get("CODAmount")) > 0}
    rows, tot = [], 0
    for p in parcels():
        if p.get("state") == "delivered" and str(p["cn"]) not in paid:
            try:
                cod = int(float(A._order_totals.get(p.get("order"), 0)))
            except (TypeError, ValueError):
                cod = 0
            tot += cod
            d = pdate(p.get("delivered_on"))
            rows.append([p.get("order"), p["cn"], p.get("delivered_on"), days_since(d) if d else "", cod])
    rows.sort(key=lambda r: -(r[3] if r[3] != "" else -1))
    rows.append(["TOTAL", "", "", "", tot])
    return T(["Order", "CN", "Delivered on", "Days waiting", "COD (Shopify total)"], rows,
             note="Delivered parcels with no COD line in any weekly report yet.")


@tool("running_net", "Money", "Running total of line nets")
def running_net():
    rows, run = [], 0.0
    for w in sorted(weeks_desc(), key=A._wk):
        net = sum(num(l.get("BalPayable")) for l in lines() if l["_week"] == w)
        run += net
        rows.append([w, m2(net), m2(run)])
    return T(["Billing week", "Week", "Running total"], rows)


@tool("week_compare", "Money", "Compare the last two weeks")
def week_compare():
    ws = weeks_desc()[:2]
    if len(ws) < 2:
        return TEXT("Needs two weekly reports.")
    def stats(w):
        ls = [l for l in lines() if l["_week"] == w]
        return [len(ls), m2(sum(num(l.get("CODAmount")) for l in ls)), m2(sum(fee_of(l) for l in ls)),
                m2(sum(num(l.get("BalPayable")) for l in ls))]
    a, b = stats(ws[0]), stats(ws[1])
    return T(["", ws[0], ws[1]], [["Lines", a[0], b[0]], ["COD", a[1], b[1]], ["Fees cut", a[2], b[2]], ["Sum of line nets", a[3], b[3]]])


@tool("lines_search", "Money", "Search statement lines", param="CN, order or name")
def lines_search():
    q = (request.args.get("q") or "").strip().lower()
    rows = []
    for l in lines():
        txt = " ".join(str(l.get(k, "")) for k in ("ConsignmentNumber", "CustomerReference", "Consignee", "Destination")).lower()
        if not q or q in txt:
            rows.append([l["_week"][:12], l.get("ConsignmentNumber"), l.get("CustomerReference"), l.get("Consignee"),
                         l.get("DeliveryStatus"), l.get("CODAmount"), l.get("ShippingChg"), l.get("BalPayable")])
    return T(["Week", "CN", "Order", "Consignee", "Status", "COD", "Shipping", "Net payable"], rows[:300])


# ================================================================== WEBHOOK
@tool("ev_status", "Webhook", "Events by status")
def ev_status():
    c = Counter(e["status"] for e in events())
    return T(["Status", "Events"], [[k, n] for k, n in c.most_common()], bar=1)


@tool("ev_hours", "Webhook", "Events per hour (last 24 h)")
def ev_hours():
    now = datetime.utcnow()
    c = Counter()
    for e in events():
        try:
            t = datetime.strptime(e["at"], "%Y-%m-%d %H:%M:%S UTC")
        except ValueError:
            continue
        if (now - t).total_seconds() < 86400:
            c[t.strftime("%Y-%m-%d %H:00")] += 1
    return T(["Hour (UTC)", "Events"], [[k, c[k]] for k in sorted(c, reverse=True)], bar=1)


@tool("ev_parcels", "Webhook", "Parcels with most events")
def ev_parcels():
    c = Counter(e["cn"] for e in events())
    by = {str(p["cn"]): p.get("order") for p in parcels()}
    return T(["CN", "Order", "Events"], [[k, by.get(k, ""), n] for k, n in c.most_common(20)], bar=2)


@tool("ev_for_cn", "Webhook", "Events of one parcel", param="CN")
def ev_for_cn():
    q = (request.args.get("q") or "").strip()
    rows = [[e["at"], e["status"], e["when"], e["city"], e["by"]] for e in events() if q and e["cn"] == q]
    return T(["Received", "Status", "Event time", "City", "Received by"], rows[::-1]) if q else TEXT("Type a CN above.")


@tool("ev_latency", "Webhook", "How late do events arrive?")
def ev_latency():
    rows = []
    for e in events()[-30:]:
        try:
            a = datetime.strptime(e["at"], "%Y-%m-%d %H:%M:%S UTC")
            b = datetime.strptime(e["when"][:19], "%Y-%m-%d %H:%M:%S")
            rows.append([e["cn"], e["status"], e["when"], e["at"], round((a - b).total_seconds() / 60)])
        except ValueError:
            continue
    return T(["CN", "Status", "Event time", "Received", "Minutes between"], rows[::-1],
             note="Event time is TCS's clock (local), received is UTC: a constant offset of hours is normal.")


@tool("ev_silent", "Webhook", "In-transit parcels with no event for 3+ days")
def ev_silent():
    last = {}
    for e in events():
        last[e["cn"]] = e["at"]
    rows = []
    for p in parcels():
        if state_group(p) == "transit":
            d = pdate(p.get("last_time"))
            if d and days_since(d) >= 3:
                rows.append([p.get("order"), p["cn"], p.get("city"), p.get("last_time"), last.get(str(p["cn"]), "never")])
    return T(["Order", "CN", "City", "Last tracking step", "Last webhook event"], rows)


@tool("ev_transitions", "Webhook", "Status changes seen")
def ev_transitions():
    seq = defaultdict(list)
    for e in events():
        seq[e["cn"]].append(e["status"])
    c = Counter()
    for sts in seq.values():
        for a, b in zip(sts, sts[1:]):
            c[f"{a}  ->  {b}"] += 1
    return T(["Change", "Times"], [[k, n] for k, n in c.most_common()], bar=1)


# ================================================================== RETURNS / CUSTOMERS
@tool("ret_list", "Returns", "Returned parcels with consignee and reason")
def ret_list():
    rows = [[r.get("order"), r.get("cn"), r.get("consignee"), r.get("city"), r.get("reason"), r.get("attempts"), r.get("returned_on")]
            for r in rep().get("returns", [])]
    return T(["Order", "CN", "Consignee", "City", "Reason", "Attempts", "Back on"], rows)


@tool("ret_repeat", "Returns", "Consignees with 2+ returns")
def ret_repeat():
    c = Counter((r.get("consignee") or "").strip().lower() for r in rep().get("returns", []) if r.get("consignee"))
    rows = [[k.title(), n] for k, n in c.most_common() if n >= 2]
    return T(["Consignee", "Returns"], rows, note="Check before you ship them another order.")


@tool("ret_reasons", "Returns", "Return reasons overall")
def ret_reasons():
    c = Counter(r.get("reason") or "UNKNOWN" for r in rep().get("returns", []))
    return T(["Reason", "Parcels"], [[k, n] for k, n in c.most_common()], bar=1)


@tool("ret_city_refusals", "Returns", "Refusals by city")
def ret_city_refusals():
    c = Counter(r.get("city") or "?" for r in rep().get("returns", []) if "REFUS" in str(r.get("reason") or "").upper())
    return T(["City", "Refused deliveries"], [[k, n] for k, n in c.most_common()], bar=1)


@tool("ret_product_reason", "Returns", "Return reason by product")
def ret_product_reason():
    g = defaultdict(Counter)
    for r in rep().get("returns", []):
        for title, _q in A._order_items.get(r.get("order") or "", []):
            g[title[:48]][r.get("reason") or "UNKNOWN"] += 1
    rows = [[k, sum(v.values()), ", ".join(f"{a}: {b}" for a, b in v.most_common())] for k, v in g.items()]
    return T(["Product", "Returns", "Reasons"], sorted(rows, key=lambda r: -r[1]))


@tool("ret_months", "Returns", "Returns per booking month")
def ret_months():
    c = Counter(str(r.get("booked"))[:7] for r in rep().get("returns", []))
    return T(["Month", "Returns"], [[k, c[k]] for k in sorted(c, reverse=True)], bar=1)


# ================================================================== TOOLS
@tool("digest", "Tools", "Daily digest (copy and send)")
def digest():
    ps = parcels()
    g = Counter(state_group(p) for p in ps)
    act = [p for p in ps if any(w in str(p.get("last_event") or "").lower() for w in A._ACTION_WORDS) and state_group(p) == "transit"]
    paid_cn = {str(l.get("ConsignmentNumber")) for l in lines() if num(l.get("CODAmount")) > 0}
    waiting = sum(1 for p in ps if p.get("state") == "delivered" and str(p["cn"]) not in paid_cn)
    txt = (f"TCS report {today().isoformat()}\nParcels: {len(ps)}\nDelivered: {g['delivered']}\nIn transit: {g['transit']}\n"
           f"Returned / refused: {g['returned']}\nDelivered, not paid yet: {waiting}\nNeeds action: {len(act)}")
    if act:
        txt += "\n" + "\n".join(f"- {p.get('order')} {p.get('city')}: {p.get('last_event')}" for p in act[:10])
    return TEXT(txt)


@tool("wa_messages", "Tools", "Message text for parcels that need action")
def wa_messages():
    out = []
    for p in parcels():
        if state_group(p) == "transit" and any(w in str(p.get("last_event") or "").lower() for w in A._ACTION_WORDS):
            out.append(f"Order {p.get('order')} ({p.get('product') or 'your order'}): our courier could not deliver ({p.get('last_event')}). "
                       "Please reply with your complete address and a phone number so we can deliver it again.")
    return TEXT("\n\n".join(out) or "No parcel needs a message right now.", note="Text only: copy it and send it yourself.")


@tool("endpoints", "Tools", "Page and data addresses")
def endpoints():
    rows = sorted([[str(r.rule), ",".join(sorted(r.methods - {"HEAD", "OPTIONS"}))] for r in A.app.url_map.iter_rules() if r.endpoint != "static"])
    return T(["Address", "Methods"], rows, note="All pages and data addresses need ?key=YOURKEY except /health.")


@tool("settings", "Tools", "Settings present (names only)")
def settings():
    names = ["SHOPIFY_STORE_DOMAIN", "SHOPIFY_CLIENT_ID", "SHOPIFY_CLIENT_SECRET", "APP_BASE_URL", "ORDERS_PAGE_KEY",
             "TCS_BEARER_TOKEN", "TCS_USERNAME", "TCS_PASSWORD", "TCS_ACCOUNT_NO", "TCS_CUSTOMER_NO", "TCS_COST_CENTER_CODE",
             "ENVIO_USERNAME", "ENVIO_PASSWORD", "STUCK_DAYS", "LIVE_REFRESH_SECONDS"]
    return T(["Setting", "Status"], [[n, "set" if os.environ.get(n) else "not set"] for n in names], note="Values are never shown.")


@tool("freshness", "Tools", "How fresh is each source?")
def freshness():
    now = __import__("time").time()
    def age(t):
        return f"{int(now - t)}s ago" if t else "never"
    return T(["Source", "Last read", "Error"], [
        ["TCS tracking and report", age(A._live.get("at")), A._live.get("error") or ""],
        ["Envio portal ledger", age(A._envio_data.get("at")), A._envio_data.get("error") or ""],
        ["Webhook", events()[-1]["at"] if events() else "no event yet", ""]])


@tool("week_print", "Tools", "Printable weekly statement")
def week_print():
    ws = weeks_desc()
    return T(["Billing week", "Open"], [[w, f"/tools/print?key=KEY&week={w}"] for w in ws],
             note="Open the address (put your key where it says KEY) and press Ctrl+P.")


# ================================================================== MORE PARCEL / MONEY ANALYSIS
@tool("cod_by_city", "Parcels", "COD value by city (Shopify totals)")
def cod_by_city():
    g = defaultdict(lambda: [0, 0, 0])
    for p in parcels():
        try:
            cod = int(float(A._order_totals.get(p.get("order"), 0)))
        except (TypeError, ValueError):
            cod = 0
        i = {"delivered": 0, "transit": 1, "returned": 2}[state_group(p)]
        g[p.get("city") or "?"][i] += cod
    rows = [[c, f"{v[0]:,}", f"{v[1]:,}", f"{v[2]:,}", f"{sum(v):,}"] for c, v in g.items()]
    return T(["City", "COD delivered", "COD in transit", "COD returned / refused", "Total"], sorted(rows, key=lambda r: -int(r[4].replace(",", ""))),
             note="COD amounts are your Shopify order totals in whole rupees.")


@tool("product_value", "Parcels", "COD value by product (Shopify totals)")
def product_value():
    g = defaultdict(lambda: [0, 0, 0])
    for p in parcels():
        try:
            cod = int(float(A._order_totals.get(p.get("order"), 0)))
        except (TypeError, ValueError):
            cod = 0
        items = A._order_items.get(p.get("order") or "", [])
        i = {"delivered": 0, "transit": 1, "returned": 2}[state_group(p)]
        for title, _q in items[:1]:
            g[title[:48]][i] += cod
    rows = [[k, f"{v[0]:,}", f"{v[1]:,}", f"{v[2]:,}"] for k, v in g.items()]
    return T(["Product", "Delivered", "In transit", "Returned / refused"], sorted(rows, key=lambda r: -int(r[1].replace(",", ""))))


@tool("weekday_delivery", "Parcels", "Deliveries by weekday")
def weekday_delivery():
    names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    c = Counter(pdate(p.get("delivered_on")).weekday() for p in parcels() if p.get("state") == "delivered" and pdate(p.get("delivered_on")))
    return T(["Weekday", "Delivered"], [[names[i], c[i]] for i in range(7)], bar=1)


@tool("stuck_by_city", "Parcels", "Parcels in transit by city")
def stuck_by_city():
    c = Counter(p.get("city") or "?" for p in parcels() if state_group(p) == "transit")
    return T(["City", "In transit"], [[k, n] for k, n in c.most_common()], bar=1)


@tool("action_steps", "Parcels", "Needs-action parcels by step")
def action_steps():
    c = Counter(p.get("last_event") for p in parcels() if state_group(p) == "transit"
                and any(w in str(p.get("last_event") or "").lower() for w in A._ACTION_WORDS))
    return T(["Step", "Parcels"], [[k, n] for k, n in c.most_common()], bar=1)


@tool("week_status", "Parcels", "Each booking week: how parcels ended up")
def week_status():
    g = defaultdict(Counter)
    for p in parcels():
        b = pdate(p.get("booked"))
        if b:
            g[A._week_of(b)[1]][state_group(p)] += 1
    rows = [[w, sum(v.values()), v["delivered"], v["transit"], v["returned"]] for w, v in g.items()]
    return T(["Booking week", "Parcels", "Delivered", "In transit", "Returned / refused"], sorted(rows, key=lambda r: A._wk(r[0]), reverse=True))


@tool("step_freq", "Parcels", "Most common current steps")
def step_freq():
    c = Counter(p.get("last_event") for p in parcels() if state_group(p) == "transit")
    return T(["Current step", "Parcels"], [[k, n] for k, n in c.most_common(15)], bar=1)


@tool("step_age", "Parcels", "Days since the last step (in transit)")
def step_age():
    c = Counter()
    for p in parcels():
        d = pdate(p.get("last_time"))
        if state_group(p) == "transit" and d:
            c[min(days_since(d), 10)] += 1
    return T(["Days (10 = 10 or more)", "Parcels"], [[k, c[k]] for k in sorted(c)], bar=1)


@tool("city_share", "Parcels", "Share of parcels by destination")
def city_share():
    c = Counter(p.get("city") or "?" for p in parcels())
    t = max(1, sum(c.values()))
    return T(["City", "Parcels", "Share %"], [[k, n, round(100 * n / t, 1)] for k, n in c.most_common()], bar=2)


@tool("ev_hours_local", "Webhook", "Events by hour of day")
def ev_hours_local():
    c = Counter()
    for e in events():
        try:
            c[(datetime.strptime(e["at"], "%Y-%m-%d %H:%M:%S UTC") + timedelta(hours=A.DISPLAY_UTC_OFFSET_HOURS)).hour] += 1
        except ValueError:
            continue
    return T(["Hour (Pakistan time)", "Events"], [[f"{h:02d}:00", c[h]] for h in sorted(c)], bar=1)


@tool("ev_cities", "Webhook", "Events by city")
def ev_cities():
    c = Counter(e["city"] or "?" for e in events())
    return T(["City", "Events"], [[k, n] for k, n in c.most_common()], bar=1)


@tool("fee_by_week_parcel", "Money", "Fee cut per parcel by week")
def fee_by_week_parcel():
    rows = []
    for w in weeks_desc():
        for l in lines():
            if l["_week"] == w and num(l.get("ShippingChg")) > 0:
                rows.append([w[:12], l.get("CustomerReference"), l.get("DeliveryStatus"), m2(fee_of(l))])
    return T(["Week", "Order", "Status", "Fee cut"], rows[:400])


@tool("cod_by_week", "Money", "COD paid out per week")
def cod_by_week():
    rows = [[w, m2(sum(num(l.get("CODAmount")) for l in lines() if l["_week"] == w))] for w in weeks_desc()]
    return T(["Billing week", "COD in statement"], rows, bar=1)


# ================================================================== CHANGE LOG + NOTIFICATION BAR
import threading
import time as _time

# ids start from the clock (milliseconds) so they keep growing across restarts: a browser that remembers "seen up to id N"
# from before a restart must not hide the new changes
CH = {"log": [], "seq": int(_time.time() * 1000), "snap": None, "seen": set(), "marks": (None, None)}
_ch_lock = threading.Lock()


def _snapshot():
    r = rep()
    return {
        "parcels": {str(p["cn"]): {k: p.get(k) for k in ("order", "status", "last_event", "last_time", "city", "delivered_on",
                                                          "pay_status", "pay_date", "cod", "product")} for p in r.get("parcels", [])},
        "lines": {(l["_week"], str(l.get("ConsignmentNumber"))): {k: l.get(k) for k in (
            "CustomerReference", "DeliveryStatus", "CODAmount", "ShippingChg", "GSTWH", "ITWH", "BalPayable")} for l in lines()},
        "ledger": {v["BILLING_WEEK"]: {k: v.get(k) for k in ("AMOUNTPAYABLE", "AMOUNTPAID", "INVOICEDATE", "PAYMENTDATE")} for v in ledger()},
        "unbooked": {u["order"]: u for u in r.get("unbooked", [])},
    }


def _add(kind, text, t=None):
    CH["seq"] += 1
    CH["log"].append({"id": CH["seq"], "t": t or _time.time(), "kind": kind, "text": text})
    del CH["log"][:-500]


BACKFILL_HOURS = 72


def _tcs_epoch(text):
    """'Tuesday Oct 13, 2026 16:22' (Pakistan time) -> seconds since 1970, or None."""
    m = re.search(r"([A-Za-z]{3}) (\d{1,2}), (\d{4})\s+(\d{1,2}):(\d{2})", str(text or ""))
    if not m:
        return None
    try:
        d = datetime.strptime(" ".join(m.groups()), "%b %d %Y %H %M")
        return d.replace(tzinfo=timezone(timedelta(hours=5))).timestamp()
    except ValueError:
        return None


def _backfill(snap):
    """After a restart the log is empty and the first snapshot has nothing to compare with. So the bell would stay silent
    about what happened while the service was down. TCS keeps the time of every parcel's last step: list the steps of the
    last 72 hours with their real TCS time (oldest first, so the newest ends on top)."""
    now = _time.time()
    found = []
    for cn, p in snap["parcels"].items():
        t = _tcs_epoch(p.get("last_time"))
        if t and now - BACKFILL_HOURS * 3600 <= t <= now + 3600:
            found.append((t, p))
    for t, p in sorted(found, key=lambda x: x[0]):
        name = p.get("order") or "parcel"
        _add("status", f"{name}: {_fmt(p.get('status'))} - {_fmt(p.get('last_event'))} (TCS time {_fmt(p.get('last_time'))})", t)


def _fmt(v):
    return "none" if v in (None, "") else str(v)


def _diff(old, new):
    """Every difference between two snapshots becomes a plain sentence. Nothing is skipped."""
    for cn, n in new["parcels"].items():
        o = old["parcels"].get(cn)
        name = n.get("order") or cn
        if o is None:
            if cn not in CH["seen"] and old["parcels"]:
                _add("new", f"{name}: new parcel booked ({_fmt(n.get('status'))}, {_fmt(n.get('city'))})")
            continue
        if o.get("status") != n.get("status"):
            _add("status", f"{name}: {_fmt(o.get('status'))}  ->  {_fmt(n.get('status'))}")
        elif o.get("last_event") != n.get("last_event"):
            _add("step", f"{name}: new step '{_fmt(n.get('last_event'))}' ({_fmt(n.get('last_time'))})")
        if o.get("cod") != n.get("cod"):
            _add("money", f"{name}: COD {_fmt(o.get('cod'))}  ->  {_fmt(n.get('cod'))}")
        if o.get("pay_status") != n.get("pay_status") or o.get("pay_date") != n.get("pay_date"):
            _add("money", f"{name}: payment {_fmt(o.get('pay_status'))}  ->  {_fmt(n.get('pay_status'))} {_fmt(n.get('pay_date')) if n.get('pay_date') else ''}".rstrip())
        if o.get("delivered_on") != n.get("delivered_on") and n.get("delivered_on"):
            _add("status", f"{name}: delivery date {_fmt(n.get('delivered_on'))}")
        if o.get("city") != n.get("city"):
            _add("step", f"{name}: destination {_fmt(o.get('city'))}  ->  {_fmt(n.get('city'))}")
    CH["seen"].update(new["parcels"])
    for key, n in new["lines"].items():
        o = old["lines"].get(key)
        wk, cn = key
        ref = n.get("CustomerReference") or cn
        if o is None:
            if old["lines"]:
                _add("list", f"{ref}: new line in weekly report {wk[:12]}: {_fmt(n.get('DeliveryStatus'))}, net {_fmt(n.get('BalPayable'))}")
            continue
        for f, label in (("DeliveryStatus", "status"), ("CODAmount", "COD"), ("ShippingChg", "shipping"), ("GSTWH", "GST WH"),
                         ("ITWH", "IT WH"), ("BalPayable", "net payable")):
            if o.get(f) != n.get(f):
                _add("list", f"{ref}: weekly report {wk[:12]} {label} {_fmt(o.get(f))}  ->  {_fmt(n.get(f))}")
    for key, o in old["lines"].items():
        if key not in new["lines"] and new["lines"]:
            _add("list", f"{o.get('CustomerReference') or key[1]}: line removed from weekly report {key[0][:12]}")
    for wk, n in new["ledger"].items():
        o = old["ledger"].get(wk)
        if o is None:
            if old["ledger"]:
                _add("money", f"New weekly report {wk}: payable {_fmt(n.get('AMOUNTPAYABLE'))}")
            continue
        for f, label in (("AMOUNTPAYABLE", "payable"), ("AMOUNTPAID", "paid amount"), ("INVOICEDATE", "invoice date"),
                         ("PAYMENTDATE", "payment date")):
            if o.get(f) != n.get(f):
                _add("money", f"Weekly report {wk}: {label} {_fmt(o.get(f))}  ->  {_fmt(n.get(f))}")
    for order in new["unbooked"]:
        if order not in old["unbooked"] and old["unbooked"] is not None and (old["parcels"] or old["unbooked"]):
            _add("new", f"{order}: new Shopify order waiting to be booked")
    for order in old["unbooked"]:
        if order not in new["unbooked"] and new["unbooked"] is not None and (new["parcels"]):
            _add("status", f"{order}: no longer waiting (booked or cancelled)")


def _watch():
    while True:
        try:
            marks = (A._live.get("at"), A._envio_data.get("at"))
            if marks != CH["marks"] and rep():
                snap = _snapshot()
                with _ch_lock:
                    if CH["snap"] is not None:
                        _diff(CH["snap"], snap)
                    else:
                        CH["seen"].update(snap["parcels"])
                    if snap["parcels"] and not CH.get("filled"):  # the first snapshot that really holds parcels
                        CH["filled"] = True
                        _backfill(snap)
                    CH["snap"], CH["marks"] = snap, marks
        except Exception:
            A.log.exception("Change watcher failed")
        _time.sleep(3)


@tool("changes", "Tools", "Change log (everything that changed)")
def changes():
    rows = [[_time.strftime("%Y-%m-%d %H:%M:%S", _time.gmtime(c["t"])) + " UTC", c["kind"], c["text"]] for c in reversed(CH["log"])]
    return T(["When", "Kind", "Change"], rows, note="Every difference the report has seen since the service started.")


NOTIFY_JS = '<script>\n(function(){\nvar KEY=new URLSearchParams(location.search).get(\'key\')||\'\';\nif(document.getElementById(\'chbell\')) return;\nvar COL={status:\'#e63946\',step:\'#f4a261\',money:\'#2a9d8f\',list:\'#457b9d\',new:\'#8ecae6\'};\nvar LBL={status:\'Status\',step:\'Step\',money:\'Money\',list:\'Weekly report\',new:\'New\'};\nfunction ls(k,v){try{if(v===undefined)return localStorage.getItem(k);localStorage.setItem(k,v)}catch(e){return null}}\nvar st=document.createElement(\'style\');\nst.textContent=\'#chbell{position:fixed;top:10px;right:14px;z-index:80;width:42px;height:42px;border-radius:50%;border:1px solid #888;background:#fff;color:#222;font-size:20px;cursor:pointer;box-shadow:0 2px 8px rgba(0,0,0,.25)}\'+\n \'#chbell .bd{position:absolute;top:-6px;right:-6px;background:#e63946;color:#fff;border-radius:10px;font:bold 11px system-ui;padding:1px 6px;min-width:16px;display:none}\'+\n \'#chbell.shake{animation:chs .6s}@keyframes chs{0%,100%{transform:rotate(0)}20%{transform:rotate(-18deg)}40%{transform:rotate(16deg)}60%{transform:rotate(-10deg)}80%{transform:rotate(8deg)}}\'+\n \'#chpanel{position:fixed;top:60px;right:14px;z-index:80;width:min(400px,calc(100vw - 28px));max-height:72vh;overflow:auto;background:#fff;color:#111;border:1px solid #888;border-radius:10px;box-shadow:0 6px 24px rgba(0,0,0,.35);font:13px system-ui,sans-serif}\'+\n \'#chpanel .hd{position:sticky;top:0;background:#1d3557;color:#fff;padding:8px 12px;display:flex;gap:8px;align-items:center}\'+\n \'#chpanel .hd b{flex:1}#chpanel button{cursor:pointer;padding:2px 9px}\'+\n \'#chpanel .it{padding:7px 12px;border-bottom:1px solid #e3e3e3}#chpanel .it.un{background:#fff7e6}\'+\n \'#chpanel .it small{display:block;color:#666;margin-top:2px}#chpanel .tag{font-size:11px;font-weight:bold}\'+\n \'@media (prefers-color-scheme:dark){#chbell{background:#222;color:#eee}#chpanel{background:#1b1b1b;color:#eee;border-color:#555}#chpanel .it{border-color:#333}#chpanel .it.un{background:#2a2412}#chpanel .it small{color:#aaa}}\';\ndocument.head.appendChild(st);\nvar bell=document.createElement(\'button\'); bell.id=\'chbell\'; bell.title=\'Changes\'; bell.innerHTML=\'&#128276;<span class="bd" id="chbd"></span>\';\nvar panel=document.createElement(\'div\'); panel.id=\'chpanel\'; panel.hidden=true;\ndocument.body.appendChild(bell); document.body.appendChild(panel);\nvar items=[], since=0, primed=false;\nvar seen=parseInt(ls(\'chseen\')||\'0\',10)||0, cleared=parseInt(ls(\'chcleared\')||\'0\',10)||0;\nfunction esc(s){return String(s==null?\'\':s).replace(/[&<>"]/g,function(c){return {\'&\':\'&amp;\',\'<\':\'&lt;\',\'>\':\'&gt;\',\'"\':\'&quot;\'}[c]})}\nfunction ago(t){var s=Math.max(0,Math.round(Date.now()/1000-t));\n  if(s<10)return \'just now\'; if(s<60)return s+\' seconds ago\';\n  var m=Math.round(s/60); if(m<60)return m+(m===1?\' minute ago\':\' minutes ago\');\n  var h=Math.floor(s/3600), mm=Math.round((s%3600)/60); if(h<24)return h+(h===1?\' hour \':\' hours \')+(mm?mm+\' min \':\'\')+\'ago\';\n  var d=Math.floor(s/86400), hh=Math.floor((s%86400)/3600); return d+(d===1?\' day \':\' days \')+(hh?hh+\' h \':\'\')+\'ago\'}\nfunction when(t){return new Date(t*1000).toLocaleString(\'en-GB\',{day:\'2-digit\',month:\'short\',year:\'numeric\',hour:\'2-digit\',minute:\'2-digit\',second:\'2-digit\'})}\nfunction visible(){return items.filter(function(i){return i.id>cleared})}\nfunction unread(){return visible().filter(function(i){return i.id>seen}).length}\nfunction maxId(){return Math.max.apply(null,[0].concat(items.map(function(i){return i.id})))}\nfunction draw(){\n  var u=unread(), b=document.getElementById(\'chbd\'); b.style.display=u?\'\':\'none\'; b.textContent=u>99?\'99+\':u;\n  if(panel.hidden) return;\n  var v=visible();\n  panel.innerHTML=\'<div class="hd"><b>Changes (\'+v.length+\')</b><label><input type="checkbox" id="chsnd"\'+(ls(\'chsound\')===\'1\'?\' checked\':\'\')+\'> sound</label><button id="chclr">Clear all</button></div>\'+\n   (v.map(function(i){return \'<div class="it\'+(i.id>seen?\' un\':\'\')+\'"><span class="tag" style="color:\'+(COL[i.kind]||\'#333\')+\'">&#9679; \'+(LBL[i.kind]||\'\')+\'</span> \'+esc(i.text)+\'<small>\'+ago(i.t)+\' &middot; \'+when(i.t)+\'</small></div>\'}).join(\'\')||\'<div class="it">No changes.</div>\');\n  document.getElementById(\'chclr\').onclick=function(){cleared=maxId();ls(\'chcleared\',String(cleared));seen=cleared;ls(\'chseen\',String(seen));draw()};\n  document.getElementById(\'chsnd\').onchange=function(e){ls(\'chsound\',e.target.checked?\'1\':\'0\')};\n}\nbell.onclick=function(e){e.stopPropagation();panel.hidden=!panel.hidden;if(!panel.hidden){draw();seen=Math.max(seen,maxId());ls(\'chseen\',String(seen));var b=document.getElementById(\'chbd\');b.style.display=\'none\'}};\ndocument.addEventListener(\'click\',function(e){if(!panel.hidden&&!panel.contains(e.target)&&e.target!==bell){panel.hidden=true}});\nfunction beep(){try{var a=new (window.AudioContext||window.webkitAudioContext)();var o=a.createOscillator();o.frequency.value=660;o.connect(a.destination);o.start();setTimeout(function(){o.stop();a.close()},150)}catch(e){}}\nfunction poll(){\n  fetch(\'/changes.json?key=\'+encodeURIComponent(KEY)+\'&since=\'+since,{cache:\'no-store\'}).then(function(r){return r.json()}).then(function(d){\n    var got=d.changes||[];\n    got.forEach(function(c){items.push(c); if(c.id>since)since=c.id});\n    items.sort(function(a,b){return b.id-a.id});\n    if(got.length&&primed){bell.classList.remove(\'shake\');void bell.offsetWidth;bell.classList.add(\'shake\'); if(ls(\'chsound\')===\'1\')beep()}\n    if(got.length||!primed) draw();\n    primed=true;\n  }).catch(function(){});\n}\npoll(); setInterval(poll,3000); setInterval(function(){draw()},30000);\n})();\n</script>\n'


LINK_HTML = '<script>\n(function(){\nvar KEY=new URLSearchParams(location.search).get(\'key\')||\'\';\nif(document.getElementById(\'xtlink\')) return;\nfunction esc(s){return String(s==null?\'\':s).replace(/[&<>"]/g,function(c){return {\'&\':\'&amp;\',\'<\':\'&lt;\',\'>\':\'&gt;\',\'"\':\'&quot;\'}[c]})}\nvar a=document.createElement(\'a\'); a.id=\'xtlink\'; a.href=\'/tools?key=\'+encodeURIComponent(KEY); a.textContent=\'Analysis & tools\';\na.style.cssText=\'position:fixed;top:14px;right:64px;z-index:80;background:#1d3557;color:#fff;padding:6px 12px;border-radius:18px;text-decoration:none;font:13px system-ui,sans-serif;box-shadow:0 2px 8px rgba(0,0,0,.25)\';\ndocument.body.appendChild(a);\nvar sec=document.createElement(\'div\'); sec.id=\'xtsec\'; sec.style.cssText=\'margin:24px 16px\';\nsec.innerHTML=\'<h3>More tables <small style="font-weight:normal;color:#777">(hidden until you add them; each one loads only when added)</small></h3><select id="xtsel" style="padding:5px;max-width:360px"><option value="">Add a table...</option></select><div id="xtlist"></div>\';\ndocument.body.appendChild(sec);\nvar META=[], chosen=[];\nfunction ls(k,v){try{if(v===undefined)return localStorage.getItem(k);localStorage.setItem(k,v)}catch(e){return null}}\ntry{chosen=JSON.parse(ls(\'xtools\')||\'[]\')}catch(e){chosen=[]}\nfunction save(){ls(\'xtools\',JSON.stringify(chosen))}\nfunction box(id){return document.getElementById(\'xt_\'+id)}\nfunction draw(id,d){\n  var b=box(id); if(!b) return; var h=\'\';\n  if(d.note) h+=\'<p><small>\'+esc(d.note)+\'</small></p>\';\n  if(d.type===\'text\'){h+=\'<pre style="white-space:pre-wrap">\'+esc(d.text)+\'</pre>\'}\n  else{var mx=1; if(d.bar!=null) d.rows.forEach(function(r){mx=Math.max(mx,parseFloat(String(r[d.bar]).replace(/,/g,\'\'))||0)});\n    h+=\'<div class="w"><table><tr>\'+d.columns.map(function(c){return \'<th>\'+esc(c)+\'</th>\'}).join(\'\')+\'</tr>\';\n    d.rows.forEach(function(r){h+=\'<tr>\'+r.map(function(c,i){var bb=\'\';if(d.bar===i){var v=parseFloat(String(c).replace(/,/g,\'\'))||0;bb=\'<span style="display:inline-block;height:10px;background:#12803c;margin-left:6px;width:\'+Math.round(140*v/mx)+\'px"></span>\'}return \'<td>\'+esc(c)+bb+\'</td>\'}).join(\'\')+\'</tr>\'});\n    h+=\'</table></div>\'+(d.rows.length?\'\':\'<p>Nothing to show yet.</p>\')}\n  b.querySelector(\'.xtbody\').innerHTML=h;\n}\nfunction load(id){fetch(\'/tools/data/\'+id+\'.json?key=\'+encodeURIComponent(KEY),{cache:\'no-store\'}).then(function(r){return r.json()}).then(function(d){draw(id,d)}).catch(function(){})}\nfunction add(id){\n  var m=META.filter(function(t){return t.id===id})[0]; if(!m||box(id)) return;\n  var d=document.createElement(\'div\'); d.id=\'xt_\'+id; d.style.cssText=\'margin:14px 0;border:1px solid #bbb;border-radius:8px;padding:8px 12px\';\n  d.innerHTML=\'<b>\'+esc(m.group)+\': \'+esc(m.title)+\'</b> <button class="xtx" style="float:right">Hide</button><div class="xtbody">Loading...</div>\';\n  d.querySelector(\'.xtx\').onclick=function(){d.remove();chosen=chosen.filter(function(x){return x!==id});save()};\n  document.getElementById(\'xtlist\').appendChild(d); load(id);\n}\nfetch(\'/tools/list.json?key=\'+encodeURIComponent(KEY)).then(function(r){return r.json()}).then(function(l){\n  META=l.filter(function(t){return !t.param});\n  var sel=document.getElementById(\'xtsel\'), g=\'\', og=null;\n  META.forEach(function(t){if(t.group!==g){g=t.group;og=document.createElement(\'optgroup\');og.label=g;sel.appendChild(og)}var o=document.createElement(\'option\');o.value=t.id;o.textContent=t.title;og.appendChild(o)});\n  sel.onchange=function(){var id=sel.value;sel.value=\'\';if(!id)return;if(chosen.indexOf(id)<0){chosen.push(id);save()}add(id)};\n  chosen.forEach(add);\n});\nsetInterval(function(){if(document.hidden)return;chosen.forEach(load)},30000);\n})();\n</script>\n'


# ================================================================== EXTRA PAGES (each one stacks tools, or is a board)
PAGES = {
    "dashboard": ("Dashboard", ["status_mix", "bookings_day", "deliveries_day", "city_share"]),
    "operations": ("Operations", ["aging", "longest_transit", "action_steps", "step_freq", "step_age", "stuck_by_city", "unbooked_age", "dup_refs"]),
    "cities": ("Cities", ["city_share", "city_returns", "slow_cities", "fast_cities", "cod_by_city", "stuck_by_city"]),
    "products": ("Products", ["product_matrix", "product_value", "ret_product_reason"]),
    "finance": ("Finance", ["week_money", "gap_tracker", "pay_delay", "tax_summary", "return_loss", "avg_fee", "cod_waiting",
                            "running_net", "week_compare", "cod_by_week"]),
    "returns": ("Returns", ["ret_list", "ret_repeat", "ret_reasons", "ret_city_refusals", "ret_months"]),
    "webhooks": ("Webhook", ["ev_status", "ev_hours", "ev_hours_local", "ev_parcels", "ev_latency", "ev_silent", "ev_transitions", "ev_cities"]),
}
NAV_ORDER = ["dashboard", "board", "operations", "cities", "products", "finance", "returns", "webhooks"]

SHELL_CSS = """<style>
body{font-family:system-ui,sans-serif;margin:0;padding:0 0 40px}
#pnav{display:flex;gap:6px;flex-wrap:wrap;padding:10px 70px 10px 14px;background:#1d3557;position:sticky;top:0;z-index:50}
#pnav a{color:#fff;text-decoration:none;padding:5px 11px;border-radius:14px;font-size:14px;background:rgba(255,255,255,.12)}
#pnav a.on{background:#fff;color:#1d3557;font-weight:bold}
#pbody{padding:14px}
.box{border:1px solid #bbb;border-radius:8px;padding:8px 14px;margin:14px 0}
.box h3{margin:4px 0}
table{border-collapse:collapse;margin:6px 0}th,td{border:1px solid #ccc;padding:4px 9px;text-align:left;font-size:14px}th{background:#f2f2f2}
.bar{display:inline-block;height:10px;background:#12803c;margin-left:6px}
.cards{display:flex;flex-wrap:wrap;gap:10px}.cd{border:1px solid #bbb;border-radius:8px;padding:8px 16px;min-width:130px}.cd b{display:block;font-size:24px}
.col{flex:1;min-width:210px;background:#f4f6f8;border-radius:8px;padding:8px}.col h4{margin:2px 0 8px}
.card{background:#fff;border:1px solid #ccc;border-left:5px solid #888;border-radius:6px;padding:6px 8px;margin:6px 0;font-size:13px}
@media (prefers-color-scheme:dark){body{background:#111;color:#eee}th{background:#222}th,td,.box,.cd{border-color:#444}.col{background:#1b1b1b}.card{background:#222;border-color:#444}}
</style>"""

SHELL_JS = """<script>
var KEY=new URLSearchParams(location.search).get('key')||'', IDS=IDSJSON;
function esc(s){return String(s==null?'':s).replace(/[&<>"]/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]})}
function draw(id,d){var b=document.getElementById('b_'+id); if(!b)return; var h='';
  if(d.note)h+='<p><small>'+esc(d.note)+'</small></p>';
  if(d.type==='text'){h+='<pre style="white-space:pre-wrap">'+esc(d.text)+'</pre>'}
  else{var mx=1;if(d.bar!=null)d.rows.forEach(function(r){mx=Math.max(mx,parseFloat(String(r[d.bar]).replace(/,/g,''))||0)});
    h+='<div style="overflow-x:auto"><table><tr>'+d.columns.map(function(c){return '<th>'+esc(c)+'</th>'}).join('')+'</tr>';
    d.rows.forEach(function(r){h+='<tr>'+r.map(function(c,i){var bb='';if(d.bar===i){var v=parseFloat(String(c).replace(/,/g,''))||0;bb='<span class="bar" style="width:'+Math.round(150*v/mx)+'px"></span>'}return '<td>'+esc(c)+bb+'</td>'}).join('')+'</tr>'});
    h+='</table></div>'+(d.rows.length?'':'<p>Nothing to show yet.</p>')+'<a href="/tools/data/'+id+'.csv?key='+encodeURIComponent(KEY)+'">Download CSV</a>'}
  b.innerHTML=h}
function load(id){fetch('/tools/data/'+id+'.json?key='+encodeURIComponent(KEY),{cache:'no-store'}).then(function(r){return r.json()}).then(function(d){draw(id,d)}).catch(function(){})}
function loadAll(){IDS.forEach(function(x){load(x[0])})}
var body=document.getElementById('pbody'); IDS.forEach(function(x){var d=document.createElement('div');d.className='box';d.innerHTML='<h3>'+esc(x[1])+'</h3><div id="b_'+x[0]+'">Loading...</div>';body.appendChild(d)});
loadAll(); setInterval(function(){if(!document.hidden)loadAll()},20000);
function kpi(){var el=document.getElementById('kpis'); if(!el)return;
  fetch('/insights.json?key='+encodeURIComponent(KEY),{cache:'no-store'}).then(function(r){return r.json()}).then(function(d){if(!d.cards)return;var c=d.cards;
   el.innerHTML='<div class="cd">Success rate<b>'+c.success+'%</b></div><div class="cd">Avg delivery<b>'+c.avg_days+' days</b></div><div class="cd">Delivered<b>'+c.delivered+'</b></div><div class="cd">In transit<b>'+c.transit+'</b></div><div class="cd">Returned / refused<b>'+c.returned+'</b></div><div class="cd">Needs action<b>'+c.action+'</b></div><div class="cd">Stuck<b>'+(d.stuck||[]).length+'</b></div>'})}
kpi(); setInterval(kpi,20000);
</script>"""

BOARD_JS = """<script>
var KEY=new URLSearchParams(location.search).get('key')||'';
function esc(s){return String(s==null?'':s).replace(/[&<>"]/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]})}
function load(){fetch('/board.json?key='+encodeURIComponent(KEY),{cache:'no-store'}).then(function(r){return r.json()}).then(function(d){
  if(!d.columns)return; document.getElementById('cols').innerHTML=d.columns.map(function(c){
   return '<div class="col"><h4>'+esc(c.title)+' ('+c.total+')</h4>'+c.cards.map(function(k){return '<div class="card" style="border-left-color:'+c.color+'"><b>'+esc(k.order)+'</b> &middot; '+esc(k.city)+'<br><small>'+esc(k.step)+(k.when?' &middot; '+esc(k.when):'')+'</small></div>'}).join('')+(c.total>c.cards.length?'<small>+ '+(c.total-c.cards.length)+' more</small>':'')+'</div>'}).join('')})}
load(); setInterval(function(){if(!document.hidden)load()},15000);
</script>"""


def nav_html(active, key):
    # the pages made earlier, then the new ones
    old = [("Report", "/report"), ("Orders", "/orders"), ("Product report", "/products"), ("Track", "/track")]
    items = [("dashboard", "Dashboard"), ("board", "Board"), ("operations", "Operations"), ("cities", "Cities"),
             ("products", "Product analysis"), ("finance", "Finance"), ("returns", "Returns"), ("webhooks", "Webhook")]
    h = "<div id='pnav'>" + "".join(f"<a href='{u}?key={key}'>{l}</a>" for l, u in old)
    for k, label in items:
        url = f"/board?key={key}" if k == "board" else f"/p/{k}?key={key}"
        h += f"<a href='{url}'{' class=on' if k == active else ''}>{label}</a>"
    return h + f"<a href='/tools?key={key}'{' class=on' if active == 'tools' else ''}>All tools</a></div>"


def board_data():
    ps = parcels()
    act = [p for p in ps if state_group(p) == "transit" and any(w in str(p.get("last_event") or "").lower() for w in A._ACTION_WORDS)]
    act_cn = {p["cn"] for p in act}
    paid = {str(l.get("ConsignmentNumber")) for l in lines() if num(l.get("CODAmount")) > 0}

    def card(p):
        return {"order": p.get("order") or p["cn"], "city": p.get("city") or "", "step": p.get("last_event") or p.get("status") or "",
                "when": p.get("last_time") or ""}
    cols = [("Needs action", "#e63946", act),
            ("In transit", "#457b9d", [p for p in ps if state_group(p) == "transit" and p["cn"] not in act_cn]),
            ("Delivered, not paid", "#f4a261", [p for p in ps if p.get("state") == "delivered" and str(p["cn"]) not in paid]),
            ("Refused (not returning yet)", "#8b4513", [p for p in ps if p.get("state") == "rejected"]),
            ("Return to origin", "#7a1fa2", [p for p in ps if p.get("state") == "returning"]),
            ("Return to shipper", "#555555", [p for p in ps if p.get("state") == "returned"]),
            ("Delivered and paid", "#12803c", [p for p in ps if p.get("state") == "delivered" and str(p["cn"]) in paid])]
    return [{"title": t, "color": c, "total": len(items), "cards": [card(p) for p in items[:60]]} for t, c, items in cols]


@tool("shopify_webhooks", "Tools", "Shopify order webhook (old connection) health")
def shopify_webhooks():
    """Read-only: is the Shopify orders/create webhook registered for this service, and has it fired?"""
    rows = []
    try:
        r = A.requests.get(f"{A.SHOPIFY_BASE}/webhooks.json", headers=A.shopify_headers(), timeout=(10, 20))
        r.raise_for_status()
        ours = f"{A.APP_BASE_URL}/webhooks/orders-create" if A.APP_BASE_URL else ""
        for w in r.json().get("webhooks", []):
            rows.append([w.get("topic"), w.get("address"), "this service" if w.get("address") == ours else "other", w.get("created_at", "")[:10]])
        state = "registered" if any(x[2] == "this service" for x in rows) else "NOT registered"
    except Exception as e:
        return TEXT(f"Could not read Shopify webhooks: {type(e).__name__}")
    rows.append(["last order webhook received", A.status.get("last_webhook_at") or "none since the service started", "", ""])
    rows.append(["backup check of orders", A.status.get("last_catch_up_at") or "", "every 15 minutes finds any order the webhook missed", ""])
    return T(["Topic", "Address", "Belongs to", "Created"], rows,
             note=f"orders/create webhook: {state}. Shopify pushes new orders at once; the catch-up pass is the backup.")


@tool("redundancy", "Tools", "Which path is carrying the data? (webhook and backup)")
def redundancy():
    import time as _t
    now = _t.time()
    last_ev = events()[-1]["at"] if events() else ""
    def ago(t):
        try:
            return int(now - datetime.strptime(t, "%Y-%m-%d %H:%M:%S UTC").replace(tzinfo=timezone.utc).timestamp())
        except (ValueError, TypeError):
            return None
    ev_age = ago(last_ev)
    hook_ok = ev_age is not None and ev_age < 86400
    poll_ok = bool(A._live.get("at")) and now - A._live["at"] < 180 and not A._live.get("error")
    sh_hook = A.status.get("last_webhook_at")
    catch_ok = bool(A.status.get("last_catch_up_at")) and A.status.get("stage") != "failed"

    def cover(primary, backup):
        return "both working" if primary and backup else "backup only: the primary is silent" if backup else "primary only: the backup failed" if primary else "NOTHING working: check the system tool"
    rows = [
        ["Parcel updates", "Envio webhook: " + (f"last event {ev_age}s ago" if ev_age is not None else "no event yet"),
         "TCS tracking, re-read every 15 s: " + ("ok" if poll_ok else "FAILED"), cover(hook_ok, poll_ok)],
        ["New Shopify orders", "Shopify webhook: " + (sh_hook or "none since the service started"),
         "Catch-up pass every 15 min: " + ("ok" if catch_ok else "FAILED"), cover(bool(sh_hook), catch_ok)],
        ["Weekly statement and invoices", "no webhook exists", "Envio portal read: " + ("ok" if (A._envio_data.get("at") and not A._envio_data.get("error")) else "FAILED"),
         "portal read only" if (A._envio_data.get("at") and not A._envio_data.get("error")) else "NOTHING working"],
    ]
    return T(["What", "Primary (pushed at once)", "Backup (checks regularly)", "Covered by"], rows,
             note="A silent webhook can be normal when nothing changed. The backup always keeps the report up to date.")


# ================================================================== routes
def _ok():
    if not A.ORDERS_PAGE_KEY or not hmac.compare_digest(request.args.get("key", ""), A.ORDERS_PAGE_KEY):
        abort(401)
    A._ensure_live_thread()


GROUPS = ["Parcels", "Money", "Webhook", "Returns", "Tools"]


def _ordered():
    return sorted(TOOLS, key=lambda t: GROUPS.index(t["group"]) if t["group"] in GROUPS else 99)


def _find(tid):
    return next((t for t in TOOLS if t["id"] == tid), None)


def _run(t):
    try:
        out = t["fn"]()
    except Exception as e:  # one broken tool must never take the page down
        out = TEXT(f"This tool could not run: {type(e).__name__}")
    out["id"], out["title"] = t["id"], t["title"]
    return out


PAGE = """<!doctype html><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'>
<title>TCS tools</title>
<style>
body{font-family:system-ui,sans-serif;margin:0;display:flex;min-height:100vh}
#nav{width:260px;border-right:1px solid #ccc;padding:10px;overflow:auto;max-height:100vh;position:sticky;top:0}
#main{flex:1;padding:14px;overflow:auto}
#nav input{width:100%;padding:5px;margin-bottom:8px;box-sizing:border-box}
#nav h4{margin:12px 0 4px;color:#666;font-size:12px;text-transform:uppercase}
#nav a{display:block;padding:3px 6px;cursor:pointer;border-radius:4px;font-size:14px}
#nav a.on{background:#222;color:#fff}
table{border-collapse:collapse;margin:8px 0}th,td{border:1px solid #ccc;padding:4px 9px;text-align:left;font-size:14px}th{background:#f2f2f2}
.bar{display:inline-block;height:10px;background:#12803c;vertical-align:middle;margin-left:6px}
pre{white-space:pre-wrap;background:#f6f6f6;padding:10px;border-radius:6px}
@media (prefers-color-scheme:dark){body{background:#111;color:#eee}#nav{border-color:#444}th{background:#222}th,td{border-color:#444}pre{background:#1b1b1b}}
@media (max-width:700px){body{display:block}#nav{width:auto;position:static;max-height:none}}
</style>
<div id='nav'><a href='/report?key=KEYVAL' style='font-weight:bold'>&larr; Back to the report</a><div style='margin:6px 0 10px;line-height:1.9'><a href='/orders?key=KEYVAL'>Orders</a> <a href='/products?key=KEYVAL'>Product report</a> <a href='/track?key=KEYVAL'>Track</a><br><a href='/p/dashboard?key=KEYVAL'>Dashboard</a> <a href='/board?key=KEYVAL'>Board</a> <a href='/p/operations?key=KEYVAL'>Operations</a> <a href='/p/cities?key=KEYVAL'>Cities</a> <a href='/p/products?key=KEYVAL'>Product analysis</a> <a href='/p/finance?key=KEYVAL'>Finance</a> <a href='/p/returns?key=KEYVAL'>Returns</a> <a href='/p/webhooks?key=KEYVAL'>Webhook</a></div><input id='q' placeholder='Find a tool...'><div id='list'></div></div>
<div id='main'><h2 id='ttl'>Pick a tool on the left</h2><div id='ptool'></div><div id='out'></div></div>
<script>
var KEY=new URLSearchParams(location.search).get('key')||'', TOOLS=TOOLSJSON, cur=null, timer=null;
function esc(s){return String(s==null?'':s).replace(/[&<>"]/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]})}
function nav(){var q=document.getElementById('q').value.toLowerCase(),g='',h='';
  TOOLS.forEach(function(t){if(q&&(t.title+t.group).toLowerCase().indexOf(q)<0)return; if(t.group!==g){g=t.group;h+='<h4>'+esc(g)+'</h4>'}
    h+='<a data-id="'+t.id+'" class="'+(cur&&cur.id===t.id?'on':'')+'">'+esc(t.title)+'</a>'});
  document.getElementById('list').innerHTML=h}
document.getElementById('q').addEventListener('input',nav);
document.getElementById('list').addEventListener('click',function(e){var a=e.target.closest('a');if(!a)return;open(a.getAttribute('data-id'))});
function open(id){cur=TOOLS.filter(function(t){return t.id===id})[0];try{localStorage.setItem('tool',id)}catch(e){}
  document.getElementById('ttl').textContent=cur.title; var p=document.getElementById('ptool');
  p.innerHTML=cur.param?'<input id="par" placeholder="'+esc(cur.param)+'" style="padding:5px;width:260px"> <button id="go">Show</button>':'';
  if(cur.param){document.getElementById('go').onclick=load;document.getElementById('par').addEventListener('keydown',function(e){if(e.key==='Enter')load()})}
  nav();load();clearInterval(timer);timer=setInterval(function(){if(!document.hidden&&!(cur.param&&document.activeElement&&document.activeElement.id==='par'))load()},15000)}
function load(){var par=document.getElementById('par'),qs=par?'&q='+encodeURIComponent(par.value):'';
  fetch('/tools/data/'+cur.id+'.json?key='+encodeURIComponent(KEY)+qs,{cache:'no-store'}).then(function(r){return r.json()}).then(draw)}
function draw(d){var o=document.getElementById('out'),h='';
  if(d.note)h+='<p><small>'+esc(d.note)+'</small></p>';
  if(d.type==='text'){h+='<pre id="txt">'+esc(d.text)+'</pre><button onclick="navigator.clipboard.writeText(document.getElementById(\\'txt\\').textContent)">Copy</button>'}
  else{var mx=1;if(d.bar!=null)d.rows.forEach(function(r){mx=Math.max(mx,parseFloat(String(r[d.bar]).replace(/,/g,''))||0)});
    h+='<table><tr>'+d.columns.map(function(c){return '<th>'+esc(c)+'</th>'}).join('')+'</tr>';
    d.rows.forEach(function(r){h+='<tr>'+r.map(function(c,i){var b='';if(d.bar===i){var v=parseFloat(String(c).replace(/,/g,''))||0;b='<span class="bar" style="width:'+Math.round(160*v/mx)+'px"></span>'}return '<td>'+esc(c)+b+'</td>'}).join('')+'</tr>'});
    h+='</table>'+(d.rows.length?'':'<p>Nothing to show yet.</p>')+'<a href="/tools/data/'+cur.id+'.csv?key='+encodeURIComponent(KEY)+(document.getElementById('par')?'&q='+encodeURIComponent(document.getElementById('par').value):'')+'">Download CSV</a>'}
  o.innerHTML=h}
nav();var last='';try{last=localStorage.getItem('tool')||''}catch(e){} if(last&&TOOLS.some(function(t){return t.id===last}))open(last);
</script>"""


def register(flask_app, app_module):
    global A
    A = app_module

    threading.Thread(target=_watch, daemon=True, name="change-watch").start()

    @flask_app.route("/changes.json")
    def changes_json():
        _ok()
        try:
            since = int(request.args.get("since", "0"))
        except ValueError:
            since = 0
        return jsonify({"changes": [c for c in CH["log"] if c["id"] > since][-500:], "max": CH["seq"]}), 200

    @flask_app.route("/tools")
    def tools_page():
        _ok()
        meta = [{"id": t["id"], "group": t["group"], "title": t["title"], "param": t["param"]} for t in _ordered()]
        return (PAGE.replace("TOOLSJSON", json.dumps(meta)).replace("KEYVAL", request.args.get("key", ""))
                + NOTIFY_JS), 200

    @flask_app.route("/p/<name>")
    def page_of_tools(name):
        _ok()
        if name not in PAGES:
            abort(404)
        title, ids = PAGES[name]
        key = request.args.get("key", "")
        meta = [[i, (_find(i) or {"title": i})["title"]] for i in ids if _find(i)]
        kpis = "<div class='cards' id='kpis' style='margin:6px 0'></div>" if name == "dashboard" else ""
        return ("<!doctype html><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'>"
                f"<title>{title} - TCS</title>" + SHELL_CSS + nav_html(name, key)
                + f"<div id='pbody'><h2>{title}</h2>{kpis}</div>" + SHELL_JS.replace("IDSJSON", json.dumps(meta)) + NOTIFY_JS), 200

    @flask_app.route("/board")
    def board_page():
        _ok()
        key = request.args.get("key", "")
        return ("<!doctype html><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'>"
                "<title>Board - TCS</title>" + SHELL_CSS + nav_html("board", key)
                + "<div id='pbody'><h2>Parcel board</h2><div id='cols' style='display:flex;gap:10px;flex-wrap:wrap;align-items:flex-start'>Loading...</div></div>"
                + BOARD_JS + NOTIFY_JS), 200

    @flask_app.route("/board.json")
    def board_json():
        _ok()
        return jsonify({"columns": board_data()}), 200

    @flask_app.route("/tools/list.json")
    def tools_list():
        _ok()
        return jsonify([{"id": t["id"], "group": t["group"], "title": t["title"], "param": t["param"]} for t in _ordered()]), 200

    @flask_app.route("/tools/data/<tid>.json")
    def tools_data(tid):
        _ok()
        t = _find(tid)
        if not t:
            abort(404)
        return jsonify(_run(t)), 200

    @flask_app.route("/tools/data/<tid>.csv")
    def tools_csv(tid):
        _ok()
        t = _find(tid)
        if not t:
            abort(404)
        d = _run(t)
        buf = io.StringIO()
        w = csv.writer(buf)
        if d["type"] == "table":
            def safe(x):
                x = "" if x is None else str(x)
                return "'" + x if x[:1] in ("=", "+", "-", "@") else x
            w.writerow(d["columns"])
            for r in d["rows"]:
                w.writerow([safe(c) for c in r])
        else:
            w.writerow([d["text"]])
        return Response(buf.getvalue(), mimetype="text/csv", headers={"Content-Disposition": f"attachment; filename={tid}.csv"})

    @flask_app.route("/tools/print")
    def tools_print():
        _ok()
        week = request.args.get("week", "")
        ls = [l for l in lines() if l["_week"] == week]
        v = next((x for x in ledger() if x["BILLING_WEEK"] == week), {})
        esc = __import__("html").escape
        rows = "".join("<tr>" + "".join(f"<td>{esc(str(l.get(k, '')))}</td>" for k in (
            "ConsignmentNumber", "CustomerReference", "BookingDate", "Consignee", "Destination", "DeliveryStatus", "CODAmount",
            "ShippingChg", "GSTWH", "ITWH", "BalPayable")) + "</tr>" for l in ls)
        return (f"<!doctype html><meta charset='utf-8'><title>Statement {esc(week)}</title>"
                "<style>body{font-family:Arial;margin:20px}table{border-collapse:collapse;width:100%}th,td{border:1px solid #999;padding:4px 6px;font-size:12px;text-align:left}"
                "@media print{button{display:none}}</style>"
                f"<h2>TCS payment statement {esc(week)}</h2><p>Invoice payable: {esc(str(v.get('AMOUNTPAYABLE', 'not issued')))} "
                f"&middot; Invoice date: {esc(str(v.get('INVOICEDATE', '')))} &middot; Paid on: {esc(str(v.get('PAYMENTDATE', '')))}</p>"
                "<button onclick='window.print()'>Print</button><table><tr><th>CN</th><th>Reference</th><th>Booked</th><th>Consignee</th>"
                "<th>Destination</th><th>Status</th><th>COD</th><th>Shipping</th><th>GST WH</th><th>IT WH</th><th>Net payable</th></tr>"
                + rows + "</table>"), 200

    @flask_app.route("/tools/backup.json")
    def tools_backup():
        _ok()
        return Response(json.dumps({"parcels": parcels(), "ledger": ledger(), "statement_lines": lines(),
                                    "events": events()}, indent=1), mimetype="application/json",
                        headers={"Content-Disposition": "attachment; filename=tcs-backup.json"})
