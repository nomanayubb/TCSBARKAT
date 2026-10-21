# Feature list (webhook + portal powered)

Status: **done** = live on the report, **next** = designed, not built yet. Everything uses real TCS data only
(tracking API, Envio portal reports, Envio webhook). Nothing is estimated from typed-in numbers.

## Instant updates (webhook)
1. done - Envio webhook receiver (`/webhooks/tcs`), key protected, accepts the real message shape.
2. done - A webhook event forgets the cached tracking of that parcel and wakes the live refresh: tables update in seconds.
3. done - Live events feed (newest first) with order number, status, event time, city, receiver.
4. done - Webhook health line: connected / no event yet, events in the last hour, last event time.
5. done - Toast message on every new event.
6. done - Browser tab title counter "(n)" while the tab is in the background.
7. done - Desktop notifications (opt-in button) and optional beep.
8. next - Per-parcel event history stored from webhook events (merge with tracking steps).
9. next - Webhook event replay: re-apply the last N events after a restart.
10. next - Duplicate-event filter (same CN, same status, same time).

## Insights
11. done - Success rate (delivered / (delivered + returned)).
12. done - Average delivery time in days.
13. done - Destination table: parcels, delivered, in transit, returned, success % per city.
14. done - Needs-action list (address needed, on hold, consignee not home, awaiting collection, refused).
15. next - Delivery time per city (average and slowest).
16. next - Return rate per product.
17. next - Return reasons chart (refused / bad address / on hold) by week.
18. next - Best and worst cities ranking with minimum volume.
19. next - COD amount delivered vs returned per week.
20. next - Trend line: parcels booked per day and delivered per day.

## Payments (portal)
21. done - Weekly ledger read from the Envio portal automatically (invoice, paid date, withholding).
22. done - Weekly report rows with colour rules (blue, black, red, brown) and the net payable sum.
23. done - "Paid on" confirmed from the ledger only.
24. done - Self-check: warns when a week's lines are not all classified or do not add up.
25. done - Fee rate watch: alert when TCS bills a different fee, accept to apply, expected-payout calculator.
26. done - Rows for delivered not paid, in transit, refused, return to origin, return to shipper, with List buttons.
27. next - Invoice vs lines difference tracker over weeks (the 34.76 / 127.56 style gaps).
28. next - Alert when a paid date appears or an invoice is created.
29. next - Cash-flow forecast from delivered-not-paid parcels (clearly labelled as expectation).
30. next - Per-product profit after TCS charges (needs your product cost).

## Operations
31. done - CSV export of all parcels (formula-safe).
32. next - CSV export of every table and of the payout table.
33. next - Printable weekly statement (PDF) with the colours.
34. next - Search box across CN, order, consignee, city.
35. next - Saved filters (for example "Karachi, in transit, older than 4 days").
36. next - Parcels stuck too long (no new step for N days) alert.
37. next - Bulk actions list: copy all CNs of a list, copy all order numbers of a list.
38. next - Shipper advice view (pending tickets from the TCS call) with reply status.
39. next - Complaint / ticket creator (needs the TCS complaint API on the live host).
40. next - Cost-center and pickup-address viewer.

## Customer side
41. next - Customer tracking page (customer types order number, sees steps) with no internal data.
42. next - WhatsApp / SMS template per status (message is prepared, you press send).
43. next - Automatic "delivery failed, please confirm address" prompt list.
44. next - Repeat-refuser list (phone numbers that refused before) before you ship the next order.
45. next - Address quality score before booking (short or missing address).

## Platform
46. done - Everything protected by the page key, with no secrets in the page.
47. done - Self-healing sign-in to the Envio portal (renews its session by itself).
48. done - Idle server stops calling TCS when nobody looks.
49. next - Persistent store (database) so events and history survive restarts.
50. next - Roles: read-only view for staff, full view for you.
51. next - Health page: portal login, tracking, webhook, last refresh times.
52. next - Daily e-mail / WhatsApp summary of the day.
53. next - Settings page (refresh speed, alert words, columns shown) stored on the server.
54. next - Multi-store support (more than one Shopify store and TCS account).
55. next - Order-book bridge: book parcels from the report with the TCS booking API (needs your approval per use).

## Added in batch 3 (separate file `extras.py`, own pages, nothing earlier was changed)
- Bell icon (top right of the report and of the tools page): every change in the tables, with time ago and exact date and time, unread badge, sound option, Clear all.
- Change log covers: status changes (in transit -> refused -> return to origin -> return to shipper, delivered), new steps, COD and payment changes, any change in a weekly report line, new weekly reports and payment dates, orders waiting for booking.
- `/tools` page with 57 analysis tools in five groups (Parcels, Money, Webhook, Returns, Tools): search, auto refresh, CSV for each.
- "More tables" at the bottom of the report: hidden until you add one from the list; each loads only when added.
- Printable weekly statement, JSON backup, daily digest and message text, settings and freshness checks.
- Safety: at most 4 TCS tracking calls at once, 60 second pause after an error answer from TCS, a failed answer never wipes known data, 5 minute pause after a failed Envio sign-in (protects the account from locking).
