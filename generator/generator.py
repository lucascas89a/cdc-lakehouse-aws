"""ShopFlow live data generator.

Simulates customers shopping so that every kind of change the CDC pipeline must handle
shows up in the source database: inserts, status updates, payment retries, price changes,
PII updates and deletes.

Time is compressed: one simulated hour lasts TIME_SCALE real seconds (default 60, so a
simulated day takes 24 minutes). Every order and shipment gets its own stable delay, derived
from a hash of its id, so lead times vary realistically and roughly 15% of parcels arrive
after the promised date.
"""

import logging
import os
import random
import signal
import time
from collections import Counter
from decimal import Decimal

import psycopg

log = logging.getLogger("generator")

RATE = float(os.environ.get("RATE", "1"))                  # actions per second
TIME_SCALE = float(os.environ.get("TIME_SCALE", "60"))     # real seconds per simulated hour
SEED_CUSTOMERS = int(os.environ.get("SEED_CUSTOMERS", "200"))
BATCH = 20                                                 # max rows a lifecycle step advances per call
REPORT_EVERY = 60                                          # seconds between progress logs

FIRST_NAMES = ["Ana", "Bruno", "Camila", "Diego", "Elena", "Facundo", "Gabriela", "Hernán",
               "Isabel", "Joaquín", "Lucía", "Martín", "Natalia", "Pablo", "Romina", "Santiago",
               "Valentina", "Tomás", "Florencia", "Matías"]
LAST_NAMES = ["González", "Rodríguez", "Gómez", "Fernández", "López", "Díaz", "Martínez",
              "Pérez", "Sánchez", "Romero", "Torres", "Ramírez", "Flores", "Silva", "Castro"]
COUNTRIES = ["AR"] * 4 + ["MX"] * 3 + ["CL"] * 2 + ["CO"] * 2 + ["UY", "PE"]

CATALOG = {
    "home": [("Mate térmico", 25), ("Set de sartenes", 89), ("Lámpara de escritorio", 42),
             ("Juego de sábanas", 55), ("Cafetera de émbolo", 31)],
    "electronics": [("Auriculares bluetooth", 79), ("Cargador rápido 65W", 35),
                    ("Teclado mecánico", 120), ("Mouse inalámbrico", 28), ("Parlante portátil", 64)],
    "sports": [("Pelota de fútbol", 30), ("Mat de yoga", 22), ("Botella térmica", 18),
               ("Mochila de trekking", 95), ("Bicicleta fija plegable", 310)],
    "books": [("Novela best seller", 15), ("Libro de cocina", 27), ("Guía de viajes", 21),
              ("Libro infantil ilustrado", 13)],
    "beauty": [("Kit de skincare", 48), ("Secador de pelo", 70), ("Perfume 100 ml", 85)],
}

PROVIDERS = {"mercadopago": 0.10, "stripe": 0.06, "paypal": 0.12}   # failure probability per attempt
FAILURE_CODES = ["insufficient_funds", "card_declined", "expired_card", "fraud_suspected",
                 "processor_timeout"]
CARRIERS = ["Andreani", "OCA", "Correo Argentino", "Estafeta", "Chilexpress", "Servientrega"]
MAX_PAYMENT_ATTEMPTS = 3

# Stable pseudo-random number in [0, 1) per row, so each entity keeps its own delay.
JITTER = "(abs(hashtext(%s || {id}::text)) %% 1000) / 1000.0"


def money(value: float) -> Decimal:
    return Decimal(str(round(value, 2)))


def seed(cur: psycopg.Cursor) -> None:
    """Idempotent: load the catalog and top up the customer base."""
    for category, items in CATALOG.items():
        for i, (name, price) in enumerate(items, start=1):
            cur.execute(
                "INSERT INTO shop.products (sku, name, category, unit_price) VALUES (%s, %s, %s, %s) "
                "ON CONFLICT (sku) DO NOTHING",
                (f"{category[:3].upper()}-{i:03d}", name, category, money(price)))

    cur.execute("SELECT count(*) FROM shop.customers")
    missing = max(SEED_CUSTOMERS - cur.fetchone()[0], 0)
    for _ in range(missing):
        new_customer(cur)
    log.info("seed done: catalog loaded, %d customers added", missing)


# --- Actions --------------------------------------------------------------------------
# Each returns the list of things it did (labels for the stats); empty when nothing was due.

def new_customer(cur: psycopg.Cursor) -> list[str]:
    first, last = random.choice(FIRST_NAMES), random.choice(LAST_NAMES)
    email = f"{first}.{last}.{random.randint(1000, 999999)}@example.com".lower()
    cur.execute(
        "INSERT INTO shop.customers (email, full_name, country) VALUES (%s, %s, %s) "
        "ON CONFLICT (email) DO NOTHING",
        (email, f"{first} {last}", random.choice(COUNTRIES)))
    return ["customer_created"]


def new_order(cur: psycopg.Cursor) -> list[str]:
    cur.execute("SELECT customer_id FROM shop.customers ORDER BY random() LIMIT 1")
    customer = cur.fetchone()
    cur.execute("SELECT product_id, unit_price FROM shop.products WHERE is_active ORDER BY random() LIMIT %s",
                (random.choices([1, 2, 3, 4], weights=[50, 30, 15, 5])[0],))
    lines = cur.fetchall()
    if customer is None or not lines:
        return []

    cur.execute("INSERT INTO shop.orders (customer_id) VALUES (%s) RETURNING order_id", customer)
    order_id = cur.fetchone()[0]
    for line_no, (product_id, price) in enumerate(lines, start=1):
        cur.execute(
            "INSERT INTO shop.order_items (order_id, line_no, product_id, quantity, unit_price) "
            "VALUES (%s, %s, %s, %s, %s)",
            (order_id, line_no, product_id, random.choices([1, 2, 3], weights=[75, 20, 5])[0], price))
    recompute_total(cur, order_id)
    return ["order_created"]


def recompute_total(cur: psycopg.Cursor, order_id: int) -> None:
    cur.execute(
        "UPDATE shop.orders SET total_amount = "
        "(SELECT coalesce(sum(quantity * unit_price), 0) FROM shop.order_items WHERE order_id = %s) "
        "WHERE order_id = %s",
        (order_id, order_id))


def edit_cart(cur: psycopg.Cursor) -> list[str]:
    """A customer removes a line from a pending order: produces DELETE events."""
    cur.execute(
        "SELECT oi.order_id, oi.line_no FROM shop.order_items oi JOIN shop.orders o USING (order_id) "
        "WHERE o.status = 'pending' "
        "  AND (SELECT count(*) FROM shop.order_items x WHERE x.order_id = oi.order_id) > 1 "
        "ORDER BY random() LIMIT 1")
    row = cur.fetchone()
    if row is None:
        return []
    cur.execute("DELETE FROM shop.order_items WHERE order_id = %s AND line_no = %s", row)
    recompute_total(cur, row[0])
    return ["cart_line_removed"]


def pay(cur: psycopg.Cursor) -> list[str]:
    """Attempt payment for pending orders whose next attempt is due (0.1 to 0.6 simulated hours)."""
    jitter = JITTER.format(id="o.order_id")
    cur.execute(
        "SELECT o.order_id, o.total_amount, count(p.payment_id), "
        "       (array_agg(p.provider ORDER BY p.attempt DESC))[1] "
        "FROM shop.orders o LEFT JOIN shop.payments p USING (order_id) "
        "WHERE o.status = 'pending' "
        "GROUP BY o.order_id "
        f"HAVING coalesce(max(p.created_at), o.created_at) < now() - make_interval(secs => %s * (0.1 + 0.5 * {jitter})) "
        "LIMIT %s",
        (TIME_SCALE, "pay", BATCH))
    done = []
    for order_id, amount, attempts, last_provider in cur.fetchall():
        # Retries usually stay with the same provider; sometimes the customer switches.
        provider = last_provider if last_provider and random.random() < 0.7 else random.choice(list(PROVIDERS))
        attempt = attempts + 1

        if random.random() < PROVIDERS[provider]:
            cur.execute(
                "INSERT INTO shop.payments (order_id, provider, attempt, status, amount, failure_code) "
                "VALUES (%s, %s, %s, 'failed', %s, %s)",
                (order_id, provider, attempt, amount, random.choice(FAILURE_CODES)))
            if attempt >= MAX_PAYMENT_ATTEMPTS:
                cur.execute("UPDATE shop.orders SET status = 'cancelled' WHERE order_id = %s", (order_id,))
                done.append("order_cancelled_payment")
            else:
                done.append("payment_failed")
            continue

        # Authorize, then capture: two changes on the same payment row.
        cur.execute(
            "INSERT INTO shop.payments (order_id, provider, attempt, status, amount) "
            "VALUES (%s, %s, %s, 'authorized', %s) RETURNING payment_id",
            (order_id, provider, attempt, amount))
        cur.execute("UPDATE shop.payments SET status = 'captured' WHERE payment_id = %s", cur.fetchone())
        cur.execute("UPDATE shop.orders SET status = 'paid' WHERE order_id = %s", (order_id,))
        done.append("payment_captured")
    return done


def cancel_pending(cur: psycopg.Cursor) -> list[str]:
    cur.execute(
        "UPDATE shop.orders SET status = 'cancelled' WHERE order_id = ("
        "  SELECT order_id FROM shop.orders WHERE status = 'pending' ORDER BY random() LIMIT 1) "
        "RETURNING order_id")
    return ["order_cancelled_customer"] if cur.fetchone() else []


def ship(cur: psycopg.Cursor) -> list[str]:
    """Create shipments for paid orders after 1 to 4 simulated hours of picking and packing."""
    jitter = JITTER.format(id="o.order_id")
    cur.execute(
        "SELECT o.order_id FROM shop.orders o LEFT JOIN shop.shipments s USING (order_id) "
        "WHERE o.status = 'paid' AND s.order_id IS NULL "
        f"  AND o.updated_at < now() - make_interval(secs => %s * (1 + 3 * {jitter})) "
        "LIMIT %s",
        (TIME_SCALE, "ship", BATCH))
    done = []
    for (order_id,) in cur.fetchall():
        promised_hours = random.choice([24, 48, 72, 96])
        cur.execute(
            "INSERT INTO shop.shipments (order_id, carrier, promised_at) "
            "VALUES (%s, %s, now() + make_interval(secs => %s))",
            (order_id, random.choice(CARRIERS), promised_hours * TIME_SCALE))
        done.append("shipment_created")
    return done


def advance_shipments(cur: psycopg.Cursor) -> list[str]:
    """label_created -> in_transit after 3-9 simulated hours; in_transit -> delivered after
    12-60 simulated hours (heavy tail, which is what makes some deliveries late)."""
    pickup = JITTER.format(id="shipment_id")
    transit = JITTER.format(id="shipment_id")
    cur.execute(
        "SELECT shipment_id, order_id, status FROM shop.shipments "
        f"WHERE (status = 'label_created' AND created_at < now() - make_interval(secs => %s * (3 + 6 * {pickup}))) "
        f"   OR (status = 'in_transit' AND shipped_at < now() - make_interval(secs => %s * (12 + 48 * power({transit}, 3)))) "
        "LIMIT %s",
        (TIME_SCALE, "pickup", TIME_SCALE, "transit", BATCH))
    done = []
    for shipment_id, order_id, status in cur.fetchall():
        if status == "label_created":
            cur.execute("UPDATE shop.shipments SET status = 'in_transit', shipped_at = now() WHERE shipment_id = %s",
                        (shipment_id,))
            cur.execute("UPDATE shop.orders SET status = 'shipped' WHERE order_id = %s", (order_id,))
            done.append("shipment_in_transit")
        elif random.random() < 0.02:
            cur.execute("UPDATE shop.shipments SET status = 'returned' WHERE shipment_id = %s", (shipment_id,))
            done.append("shipment_returned")
        else:
            cur.execute("UPDATE shop.shipments SET status = 'delivered', delivered_at = now() WHERE shipment_id = %s",
                        (shipment_id,))
            cur.execute("UPDATE shop.orders SET status = 'delivered' WHERE order_id = %s", (order_id,))
            done.append("shipment_delivered")
    return done


def change_price(cur: psycopg.Cursor) -> list[str]:
    cur.execute(
        "UPDATE shop.products SET unit_price = round(unit_price * %s, 2) WHERE product_id = ("
        "  SELECT product_id FROM shop.products WHERE is_active ORDER BY random() LIMIT 1) RETURNING product_id",
        (Decimal(str(random.choice([0.85, 0.9, 0.95, 1.05, 1.1, 1.2]))),))
    return ["price_changed"] if cur.fetchone() else []


def toggle_product(cur: psycopg.Cursor) -> list[str]:
    """Rarely, a product goes out of stock or comes back."""
    cur.execute(
        "UPDATE shop.products SET is_active = NOT is_active WHERE product_id = ("
        "  SELECT product_id FROM shop.products ORDER BY random() LIMIT 1) RETURNING product_id")
    return ["product_toggled"] if cur.fetchone() else []


def update_customer(cur: psycopg.Cursor) -> list[str]:
    """Customers sometimes change their email: a PII update the silver layer must re-mask."""
    cur.execute(
        "UPDATE shop.customers "
        "SET email = 'updated.' || customer_id || '.' || floor(random() * 1e6)::int || '@example.com' "
        "WHERE customer_id = (SELECT customer_id FROM shop.customers ORDER BY random() LIMIT 1) "
        "RETURNING customer_id")
    return ["customer_updated"] if cur.fetchone() else []


# Lifecycle steps run on every tick; the rest are picked by weight.
LIFECYCLE = [pay, ship, advance_shipments]
RANDOM_ACTIONS = [
    (new_order, 80),
    (new_customer, 6),
    (edit_cart, 5),
    (cancel_pending, 3),
    (change_price, 3),
    (update_customer, 2),
    (toggle_product, 1),
]


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    stop = False

    def handle_stop(*_):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGTERM, handle_stop)
    signal.signal(signal.SIGINT, handle_stop)

    conninfo = (f"host={os.environ.get('PGHOST', 'postgres')} port={os.environ.get('PGPORT', '5432')} "
                f"dbname={os.environ.get('PGDATABASE', 'shopflow')} user={os.environ.get('PGUSER', 'generator')}")
    functions, weights = zip(*RANDOM_ACTIONS)
    stats: Counter[str] = Counter()
    last_report = time.monotonic()

    log.info("starting: %.1f actions/s, 1 simulated hour = %.0f s", RATE, TIME_SCALE)
    with psycopg.connect(conninfo, password=os.environ["PGPASSWORD"]) as conn:
        with conn.transaction(), conn.cursor() as cur:
            seed(cur)

        while not stop:
            for action in [random.choices(functions, weights=weights)[0], *LIFECYCLE]:
                try:
                    with conn.transaction(), conn.cursor() as cur:
                        stats.update(action(cur))
                except psycopg.Error as exc:
                    stats["error"] += 1
                    log.warning("%s failed: %s", action.__name__, exc)

            if time.monotonic() - last_report >= REPORT_EVERY:
                log.info("last %ds: %s", REPORT_EVERY, dict(stats.most_common()))
                stats.clear()
                last_report = time.monotonic()
            time.sleep(random.expovariate(RATE))

    log.info("stopped")


if __name__ == "__main__":
    main()
