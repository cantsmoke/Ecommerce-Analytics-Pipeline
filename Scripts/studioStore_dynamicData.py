import random
import psycopg2
from datetime import datetime, timedelta
import argparse

DB_NAME = "xxxxx"
DB_USER = "xxxxx"
DB_PASSWORD = "xxxxx"
DB_HOST = "xxxxx"
DB_PORT = "xxxx"

def get_conn():
    return psycopg2.connect(
        dbname=DB_NAME, user=DB_USER,
        password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
    )

def fetch_ids(cur):
    cur.execute("SELECT id FROM customers")
    customer_ids = [r[0] for r in cur.fetchall()]
    cur.execute("SELECT id, price FROM products")
    products = cur.fetchall()
    return customer_ids, products

def simulate_day(cur, customer_ids, products, current_date):
    num_customers = random.randint(int(len(customer_ids) * 0.02), int(len(customer_ids) * 0.06))
    active_customers = random.sample(customer_ids, num_customers)

    views_data = []
    orders_data = []
    order_items_data = []

    for customer_id in active_customers:
        num_views = random.randint(1, 10)
        viewed_products = random.sample(products, num_views)
        
        for product_id, _ in viewed_products:
            view_time = datetime.combine(current_date, datetime.min.time()) + timedelta(
                seconds=random.randint(0, 86400)
            )
            views_data.append((customer_id, product_id, view_time))

        if random.random() < 0.05:
            order_time = datetime.combine(current_date, datetime.min.time()) + timedelta(
                seconds=random.randint(0, 86400)
            )
            orders_data.append((customer_id, order_time))
            order_products = random.sample(viewed_products, random.randint(1, len(viewed_products)))
            order_items = [(product_id, 1) for product_id, _ in order_products]
            order_items_data.append(order_items)

    if views_data:
        cur.executemany(
            "INSERT INTO views (customer_id, product_id, view_timestamp) VALUES (%s, %s, %s)",
            views_data
        )

    for i, order in enumerate(orders_data):
        cur.execute(
            "INSERT INTO orders (customer_id, order_timestamp) VALUES (%s, %s) RETURNING id",
            order
        )
        order_id = cur.fetchone()[0]
        for product_id, qty in order_items_data[i]:
            cur.execute(
                "INSERT INTO order_items (order_id, product_id, quantity) VALUES (%s, %s, %s)",
                (order_id, product_id, qty)
            )

def run_for_date(date_str):
    conn = get_conn()
    cur = conn.cursor()
    customer_ids, products = fetch_ids(cur)
    current_day = datetime.strptime(date_str, "%Y-%m-%d").date()
    simulate_day(cur, customer_ids, products, current_day)
    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Данные за {date_str} сгенерированы.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Дата в формате YYYY-MM-DD")
    args = parser.parse_args()
    run_for_date(args.date)

