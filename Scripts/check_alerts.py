import psycopg2
import pandas as pd
import requests
from datetime import datetime, timedelta
from pathlib import Path

DB_PARAMS = {
    "dbname": "xxxxxx",
    "user": "xxxxxx",
    "password": "xxxxxx",
    "host": "xxxxxx",
    "port": "xxxx",
}
TELEGRAM_TOKEN = "xxxxxx"
CHAT_ID = "xxxxxx"
LAST_DATE_FILE = Path("/last_date.txt")

def get_last_date():
    if LAST_DATE_FILE.exists():
        content = LAST_DATE_FILE.read_text().strip()
        if content:
            try:
                return datetime.strptime(content, "%Y-%m-%d").date()
            except ValueError:
                pass
    return datetime.now().date()

END_DATE = get_last_date()
START_DATE = END_DATE - timedelta(days=1)

def send_alert(message):
    url = f"https://api.telegram.org/bot[xxxxxx]/sendMessage"
    payload = {"chat_id": xxxxxx, "text": message, "parse_mode": "HTML"}
    requests.post(url, json=payload)

def log_alert(alert_date, metric, message, actual_value, threshold):
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    if hasattr(actual_value, "item"):
        actual_value = actual_value.item()
    if hasattr(threshold, "item"):
        threshold = threshold.item()
    cur.execute("""
        INSERT INTO alert_logs (alert_date, alert_type, alert_message, value, threshold, created_at)
        VALUES (%s, %s, %s, %s, %s, NOW())
    """, (alert_date, metric, message, actual_value, threshold))
    conn.commit()
    cur.close()
    conn.close()


def fetch_metrics():
    conn = psycopg2.connect(**DB_PARAMS)

    #DAU
    dau = pd.read_sql_query(f"""
        SELECT COUNT(DISTINCT customer_id) AS dau
        FROM all_activity
        WHERE activity_date = '{START_DATE}';
    """, conn)["dau"].iloc[0]

    #Conversion Rate (view -> order)
    conv_df = pd.read_sql_query(f"""
        WITH views AS (
            SELECT COUNT(DISTINCT id) AS view_users
            FROM views
            WHERE view_timestamp::date = '{START_DATE}'
        ),
        orders AS (
            SELECT COUNT(DISTINCT id) AS order_users
            FROM orders
            WHERE order_timestamp::date = '{START_DATE}'
        )
        SELECT (order_users::float / NULLIF(view_users, 0)) * 100 AS cr
        FROM views, orders;
    """, conn)
    cr = conv_df["cr"].iloc[0] or 0

    #Orders
    orders_df = pd.read_sql_query(f"""
        SELECT COUNT(*) AS orders
        FROM orders
        WHERE order_timestamp::date = '{START_DATE}';
    """, conn)
    orders = orders_df["orders"].iloc[0]

    #Revenue
    revenue_df = pd.read_sql_query(f"""
        SELECT COALESCE(SUM(p.price * oi.quantity), 0) AS revenue
        FROM orders o
        JOIN order_items oi ON o.id = oi.order_id
        JOIN products p ON p.id = oi.product_id
        WHERE o.order_timestamp::date = '{START_DATE}';
    """, conn)
    revenue = revenue_df["revenue"].iloc[0]

    conn.close()
    return dau, cr, orders, revenue

def check_thresholds(dau, cr, orders, revenue, START_DATE):
    alerts = []
    
    if dau < 1250:
        msg = f"⚠️ Низкий DAU: {dau}"
        alerts.append(msg)
        log_alert(START_DATE, "DAU", msg, dau, 1250)

    if cr < 0.8:
        msg = f"⚠️ Низкий коэффициент конверсии (просмотр → заказ): {cr:.2f}%"
        alerts.append(msg)
        log_alert(START_DATE, "Conversion Rate", msg, cr, 0.8)

    if orders < 60:
        msg = f"⚠️ Мало заказов: {orders}"
        alerts.append(msg)
        log_alert(START_DATE, "Orders", msg, orders, 60)

    if revenue < 2500000:
        msg = f"⚠️ Низкая выручка: {revenue:.2f}"
        alerts.append(msg)
        log_alert(START_DATE, "Revenue", msg, revenue, 2500000)

    if alerts:
        message = f"🚨 АЛЕРТЫ за {START_DATE} 🚨\n{'-'*33}\n"
        message += "\n".join(alerts)
        send_alert(message)

if __name__ == "__main__":
    dau, cr, orders, revenue = fetch_metrics()
    check_thresholds(dau, cr, orders, revenue, START_DATE)
