import psycopg2
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from fpdf import FPDF
from datetime import datetime, timedelta
from pathlib import Path

DB_PARAMS = {
    "dbname": "xxxxx",
    "user": "xxxxx",
    "password": "xxxxx",
    "host": "xxxxx",
    "port": "xxxx",
}

def get_last_date_from_db():
    conn = psycopg2.connect(**DB_PARAMS)
    df = pd.read_sql_query("SELECT MAX(activity_date) AS last_date FROM all_activity;", conn)
    conn.close()
    last_date = df["last_date"].iloc[0]
    return last_date

END_DATE = get_last_date_from_db()
START_DATE = END_DATE - timedelta(days=6)
END_EXCL = END_DATE + timedelta(days=1)

OUTPUT_DIR = Path("/root/airflow/reports")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def _fill_missing_days(df, value_col):
    idx = pd.date_range(START_DATE, END_DATE, freq="D").date
    base = pd.DataFrame({"date": idx})
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    out = base.merge(df, on="date", how="left")
    out[value_col] = out[value_col].fillna(0).infer_objects(copy=False)
    return out

def fetch_data():
    conn = psycopg2.connect(**DB_PARAMS)
    params = {
        "start": START_DATE,
        "end": END_EXCL,
        "prev_start": START_DATE - timedelta(days=7),
        "prev_end": END_EXCL - timedelta(days=7)
    }

    queries = {
        "orders": """
            SELECT o.order_timestamp::date AS date, COUNT(*) AS orders
            FROM orders o
            WHERE o.order_timestamp >= %(start)s AND o.order_timestamp < %(end)s
            GROUP BY date ORDER BY date;
        """,
        "revenue": """
            SELECT o.order_timestamp::date AS date, SUM(p.price * oi.quantity) AS revenue
            FROM orders o
            JOIN order_items oi ON o.id = oi.order_id
            JOIN products p ON p.id = oi.product_id
            WHERE o.order_timestamp >= %(start)s AND o.order_timestamp < %(end)s
            GROUP BY date ORDER BY date;
        """,
        "wau": """
            SELECT COUNT(DISTINCT customer_id) AS wau
            FROM all_activity
            WHERE activity_date >= %(start)s AND activity_date < %(end)s;
        """,
        "prev_wau": """
            SELECT COUNT(DISTINCT customer_id) AS wau
            FROM all_activity
            WHERE activity_date >= %(prev_start)s AND activity_date < %(prev_end)s;
        """,
        "dau": """
            SELECT activity_date AS date, COUNT(DISTINCT customer_id) AS dau
            FROM all_activity
            WHERE activity_date >= %(start)s AND activity_date < %(end)s
            GROUP BY activity_date ORDER BY activity_date;
        """,
        "total_revenue": """
            SELECT COALESCE(SUM(p.price * oi.quantity), 0) AS total_revenue
            FROM orders o
            JOIN order_items oi ON o.id = oi.order_id
            JOIN products p ON p.id = oi.product_id
            WHERE o.order_timestamp >= %(start)s AND o.order_timestamp < %(end)s;
        """,
        "city_stats": """
            SELECT c.city,
                   COUNT(DISTINCT o.customer_id) AS active_users,
                   SUM(p.price * oi.quantity) AS revenue
            FROM orders o
            JOIN order_items oi ON o.id = oi.order_id
            JOIN products p ON p.id = oi.product_id
            JOIN customers c ON o.customer_id = c.id
            WHERE o.order_timestamp >= %(start)s AND o.order_timestamp < %(end)s
            GROUP BY c.city ORDER BY revenue DESC NULLS LAST;
        """
    }

    data = {k: pd.read_sql_query(q, conn, params=params) for k, q in queries.items()}
    conn.close()

    data["orders"]  = _fill_missing_days(data["orders"], "orders")
    data["revenue"] = _fill_missing_days(data["revenue"], "revenue")
    data["dau"]     = _fill_missing_days(data["dau"], "dau")
    return data

def create_charts(data):
    paths = {}
    plt.style.use("seaborn-v0_8")
    COLOR_PALETTE = ["#F2C811", "#FF8C00", "#E94E1B", "#3B3B3B"]

    def save_chart(df, x_col, y_col, title, fname, kind="line"):
        fig, ax = plt.subplots(figsize=(7, 4))
        if kind == "line":
            ax.plot(df[x_col], df[y_col], marker="o", color="#ff8800")
        elif kind == "bar":
            ax.bar(df[x_col], df[y_col], color="#ff8800")
        ax.set_title(title, fontsize=14)
        ax.grid(True, linestyle="--", alpha=0.6)
        plt.xticks(rotation=45)
        plt.tight_layout()
        path = Path(f"{fname}.png")
        plt.savefig(path, dpi=150)
        plt.close(fig)
        paths[fname] = str(path)


    save_chart(data["orders"], "date", "orders", "Orders (7d)", "orders_chart")
    save_chart(data["revenue"], "date", "revenue", "Revenue (7d)", "revenue_chart")
    save_chart(data["dau"], "date", "dau", "DAU (7d)", "dau_chart")
    save_chart(data["city_stats"], "city", "revenue", "Revenue by City (7d)", "city_chart", kind="bar")
    return paths

class PDF(FPDF):
    def __init__(self):
        super().__init__()
        font_path = Path("/airflow/fonts/DejaVuSans.ttf")
        self.add_font("Base", "", str(font_path), uni=True)
    
    def header(self):
        self.set_font("Base", "", 12)
        self.cell(0, 10, "Weekly Business Report", ln=True, align="C")
        self.ln(5)

    def add_kpi(self, title, value, change=None, change_color=None):
        self.set_font("Base", "", 12)
        self.cell(50, 8, f"{title}:", ln=0)
        self.set_font("Base", "", 12)
        self.cell(40, 8, str(value), ln=0)
        if change is not None:
            self.set_text_color(*change_color)
            self.cell(40, 8, change, ln=1)
            self.set_text_color(0, 0, 0)
        else:
            self.ln(8)

def create_pdf(data, charts):
    pdf = PDF()
    pdf.add_page()

    wau_now = int(data["wau"]["wau"].iloc[0]) if not data["wau"].empty else 0
    wau_prev = int(data["prev_wau"]["wau"].iloc[0]) if not data["prev_wau"].empty else 0
    diff_pct = ((wau_now - wau_prev) / wau_prev * 100) if wau_prev > 0 else 0

    if diff_pct > 0:
        arrow, color = "↑", (0, 150, 0)
    elif diff_pct < 0:
        arrow, color = "↓", (200, 0, 0)
    else:
        arrow, color = "→", (100, 100, 100)

    wau_change_text = f"{arrow} {abs(diff_pct):.1f}%"
    total_rev = float(data["total_revenue"]["total_revenue"].iloc[0])

    font_path = Path("/root/airflow/fonts/DejaVuSans.ttf")
    pdf.add_font("Base", "", str(font_path), uni=True)
    pdf.set_font("Base", "", 12)
    
    pdf.cell(0, 8, f"Period: {START_DATE} - {END_DATE}", ln=True)
    pdf.ln(5)

    pdf.add_kpi("WAU", wau_now, wau_change_text, color)
    pdf.add_kpi("Total Revenue", f"{total_rev:,.2f}")

    pdf.image(charts["orders_chart"], x=15, y=60, w=180)
    pdf.image(charts["revenue_chart"], x=15, y=170, w=180)

    font_path = Path("/root/airflow/fonts/DejaVuSans.ttf")
    pdf.add_font("Base", "", str(font_path), uni=True)
    pdf.set_font("Base", "", 12)
    
    pdf.add_page()
    pdf.image(charts["dau_chart"], x=15, y=30, w=180)
    pdf.image(charts["city_chart"], x=15, y=150, w=180)

    week_folder = OUTPUT_DIR / f"{START_DATE}_to_{END_DATE}"
    week_folder.mkdir(parents=True, exist_ok=True)
    filename = f"weekly_report_{START_DATE}_to_{END_DATE}.pdf"
    output_path = week_folder / filename

    pdf.output(str(output_path))
    log_report(START_DATE, END_DATE, str(output_path))
    print(f"✅ PDF saved to: {output_path}")

def log_report(start_date, end_date, file_path):
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO report_logs (start_date, end_date, file_path, generated_at)
        VALUES (%s, %s, %s, NOW())
    """, (start_date, end_date, file_path))
    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    data = fetch_data()
    charts = create_charts(data)
    create_pdf(data, charts)
