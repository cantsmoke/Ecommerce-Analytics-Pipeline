from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from datetime import datetime, timedelta
from pathlib import Path
import subprocess

LAST_DATE_FILE = Path("/last_date.txt")
START_DATE = datetime(2025, 6, 9).date()

def get_last_date():
    if LAST_DATE_FILE.exists():
        content = LAST_DATE_FILE.read_text().strip()
        if content:
            try:
                return datetime.strptime(content, "%Y-%m-%d").date()
            except ValueError:
                return START_DATE
    return START_DATE

def save_last_date(date):
    LAST_DATE_FILE.write_text(date.strftime("%Y-%m-%d"))

def generate_one_day():
    """Генерация одного дня данных"""
    current_date = get_last_date()
    subprocess.run([
        "python",
        "/airflow/dags/studioStore_dynamicData.py",
        "--date", str(current_date)
    ], check=True)
    save_last_date(current_date + timedelta(days=1))

def check_alerts():
    """Проверка метрик за последний день и отправка алертов в TG"""
    subprocess.run([
        "python",
        "/airflow/dags/check_alerts.py"
    ], check=True)

def should_generate_report():
    """Вернёт True, если сегодня конец недели (7-й день цикла)"""
    current_date = get_last_date()
    return (current_date - START_DATE).days % 7 == 0

def generate_weekly_report():
    subprocess.run([
        "python",
        "/root/airflow/dags/generate_weekly_report.py"
    ], check=True)

default_args = {
    'owner': 'arseny',
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

with DAG(
    dag_id='generate_data_alerts_and_weekly_report',
    default_args=default_args,
    description='Генерация 1 дня данных, алерты и раз в неделю отчёт',
    schedule='* * * * *',
    start_date=datetime(2025, 6, 9),
    catchup=False,
) as dag:

    gen_data = PythonOperator(
        task_id='generate_day_data',
        python_callable=generate_one_day
    )

    alerts = PythonOperator(
        task_id='check_alerts',
        python_callable=check_alerts
    )

    check_week_end = ShortCircuitOperator(
        task_id='is_week_end',
        python_callable=should_generate_report
    )

    gen_report = PythonOperator(
        task_id='generate_weekly_report',
        python_callable=generate_weekly_report
    )

    gen_data >> alerts >> check_week_end >> gen_report

