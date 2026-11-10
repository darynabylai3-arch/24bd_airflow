FROM apache/airflow:2.10.5-python3.12

# Каталог для сессии Telegram: создаём от имени airflow,
# чтобы именованный том унаследовал правильные права
RUN mkdir -p /opt/airflow/telegram_session

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
