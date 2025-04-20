FROM apache/airflow:2.8.1

# Optional: Install library tambahan jika kamu butuh
COPY ./requirements.txt /requirements.txt
RUN pip install -r /requirements.txt

# Salin DAGs, plugins, dll. jika ingin custom, tapi docker-compose sudah mount, jadi bisa di-skip
# COPY ./dags /opt/airflow/dags
# COPY ./plugins /opt/airflow/plugins

USER root
# Pastikan folder log bisa diakses jika dibutuhkan
RUN mkdir -p /opt/airflow/logs && chown -R airflow: /opt/airflow/logs

USER airflow
