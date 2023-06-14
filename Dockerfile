FROM apache/airflow:2.6.1
COPY requierements.txt /requierements.txt
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requierements.txt