FROM apache/airflow:2.9.1-python3.12

USER airflow

# Instalamos apenas as bibliotecas extras que o projeto precisa.
# O Airflow já vem com centenas de bibliotecas (incluindo as dele mesmo).
RUN pip install requests tzdata
