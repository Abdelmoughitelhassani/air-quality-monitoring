# Dockerfile pour Spark avec Python
FROM apache/spark:3.5.3-scala2.12-java17-ubuntu

USER root

# Installer Python et pip
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-venv \
    && ln -sf /usr/bin/python3 /usr/bin/python \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Installer PySpark et dépendances
RUN pip3 install --no-cache-dir \
    pyspark==3.5.3 \
    pandas \
    numpy

# Définir les variables d'environnement pour PySpark
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

WORKDIR /app