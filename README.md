Análisis de Datos COVID-19 en Tiempo Real con Spark Streaming y Kafka

Este repositorio contiene una solución de análisis de datos en tiempo real utilizando Apache Kafka y Apache Spark. Se simulan datos relacionados con casos de COVID-19 y se procesan tanto en **tiempo real (streaming)** como en **modo batch**, permitiendo visualizar y analizar información clave.

•	Fuente: https://www.datos.gov.co/resource/gt2j-8ykr.csv

---

## Tecnologías utilizadas

- Apache Kafka 
- Apache Spark Streaming 
- Python 3
- HDFS (modo batch)
- PySpark
- JSON, CSV

---

## Descripción general del sistema

El sistema está compuesto por tres componentes:

1. **Productor Kafka (`producer.py`)**  
   Simula el envío de registros JSON con información de pacientes COVID-19 a un tópico de Kafka.

2. **Consumidor Spark Streaming (`consumer.py`)**  
   Consume datos desde Kafka y realiza procesamiento en tiempo real con PySpark.

3. **Procesamiento Batch (`batch_processing.py`)**  
   Carga un archivo CSV desde HDFS, realiza limpieza de datos y un análisis exploratorio con PySpark.

## Requisitos

- Kafka y Zookeeper instalados y corriendo
- Apache Spark configurado
- Python 3 y pip
- HDFS (para procesamiento batch)

Antes de comenzar, asegúrate de tener instalados y configurados correctamente los siguientes componentes:
1. Apache Kafka y Zookeeper
Kafka depende de Zookeeper, por lo tanto, asegúrate de tener ambos instalados:
•	Descarga Apache Kafka: https://kafka.apache.org/downloads
•	Extrae el archivo y entra a la carpeta del proyecto:
tar -xzf kafka_2.12-3.7.2.tgz
cd kafka_2.12-3.7.2

2. Apache Spark
•	Descarga Apache Spark con soporte para Hadoop: https://spark.apache.org/downloads
•	Asegúrate de tener Java instalado y configurado (JAVA_HOME).
•	Configura Spark para PySpark:
export SPARK_HOME=~/spark
export PATH=$SPARK_HOME/bin:$PATH
export PYSPARK_PYTHON=python3

3. Python y dependencias
•	Python 3.7 o superior.
•	Instalar dependencias del proyecto:
pip install
apt install -y python3-pip

## Paso a paso para la ejecución

1.	Iniciar Zookeeper
Zookeeper es necesario para que Kafka funcione correctamente.
2.	Iniciar el servidor Kafka
En una nueva terminal, dentro de la misma carpeta de Kafka
3.	Crear el tópico de Kafka
4.	Ejecutar el productor de datos
python producer.py
5.	Ejecutar el consumidor Spark Streaming
spark-submit consumer.py

Procesamiento Batch con PySpark
Este script carga un archivo CSV desde HDFS, lo transforma y muestra análisis como promedio de edad, casos por departamento, etc.
1.	Subir archivo CSV a HDFS
hdfs dfs -mkdir -p /Tarea3
hdfs dfs -put gt2j-8ykr.csv /Tarea3/

