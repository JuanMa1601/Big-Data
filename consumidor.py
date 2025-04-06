# Importar SparkSession para crear la sesión de Spark
from pyspark.sql import SparkSession

# Importar funciones necesarias para procesar datos JSON
from pyspark.sql.functions import from_json, col

# Importar tipos de datos para definir el esquema del JSON
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# ============================================
# INICIALIZAR SPARK SESSION
# ============================================

# Crear la sesión de Spark para la app de análisis en tiempo real
spark = SparkSession.builder \
    .appName("COVID19_RealTime_Analysis") \
    .config("spark.ui.port", "4040") \
    .getOrCreate()

# ============================================
# DEFINIR EL ESQUEMA DE LOS MENSAJES JSON
# ============================================

# Esquema del JSON que llega desde Kafka
schema = StructType([
    StructField("ciudad", StringType()),
    StructField("edad", IntegerType()),
    StructField("sexo", StringType()),
    StructField("estado", StringType()),
    StructField("timestamp", StringType())
])

# ============================================
# LECTURA DEL STREAM DESDE KAFKA
# ============================================

# Leer mensajes del tópico 'analisis_covid_19'
df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "analisis_covid_19") \
    .load()

# ============================================
# PARSEAR LOS MENSAJES DESDE JSON
# ============================================

# Convertir el valor del mensaje (en formato JSON) a columnas con estructura
df_parsed = df_stream \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*")  # Extraer columnas individuales

# ============================================
# AGRUPAR Y CONTAR CASOS POR CIUDAD
# ============================================

# Agrupar por ciudad y contar cuántos casos han llegado por cada una
query = df_parsed.groupBy("ciudad") \
    .agg({"*": "count"}) \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()
