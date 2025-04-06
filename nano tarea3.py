# Importar librerías necesarias
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col

# Inicializar la sesión de Spark
spark = SparkSession.builder.appName('Tarea3').getOrCreate()

# Definir la ruta del archivo .csv en HDFS
file_path = 'hdfs://localhost:9000/Tarea3/gt2j-8ykr.csv'

# Leer el archivo .csv desde HDFS
df = spark.read.format('csv') \
    .option('header', 'true') \
    .option('inferSchema', 'true') \
    .load(file_path)

# =====================================
# LIMPIEZA DE DATOS
# =====================================

# Remover filas con nulos en columnas clave
df_clean = df.dropna(subset=["edad", "sexo", "ciudad_municipio_nom", "fecha_diagnostico"])

# Convertir edad a entero
df_clean = df_clean.withColumn("edad", col("edad").cast("int"))

# Mostrar después de limpieza
df_clean.show(5)

# =====================================
# ANÁLISIS EXPLORATORIO DE DATOS (EDA)
# =====================================

# 1. Casos por departamento
print("Número de casos por departamento:")
df_clean.groupBy("departamento_nom").count().orderBy(F.col("count").desc()).show()

# 2. Casos por estado (leve, fallecido, moderado, etc.)
print("Casos según el estado del paciente:")
df_clean.groupBy("estado").count().orderBy(F.col("count").desc()).show()

# 3. Casos por sexo
print("Distribución por sexo:")
df_clean.groupBy("sexo").count().show()

# 4. Casos con edad mayor a 80 años
print("Casos con edad mayor a 80:")
df_clean.filter(col("edad") > 80).select("fecha_diagnostico", "edad", "sexo", "estado").show()

# 5. Promedio de edad por tipo de recuperación
print("Promedio de edad por tipo de recuperación:")
df_clean.groupBy("tipo_recuperacion").agg(F.avg("edad").alias("promedio_edad")).show()

# 6. Casos por fecha de diagnóstico (los 10 días con más casos)
print("Top 10 fechas con más casos reportados:")
df_clean.groupBy("fecha_diagnostico").count().orderBy(F.col("count").desc()).show(10)

# =====================================
# GUARDAR RESULTADOS PROCESADOS
# =====================================

# Guardar resultados limpios como CSV en HDFS
df_clean.write.mode('overwrite').option('header', 'true') \
    .csv("hdfs://localhost:9000/Tarea3/resultado_procesado")

print("¡Procesamiento en batch completado y resultados almacenados!")
