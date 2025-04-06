# Importar librerías necesarias
from kafka import KafkaProducer
import json
import random
import time

# ============================================
# CONFIGURACIÓN DEL PRODUCTOR KAFKA
# ============================================

# Crear un productor de Kafka que envíe datos en formato JSON
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Dirección del broker de Kafka
    value_serializer=lambda x: json.dumps(x).encode('utf-8')  # Convertir dict a JSON y luego a bytes
)
# ============================================
# DATOS SIMULADOS
# ============================================

# Listado de posibles ciudades
ciudades = ["Bogotá", "Medellín", "Cali", "Barranquilla", "Cartagena"]

# Listado de posibles estados del paciente
estados = ["Leve", "Moderado", "Grave", "Fallecido"]

# ============================================
# BUCLE PARA ENVIAR MENSAJES CONTINUAMENTE
# ============================================

while True:
    # Generar un registro aleatorio (simulación)
    registro = {
        "ciudad": random.choice(ciudades),
        "edad": random.randint(1, 90),
        "sexo": random.choice(["M", "F"]),
        "estado": random.choice(estados),
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")  # Fecha y hora actuales
    }

    # Enviar el registro al tópico 'analisis_covid_19'
    producer.send('analisis_covid_19', registro)

    # Mostrar en consola el registro enviado
    print(f"Enviado: {registro}")

    # Esperar entre 0.5 y 2 segundos antes de enviar el siguiente
    time.sleep(random.uniform(0.5, 2))
