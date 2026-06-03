#!/bin/bash
# Esperar a que Kafka esté disponible
while ! nc -z kafka 9092; do
  echo "Esperando a Kafka..."
  sleep 1
done

echo "Kafka está listo. Creando tópicos..."

# Crear tópico principal (con 3 particiones para poder escalar a 3 consumidores)
kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic consultas-principal --partitions 3 --replication-factor 1

# Tópico de reintentos (también 3 particiones)
kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic consultas-reintento --partitions 3 --replication-factor 1

# Dead Letter Queue (1 partición es suficiente)
kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic consultas-dlq --partitions 1 --replication-factor 1

echo "Tópicos creados exitosamente."

# Para ejecutar localmente: chmod +x init-kafka.sh && ./init-kafka.sh