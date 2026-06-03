import os
import json
import time
import requests
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
CACHE_URL = os.getenv('CACHE_URL', 'http://cache:8000')
METRICS_URL = os.getenv('METRICS_URL', 'http://metrics:8002')
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))

TOPIC_MAIN = 'consultas-principal'
TOPIC_RETRY = 'consultas-reintento'
TOPIC_DLQ = 'consultas-dlq'

# Consecutive NEW-message failures (TOPIC_MAIN only) before declaring a system failure event.
# Retried messages from TOPIC_RETRY do not count toward this streak so successes
# from the retry queue don't mask an ongoing outage on the main path.
FAILURE_THRESHOLD = int(os.getenv('FAILURE_THRESHOLD', '3'))
# Base for exponential backoff: espera = RETRY_BASE ^ intentos
# 2 → 2s/4s/8s (realistic), 0 → no sleep (fast tests)
RETRY_BASE = float(os.getenv('RETRY_BASE', '1'))
# Report Kafka consumer lag to metrics every N messages processed
BACKLOG_REPORT_EVERY = int(os.getenv('BACKLOG_REPORT_EVERY', '5'))


def create_producer(max_attempts=15, delay=5):
    for attempt in range(1, max_attempts + 1):
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
        except NoBrokersAvailable:
            print(f"Kafka no disponible (productor), intento {attempt}/{max_attempts}...", flush=True)
            time.sleep(delay)
    raise RuntimeError("No se pudo conectar al productor Kafka.")


def process_query(datos):
    """Envía la consulta al sistema caché. Retorna (cache_hit, resultado)."""
    resp = requests.post(f"{CACHE_URL}/query", json=datos, timeout=10)
    if resp.status_code != 200:
        raise Exception(f"Cache devolvió {resp.status_code}: {resp.text[:200]}")
    data = resp.json()
    hit = data.get('source') == 'cache'
    return hit, data.get('result')


def report_metric(consulta_id, exito, latencia, reintentos_usados, topico_origen):
    try:
        requests.post(
            f"{METRICS_URL}/metric",
            json={
                'consulta_id': consulta_id,
                'exito': exito,
                'latencia': latencia,
                'reintentos_usados': reintentos_usados,
                'topico_origen': topico_origen,
            },
            timeout=1,
        )
    except Exception as e:
        print(f"Error reportando métrica para {consulta_id}: {e}", flush=True)


def report_backlog(consumer):
    """Read real Kafka consumer lag and send it to the metrics service."""
    try:
        partitions = consumer.assignment()
        if not partitions:
            return
        end_offsets = consumer.end_offsets(list(partitions))
        lag = sum(
            max(0, end_offsets.get(tp, 0) - consumer.position(tp))
            for tp in partitions
        )
        requests.post(
            f"{METRICS_URL}/backlog",
            json={"timestamp": time.time(), "backlog_size": lag},
            timeout=1,
        )
    except Exception as e:
        print(f"Error reportando backlog: {e}", flush=True)


def report_failure_event(event_type, backlog_at_failure=None):
    try:
        payload = {"event_type": event_type}
        if backlog_at_failure is not None:
            payload["backlog_at_failure"] = backlog_at_failure
        requests.post(f"{METRICS_URL}/failure_event", json=payload, timeout=1)
        print(f"[failure_event] {event_type}", flush=True)
    except Exception as e:
        print(f"Error reportando failure_event: {e}", flush=True)


def main():
    producer = create_producer()

    consumer = KafkaConsumer(
        TOPIC_MAIN, TOPIC_RETRY,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id='grupo-consumidores',
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    print("Consumidor Kafka iniciado. Esperando mensajes...", flush=True)

    failure_streak = 0
    in_failure_event = False
    messages_processed = 0

    for msg in consumer:
        consulta = msg.value
        topic = msg.topic

        messages_processed += 1
        if messages_processed % BACKLOG_REPORT_EVERY == 0:
            report_backlog(consumer)

        if topic == TOPIC_RETRY:
            espera = RETRY_BASE ** consulta.get('intentos', 1)
            print(f"Reintento para {consulta['id']}, esperando {espera:.1f}s", flush=True)
            time.sleep(espera)

        intentos = consulta.get('intentos', 0)
        datos = consulta.get('datos', {})

        try:
            hit, result = process_query(datos)
            latencia = time.time() - consulta['timestamp_creacion']

            # Only reset streak on TOPIC_MAIN successes — retry successes don't
            # mask an ongoing failure on the main ingestion path.
            if topic == TOPIC_MAIN:
                if in_failure_event:
                    report_failure_event("end")
                    in_failure_event = False
                    print(f"Sistema recuperado tras {failure_streak} fallos en principal.", flush=True)
                failure_streak = 0

            report_metric(consulta['id'], True, latencia, intentos, topic)
            print(
                f"OK {consulta['id']}: {'hit' if hit else 'miss'} "
                f"intentos={intentos} lat={latencia:.3f}s",
                flush=True,
            )
            consumer.commit()

        except Exception as e:
            # Only count failures on TOPIC_MAIN toward the outage streak
            if topic == TOPIC_MAIN:
                failure_streak += 1
            print(
                f"Fallo procesando {consulta['id']} "
                f"(intentos={intentos}, racha={failure_streak}): {e}",
                flush=True,
            )

            if failure_streak >= FAILURE_THRESHOLD and not in_failure_event:
                report_failure_event("start", backlog_at_failure=failure_streak)
                in_failure_event = True

            if intentos >= MAX_RETRIES:
                try:
                    producer.send(TOPIC_DLQ, value=consulta).get(timeout=10)
                    latencia = time.time() - consulta['timestamp_creacion']
                    report_metric(consulta['id'], False, latencia, intentos, topic)
                    print(f"DLQ {consulta['id']}: {intentos + 1} intentos totales", flush=True)
                    consumer.commit()
                except Exception as dlq_err:
                    print(f"Error enviando a DLQ {consulta['id']}: {dlq_err} — no se hace commit", flush=True)
            else:
                consulta['intentos'] = intentos + 1
                consulta['timestamp_reintento'] = time.time()
                try:
                    producer.send(TOPIC_RETRY, value=consulta).get(timeout=10)
                    print(f"Retry {consulta['id']}: intento {intentos + 1} programado", flush=True)
                    consumer.commit()
                except Exception as retry_err:
                    print(f"Error enviando a TOPIC_RETRY {consulta['id']}: {retry_err} — no se hace commit", flush=True)


if __name__ == '__main__':
    main()
