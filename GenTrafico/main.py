import time
import json
import uuid
import os
import signal
import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from src.generator import TrafficGenerator

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
METRICS_URL = os.getenv('METRICS_URL', 'http://metrics:8002')
TOPIC_MAIN = 'consultas-principal'

# Traffic control
# 0 = run forever, N = stop after N messages
TOTAL_MESSAGES = int(os.getenv('TOTAL_MESSAGES', '0'))
# Normal publishing rate in messages/second
TRAFFIC_RATE = float(os.getenv('TRAFFIC_RATE', '20'))

# Burst mode: periodically flood to build backlog and test recovery
# Set BURST_RATE > TRAFFIC_RATE to enable (e.g. BURST_RATE=200)
BURST_RATE = float(os.getenv('BURST_RATE', '0'))
BURST_DURATION = float(os.getenv('BURST_DURATION', '30'))    # seconds of burst
BURST_INTERVAL = float(os.getenv('BURST_INTERVAL', '120'))   # seconds between bursts

MODE = os.getenv('MODE', 'uniform')  # uniform or zipf

gen = TrafficGenerator(mode=MODE)

# Graceful shutdown on SIGTERM / Ctrl-C
running = True
def _stop(sig, frame):
    global running
    running = False
    print("\nSeñal de parada recibida. Cerrando...", flush=True)

signal.signal(signal.SIGTERM, _stop)
signal.signal(signal.SIGINT, _stop)


def build_kafka_message(consulta_original):
    return {
        'id': str(uuid.uuid4()),
        'timestamp_creacion': time.time(),
        'intentos': 0,
        'datos': consulta_original
    }


def create_producer(max_attempts=15, delay=5):
    for attempt in range(1, max_attempts + 1):
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                retries=5,
            )
        except NoBrokersAvailable:
            print(f"Kafka no disponible, intento {attempt}/{max_attempts}. Reintentando en {delay}s...", flush=True)
            time.sleep(delay)
    raise RuntimeError("No se pudo conectar a Kafka tras múltiples intentos.")


if __name__ == "__main__":
    if TRAFFIC_RATE <= 0:
        raise ValueError(f"TRAFFIC_RATE debe ser > 0, recibido: {TRAFFIC_RATE}")

    burst_enabled = BURST_RATE > 0
    infinite = TOTAL_MESSAGES == 0

    print(f"Conectando a Kafka en {KAFKA_BOOTSTRAP_SERVERS}...", flush=True)
    producer = create_producer()
    print(
        f"Conectado. Tópico: {TOPIC_MAIN} | Modo: {MODE} | "
        f"Rate: {TRAFFIC_RATE} msg/s | "
        f"{'Infinito' if infinite else f'{TOTAL_MESSAGES} mensajes'} | "
        f"{'Burst: ' + str(BURST_RATE) + ' msg/s cada ' + str(BURST_INTERVAL) + 's' if burst_enabled else 'Sin burst'}",
        flush=True,
    )

    sent = 0
    errores = 0
    burst_phase_start = time.time()  # when the current phase (normal/burst) started
    in_burst = False

    while running and (infinite or sent < TOTAL_MESSAGES):
        now = time.time()

        # Switch between burst and normal phases
        if burst_enabled:
            phase_elapsed = now - burst_phase_start
            if in_burst and phase_elapsed >= BURST_DURATION:
                in_burst = False
                burst_phase_start = now
                print(f"[Burst OFF] Volviendo a {TRAFFIC_RATE} msg/s", flush=True)
            elif not in_burst and phase_elapsed >= BURST_INTERVAL:
                in_burst = True
                burst_phase_start = now
                print(f"[Burst ON]  Subiendo a {BURST_RATE} msg/s por {BURST_DURATION}s", flush=True)

        current_rate = BURST_RATE if (burst_enabled and in_burst) else TRAFFIC_RATE
        target_interval = 1.0 / current_rate

        consulta_original = gen.generate()
        mensaje_kafka = build_kafka_message(consulta_original)

        send_start = time.time()
        try:
            future = producer.send(TOPIC_MAIN, value=mensaje_kafka)
            future.get(timeout=10)
            sent += 1
            if sent % 100 == 0:
                print(
                    f"[{sent}] {'BURST' if in_burst else 'normal'} "
                    f"{current_rate} msg/s | errores={errores}",
                    flush=True,
                )
        except Exception as e:
            print(f"Error enviando mensaje {sent + 1}: {e}", flush=True)
            errores += 1

        elapsed = time.time() - send_start
        time.sleep(max(0.0, target_interval - elapsed))

    producer.flush()
    print(f"Generador detenido. Enviados: {sent} | Errores: {errores}", flush=True)

    # Wait for the consumer to finish processing all messages before snapshotting
    if sent > 0:
        print(f"Esperando que el consumidor procese los {sent} mensajes...", flush=True)
        max_wait = 600  # 10 min ceiling
        waited = 0
        while waited < max_wait:
            try:
                resp = requests.get(f"{METRICS_URL}/kafka_stats", timeout=5).json()
                processed = resp.get("total_processed", 0)
                if processed >= sent:
                    print(f"Consumidor listo: {processed}/{sent} procesados.", flush=True)
                    break
                print(f"Procesados {processed}/{sent}... siguiente revisión en 15s", flush=True)
            except Exception as e:
                print(f"Error consultando métricas: {e}", flush=True)
            time.sleep(15)
            waited += 15
        if waited >= max_wait:
            print("Timeout esperando al consumidor. Guardando snapshot parcial.", flush=True)

    try:
        requests.post(f"{METRICS_URL}/snapshot", timeout=5)
        print("Snapshot de métricas guardado.", flush=True)
    except Exception as e:
        print(f"No se pudo guardar snapshot: {e}", flush=True)
