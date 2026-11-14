"""
stream/dispatcher.py

Zomato / Rapido-style food & ride dispatch simulator using RabbitMQ topic exchange.
Publishes a dispatch request every 2 seconds to the 'rapido-dispatch' topic exchange.

Requirements (Parts D2 & D3):
- Randomly picks city (Bangalore or Chennai), zone, and type (ride or food)
- Builds routing key: f"{city.lower()}.{zone.lower()}.{type_.lower()}"
- Publishes JSON message: {"id", "city", "zone", "type", "ts"}
- Every 10th message intentionally omits the "zone" field (D3 test for NACK)
"""

import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

import pika

EXCHANGE_NAME = "rapido-dispatch"
EXCHANGE_TYPE = "topic"

# Cities and matching zones for realistic routing
ZONES_BY_CITY = {
    "Bangalore": ["koramangala", "indiranagar", "whitefield"],
    "Chennai": ["tnagar", "whitefield"],
}
TYPES = ["food", "ride"]


def get_connection():
    """Connect using RABBITMQ_URL if set, else connect to localhost."""
    url = os.getenv("RABBITMQ_URL")
    if url:
        params = pika.URLParameters(url)
    else:
        params = pika.ConnectionParameters(host="localhost")
    return pika.BlockingConnection(params)


def run_dispatcher():
    print(f"[*] Starting dispatcher... publishing to exchange '{EXCHANGE_NAME}'")
    connection = get_connection()
    channel = connection.channel()

    # Declare topic exchange
    channel.exchange_declare(
        exchange=EXCHANGE_NAME,
        exchange_type=EXCHANGE_TYPE,
        durable=True,
    )

    message_count = 0

    try:
        while True:
            message_count += 1
            city = random.choice(list(ZONES_BY_CITY.keys()))
            zone = random.choice(ZONES_BY_CITY[city])
            order_type = random.choice(TYPES)

            # D3: Every 10th message is intentionally malformed (missing 'zone')
            is_bad_message = (message_count % 10 == 0)

            # Routing key: city.zone.type (lowercase)
            routing_key = f"{city.lower()}.{zone.lower()}.{order_type.lower()}"

            payload = {
                "id": str(uuid.uuid4())[:8],
                "city": city,
                "type": order_type,
                "ts": datetime.now(timezone.utc).isoformat(),
            }

            if is_bad_message:
                print(f"\n[!] Generating intentionally BAD message #{message_count} (missing 'zone')")
            else:
                payload["zone"] = zone

            message_body = json.dumps(payload)

            channel.basic_publish(
                exchange=EXCHANGE_NAME,
                routing_key=routing_key,
                body=message_body,
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Transient,
                    content_type="application/json",
                ),
            )

            status_tag = "BAD (NO ZONE)" if is_bad_message else "OK"
            print(
                f"[#{message_count:03d} {status_tag}] Published with key '{routing_key}': {message_body}"
            )

            time.sleep(2)

    except KeyboardInterrupt:
        print("\n[*] Dispatcher stopped by user.")
    finally:
        connection.close()


if __name__ == "__main__":
    run_dispatcher()
