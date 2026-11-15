"""
stream/driver.py
Driver consumer script with manual ACK / NACK handling.
"""

import json
import os
import sys
import pika

EXCHANGE_NAME = "rapido-dispatch"
EXCHANGE_TYPE = "topic"


def get_connection():
    url = os.getenv("RABBITMQ_URL")
    if url:
        return pika.BlockingConnection(pika.URLParameters(url))
    return pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))


def main():
    if len(sys.argv) < 3:
        print("Usage: python driver.py <NAME> <PATTERN>", flush=True)
        sys.exit(1)

    driver_name = sys.argv[1]
    binding_pattern = sys.argv[2]

    try:
        connection = get_connection()
        channel = connection.channel()

        channel.exchange_declare(
            exchange=EXCHANGE_NAME,
            exchange_type=EXCHANGE_TYPE,
            durable=True,
        )

        queue_name = f"queue_{driver_name.lower()}"
        channel.queue_declare(queue=queue_name, exclusive=False, auto_delete=True)

        channel.queue_bind(
            exchange=EXCHANGE_NAME,
            queue=queue_name,
            routing_key=binding_pattern,
        )

        print(f"[{driver_name}] Listening for orders matching '{binding_pattern}' on queue '{queue_name}'...", flush=True)

        def callback(ch, method, properties, body):
            try:
                data = json.loads(body.decode("utf-8"))
            except Exception as e:
                print(f"[{driver_name}] [MALFORMED JSON] Error decoding: {e}", flush=True)
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                return

            order_id = data.get("id")
            city = data.get("city")
            zone = data.get("zone")
            order_type = data.get("type")

            # D3: Reject message missing 'zone' with requeue=False
            if not zone:
                print(
                    f"[{driver_name}] [REJECTED - NACK] Order #{order_id} missing 'zone'! "
                    f"Key: '{method.routing_key}'. Dropping (requeue=False).",
                    flush=True
                )
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            else:
                print(
                    f"[{driver_name}] [ACCEPTED - ACK] Order #{order_id} | "
                    f"{city} / {zone} / {order_type} | Key: '{method.routing_key}'",
                    flush=True
                )
                ch.basic_ack(delivery_tag=method.delivery_tag)

        channel.basic_consume(
            queue=queue_name,
            on_message_callback=callback,
            auto_ack=False,
        )

        channel.start_consuming()

    except KeyboardInterrupt:
        print(f"[{driver_name}] Stopped.", flush=True)
    except Exception as e:
        print(f"[{driver_name}] ERROR: {e}", flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
