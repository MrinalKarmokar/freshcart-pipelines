"""
stream/driver.py

Driver consumer script for RabbitMQ topic exchange.
Usage:
    python driver.py <NAME> <PATTERN>

Examples:
    python driver.py Ravi "bangalore.koramangala.*"
    python driver.py Priya "bangalore.indiranagar.*"
    python driver.py Arjun "bangalore.*.ride"
    python driver.py Meena "bangalore.*.food"
    python driver.py Karthik "chennai.tnagar.food"
    python driver.py Control "bangalore.#"

Requirements (Parts D2 & D3):
- Binds a dedicated private queue to 'rapido-dispatch' with PATTERN
- Prints each received message
- D3: If 'zone' field is missing or None -> rejects with basic_nack(requeue=False)
- D3: If message is valid -> acknowledges with basic_ack()
"""

import json
import os
import sys
import pika

EXCHANGE_NAME = "rapido-dispatch"
EXCHANGE_TYPE = "topic"


def get_connection():
    """Connect using RABBITMQ_URL if set, else connect to localhost."""
    url = os.getenv("RABBITMQ_URL")
    if url:
        params = pika.URLParameters(url)
    else:
        params = pika.ConnectionParameters(host="localhost")
    return pika.BlockingConnection(params)


def main():
    if len(sys.argv) < 3:
        print("Usage: python driver.py <NAME> <PATTERN>")
        print("Example: python driver.py Ravi 'bangalore.koramangala.*'")
        sys.exit(1)

    driver_name = sys.argv[1]
    binding_pattern = sys.argv[2]

    connection = get_connection()
    channel = connection.channel()

    # Ensure exchange exists
    channel.exchange_declare(
        exchange=EXCHANGE_NAME,
        exchange_type=EXCHANGE_TYPE,
        durable=True,
    )

    # Declare a private, exclusive queue for this driver
    queue_name = f"queue_{driver_name.lower()}"
    channel.queue_declare(queue=queue_name, exclusive=False, auto_delete=True)

    # Bind queue to topic exchange with driver's binding pattern
    channel.queue_bind(
        exchange=EXCHANGE_NAME,
        queue=queue_name,
        routing_key=binding_pattern,
    )

    print(f"[{driver_name}] Listening for orders matching '{binding_pattern}' on queue '{queue_name}'...")
    print(f"[{driver_name}] Press CTRL+C to exit.\n")

    def callback(ch, method, properties, body):
        try:
            data = json.loads(body.decode("utf-8"))
        except Exception as e:
            print(f"[{driver_name}] [MALFORMED JSON] Error decoding: {e}. NACK (requeue=False).")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        order_id = data.get("id")
        city = data.get("city")
        zone = data.get("zone")
        order_type = data.get("type")

        # -------------------------------------------------------------
        # D3 Logic: Check if 'zone' is missing
        # -------------------------------------------------------------
        if not zone:
            print(
                f"[{driver_name}] [REJECTED - NACK] Message #{order_id} missing 'zone'! "
                f"routing_key='{method.routing_key}'. Dropping message (requeue=False)."
            )
            # D3: Reject without requeue
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        else:
            print(
                f"[{driver_name}] [ACCEPTED - ACK] Order #{order_id} | "
                f"City: {city} | Zone: {zone} | Type: {order_type} | Key: '{method.routing_key}'"
            )
            # D3: Acknowledge valid message
            ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(
        queue=queue_name,
        on_message_callback=callback,
        auto_ack=False,  # Manual ACK/NACK required
    )

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print(f"\n[{driver_name}] Stopping consumer...")
        channel.stop_consuming()
    finally:
        connection.close()


if __name__ == "__main__":
    main()
