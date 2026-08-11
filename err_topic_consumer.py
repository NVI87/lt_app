# consumer.py
import asyncio
import json
import os
from pathlib import Path

from aiokafka import AIOKafkaConsumer


KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "json-file-writer")
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "intel-assist-index-errors-loc"
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "./err_events"))

async def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
    )

    await consumer.start()
    try:
        async for message in consumer:
            filename = (
                f"{message.topic}-"
                f"{message.partition}-"
                f"{message.offset}.json"
            )
            target = OUTPUT_DIR / filename
            temporary = target.with_suffix(".tmp")

            # Атомарная запись: сначала tmp, затем rename.
            with temporary.open("w", encoding="utf-8") as f:
                json.dump(
                    message.value,
                    f,
                    ensure_ascii=False,
                    indent=2,
                )
                f.write("\n")

            temporary.replace(target)

            # Коммитим offset только после записи файла.
            await consumer.commit()

            print(f"Saved: {target}")

    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())