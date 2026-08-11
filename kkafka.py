import asyncio
import copy
import datetime
import json
import os
import csv
from pprint import pprint
from uuid import uuid4

from aiokafka import AIOKafkaProducer
from logging import getLogger

from __ops.infratest.prod_t1.dto import Attachment

logger = getLogger(__name__)

MAX_LAG = 3
MAX_MESSAGES = 18 # 1502
LAG_CONTROL = False

class KafkaProducer:
    """Продюсер подключается/отключается к брокеру, генерирует и отправляет сообщения"""
    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
        self.producer = None

    async def init_producer(self):
        self.producer = AIOKafkaProducer(bootstrap_servers=self.bootstrap_servers)

    async def start(self):
        if not self.producer:
            await self.init_producer()
        await self.producer.start()  # Подключаемся к Kafka

    async def stop(self):
        if self.producer is not None:
            await self.producer.stop()

    async def send_and_wait(
            self,
            topic=None,
            value=None,
            key=None,
            partition=None,
            timestamp_ms=None,
    ):
        try:
            await self.producer.send_and_wait(
                topic=topic,
                value=value,
                key=key,
                partition=partition,
                timestamp_ms=timestamp_ms,
            )
        except Exception as e:
            logger.error(f"KafkaProducer sending error: {e}")


def read_csv(csv_file_name) -> dict:
    path = os.path.join(os.getcwd(), csv_file_name)
    print(path)
    messages_ = dict()

    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=';')

        for row in reader:
            offset = int(row['offset'])
            timestamp = int(row['timestamp'])
            partition = int(row['partition'])
            msg_key = row['key'].encode('utf-8')
            k = timestamp, partition, offset, msg_key
            # messages_[k] = json.loads(row['value'])
            data = json.loads(row['value'])
            try:
                data['fields']['spaceId'] = '8a872f29-369f-46a7-a15c-762c927e8a92'
                value = json.dumps(data).encode('utf-8')
                # print(json.loads(row['value']).get('fields', {}).)
                messages_[k] = value
            except Exception as e:
                print(e)


    return messages_

# from kafka import KafkaAdminClient, KafkaConsumer
# from kafka.structs import TopicPartition
#
# def get_lag_simple(bootstrap_servers, group_id, topic):
#     """Самый простой способ без ребалансировки"""
#
#     admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
#
#     # Временный consumer БЕЗ group_id для получения end offsets
#     consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers)
#
#     try:
#         partitions = consumer.partitions_for_topic(topic)
#         topic_partitions = [TopicPartition(topic, p) for p in partitions]
#
#         # Получаем committed offsets через Admin API (не вызывает rebalance)
#         group_offsets = admin.list_consumer_group_offsets(group_id)
#
#         # End offsets
#         end_offsets = consumer.end_offsets(topic_partitions)
#
#         total_lag = 0
#         for tp in topic_partitions:
#             committed = group_offsets.get(tp)
#             committed_offset = committed.offset if committed else 0
#             end_offset = end_offsets[tp]
#
#             lag = end_offset - committed_offset
#             total_lag += max(0, lag)
#
#         return total_lag
#
#     finally:
#         consumer.close()
#         admin.close()

from kafka import KafkaAdminClient, KafkaConsumer
from kafka.structs import TopicPartition


def get_effective_lag(bootstrap_servers, group_id, topic):
    """
    Эффективный расчет реального lag с учетом retention.
    Если committed offset < beginning offset - считаем lag от начала доступных данных.
    """
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers)

    try:
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            return 0

        topic_partitions = [TopicPartition(topic, p) for p in partitions]

        # Получаем все оффсеты одним запросом
        group_offsets = admin.list_consumer_group_offsets(group_id)
        beginning_offsets = consumer.beginning_offsets(topic_partitions)
        end_offsets = consumer.end_offsets(topic_partitions)

        total_lag = 0
        for tp in topic_partitions:
            end_offset = end_offsets[tp]
            beginning_offset = beginning_offsets[tp]

            # Получаем committed offset или используем beginning если нет коммита
            committed_meta = group_offsets.get(tp)
            if committed_meta is None:
                # Нет committed offset - считаем от начала
                committed_offset = beginning_offset
            else:
                committed_offset = committed_meta.offset
                # Если committed меньше beginning - данные удалены, считаем от beginning
                if committed_offset < beginning_offset:
                    committed_offset = beginning_offset

            lag = end_offset - committed_offset
            total_lag += max(0, lag)

        return total_lag

    finally:
        consumer.close()
        admin.close()

# Использование в скрипте нагрузки
# from kafka import KafkaProducer
# import time
#
#
# def send_with_lag_control(bootstrap_servers, group_id, topic, messages, max_lag=500):
#     producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
#
#     for i, message in enumerate(messages):
#         # Проверяем каждые 100 сообщений
#         if i % 100 == 0:
#             lag = get_effective_lag(bootstrap_servers, group_id, topic)
#
#             while lag > max_lag:
#                 print(f"Lag: {lag}, ожидание...")
#                 time.sleep(5)
#                 lag = get_effective_lag(bootstrap_servers, group_id, topic)
#
#         producer.send(topic, message)
#
#     producer.flush()
#     producer.close()


# # Запуск
# messages = [f'message_{i}'.encode() for i in range(5000)]
# send_with_lag_control(
#     bootstrap_servers=['localhost:9092'],
#     group_id='your-consumer-group',
#     topic='your-topic',
#     messages=messages,
#     max_lag=500
# )

async def main(csv_file_name, kafka_bootstrap, topic, cons_group_id):
    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap,
    )
    await producer.start()

    messages = read_csv(csv_file_name)
    # print(messages)
    # eco = [
    #     522, 524, 525, 3902, 3899, 3911, 548, 529, 3898,
    #     3900, 3895, 3901, 3905, 3894, 3915, 3922, 3929,
    #     3907, 550, 523, 528, 551, 547, 3916, 521, 3919,
    #     3909, 549, 543, 545, 3908, 519, 544, 546, 527
    # ]
    max_lag = MAX_LAG
    check_interval = 1
    count = 0

    sorted_keys = sorted(list(messages.keys()))
    while count < MAX_MESSAGES:
        for key in sorted_keys:
            timestamp, partition, offset, msg_key = key

            # print(key, type(messages[key]), key[2])
            timestamp = int(datetime.datetime.utcnow().timestamp())
            # if count < 11:
            #     count += 1
            #     continue

            print(msg_key)
            if msg_key != '71094be5-361f-4517-a6eb-2bb9bd093b75'.encode('utf-8'):
                count += 1
                continue

            if True:
                msg_dct = json.loads(messages[key].decode("utf-8"))

                if LAG_CONTROL:
                    lag = get_effective_lag(
                        bootstrap_servers=kafka_bootstrap,
                        group_id=cons_group_id,
                        topic=topic
                    )

                    if lag > max_lag:
                        print(f"lag {lag}, waiting ", end='', flush=True)

                    while lag > max_lag:
                        print(f".", end='', flush=True)
                        await asyncio.sleep(check_interval)
                        lag = get_effective_lag(bootstrap_servers=kafka_bootstrap, group_id=cons_group_id,
                                                topic=topic)
                    print()

                if True:
                    if count >= 0:
                        await producer.send_and_wait(
                            topic=topic,
                            value=json.dumps(msg_dct).encode("utf-8"),
                            key=msg_key,
                            partition=partition,
                            timestamp_ms=timestamp,
                        )
                        print(f"{count}: {msg_dct}")
                        await asyncio.sleep(20)
                    count += 1
                    if count == MAX_MESSAGES:
                        break

                # continue
                # break
                # if msg_dct.get('contentFileId'):
                #     for att in msg_dct.get('attachments'):
                #         new_msg_dct = copy.deepcopy(msg_dct)
                #
                #         # new_key = str(uuid4())
                #         new_key = att['id']
                #
                #         new_msg_dct["primaryKey"] = new_key
                #         new_msg_dct['attachments'] = [att]
                #         print(count, new_key, att)
                #
                #
                #         if LAG_CONTROL:
                #             lag = get_effective_lag(
                #                 bootstrap_servers=kafka_bootstrap,
                #                 group_id=cons_group_id,
                #                 topic=topic
                #             )
                #
                #             if lag > max_lag:
                #                 print(f"lag {lag}, waiting ", end='', flush=True)
                #
                #             while lag > max_lag:
                #                 print(f".", end='', flush=True)
                #                 await asyncio.sleep(check_interval)
                #                 lag = get_effective_lag(bootstrap_servers=kafka_bootstrap, group_id=cons_group_id,
                #                                         topic=topic)
                #             print()
                #
                #         if True:
                #             await producer.send_and_wait(
                #                 topic=topic,
                #                 value=json.dumps(new_msg_dct).encode("utf-8"),
                #                 key=new_key.encode("utf-8"),
                #                 partition=partition,
                #                 timestamp_ms=timestamp,
                #             )
                #
                #             count += 1


                # pprint(json.loads(messages[key].decode("utf-8")))
                # break
            # if key[2] > 50:
            #     break
            # else:
            #     # print(key, type(messages[key]), key[2])
            #     # timestamp, partition, offset, msg_key = key
            #     # break
            #     # if any([int(offset) in eco, count in eco, (count + 1) in eco, True]):
            #     lag = get_effective_lag(bootstrap_servers=kafka_bootstrap, group_id=cons_group_id, topic=topic)
            #     if lag > max_lag:
            #         print(f"lag {lag}, waiting ", end='', flush=True)
            #     while lag > max_lag:
            #         print(f".", end='', flush=True)
            #         await asyncio.sleep(check_interval)
            #         lag = get_effective_lag(bootstrap_servers=kafka_bootstrap, group_id=cons_group_id, topic=topic)
            #     print()
            #     if True:
            #         await producer.send_and_wait(
            #             topic=topic,
            #             value=messages[key],
            #             key=msg_key,
            #             partition=partition,
            #             timestamp_ms=timestamp,
            #         )
            #
            #         count += 1
            # await asyncio.sleep(10)
            # break
    # if count > 10:
    #     break
    await producer.stop()
    print(f"TOTAL MESSAGES: {count}")

if __name__ == '__main__':

    csv_file = 'export-lt-all-types-1.csv'


    # local kafka
    kafka_bootstrap_servers = 'localhost:9092'

    group_id = "etl-consumer-test-1070"


    kafka_topic = "intel-assist-index-test-etl-11"



    # group_id = "etl-consumer-test-200"

    asyncio.run(main(csv_file, kafka_bootstrap_servers, kafka_topic, group_id))

