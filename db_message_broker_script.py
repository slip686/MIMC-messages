import datetime

from psycopg2.extensions import AsIs
import asyncio
import psycopg2
from threading import Thread
import pika
from pika.exceptions import AMQPError
import json


class MessagesBroker:
    def __init__(self):
        self.db_connection = None
        self.db_cursor = None
        self.mq_connection = None
        self.mq_channel_broker = None
        self.set_job_connections()

    def connect_to_db(self):
        self.db_connection = psycopg2.connect(user="server_messages_script",
                                              password="ddtlbnt yjdsq",
                                              host="exon-db.sliplab.net",
                                              port="5433",
                                              database="exon",
                                              keepalives=1,
                                              keepalives_idle=30)
        self.db_connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        self.db_cursor = self.db_connection.cursor()
        self.db_cursor.execute(f'LISTEN "send_message"')

    def connect_to_mq(self):
        credentials = pika.PlainCredentials('server_messages_script', 'qwe321qwe')
        parameters = pika.ConnectionParameters('messages.sliplab.net', 5672, '/', credentials,)

        self.mq_connection = pika.BlockingConnection(parameters)
        self.mq_channel_broker = self.mq_connection.channel()
        self.mq_channel_broker.queue_declare(queue="broker_msg_channel")

    def set_job_connections(self):
        if self.db_connection:
            self.db_connection.close()
            self.db_cursor = None
        if self.mq_connection:
            self.mq_connection.close()
            self.mq_channel_broker = None

        self.connect_to_mq()
        self.connect_to_db()

    def send_message(self, channel, message):
        while True:
            try:
                print(f'{datetime.datetime.now()} start sending to mq')
                self.mq_channel_broker.basic_publish(exchange='',
                                                     routing_key=channel,
                                                     body=message)
                print(f'{datetime.datetime.now()} {message} sent to channel {channel}')
                break
            except AMQPError as err:
                print(err)
                print(f'{datetime.datetime.now()} reconnection')
                self.mq_connection = None
                self.mq_channel_broker = None
                self.connect_to_mq()
                print(f'{datetime.datetime.now()} reconnected')
                continue

    def handle_notify(self):
        try:
            self.db_connection.poll()
            for notify in self.db_connection.notifies:
                message = json.loads(notify.payload)
                channel = message['receiver_channel']
                self.send_message(channel=channel, message=notify.payload)
            self.db_connection.notifies.clear()
        except Exception as err:
            print(err)

    def run_loop(self, loop):
        asyncio.set_event_loop(loop)
        loop.run_forever()

    def run_job(self):
        message_wait_loop = asyncio.new_event_loop()
        message_wait_loop.add_reader(self.db_connection, self.handle_notify)

        run_loop_thread = Thread(target=self.run_loop, args=(message_wait_loop,), name='message_wait_loop')
        run_loop_thread.start()


if __name__ == '__main__':
    broker = MessagesBroker()
    broker.run_job()

# test