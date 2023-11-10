import datetime
import json

from psycopg2.extensions import AsIs
import asyncio
import psycopg2
from threading import Thread
import pika
from pika.exceptions import AMQPError


class MessagesBroker:
    def __init__(self):
        self.CHANNELS = []
        self.db_connection = None
        self.db_cursor = None
        self.mq_connection = None
        self.mq_channel_broker = None

        self.get_existing_channels()
        self.set_job_connections()
        self.declare_channels()

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
        self.db_cursor.execute(f'LISTEN new_message;')
        self.db_cursor.execute(f'LISTEN new_channel;')

    def connect_to_mq(self):
        credentials = pika.PlainCredentials('server_messages_script', 'qwe321qwe')
        parameters = pika.ConnectionParameters('messages.sliplab.net', 5672, '/', credentials,)

        self.mq_connection = pika.BlockingConnection(parameters)
        self.mq_channel_broker = self.mq_connection.channel()

    def get_existing_channels(self):
        self.connect_to_db()
        self.db_cursor.execute('SELECT ntfcn_channel FROM user_model')
        existing_channels = self.db_cursor.fetchall()
        self.db_connection.close()
        for i in existing_channels:
            for k in i:
                self.CHANNELS.append(k)

    def set_job_connections(self):
        if self.db_connection:
            self.db_connection.close()
            self.db_cursor = None
        if self.mq_connection:
            self.mq_connection.close()
            self.mq_channel_broker = None

        self.connect_to_db()
        self.connect_to_mq()

    def send_message(self, channel, message):
        while True:
            try:
                print(f'{datetime.datetime.now()} start sending to mq')
                self.mq_channel_broker.basic_publish(exchange='',
                                                     routing_key=channel,
                                                     body=message)
                print(f'{datetime.datetime.now()} sent to mq')
                break
            except AMQPError as err:
                print(err)
                self.recon()
                continue

    def declare_channels(self):
        for channel in self.CHANNELS:
            self.mq_channel_broker.queue_declare(queue=channel)

    def handle_notify(self):
        try:
            self.db_connection.poll()
            for notify in self.db_connection.notifies:
                if notify.channel == 'new_channel':
                    new_channel = json.loads(notify.payload)['new_channel']
                    self.CHANNELS.append(new_channel)
                    print(new_channel)
                    while True:
                        try:
                            self.mq_channel_broker.queue_declare(queue=new_channel)
                            print('new channel listening')
                            break
                        except AMQPError as err:
                            print(err)
                            self.recon()
                            continue
                else:
                    channel = json.loads(notify.payload)['receiver_channel']
                    print(channel, notify.payload)
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

    def recon(self):
        print(f'{datetime.datetime.now()} reconnection')
        self.mq_connection = None
        self.mq_channel_broker = None
        self.connect_to_mq()
        print(f'{datetime.datetime.now()} reconnected')


if __name__ == '__main__':
    broker = MessagesBroker()
    broker.run_job()
