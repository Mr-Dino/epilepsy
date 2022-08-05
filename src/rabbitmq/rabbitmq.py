from pika.exceptions import AMQPConnectionError, StreamLostError

import multiprocessing
import threading
import queue
import time
import pika


class Creator(threading.Thread):
    """Класс изображений rabbitmq"""

    def __init__(self, config):
        super().__init__(daemon=True)
        self._manager = multiprocessing.Manager()
        self._queue = self._manager.Queue(maxsize=100)
        self._run_flag = config['active']

        self.host = config['host']
        self.port = config['port']
        self.username = config['username']
        self.password = config['password']
        self.reconnect_time = config['reconnect_time']

    def send_data(self, data):
        self._queue.put(data)

    def get_data(self):
        return self._queue.get()

    def shutdown(self):
        self._run_flag = False

    def run(self):
        while self._run_flag:
            try:
                connection = pika.BlockingConnection(pika.ConnectionParameters(
                    host=self.host,
                    port=self.port,
                    credentials=pika.PlainCredentials(
                        self.username,
                        self.password
                    ),
                    heartbeat=0
                ))
            except AMQPConnectionError:
                time.sleep(self.reconnect_time)
                continue

            channel = connection.channel()
            channel.queue_declare(
                queue="video_create",
                auto_delete=False,
                durable=True
            )

            while True:
                try:
                    data = self._queue.get(True, 2)
                except queue.Empty:
                    self._run_flag = False
                    break
                except EOFError:
                    self._run_flag = False
                    break
                video_queue = data["queue"]
                body_queue = data["img_path"]

                try:
                    channel.basic_publish(
                        exchange='',
                        routing_key=video_queue,
                        body=body_queue
                    )
                    continue
                except Exception as err:
                    print(err)

                if data:
                    self._queue.put(data)
                if connection.is_open():
                    try:
                        connection.close()
                    except Exception as err:
                        print(err)
                break
