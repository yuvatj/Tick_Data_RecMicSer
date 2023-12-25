
import ast
import pika
import time

class RabbitMQQueue:
    def __init__(self, exchange_name, queue_name, host='localhost', username='guest', password='guest'):
        self.exchange_name = exchange_name
        self.queue_name = queue_name
        self.host = host
        self.credentials = pika.PlainCredentials(username, password)
        self.connection_params = pika.ConnectionParameters(host=self.host, credentials=self.credentials)
        self.connection = self.connect()

    def connect(self):
        try:
            connection = pika.BlockingConnection(self.connection_params)
            return connection
        except pika.exceptions.AMQPError as e:
            print(f"Error connecting to RabbitMQ: {e}")
            return None

    def declare_exchange(self):
        try:
            channel = self.connection.channel()
            channel.exchange_declare(exchange=self.exchange_name, exchange_type='direct')
            print(f"Exchange '{self.exchange_name}' declared successfully")
        except pika.exceptions.AMQPError as e:
            print(f"Error declaring exchange: {e}")

    def declare_queue(self):
        try:
            channel = self.connection.channel()
            channel.queue_declare(queue=self.queue_name)
            print(f"Queue '{self.queue_name}' declared successfully")
        except pika.exceptions.AMQPError as e:
            print(f"Error declaring queue: {e}")

    def bind_queue_to_exchange(self):
        try:
            channel = self.connection.channel()
            channel.queue_bind(exchange=self.exchange_name, queue=self.queue_name, routing_key=self.queue_name)
            print(f"Queue '{self.queue_name}' bound to exchange '{self.exchange_name}'")
        except pika.exceptions.AMQPError as e:
            print(f"Error binding queue to exchange: {e}")

    def publish_message(self, message):
        try:
            channel = self.connection.channel()
            channel.basic_publish(exchange=self.exchange_name, routing_key=self.queue_name, body=message)
            print(f"Message sent to exchange '{self.exchange_name}' with routing key '{self.queue_name}': {message}")
        except pika.exceptions.AMQPError as e:
            print(f"Error publishing message: {e}")

    def close_connection(self):
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            print("Connection to RabbitMQ closed")


# Example usage for three exchanges and queues:
if __name__ == "__main__":
    # Creating instances for each exchange and queue
    # exchange_1_queue_1 = RabbitMQQueue('INDEX', 'index_queue')
    # exchange_2_queue_2 = RabbitMQQueue('exchange_2', 'queue_2')
    # exchange_3_queue_3 = RabbitMQQueue('exchange_3', 'queue_3')

    # Declare exchanges and queues
    # exchange_1_queue_1.declare_exchange()
    # exchange_1_queue_1.declare_queue()
    # exchange_1_queue_1.bind_queue_to_exchange()

    # exchange_2_queue_2.declare_exchange()
    # exchange_2_queue_2.declare_queue()
    # exchange_2_queue_2.bind_queue_to_exchange()
    #
    # exchange_3_queue_3.declare_exchange()
    # exchange_3_queue_3.declare_queue()
    # exchange_3_queue_3.bind_queue_to_exchange()

    # Example of publishing messages to different exchanges and queues
    # exchange_1_queue_1.publish_message('Message for exchange 1, queue 2')
    # exchange_2_queue_2.publish_message('Message for exchange 2, queue 2')
    # exchange_3_queue_3.publish_message('Message for exchange 3, queue 3')

    # Close connections
    # exchange_1_queue_1.close_connection()
    # exchange_2_queue_2.close_connection()
    # exchange_3_queue_3.close_connection

    ##_________________________________________________________________________________________________________________________

    exchange = 'NFO'
    summery_date = "2023-12-15"

    file_path = f'E:/Market Analysis/Programs/Deployed/utility/Ticks_txt/{exchange}/{summery_date}.txt'

    exchange_1_queue_1 = RabbitMQQueue('NFO', 'nfo_queue')

    # Declare exchanges and queues
    exchange_1_queue_1.declare_exchange()
    exchange_1_queue_1.declare_queue()
    exchange_1_queue_1.bind_queue_to_exchange()

    with open(file_path, 'r') as file:
        for line in file:
            exchange_1_queue_1.publish_message(line)
            time.sleep(1)

    # Close connections
    exchange_1_queue_1.close_connection()