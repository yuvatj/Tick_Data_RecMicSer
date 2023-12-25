import pika
import time

class RabbitMQConsumer:
    def __init__(self, queue_name, host='localhost', username='guest', password='guest'):
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

    def consume_messages(self):
        try:
            channel = self.connection.channel()
            channel.queue_declare(queue=self.queue_name)

            def callback(ch, method, properties, body):
                print(f"Received message: {body.decode()}")

            channel.basic_consume(queue=self.queue_name, on_message_callback=callback, auto_ack=True)

            print(f"Waiting for messages from queue '{self.queue_name}'. To exit, press CTRL+C")
            channel.start_consuming()

        except pika.exceptions.AMQPError as e:
            print(f"Error consuming messages: {e}")

    def close_connection(self):
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            print("Connection to RabbitMQ closed")

# Example usage for consuming messages from a queue:
if __name__ == "__main__":
    # Create an instance of RabbitMQConsumer for your_queue
    consumer = RabbitMQConsumer('nfo_queue')

    # Consume messages from the queue
    consumer.consume_messages()

    # To stop consuming, press CTRL+C or handle an exit condition

    # Close the connection when done consuming messages
    consumer.close_connection()
