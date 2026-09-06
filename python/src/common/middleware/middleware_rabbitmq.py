import pika

from .middleware import (
    MessageMiddlewareCloseError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareExchange,
    MessageMiddlewareMessageError,
    MessageMiddlewareQueue,
)


_DEFAULT_EXCHANGE = ""
_PREFETCH_COUNT = 1
_QUEUE_DURABLE = True
_REQUEUE_REJECTED_MESSAGES = True

_CONNECTION_ERRORS = (
    pika.exceptions.AMQPConnectionError,
    pika.exceptions.ConnectionClosed,
    pika.exceptions.ConnectionWrongStateError,
)


class _MessageMiddlewareRabbitMQ:
    """Common RabbitMQ connection and consumption behavior."""

    def __init__(self, host):
        self._connection = None
        self._channel = None
        self._consumer_queue_name = None
        self._is_consuming = False

        try:
            self._connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=host)
            )
            self._channel = self._connection.channel()
        except _CONNECTION_ERRORS as error:
            self._cleanup_after_setup_failure(error)
            raise MessageMiddlewareDisconnectedError(
                "Could not connect to RabbitMQ"
            ) from error
        except pika.exceptions.AMQPError as error:
            self._cleanup_after_setup_failure(error)
            raise MessageMiddlewareMessageError(
                "Could not initialize the RabbitMQ channel"
            ) from error

    def start_consuming(self, on_message_callback):
        if not callable(on_message_callback):
            raise MessageMiddlewareMessageError(
                "The message callback must be callable"
            )
        if self._is_consuming:
            raise MessageMiddlewareMessageError(
                "This middleware instance is already consuming"
            )

        self._ensure_connection_is_open()

        try:
            self._channel.basic_consume(
                queue=self._consumer_queue_name,
                on_message_callback=self._build_message_callback(
                    on_message_callback
                ),
                auto_ack=False,
            )
        except _CONNECTION_ERRORS as error:
            self._raise_disconnected(error)
        except (pika.exceptions.AMQPError, TypeError, ValueError) as error:
            self._raise_message_error("Could not register the consumer", error)

        self._is_consuming = True
        try:
            self._channel.start_consuming()
        except _CONNECTION_ERRORS as error:
            self._raise_disconnected(error)
        except pika.exceptions.AMQPError as error:
            self._raise_message_error("Could not consume messages", error)
        finally:
            self._is_consuming = False

    def stop_consuming(self):
        if not self._is_consuming:
            return

        self._ensure_connection_is_open()
        try:
            self._channel.stop_consuming()
        except _CONNECTION_ERRORS as error:
            self._raise_disconnected(error)
        except pika.exceptions.AMQPError as error:
            self._raise_message_error("Could not stop the consumer", error)

    def close(self):
        if self._connection is None or self._connection.is_closed:
            return

        close_error = None

        if self._is_consuming:
            try:
                self.stop_consuming()
            except Exception as error:
                close_error = error

        if self._channel is not None and self._channel.is_open:
            try:
                self._channel.close()
            except Exception as error:
                close_error = close_error or error

        if self._connection.is_open:
            try:
                self._connection.close()
            except Exception as error:
                close_error = close_error or error

        if close_error is not None:
            raise MessageMiddlewareCloseError(
                "Could not close the RabbitMQ middleware cleanly"
            ) from close_error

    def _build_message_callback(self, on_message_callback):
        def callback(channel, method, _properties, body):
            delivery_tag = method.delivery_tag

            def ack():
                self._acknowledge(channel, delivery_tag)

            def nack():
                self._reject(channel, delivery_tag)

            on_message_callback(body, ack, nack)

        return callback

    def _acknowledge(self, channel, delivery_tag):
        try:
            channel.basic_ack(delivery_tag=delivery_tag)
        except _CONNECTION_ERRORS as error:
            self._raise_disconnected(error)
        except pika.exceptions.AMQPError as error:
            self._raise_message_error("Could not acknowledge the message", error)

    def _reject(self, channel, delivery_tag):
        try:
            channel.basic_nack(
                delivery_tag=delivery_tag,
                requeue=_REQUEUE_REJECTED_MESSAGES,
            )
        except _CONNECTION_ERRORS as error:
            self._raise_disconnected(error)
        except pika.exceptions.AMQPError as error:
            self._raise_message_error("Could not reject the message", error)

    def _ensure_connection_is_open(self):
        if self._connection is None or self._connection.is_closed:
            raise MessageMiddlewareDisconnectedError(
                "The RabbitMQ connection is closed"
            )
        if self._channel is None or self._channel.is_closed:
            raise MessageMiddlewareMessageError(
                "The RabbitMQ channel is closed"
            )

    def _cleanup_after_setup_failure(self, original_error):
        if self._connection is None or self._connection.is_closed:
            return

        try:
            self._connection.close()
        except Exception as cleanup_error:
            original_error.add_note(
                f"The partially opened connection could not be closed: "
                f"{cleanup_error}"
            )

    @staticmethod
    def _raise_disconnected(error):
        raise MessageMiddlewareDisconnectedError(
            "The RabbitMQ connection was lost"
        ) from error

    @staticmethod
    def _raise_message_error(message, error):
        raise MessageMiddlewareMessageError(message) from error


class MessageMiddlewareQueueRabbitMQ(
    _MessageMiddlewareRabbitMQ,
    MessageMiddlewareQueue,
):

    def __init__(self, host, queue_name):
        super().__init__(host)
        self._queue_name = queue_name
        self._consumer_queue_name = queue_name

        try:
            self._channel.queue_declare(
                queue=queue_name,
                durable=_QUEUE_DURABLE,
            )
            self._channel.basic_qos(prefetch_count=_PREFETCH_COUNT)
        except _CONNECTION_ERRORS as error:
            self._cleanup_after_setup_failure(error)
            self._raise_disconnected(error)
        except (pika.exceptions.AMQPError, TypeError, ValueError) as error:
            self._cleanup_after_setup_failure(error)
            self._raise_message_error("Could not declare the queue", error)

    def send(self, message):
        self._ensure_connection_is_open()

        try:
            self._channel.basic_publish(
                exchange=_DEFAULT_EXCHANGE,
                routing_key=self._queue_name,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
                ),
            )
        except _CONNECTION_ERRORS as error:
            self._raise_disconnected(error)
        except (pika.exceptions.AMQPError, TypeError, ValueError) as error:
            self._raise_message_error("Could not publish the message", error)


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):

    def __init__(self, host, exchange_name, routing_keys):
        pass
