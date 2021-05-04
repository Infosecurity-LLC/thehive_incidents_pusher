import logging
import os
import sys
import threading

from appmetrics import metrics
from socutils import get_settings

from modules.app_metrics import register_app_metrics
from modules.logging import prepare_logging

logger = logging.getLogger('thehive_incidents_pusher')


def main(settings_file_path: str = 'data/settings.yaml'):
    settings_file_path = os.getenv("APP_CONFIG_PATH", settings_file_path)
    settings = get_settings(settings_file_path)
    prepare_logging(settings)
    register_app_metrics()
    logger.info("Application start")
    logger.info("Load config from %s", settings_file_path)

    from modules.pusher import TheHivePusher
    pusher = TheHivePusher(settings['thehive'], settings['hbase_event_loader'])

    from modules.kafka_consumer import prepare_consumer
    consumer = prepare_consumer(settings)
    consumer.create_consumer()

    from modules.app_metrics import run_metrics_webserver
    metrics_thread = threading.Thread(target=run_metrics_webserver, daemon=True)
    metrics_thread.start()

    try:
        for message in consumer.read_topic():
            logger.info("Read message from topic %s: %s", message.topic, str(message.value))
            metrics.notify('received_kafka_messages', 1)
            pusher.push(message.value)
            logger.info("Successfully processed message")
            consumer.consumer.commit()
    except Exception as err:
        logger.error("Exception, which type is %s, is detecting during consuming messages: %s", type(err), str(err))
        sys.exit(1)
    except (KeyboardInterrupt, StopIteration) as err:
        logger.warning("Unexpected processing interruption: %s", str(err))
        sys.exit(0)
    except BaseException as e:
        logger.error("Some wtf shit is happened: %s", str(e))
        sys.exit(42)


if __name__ == '__main__':
    main()
