import logging
from typing import Dict

from socutils import kafkaconn

logger = logging.getLogger('thehive_incidents_pusher')


def prepare_consumer(settings: Dict) -> kafkaconn.confluentkafka.Consumer:
    """
    Create confluentkafka.Consumer from application settings

    :param settings: application settings (i.e. data/settings.yaml)
    :return: kafkaconn.confluentkafka.Consumer
    """
    logger.info('Started incident consumer', settings)
    auth = kafkaconn.auth.Auth(auth=settings['kafka']['auth_type'], **settings['kafka']['auth_params'])

    consumer = kafkaconn.confluentkafka.Consumer(
        servers=settings['kafka']['servers'],
        group_id=settings['kafka']['group_id'],
        topics=settings['kafka']['topics'],
        enable_auto_commit=False,
        auth_params=auth.get_params(),
    )

    logger.info('Consumer created. Settings: %s', str(consumer.consumer_settings))
    logger.info('Topics: %s', ','.join(consumer.topics))
    return consumer
