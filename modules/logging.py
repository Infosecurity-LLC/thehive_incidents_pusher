import logging
from typing import Dict, NoReturn

from raven.conf import setup_logging
from raven.handlers.logging import SentryHandler

logger = logging.getLogger('thehive_incidents_pusher')


def prepare_logging(settings: Dict) -> NoReturn:
    """
    Prepares logger for application

    :param settings: Application settings (i.e. from data/settings.yaml)
    :return:
    """
    logger.setLevel(settings['logging']['basic_level'])
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(settings['logging']['term_level'])
    stream_handler.setFormatter(
        logging.Formatter('%(asctime)s - %(levelname)-10s - [in %(pathname)s:%(lineno)d]: - %(message)s')
    )
    logger.addHandler(stream_handler)
    if settings.get('sentry_url'):
        sentry_handler = SentryHandler(settings['sentry_url'])
        sentry_handler.setLevel(settings['logging']['sentry_level'])
        setup_logging(sentry_handler)
        logger.addHandler(sentry_handler)
