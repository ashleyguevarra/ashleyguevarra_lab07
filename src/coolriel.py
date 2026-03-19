"""
Coolriel: Event-Driven Email Sender
SPDX-License-Identifier: LGPL-3.0-or-later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""
import config
from consumers.user_event_history_consumer import UserEventHistoryConsumer
from logger import Logger
from consumers.user_event_consumer import UserEventConsumer
from handlers.handler_registry import HandlerRegistry
from handlers.user_created_handler import UserCreatedHandler
from handlers.user_deleted_handler import UserDeletedHandler

logger = Logger.get_instance("Coolriel")

def main():
    """Main entry point for the Coolriel service"""
    registry = HandlerRegistry()
    registry.register(UserCreatedHandler(output_dir=config.OUTPUT_DIR))
    registry.register(UserDeletedHandler(output_dir=config.OUTPUT_DIR))

    # NOTE: le consommateur peut écouter 1 ou plusieurs topics (str or array)
    consumer_service = UserEventConsumer(
        bootstrap_servers=config.KAFKA_HOST,
        topic=config.KAFKA_TOPIC,
        group_id=config.KAFKA_GROUP_ID,
        registry=registry,
    )
    consumer_service.start()

if __name__ == "__main__":
    main()
