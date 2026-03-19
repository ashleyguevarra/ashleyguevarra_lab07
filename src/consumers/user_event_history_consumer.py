"""
Kafka Historical User Event Consumer (Event Sourcing)
SPDX-License-Identifier: LGPL-3.0-or-later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""

import json
from logger import Logger
from typing import Optional
from kafka import KafkaConsumer
from handlers.handler_registry import HandlerRegistry

class UserEventHistoryConsumer:
    """A consumer that starts reading Kafka events from the earliest point from a given topic"""
    
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        registry: HandlerRegistry
    ):
        # TODO: définir les paramètres corrects
        self.consumer: Optional[KafkaConsumer] = None
        self.logger = Logger.get_instance("UserEventHistoryConsumer")
    
    def start(self) -> None:
        """Start consuming messages from Kafka"""
        self.logger.info(f"Démarrer un consommateur : {self.group_id}")
        
        try:
            # TODO: implémentation basée sur UserEventConsumer
            # TODO: enregistrez les événements dans un fichier JSON
            self.consumer = None
            self.logger.debug("Aucune implémentation!")            
        except Exception as e:
            self.logger.error(f"Erreur: {e}", exc_info=True)
        finally:
            self.stop()

    def stop(self) -> None:
        """Stop the consumer gracefully"""
        if self.consumer:
            self.consumer.close()
            self.logger.info("Arrêter le consommateur!")