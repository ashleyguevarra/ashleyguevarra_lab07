"""
Kafka Historical User Event Consumer (Event Sourcing)
SPDX-License-Identifier: LGPL-3.0-or-later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""

import json
import os
from typing import List, Optional

from kafka import KafkaConsumer
from logger import Logger


class UserEventHistoryConsumer:
    """Reads the full retained history of user-events from the earliest offset."""

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        output_path: str,
        consumer_timeout_ms: int = 5000,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.output_path = output_path
        self.consumer_timeout_ms = consumer_timeout_ms
        self.consumer: Optional[KafkaConsumer] = None
        self.logger = Logger.get_instance("UserEventHistoryConsumer")

    def start(self) -> None:
        self.logger.info(f"Démarrage consommateur historique : {self.group_id}")

        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            consumer_timeout_ms=self.consumer_timeout_ms,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )

        events: List[dict] = []
        try:
            for message in self.consumer:
                events.append(message.value)
                self.logger.debug(f"Événement historique : {message.value}")
        except Exception as e:
            self.logger.debug(f"Fin de lecture (timeout ou arrêt) : {e}")
        finally:
            self.stop()

        out_dir = os.path.dirname(self.output_path)
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)
        with open(self.output_path, "w", encoding="utf-8") as f:
            f.write(json.dumps(events, indent=2, ensure_ascii=False))

        self.logger.info(
            f"Historique enregistré : {len(events)} événement(s) → {self.output_path}"
        )

    def stop(self) -> None:
        if self.consumer:
            self.consumer.close()
            self.consumer = None
            self.logger.info("Consommateur historique arrêté.")
