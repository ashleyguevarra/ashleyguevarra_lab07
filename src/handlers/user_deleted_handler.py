"""
Handler: User Deleted
SPDX-License-Identifier: LGPL-3.0-or-later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""

import os
from pathlib import Path
from handlers.base import EventHandler
from typing import Any, Dict

_GOODBYE_BY_TYPE = {
    1: "goodbye_client_template.html",
    2: "goodbye_employee_template.html",
    3: "goodbye_manager_template.html",
}


class UserDeletedHandler(EventHandler):
    """Handles UserDeleted events"""

    def __init__(self, output_dir: str = "output"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        super().__init__()

    def get_event_type(self) -> str:
        return "UserDeleted"

    def handle(self, event_data: Dict[str, Any]) -> None:
        user_id = event_data.get("id")
        name = event_data.get("name")
        email = event_data.get("email")
        deletion_date = event_data.get("datetime")
        user_type_id = int(event_data.get("user_type_id", 1))

        template_name = _GOODBYE_BY_TYPE.get(user_type_id, _GOODBYE_BY_TYPE[1])
        project_root = Path(__file__).parent.parent
        template_path = project_root / "templates" / template_name
        with open(template_path, "r", encoding="utf-8") as file:
            html_content = file.read()
        html_content = html_content.replace("{{user_id}}", str(user_id))
        html_content = html_content.replace("{{name}}", name or "")
        html_content = html_content.replace("{{email}}", email or "")
        html_content = html_content.replace("{{deletion_date}}", deletion_date or "")

        filename = os.path.join(self.output_dir, f"goodbye_{user_id}.html")
        with open(filename, "w", encoding="utf-8") as f:
            f.write(html_content)

        self.logger.debug(
            f"Courriel au revoir généré pour {name} (ID: {user_id}, type: {user_type_id}), {filename}"
        )
