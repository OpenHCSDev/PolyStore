"""Guard the deleted ACK alias across every production streaming consumer.

The historical handlers package cannot import with current metaclass-registry;
its separate failure witness is retained rather than mocked away here. Actual
producer/viewer delivery is exercised in OpenHCS's continuous source journey.
"""

import ast
from pathlib import Path


def test_no_consumer_retains_the_deleted_global_ack_alias():
    root = Path(__file__).resolve().parents[1] / "src" / "polystore"
    for source in root.rglob("*.py"):
        tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
        assert not any(
            isinstance(node, ast.Attribute) and node.attr in {"_send_ack", "_setup_ack_socket"}
            for node in ast.walk(tree)
        ), source
