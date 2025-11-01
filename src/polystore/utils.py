"""
Utility functions for polystore.
"""

import re
from typing import List


def natural_sort(items: List[str]) -> List[str]:
    """
    Sort strings in natural/human order (e.g., 'file2' before 'file10').

    Args:
        items: List of strings to sort

    Returns:
        Sorted list of strings
    """
    def natural_key(text: str):
        """Generate sort key for natural sorting."""
        return [int(c) if c.isdigit() else c.lower() for c in re.split(r'(\d+)', text)]

    return sorted(items, key=natural_key)


def get_zmq_transport_url(port: int, transport_mode: str = "tcp") -> str:
    """
    Get ZeroMQ transport URL.

    Args:
        port: Port number
        transport_mode: Transport mode ('tcp' or 'ipc')

    Returns:
        ZeroMQ transport URL
    """
    if transport_mode == "tcp":
        return f"tcp://127.0.0.1:{port}"
    elif transport_mode == "ipc":
        return f"ipc:///tmp/polystore_{port}.ipc"
    else:
        raise ValueError(f"Unknown transport mode: {transport_mode}")
