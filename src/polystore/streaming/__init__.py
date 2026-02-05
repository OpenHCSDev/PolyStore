"""
Streaming package for polystore.

This package contains:
- StreamingBackend base class
- receivers subpackage with batch processors for Fiji and Napari
"""

# Import StreamingBackend from the _streaming_backend module
# This allows both:
#   from polystore.streaming import StreamingBackend
#   from polystore.streaming.receivers import FijiBatchProcessor
from polystore.streaming._streaming_backend import StreamingBackend

__all__ = ["StreamingBackend"]

