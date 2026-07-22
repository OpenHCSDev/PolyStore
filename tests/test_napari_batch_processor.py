from polystore.streaming.receivers.napari import NapariBatchProcessor


class RecordingNapariServer:
    def __init__(self, display_work):
        self.display_work = display_work
        self.calls = []

    def display_layer_batch(self, **kwargs):
        self.calls.append(kwargs)
        return self.display_work


def test_napari_batch_processor_returns_handler_owned_display_work():
    display_work = object()
    server = RecordingNapariServer(display_work)
    processor = NapariBatchProcessor(server)
    items = (object(), object())
    display_payload = object()
    component_names_metadata = object()

    result = processor.add_items(
        "objects",
        items,
        display_payload,
        component_names_metadata,
    )

    assert result is display_work
    assert server.calls == [
        {
            "layer_key": "objects",
            "items": items,
            "display_payload": display_payload,
            "component_names_metadata": component_names_metadata,
        }
    ]
