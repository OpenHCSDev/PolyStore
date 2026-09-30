"""The registered Fiji ROI handler consumes the canonical transfer boundary."""

from uuid import uuid4

import pytest

from zmqruntime.messages import AckReturnRoute, ImageTransferIdentity, ProcessIdentity

from polystore.streaming.handlers.fiji_rois import FijiROIWireItem


@pytest.mark.parametrize("rois", [[], ["synthetic-roi"]])
def test_roi_handler_item_retains_exact_return_contract(rois):
    route = AckReturnRoute("tcp://127.0.0.1:8111", str(uuid4()), ProcessIdentity.current())
    transfer = ImageTransferIdentity("synthetic-rois", route)
    item = FijiROIWireItem.from_payload({**transfer.to_dict(), "rois": rois, "metadata": {}})
    assert item.transfer == transfer
    assert item.rois == rois


def test_untracked_roi_item_has_no_ack_contract():
    assert FijiROIWireItem.from_payload({"rois": [], "metadata": {}}).transfer is None


def test_tracked_roi_item_without_return_route_is_rejected():
    with pytest.raises(KeyError, match="return_route"):
        FijiROIWireItem.from_payload({"image_id": "missing-route", "rois": [], "metadata": {}})
