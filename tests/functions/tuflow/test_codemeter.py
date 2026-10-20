"""Focused tests for CodeMeter WebAdmin parsing."""

from unittest.mock import patch

from ryan_library.functions.tuflow import codemeter


MONITORING_HTML = """
<table>
  <tr><th>Product Code</th><th>Name</th><th>Feature Map</th><th>License Quantity</th>
      <th>Used</th><th>Available</th></tr>
  <tr><td>20</td><td>TUFLOW Local</td><td>0x2026</td><td>4</td><td>2</td><td>2</td></tr>
  <tr><td>99</td><td>Future TUFLOW Feature</td><td>0x2026</td><td>1</td><td>1</td><td>0</td></tr>
</table>
<a href="/license_monitoring/license_monitoring_details.html?FirmCode=101139&amp;ProductCode=20">20</a>
<a href="/license_monitoring/license_monitoring_details.html?FirmCode=101139&amp;ProductCode=99">99</a>
"""

DETAIL_HTML = """
<table>
  <tr><th>ID</th><th>Client (User)</th><th>Client Process ID</th><th>Application Information</th>
      <th>Access Mode</th><th>First Access</th><th>Last Access</th><th>Expires</th><th>Action</th></tr>
  <tr><td>115</td><td>::1 (BGER\\Sandy.Gunasekara)</td><td>47304</td><td></td><td>User Limit</td>
      <td>2026-10-20 13:54:57</td><td>2026-10-20 14:06:50</td><td></td><td>Remove allocation</td></tr>
</table>
"""

EVENTS_HTML = (
    "<textarea>2026-10-19T22:40:24.234+08:00 Entry (101139:26) not found - Event WB0212 "
    "(NO MORE LICENSES), Request IP-Address local(IPv6)(ts)(BGER\\Sandy.Gunasekara) with UserLimit</textarea>"
)


def test_parses_product_quantities_and_active_allocation() -> None:
    products, detail_urls = codemeter.parse_license_monitoring_html(MONITORING_HTML)
    allocations = codemeter.parse_product_detail_html(
        DETAIL_HTML,
        product_code=20,
        product_name="TUFLOW Local",
    )

    assert [(item.product_code, item.used, item.total, item.available) for item in products] == [
        (20, 2, 4, 2),
        (99, 1, 1, 0),
    ]
    assert set(detail_urls) == {20, 99}
    assert len(allocations) == 1
    assert allocations[0].user == r"BGER\Sandy.Gunasekara"
    assert allocations[0].process_id == 47304


def test_snapshot_warns_about_unknown_product_code() -> None:
    def fake_fetch(url: str, *, timeout_seconds: float) -> str:
        del timeout_seconds
        if url.endswith("license_monitoring.html"):
            return MONITORING_HTML
        if url.endswith("events.html"):
            return EVENTS_HTML
        return DETAIL_HTML

    with patch.object(codemeter, "_fetch_text", side_effect=fake_fetch):
        snapshot = codemeter.fetch_codemeter_snapshot(base_url="http://localhost:22352")

    assert any("Unknown CodeMeter product code 99" in warning for warning in snapshot.warnings)
    assert {item.product_code for item in snapshot.allocations} == {20, 99}
