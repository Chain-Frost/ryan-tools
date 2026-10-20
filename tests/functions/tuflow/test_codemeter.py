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


def test_mixed_firms_never_leak_other_product_links_or_denials() -> None:
    mixed_html = MONITORING_HTML.replace(
        "</table>",
        "<tr><td>47</td><td>Other Software</td><td>0x1</td><td>9</td><td>5</td><td>4</td></tr></table>",
    ) + """
<a href="/license_monitoring/license_monitoring_details.html?FirmCode=777&amp;ProductCode=47">47</a>
<a href="/license_monitoring/license_monitoring_details.html?FirmCode=777&amp;ProductCode=20">20</a>
"""
    products, urls = codemeter.parse_license_monitoring_html(mixed_html, firm_code=101139)
    # 20 is ambiguous across firms and cannot safely supply a quantity.
    assert [item.product_code for item in products] == [99]
    assert set(urls) == {20, 99}
    assert all("FirmCode=101139" in href for href in urls.values())

    events = EVENTS_HTML.replace(
        "</textarea>",
        "\n2026-10-19T22:41:24.234+08:00 Entry (777:47) not found - Event WB0212 "
        "(NO MORE LICENSES), Request IP-Address local(IPv6)(ts)(OTHER\\\\USER) with UserLimit"
        "</textarea>",
    )
    only_tuflow = codemeter.parse_events_html(events, firm_code=101139)
    assert [(event.firm_code, event.product_code) for event in only_tuflow] == [(101139, 26)]
    assert not codemeter.parse_events_html(events, firm_code=101139, product_codes={20})


def test_other_vendor_firm_and_product_codes_are_configurable() -> None:
    other_html = """
<table>
<tr><th>Product Code</th><th>Name</th><th>Feature Map</th>
<th>License Quantity</th><th>Used</th><th>Available</th></tr>
<tr><td>31</td><td>Vendor Feature</td><td>-</td><td>4</td><td>2</td><td>2</td></tr>
<tr><td>20</td><td>Unrelated Licence</td><td>-</td><td>9</td><td>7</td><td>2</td></tr>
</table>
<a href="/license_monitoring/license_monitoring_details.html?FirmCode=777&amp;ProductCode=31">31</a>
<a href="/license_monitoring/license_monitoring_details.html?FirmCode=101139&amp;ProductCode=20">20</a>
"""
    urls_seen: list[str] = []

    def fake_fetch(url: str, *, timeout_seconds: float) -> str:
        del timeout_seconds
        urls_seen.append(url)
        if url.endswith("license_monitoring.html"):
            return other_html
        if url.endswith("events.html"):
            return EVENTS_HTML
        assert "FirmCode=777" in url
        assert "ProductCode=31" in url
        return DETAIL_HTML

    with patch.object(codemeter, "_fetch_text", side_effect=fake_fetch):
        snapshot = codemeter.fetch_codemeter_snapshot(
            base_url="http://localhost:22352",
            firm_code=777,
            product_codes={31},
            product_names={31: "Vendor Feature"},
        )

    assert [(p.firm_code, p.product_code, p.used) for p in snapshot.products] == [(777, 31, 2)]
    assert [item.product_code for item in snapshot.allocations] == [31]
    assert snapshot.denials == ()
    assert not snapshot.warnings
    assert len(urls_seen) == 3


def test_product_row_without_matching_firm_link_is_not_attributed() -> None:
    html = MONITORING_HTML.replace(
        "</table>",
        "<tr><td>88</td><td>Unlinked product</td><td>0x1</td><td>3</td><td>1</td><td>2</td></tr></table>",
    )
    products, links = codemeter.parse_license_monitoring_html(html)
    assert [product.product_code for product in products] == [20, 99]
    assert set(links) == {20, 99}
