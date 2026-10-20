"""Read-only parsing and retrieval of local CodeMeter WebAdmin licence data."""

from __future__ import annotations

import re
from collections.abc import Collection, Mapping
from dataclasses import dataclass
from datetime import datetime
from html.parser import HTMLParser
from types import MappingProxyType
from urllib.parse import parse_qs, urljoin, urlparse
from urllib.request import urlopen

KNOWN_TUFLOW_PRODUCT_NAMES = MappingProxyType(
    {
        20: "TUFLOW Local",
        21: "TUFLOW Local Threads",
        22: "TUFLOW Local Multiple Domains",
        26: "TUFLOW Local GPU",
    }
)

_PRODUCT_CODE_PATTERN = re.compile(r"(?P<code>\d+)\s*$")
_CLIENT_USER_PATTERN = re.compile(r"\((?P<user>[^()]+)\)\s*$")
_EVENT_TIMESTAMP_PATTERN = re.compile(r"^(?P<timestamp>\d{4}-\d{2}-\d{2}T[^ ]+)")
_NO_LICENSE_PATTERN = re.compile(
    r"Entry \((?P<firm_code>\d+):(?P<product_code>\d+)\) not found .*\(NO MORE LICENSES\).*"
    r"\((?P<user>[^()]+)\)",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class CodeMeterProductUsage:
    """Current quantity information for one CodeMeter product item."""

    firm_code: int
    product_code: int
    name: str
    total: int
    used: int
    available: int


@dataclass(frozen=True, slots=True)
class CodeMeterAllocation:
    """One active CodeMeter product allocation."""

    handle_id: int
    user: str
    process_id: int | None
    product_code: int
    product_name: str
    first_access: datetime | None
    last_access: datetime | None


@dataclass(frozen=True, slots=True)
class CodeMeterLicenseDenial:
    """A recorded request that failed because no licence was available."""

    timestamp: datetime
    user: str
    firm_code: int
    product_code: int


@dataclass(frozen=True, slots=True)
class CodeMeterSnapshot:
    """Current allocations, quantities, recent denials, and discovery warnings."""

    products: tuple[CodeMeterProductUsage, ...]
    allocations: tuple[CodeMeterAllocation, ...]
    denials: tuple[CodeMeterLicenseDenial, ...]
    warnings: tuple[str, ...]


class _TableParser(HTMLParser):
    """Extract plain table cells, links, and textarea content from WebAdmin HTML."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.tables: list[list[list[str]]] = []
        self.links: list[str] = []
        self.textareas: list[str] = []
        self._table_depth = 0
        self._table: list[list[str]] | None = None
        self._row: list[str] | None = None
        self._cell_parts: list[str] | None = None
        self._textarea_parts: list[str] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attributes = dict(attrs)
        if tag == "a" and attributes.get("href"):
            self.links.append(str(attributes["href"]))
        if tag == "textarea":
            self._textarea_parts = []
        if tag == "table":
            self._table_depth += 1
            if self._table_depth == 1:
                self._table = []
        elif self._table_depth and tag == "tr":
            self._row = []
        elif self._table_depth and tag in {"td", "th"}:
            self._cell_parts = []

    def handle_data(self, data: str) -> None:
        if self._cell_parts is not None:
            self._cell_parts.append(data)
        if self._textarea_parts is not None:
            self._textarea_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag in {"td", "th"} and self._cell_parts is not None and self._row is not None:
            self._row.append(" ".join("".join(self._cell_parts).split()))
            self._cell_parts = None
        elif tag == "tr" and self._row is not None and self._table is not None:
            if any(self._row):
                self._table.append(self._row)
            self._row = None
        elif tag == "table" and self._table_depth:
            if self._table_depth == 1 and self._table:
                self.tables.append(self._table)
            self._table = None
            self._table_depth -= 1
        elif tag == "textarea" and self._textarea_parts is not None:
            self.textareas.append("".join(self._textarea_parts))
            self._textarea_parts = None


def _parse_html(html_text: str) -> _TableParser:
    parser = _TableParser()
    parser.feed(html_text)
    return parser


def _integer(value: str) -> int | None:
    match = re.search(r"-?\d+", value)
    return int(match.group()) if match is not None else None


def _monitoring_detail_links(
    parser: _TableParser, *, firm_code: int
) -> tuple[dict[int, str], set[int]]:
    """Select this firm's detail URLs and find codes shared with other firms."""
    detail_urls: dict[int, str] = {}
    foreign_codes: set[int] = set()
    for href in parser.links:
        if not urlparse(href).path.endswith("license_monitoring_details.html"):
            continue
        query = parse_qs(urlparse(href).query)
        try:
            link_firm = int(query["FirmCode"][0])
            product_code = int(query["ProductCode"][0])
        except (KeyError, ValueError, IndexError):
            continue
        if link_firm == firm_code:
            detail_urls[product_code] = href
        else:
            foreign_codes.add(product_code)
    return detail_urls, foreign_codes


def parse_license_monitoring_html(
    html_text: str,
    *,
    firm_code: int = 101139,
    product_codes: Collection[int] | None = None,
    product_names: Mapping[int, str] | None = None,
) -> tuple[tuple[CodeMeterProductUsage, ...], dict[int, str]]:
    """Parse firm-scoped quantities, excluding rows ambiguous across firms."""
    parser = _parse_html(html_text)
    detail_urls, foreign_codes = _monitoring_detail_links(parser, firm_code=firm_code)
    if product_codes is not None:
        detail_urls = {code: href for code, href in detail_urls.items() if code in product_codes}
    names = product_names if product_names is not None else (
        KNOWN_TUFLOW_PRODUCT_NAMES if firm_code == 101139 else {}
    )
    products: list[CodeMeterProductUsage] = []
    for table in parser.tables:
        if not table or "Product Code" not in table[0] or "License Quantity" not in table[0]:
            continue
        for row in table[1:]:
            if len(row) < 5:
                continue
            code_match = _PRODUCT_CODE_PATTERN.search(row[0])
            total = _integer(row[3])
            available = _integer(row[-1])
            if code_match is None or total is None or available is None:
                continue
            product_code = int(code_match.group("code"))
            # Product codes are only unique within a firm. If two firms use
            # the same code, the unscoped quantity row cannot be attributed.
            if product_code not in detail_urls or product_code in foreign_codes:
                continue
            products.append(
                CodeMeterProductUsage(
                    firm_code=firm_code,
                    product_code=product_code,
                    name=row[1] or names.get(product_code, "Unknown product"),
                    total=total,
                    used=max(0, total - available),
                    available=available,
                )
            )
    return tuple(sorted(products, key=lambda item: item.product_code)), detail_urls

def parse_product_detail_html(
    html_text: str,
    *,
    product_code: int,
    product_name: str,
) -> tuple[CodeMeterAllocation, ...]:
    """Parse active allocations from one product-detail page."""
    parser = _parse_html(html_text)
    allocations: list[CodeMeterAllocation] = []
    for table in parser.tables:
        if not table or table[0][:3] != ["ID", "Client (User)", "Client Process ID"]:
            continue
        for row in table[1:]:
            if len(row) < 8:
                continue
            handle_id = _integer(row[0])
            if handle_id is None:
                continue
            user_match = _CLIENT_USER_PATTERN.search(row[1])
            user = user_match.group("user") if user_match is not None else row[1]
            process_id = _integer(row[2])
            allocations.append(
                CodeMeterAllocation(
                    handle_id=handle_id,
                    user=user,
                    process_id=process_id,
                    product_code=product_code,
                    product_name=product_name,
                    first_access=_parse_webadmin_datetime(row[5]),
                    last_access=_parse_webadmin_datetime(row[6]),
                )
            )
    return tuple(allocations)


def _parse_webadmin_datetime(value: str) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def parse_events_html(
    html_text: str,
    *,
    firm_code: int | None = None,
    product_codes: Collection[int] | None = None,
) -> tuple[CodeMeterLicenseDenial, ...]:
    """Parse no-more-licences events from the WebAdmin event textarea."""
    parser = _parse_html(html_text)
    denials: list[CodeMeterLicenseDenial] = []
    for textarea in parser.textareas:
        for line in textarea.splitlines():
            timestamp_match = _EVENT_TIMESTAMP_PATTERN.match(line)
            denial_match = _NO_LICENSE_PATTERN.search(line)
            if timestamp_match is None or denial_match is None:
                continue
            try:
                timestamp = datetime.fromisoformat(timestamp_match.group("timestamp"))
            except ValueError:
                continue
            event_firm_code = int(denial_match.group("firm_code"))
            event_product_code = int(denial_match.group("product_code"))
            if firm_code is not None and event_firm_code != firm_code:
                continue
            if product_codes is not None and event_product_code not in product_codes:
                continue
            denials.append(
                CodeMeterLicenseDenial(
                    timestamp=timestamp,
                    user=denial_match.group("user"),
                    firm_code=event_firm_code,
                    product_code=event_product_code,
                )
            )
    return tuple(sorted(denials, key=lambda item: item.timestamp, reverse=True))


def _fetch_text(url: str, *, timeout_seconds: float) -> str:
    with urlopen(url, timeout=timeout_seconds) as response:  # noqa: S310 - caller explicitly configures WebAdmin URL
        encoding = response.headers.get_content_charset() or "utf-8"
        return response.read().decode(encoding, errors="replace")


def fetch_codemeter_snapshot(
    *,
    base_url: str,
    timeout_seconds: float = 5.0,
    firm_code: int = 101139,
    product_codes: Collection[int] | None = None,
    product_names: Mapping[int, str] | None = None,
) -> CodeMeterSnapshot:
    """Fetch firm-scoped CodeMeter data; defaults target TUFLOW."""

    normalized_base_url = f"{base_url.rstrip('/')}/"
    monitoring_html = _fetch_text(
        urljoin(normalized_base_url, "license_monitoring.html"),
        timeout_seconds=timeout_seconds,
    )
    names = product_names if product_names is not None else (
        KNOWN_TUFLOW_PRODUCT_NAMES if firm_code == 101139 else {}
    )
    products, detail_urls = parse_license_monitoring_html(
        monitoring_html, firm_code=firm_code, product_codes=product_codes, product_names=names
    )
    product_by_code = {item.product_code: item for item in products}
    warnings: list[str] = []
    _, foreign_codes = _monitoring_detail_links(_parse_html(monitoring_html), firm_code=firm_code)
    for product_code in sorted(foreign_codes & detail_urls.keys()):
        warnings.append(
            f"Firm {firm_code} product {product_code}: ambiguous quantities (shared product code with "
            "another firm); allocations are still firm-scoped."
        )
    allocations: list[CodeMeterAllocation] = []

    for product_code, relative_url in sorted(detail_urls.items()):
        product_name = product_by_code.get(
            product_code,
            CodeMeterProductUsage(
                firm_code=firm_code,
                product_code=product_code,
                name=names.get(product_code, "Unknown product"),
                total=0,
                used=0,
                available=0,
            ),
        ).name
        try:
            detail_html = _fetch_text(
                urljoin(normalized_base_url, relative_url),
                timeout_seconds=timeout_seconds,
            )
        except OSError as exc:
            warnings.append(f"Could not read CodeMeter product {product_code} details: {exc}")
            continue
        allocations.extend(
            parse_product_detail_html(
                detail_html,
                product_code=product_code,
                product_name=product_name,
            )
        )

    try:
        events_html = _fetch_text(
            urljoin(normalized_base_url, "diagnosis/events.html"),
            timeout_seconds=timeout_seconds,
        )
        denials = parse_events_html(events_html, firm_code=firm_code, product_codes=product_codes)
    except OSError as exc:
        denials = ()
        warnings.append(f"Could not read CodeMeter events: {exc}")

    discovered_codes = (
        set(detail_urls)
        | {item.product_code for item in products}
        | {item.product_code for item in allocations}
        | {item.product_code for item in denials}
    )
    for product_code in sorted(discovered_codes - names.keys()):
        discovered_name = product_by_code.get(product_code)
        name_text = discovered_name.name if discovered_name is not None else "name unavailable"
        warnings.append(
            f"Unknown CodeMeter product code {product_code} for firm {firm_code} ({name_text}); "
            "supply product_names if this code is expected."
        )

    return CodeMeterSnapshot(
        products=products,
        allocations=tuple(sorted(allocations, key=lambda item: (item.user.casefold(), item.product_code))),
        denials=denials,
        warnings=tuple(warnings),
    )
