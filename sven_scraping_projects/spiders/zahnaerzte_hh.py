# Zahnarztsuche (KZV Hamburg / Zahnärztekammer Hamburg) dentist directory.
# Data is embedded as JSON in data-filter-data attribute on the search page.
# owner=0: employed (angestellter Zahnarzt); owner!=0: practice owner (Praxisinhaber).

import html
import json
from urllib.parse import urljoin

from scrapy import Spider
from scrapy.http import Request


# Dental titles for name parsing (extended from kvhh for dentists)
DENTAL_TITLES = [
    "Dr. med. dent.",
    "Dr. med.",
    "Dr.",
    "Prof. Dr. med. dent.",
    "Prof. Dr. med.",
    "Prof. Dr.",
    "PD Dr. med. dent.",
    "PD Dr. med.",
    "PD Dr.",
]


def parse_dentist_name(raw_name):
    """Parse dentist name into title, first_name, last_name. Handles 'Frau/Herr' prefix."""
    if not raw_name:
        return {"title": "", "first_name": "", "last_name": ""}
    name = raw_name.strip()
    # Strip Frau/Herr
    for prefix in ("Frau ", "Herr "):
        if name.startswith(prefix):
            name = name[len(prefix) :].strip()
            break
    matched_title = None
    for t in DENTAL_TITLES:
        if name.startswith(t):
            matched_title = t
            name = name.replace(t, "", 1).strip()
            break
    parts = name.split()
    if len(parts) <= 1:
        first_name = parts[0] if parts else ""
        last_name = ""
    else:
        first_name = parts[0]
        last_name = " ".join(parts[1:])
    return {
        "title": matched_title or "",
        "first_name": first_name,
        "last_name": last_name,
    }


def _determine_practice_relation(owner):
    """Map owner field to practice_relation. owner=0 means employed, else owner."""
    if owner == 0:
        return "employed"
    return "owner"


class ZahnaerzteHhSpider(Spider):
    name = "zahnaerzte_hh"
    allowed_domains = ["zahnaerzte-hh.de"]
    start_url = "https://www.zahnaerzte-hh.de/zahnaerzte-portal/zahnaerzte/zahnarztsuche"

    custom_settings = {
        "ROBOTSTXT_OBEY": False,
        "CONCURRENT_REQUESTS": 128,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 64,
        "DOWNLOAD_DELAY": 0,
        "USER_AGENT": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36"
        ),
    }

    def start_requests(self):
        yield Request(
            url=self.start_url,
            callback=self.parse,
        )

    def parse(self, response):
        # Data is embedded in data-filter-data attribute as HTML-entity-encoded JSON
        elem = response.xpath("//*[@data-filter-data]")
        raw_data = elem.xpath("./@data-filter-data").get()
        if not raw_data:
            self.logger.warning("No data-filter-data found on page")
            return

        try:
            decoded = html.unescape(raw_data)
            dentists = json.loads(decoded)
        except (json.JSONDecodeError, TypeError) as e:
            self.logger.error("Failed to parse dentist JSON: %s", e)
            return

        self.logger.info("Found %d dentists in embedded data", len(dentists))

        base_url = response.url

        for d in dentists:

            detail_link = d.get("detailLink") or ""
            if detail_link and not detail_link.startswith("http"):
                detail_link = urljoin(base_url, detail_link)

            label = d.get("label") or ""
            parsed = parse_dentist_name(label)
            # Prefer JSON fields if present
            first_name = d.get("firstname") or parsed["first_name"]
            last_name = d.get("lastname") or parsed["last_name"]
            title = d.get("academic_title") or parsed["title"]
            if isinstance(title, str) and title:
                pass
            else:
                title = parsed["title"]

            # Build full name
            name_parts = [title, first_name, last_name]
            full_name = " ".join(p for p in name_parts if p) or label

            practice_name = d.get("title") or ""

            street = d.get("street") or ""
            zip_code = d.get("zip") or ""
            city = d.get("city") or ""
            address_parts = [p for p in [street, f"{zip_code} {city}".strip()] if p]
            address = ", ".join(address_parts)

            phone = (d.get("phone") or "").strip()
            internet = (d.get("internet") or "").strip()
            if internet and not internet.startswith("http"):
                internet = f"https://{internet}"

            owner_val = d.get("owner")
            practice_relation = _determine_practice_relation(
                owner_val if owner_val is not None else -1
            )

            # Expertise/specialization from JSON arrays (items may be dicts with "label" key)
            expertise = d.get("expertise") or []
            if isinstance(expertise, list):
                labels = []
                for x in expertise:
                    if isinstance(x, dict) and "label" in x:
                        labels.append(str(x["label"]))
                    elif isinstance(x, str):
                        labels.append(x)
                specialization = ", ".join(labels)
            else:
                specialization = ""

            yield {
                "url": detail_link or base_url,
                "title": title,
                "first_name": first_name,
                "last_name": last_name,
                "name": full_name,
                "position": specialization,
                "area_of_work": specialization,
                "department": practice_name,
                "phone": phone,
                "email": "",  # Not in list data
                "address": address,
                "street": street,
                "zip": zip_code,
                "city": city,
                "practice_name": practice_name,
                "practice_relation": practice_relation,
                "website": internet,
                "specialization": specialization,
                "main_areas_of_activity": specialization,
                "llm_content": "",
                "field_membership": "",
            }
