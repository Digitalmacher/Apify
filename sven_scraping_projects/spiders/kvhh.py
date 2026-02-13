# KVHH (Kassenärztliche Vereinigung Hamburg) physician directory.
# Discovery: sitemap.xml; profile pages: net-kvhh-physician-*.

import re
from scrapy import Spider
from scrapy.http import Request


TITLES = [
    "Dr. med.",
    "Dr.",
    "Prof. Dr. med.",
    "Prof. Dr.",
    "Dipl.-Psych.",
    "PD Dr. med.",
    "PD Dr.",
    "Med. pract.",
]


def parse_doctor_name(raw_name):
    if not raw_name:
        return {"title": None, "first_name": None, "last_name": None}
    name = raw_name.strip()
    matched_title = None
    for t in TITLES:
        if name.startswith(t):
            matched_title = t
            name = name.replace(t, "", 1).strip()
            break
    parts = name.split()
    if len(parts) <= 1:
        first_name = parts[0] if parts else None
        last_name = None
    else:
        first_name = parts[0]
        last_name = " ".join(parts[1:])
    return {
        "title": matched_title,
        "first_name": first_name,
        "last_name": last_name,
    }


def _dd_text_by_dt_label(response, label_substring):
    """Get full text of the first <dd> that follows a <dt> containing label_substring."""
    dd = response.xpath(
        f"//dt[contains(normalize-space(.), \"{label_substring}\")]/following-sibling::dd[1]"
    )
    if not dd:
        return None
    s = (dd.xpath("string()").get() or "").strip()
    return s if s else None


def _dd_html_split_br(response, label_substring):
    """Get <dd> inner HTML and split by <br>, return comma-separated trimmed lines."""
    dd = response.xpath(
        f"//dt[contains(normalize-space(.), \"{label_substring}\")]/following-sibling::dd[1]"
    )
    if not dd:
        return None
    html = dd.get() or ""
    inner = re.sub(r"^<dd[^>]*>", "", html, flags=re.I)
    inner = re.sub(r"</dd>\s*$", "", inner, flags=re.I)
    parts = re.split(r"<br\s*/?>", inner, flags=re.I)
    texts = []
    for p in parts:
        t = re.sub(r"<[^>]+>", "", p).strip()
        if t:
            texts.append(t)
    return ", ".join(texts) if texts else None


class KvhhSpider(Spider):
    name = "kvhh"
    allowed_domains = ["kvhh.net"]
    doctor_url_prefix = "https://www.kvhh.net/de/medicalregister/net-kvhh-physician-"

    custom_settings = {
        "ROBOTSTXT_OBEY": False,
        "CONCURRENT_REQUESTS": 64,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 32,
        "DOWNLOAD_DELAY": 0,
        "USER_AGENT": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36"
        ),
    }

    def start_requests(self):
        yield Request(
            url="https://www.kvhh.net/de/sitemap.xml",
            callback=self.parse_sitemap,
            headers={"Accept": "application/xml, text/xml, */*"},
        )

    def parse_sitemap(self, response):
        # Sitemap XML: extract <loc> URLs (namespace-agnostic)
        locs = response.xpath("//*[local-name()='loc']/text()").getall()
        doctor_urls = [
            u.strip()
            for u in locs
            if u and u.strip().startswith(self.doctor_url_prefix)
        ]
        self.logger.info("Found %d KVHH doctor profile URLs", len(doctor_urls))
        for url in doctor_urls:
            yield Request(url, callback=self.parse_profile, dont_filter=True)

    def parse_profile(self, response):
        raw_name = (response.xpath("string(//h1)").get() or "").strip()
        parsed = parse_doctor_name(raw_name)

        phone_sel = response.xpath("//a[starts-with(@href,'tel:')]/text()").get()
        phone = (phone_sel or "").strip()

        email_href = response.xpath("//a[starts-with(@href,'mailto:')]/@href").get()
        email = ""
        if email_href and email_href.startswith("mailto:"):
            email = email_href[7:].strip()

        specialty = _dd_text_by_dt_label(response, "Fachgebiet")
        languages = _dd_html_split_br(response, "Fremdsprachen")
        main_areas = _dd_html_split_br(response, "Leistungen")

        name_parts = [parsed["title"], parsed["first_name"], parsed["last_name"]]
        full_name = " ".join(p for p in name_parts if p) or ""

        yield {
            "url": response.url,
            "title": parsed["title"],
            "first_name": parsed["first_name"],
            "last_name": parsed["last_name"],
            "name": full_name,
            "position": specialty,
            "area_of_work": specialty,
            "department": specialty,
            "phone": phone,
            "email": email,
            "languages": languages,
            "specialization": specialty,
            "main_areas_of_activity": main_areas,
            "llm_content": "",
            "field_membership": "",
        }
