# Asklepios physician profile directory.
# Discovery: sitemap-index.xml → profile sitemap → /profil/ pages.
#
# Throttling: asklepios.com returns 503/504 under high load. Use conservative
# concurrency, delay, AutoThrottle, and retries to avoid rate limiting and
# server overload.

import gzip
from io import BytesIO

from scrapy import Spider
from scrapy.http import Request
from parsel import Selector


class AsklepiosSpider(Spider):
    name = "asklepios"
    allowed_domains = ["asklepios.com"]

    custom_settings = {
        "ROBOTSTXT_OBEY": False,
        "CONCURRENT_REQUESTS": 64,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 8,
        "DOWNLOAD_DELAY": 0.5,
        "DOWNLOAD_TIMEOUT": 90,
        "RETRY_ENABLED": True,
        "RETRY_TIMES": 5,
        "RETRY_HTTP_CODES": [500, 502, 503, 504, 408, 429],
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 1.0,
        "AUTOTHROTTLE_MAX_DELAY": 30.0,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 2.0,
        "AUTOTHROTTLE_DEBUG": False,
        "USER_AGENT": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36"
        ),
    }

    @staticmethod
    def _text(selector):
        """Safely extract and strip a single text value."""
        return (selector.get() or "").strip()

    @staticmethod
    def _join_texts(selector):
        """Extract multiple text nodes, clean them, and join."""
        return ", ".join(t.strip() for t in selector.getall() if t.strip())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.profile_headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "en-US,en;q=0.9,pl;q=0.8,de;q=0.7,sr;q=0.6,bs;q=0.5,nl;q=0.4",
            "cache-control": "no-cache",
            "pragma": "no-cache",
            "sec-ch-ua": '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Linux"',
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "same-origin",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36"
            ),
        }

    def start_requests(self):
        yield Request(
            "https://www.asklepios.com/sitemap-index.xml",
            callback=self.parse_sitemap_index,
        )

    @staticmethod
    def _extract_sitemap_locs(response):
        """
        Extract <loc> URLs from a sitemap or sitemap index.
        Handles plain XML and .xml.gz responses.
        """
        body = response.body or b""
        # Some sitemaps are served as .gz or with gzip content.
        if response.url.endswith(".gz"):
            try:
                body = gzip.GzipFile(fileobj=BytesIO(body)).read()
            except Exception:
                # Fall back to raw body; selector will likely yield nothing.
                pass

        text = None
        try:
            text = body.decode("utf-8", errors="replace")
        except Exception:
            text = (response.text or "")

        sel = Selector(text=text)
        return [u.strip() for u in sel.xpath("//*[local-name()='loc']/text()").getall() if u and u.strip()]

    def parse_sitemap_index(self, response):
        sitemap_urls = self._extract_sitemap_locs(response)
        self.logger.info("Asklepios sitemap-index: found %d sitemap URLs", len(sitemap_urls))
        # Be permissive: follow all sitemap URLs; filtering by a specific naming convention is brittle.
        for url in sitemap_urls:
            yield Request(url, callback=self.parse_profile_sitemap)

    def parse_profile_sitemap(self, response):
        locs = self._extract_sitemap_locs(response)
        if not locs:
            self.logger.warning("Asklepios sitemap: no <loc> found for %s", response.url)
            return

        # Track coverage for validation.
        self.sitemap_locs_total = int(getattr(self, "sitemap_locs_total", 0) or 0) + len(locs)

        scheduled = 0
        for url in locs:
            u = (url or "").strip()
            if not u:
                continue
            # Avoid scheduling obvious non-HTML assets.
            lower = u.lower()
            if any(lower.endswith(ext) for ext in (".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".pdf", ".xml", ".gz")):
                continue
            scheduled += 1
            yield Request(
                u,
                headers=self.profile_headers,
                callback=self.parse_profile,
                dont_filter=True,
            )

        self.sitemap_locs_scheduled = int(getattr(self, "sitemap_locs_scheduled", 0) or 0) + scheduled
        self.logger.info(
            "Asklepios sitemap: %d locs, scheduled %d URLs (%s)",
            len(locs),
            scheduled,
            response.url,
        )

    def parse_profile(self, response):
        # Many sitemap URLs are not physician profiles. Only parse pages that look like profiles.
        h1 = self._text(response.xpath("//h1/text()"))
        if not h1:
            self.logger.debug("Skipping non-profile page (no h1): %s", response.url)
            return

        clinics = []
        clinic_nodes = response.xpath('//article[@data-test-id="facility-teaser"]')
        for clinic in clinic_nodes:
            clinic_name = self._text(
                clinic.xpath('.//p[@class="text-[14px] truncate ..."]/text()')
            )
            clinic_type = self._text(clinic.xpath('.//h3/text()'))
            clinic_address = self._join_texts(
                clinic.xpath(
                    './/*[local-name()="svg"][@aria-label="Adresse"]/following-sibling::p/text()'
                )
            )
            clinic_phone = self._text(
                clinic.xpath(
                    './/*[local-name()="svg"][@aria-label="Telefonnummer"]/following-sibling::p/text()'
                )
            )
            clinic_data = (
                f"clinic_name: {clinic_name}\n"
                f"clinic_type: {clinic_type}\n"
                f"clinic_address: {clinic_address}\n"
                f"clinic_phone: {clinic_phone}"
            )
            clinics.append(clinic_data)

        clinic_1 = clinics[0] if len(clinics) > 0 else None
        clinic_2 = clinics[1] if len(clinics) > 1 else None

        yield {
            "url": response.url,
            "name": h1,
            "position": self._text(
                response.xpath('//span[text()="Position"]/following-sibling::span/text()')
            ),
            "area_of_responsibility": self._text(
                response.xpath(
                    '//span[text()="Zuständigkeitsbereich"]/following-sibling::span/text()'
                )
            ),
            "specialty": self._text(
                response.xpath('//span[text()="Facharzt"]/following-sibling::span/text()')
            ),
            "einrichtung": self._text(
                response.xpath(
                    '//span[text()="Einrichtung"]/following-sibling::span/text()'
                )
            ),
            "phone": self._text(
                response.xpath(
                    '//*[name()="svg"][@aria-label="Telefonnummer"]/following-sibling::text()'
                )
            ),
            "fax": self._text(
                response.xpath(
                    '//*[name()="svg"][@aria-label="Faxnummer"]/following-sibling::*/text()'
                )
            ),
            "career_highlights": self._join_texts(
                response.xpath(
                    '//div[@aria-labelledby="accordion-Höhepunkte der beruflichen Laufbahn-0-heading"]//p/text()'
                )
            ),
            "clinic_1": clinic_1,
            "clinic_2": clinic_2,
            "img_url": self._text(
                response.xpath('//meta[@property="og:image"]/@content')
            ),
            "llm_content": "",
            "field_membership": "",
        }
