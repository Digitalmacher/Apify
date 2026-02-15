# Asklepios physician profile directory.
# Discovery: sitemap-index.xml → profile sitemap → /profil/ pages.

from scrapy import Spider
from scrapy.http import Request


class AsklepiosSpider(Spider):
    name = "asklepios"
    allowed_domains = ["asklepios.com"]

    custom_settings = {
        "ROBOTSTXT_OBEY": False,
        "CONCURRENT_REQUESTS": 128,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 96,
        "DOWNLOAD_DELAY": 0,
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

    def parse_sitemap_index(self, response):
        for url in response.xpath("//text()").getall():
            url = (url or "").strip()
            if url and "/konzern@PROFILE-" in url:
                yield Request(url, callback=self.parse_profile_sitemap)

    def parse_profile_sitemap(self, response):
        for url in response.xpath("//text()").getall():
            url = (url or "").strip()
            if url and "/profil/" in url:
                yield Request(
                    url,
                    headers=self.profile_headers,
                    callback=self.parse_profile,
                )

    def parse_profile(self, response):
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
            "name": self._text(response.xpath("//h1/text()")),
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
