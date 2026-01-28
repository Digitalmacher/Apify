import json
from scrapy import Spider
from scrapy.http import Request
from scrapy.shell import inspect_response
from scrapy.utils.response import open_in_browser


class ApothekerkammerHamburgSpider(Spider):
    name = "apothekerkammer-hamburg"
    allowed_domains = ["portal.apothekerkammer-hamburg.de"]
    handle_httpstatus_list = [404]

    custom_settings = {
        "ROBOTSTXT_OBEY": False,
        "CONCURRENT_REQUESTS": 16,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 16,
        "DOWNLOAD_DELAY": 0.5,
        "USER_AGENT": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36"
        ),
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Headers reused for initial page + pagination
        self.page_headers = {
            "accept": (
                "text/html,application/xhtml+xml,application/xml;q=0.9,"
                "image/avif,image/webp,image/apng,*/*;q=0.8"
            ),
            "accept-language": "en-US,en;q=0.9,de;q=0.7",
            "cache-control": "max-age=0",
            "upgrade-insecure-requests": "1",
            "user-agent": self.custom_settings["USER_AGENT"],
        }

    def start_requests(self):
        yield Request(
            url="https://portal.apothekerkammer-hamburg.de/apothekenfinder/",
            headers=self.page_headers,
            callback=self.parse,
        )

    # ------------------------
    # Helper extraction methods
    # ------------------------

    @staticmethod
    def _text(selector):
        """Extract single text value and strip safely."""
        return (selector.get() or "").strip()

    @staticmethod
    def _join_texts(selector):
        """Extract multiple text nodes, strip, and join."""
        return ", ".join(
            t.strip() for t in selector.getall() if t.strip()
        )

    # ------------------------
    # Main parsing logic
    # ------------------------

    def parse(self, response):
        # open_in_browser(response)
        # inspect_response(response, self)

        listings = response.xpath(
            '//div[@class="container mt-3"]'
            '//div[@class="searchhit-icon searchhit-icon-site"]/parent::div'
        )

        for listing in listings:
            yield {
                "name": self._text(listing.xpath(".//h3/a/text()")),

                "address": self._join_texts(
                    listing.xpath(
                        './/label[text()="Anschrift"]/parent::div/following-sibling::div[1]/span[1]/text()',
                    )
                ),
                "phone": self._text(
                    listing.xpath(
                        './/label[text()="Telefon"]'
                        "/parent::div/following-sibling::div/span/text()"
                    )
                ),
                "fax": self._text(
                    listing.xpath(
                        './/label[text()="Fax"]'
                        "/parent::div/following-sibling::div/span/text()"
                    )
                ),
                "email": self._text(
                    listing.xpath(
                        './/label[text()="E-Mail"]'
                        "/parent::div/following-sibling::div/span/a/text()"
                    )
                ),
                "website": self._text(
                    listing.xpath(
                        './/label[text()="Internet"]'
                        "/parent::div/following-sibling::div/span/a/@href"
                    )
                ),
                "url": self._text(listing.xpath(".//h3/a/@href")),
            }

        # Pagination
        next_page = response.xpath('//a[@class="next page-numbers"]/@href').get()
        if next_page:
            yield response.follow(
                next_page,
                headers=self.page_headers,
                callback=self.parse,
            )
