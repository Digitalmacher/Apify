from scrapy import Spider
from scrapy.http import Request
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError, TimeoutError, TCPTimedOutError
from twisted.web._newclient import ResponseNeverReceived


class ApothekerkammerHamburgSpider(Spider):
    name = "apothekerkammer-hamburg"
    allowed_domains = ["portal.apothekerkammer-hamburg.de"]

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
            errback=self.errback_http,
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
                # Normalize to absolute for stability + downstream matching.
                "url": response.urljoin(self._text(listing.xpath(".//h3/a/@href"))),
            }

        # Pagination
        next_page = response.xpath('//a[@class="next page-numbers"]/@href').get()
        if next_page:
            yield response.follow(
                next_page,
                headers=self.page_headers,
                callback=self.parse,
                errback=self.errback_http,
            )

    def errback_http(self, failure):
        """
        Centralized logging for failed/blocked requests.
        This makes scheduled runs debuggable when the site changes or blocks.
        """
        request = getattr(failure, "request", None)
        url = getattr(request, "url", None) if request is not None else None

        if failure.check(HttpError):
            response = failure.value.response
            self.logger.warning("HTTP error: status=%s url=%s", response.status, response.url)
            return
        if failure.check(DNSLookupError):
            self.logger.error("DNSLookupError on %s", url)
            return
        if failure.check(TimeoutError, TCPTimedOutError):
            self.logger.warning("Timeout on %s", url)
            return
        if failure.check(ResponseNeverReceived):
            self.logger.warning("ResponseNeverReceived on %s", url)
            return

        self.logger.error("Request failed: %r url=%s", failure, url)
