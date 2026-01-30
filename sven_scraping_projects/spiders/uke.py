import json
from scrapy import Spider
from scrapy.http import Request
from scrapy.shell import inspect_response
from scrapy.utils.response import open_in_browser


def extract_text(response, xpath):
    """
    Extract a single text value from an XPath and normalize it.

    Special case:
    - For the 'name' field, collapse multiple whitespaces into a single space
      (names sometimes contain line breaks or extra spacing).
    """
    if xpath == '//div[@class="name"]/text()':
        return ' '.join([name for name in response.xpath(xpath).get().split()])
    else:
        # Safe extraction: return empty string if element is missing
        return (response.xpath(xpath).get() or '').strip()


def extract_list(response, xpath):
    """
    Extract multiple text nodes, strip whitespace,
    remove empty values, and join into a comma-separated string.
    """
    return ', '.join(
        t.strip()
        for t in response.xpath(xpath).getall()
        if t.strip()
    )


class UkeSpider(Spider):
    name = 'uke'
    allowed_domains = ['uke.de']

    def __init__(self):
        self.headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "en-US,en;q=0.9,pl;q=0.8,de;q=0.7,sr;q=0.6,bs;q=0.5,nl;q=0.4",
            "Cache-Control": "max-age=0",
            "Connection": "keep-alive",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
            "sec-ch-ua": "\"Google Chrome\";v=\"143\", \"Chromium\";v=\"143\", \"Not A(Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Linux\""
        }

        self.profile_headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "en-US,en;q=0.9,pl;q=0.8,de;q=0.7,sr;q=0.6,bs;q=0.5,nl;q=0.4",
            "Cache-Control": "max-age=0",
            "Connection": "keep-alive",
            "If-Modified-Since": "Tue, 27 Jan 2026 06:05:36 GMT",
            "If-None-Match": "\"8c00bb-79115-64958697ccc00\"",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
            "sec-ch-ua": "\"Google Chrome\";v=\"143\", \"Chromium\";v=\"143\", \"Not A(Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Linux\""
        }

    # Spider-specific settings (override project-wide defaults)
    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'CONCURRENT_REQUESTS': 16,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 16,
        'DOWNLOAD_DELAY': 0.5,
        'RETRY_ENABLED': True,
        'RETRY_TIMES': 3,
        'DOWNLOAD_TIMEOUT': 180,
        'URLLENGTH_LIMIT': 500,
    }

    def start_requests(self):
        """
        Entry point of the spider.

        This endpoint returns a JSON payload containing profile URLs
        for all doctor profiles.
        """

        yield Request(
            url='https://www.uke.de/searchadapter/search?q=UKE+-+Arztprofil&p=1&n=10000&t=RAW&f=json&q.template=ARZTPROFIL&q.language=de&hl.count=1&hl.size=300&hl.prefix=%3Cmark%3E&hl.suffix=%3C/mark%3E&p.url=https://www.uke.de/suchergebnisseite/suchergebnis-arztprofilseite.html?q=UKE+-+Arztprofil&p=0&l=de&t=1',
            method='GET',
            dont_filter=True,
            headers=self.headers,
            callback=self.parse
        )

    def parse(self, response):
        # Debug helpers (uncomment when inspecting HTML/JSON responses)
        # open_in_browser(response)
        # inspect_response(response, self)

        # Parse raw JSON response manually
        jsonresponse = json.loads(response.text)

        # Extract list of profile result objects
        response_data = jsonresponse.get('response', {})
        profiles = response_data.get('hits', [])

        # Log pagination progress
        current_page = self._extract_page_number(response.url)
        num_profiles = len(profiles)
        self.logger.info(f'Processing page {current_page}: found {num_profiles} profiles')

        for profile in profiles:
            # Each hit contains a direct URL to the profile page
            profile_url = profile.get('url')

            # Request individual profile pages
            yield Request(
                profile_url,
                headers=self.profile_headers,
                callback=self.parse_profile
            )

        # Check for pagination and follow next page if available
        next_page_url = self._get_next_page_url(response.url, jsonresponse, num_profiles)
        if next_page_url:
            self.logger.info(f'Following pagination to next page: {next_page_url}')
            yield Request(
                next_page_url,
                method='GET',
                dont_filter=True,
                headers=self.headers,
                callback=self.parse
            )

    def _extract_page_number(self, url):
        """Extract current page number from URL."""
        try:
            from urllib.parse import urlparse, parse_qs
            parsed = urlparse(url)
            params = parse_qs(parsed.query)
            page = params.get('p', ['1'])[0]
            return int(page)
        except (ValueError, KeyError, IndexError):
            return 1

    def _get_next_page_url(self, current_url, jsonresponse, num_profiles):
        """
        Determine if there's a next page and return its URL.
        Checks multiple pagination patterns:
        1. Explicit pagination metadata in JSON response
        2. If we got exactly 10000 results, there might be more pages
        3. Total count vs current results
        """
        try:
            from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

            # Extract current page number
            current_page = self._extract_page_number(current_url)

            # Check for explicit pagination metadata
            response_data = jsonresponse.get('response', {})
            
            # Pattern 1: Check for pagination object
            pagination = response_data.get('pagination', {})
            total_pages = pagination.get('totalPages')
            if total_pages and current_page < total_pages:
                return self._build_next_page_url(current_url, current_page + 1)

            # Pattern 2: Check for total results vs current page results
            num_found = response_data.get('numFound')
            if num_found:
                # Calculate expected total pages (assuming n=10000 per page)
                expected_total_pages = (num_found + 9999) // 10000  # Ceiling division
                if current_page < expected_total_pages:
                    return self._build_next_page_url(current_url, current_page + 1)

            # Pattern 3: If we got exactly 10000 results, assume there might be more
            # (This is a heuristic - the API might return exactly 10000 even if it's the last page)
            if num_profiles == 10000:
                # Try next page - if it returns 0 results, Scrapy will naturally stop
                return self._build_next_page_url(current_url, current_page + 1)

            return None

        except Exception as e:
            self.logger.warning(f'Error determining next page URL: {e}')
            return None

    def _build_next_page_url(self, current_url, next_page):
        """Build URL for next page by updating the 'p' parameter."""
        try:
            from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

            parsed = urlparse(current_url)
            params = parse_qs(parsed.query)
            params['p'] = [str(next_page)]
            
            new_query = urlencode(params, doseq=True)
            new_url = urlunparse((
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                parsed.params,
                new_query,
                parsed.fragment
            ))
            return new_url
        except Exception as e:
            self.logger.warning(f'Error building next page URL: {e}')
            return None

    def parse_profile(self, response):
        # Debug helpers for individual profile pages
        # open_in_browser(response)
        # inspect_response(response, self)

        # Build structured item from profile page
        item = {
            "name": extract_text(response, '//div[@class="name"]/text()'),

            "title": extract_text(response, '//div[@class="title"]/text()'),

            "department": extract_text(response, '//div[@class="department"]/text()'),

            "specialties": extract_list(
                response, '//ul[@class="description"]/li/text()'
            ),

            "work_area": extract_list(
                response, '//div[@class="main-contact-container "]//li//text()'
            ),

            "telephone": extract_text(
                response,
                '//div[@class="contact-label"][contains(text(), "Telefon")]/following-sibling::div/*/text()'
            ),

            "fax": extract_text(
                response,
                '//div[@class="contact-label"][contains(text(), "Telefax")]/following-sibling::div/*/text()'
            ),

            "email": extract_text(
                response,
                '//div[@class="contact-label"][contains(text(), "E-Mail")]/following-sibling::div/*/text()'
            ),

            "location": extract_text(
                response,
                '//div[contains(text(), "Standort")]/following-sibling::div//div[@class="contact-data"]/text()'
            ),

            "languages": extract_list(
                response,
                '//div[contains(text(), "Sprachen")]/following-sibling::div//div[@class="contact-data"]/text()'
            ),

            "areas_of_expertise": extract_list(
                response,
                '//h2[contains(text(), "Fachgebiete")]/following-sibling::ul[1]//span/text()'
            ),

            "areas_of_activity": extract_list(
                response,
                '//h2[contains(text(), "Tätigkeitsschwerpunkte")]/following-sibling::ul[1]//span/text()'
            ),

            # Canonical profile URL
            "url": response.url,
        }

        yield item
