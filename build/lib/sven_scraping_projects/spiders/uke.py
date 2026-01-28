import json
from scrapy import Spider
from scrapy.shell import inspect_response
from scrapy.http import Request, FormRequest
from scrapy.utils.response import open_in_browser


def extract_text(response, xpath):
    if xpath == '//div[@class="name"]/text()':
        return ' '.join([name for name in response.xpath(xpath).get().split()])
    else: 
        return (response.xpath(xpath).get() or '').strip()


def extract_list(response, xpath):
    return ', '.join(
        t.strip()
        for t in response.xpath(xpath).getall()
        if t.strip()
    )

class UkeSpider(Spider):
    name = 'uke'
    allowed_domains = ['uke.de']
    start_urls = ['https://www.uke.de/searchadapter/search?q=UKE+-+Arztprofil&p=1&n=100&t=RAW&f=json&q.template=ARZTPROFIL&q.language=de&hl.count=1&hl.size=300&hl.prefix=%3Cmark%3E&hl.suffix=%3C/mark%3E&p.url=https://www.uke.de/suchergebnisseite/suchergebnis-arztprofilseite.html?q=UKE+-+Arztprofil&p=0&l=de&t=1']

    def parse(self, response):
        # open_in_browser(response)
        # inspect_response(response, self)

        jsonresponse = json.loads(response.text)

        profiles = jsonresponse.get('response').get('hits')
        for profile in profiles:
            profile_url = profile.get('url')

            yield Request(profile_url,
                          callback=self.parse_profile)

    def parse_profile(self, response):
        # open_in_browser(response)
        # inspect_response(response, self)

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

            "url": response.url,

        }

        yield item
