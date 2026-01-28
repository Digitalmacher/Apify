FROM apify/actor-python:3.11

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

COPY . ./

WORKDIR /usr/src/app

ENV SCRAPY_SETTINGS_MODULE=sven_scraping_projects.settings

CMD python -m src
