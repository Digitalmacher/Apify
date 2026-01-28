import os
import asyncio
from apify import Actor
from scrapy.crawler import CrawlerRunner
from scrapy.utils.project import get_project_settings
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks


@inlineCallbacks
def run_spider(spider_name, settings):
    runner = CrawlerRunner(settings)
    yield runner.crawl(spider_name)
    reactor.stop()

def main():

    try:
        asyncio.run(Actor.init())
    except Exception as e:

        try:
            Actor.log.warning(f'Could not initialize Apify Actor: {e}')
        except Exception:
            pass

    Actor.log.info('Starting Scrapy spider on Apify...')

    spider_name = 'uke' 
    
    try:
        input_data = asyncio.run(Actor.get_input()) or {}
        spider_name = input_data.get('spider_name', spider_name)
    except Exception as e:
        Actor.log.warning(f'Could not get input from Actor: {e}')
        spider_name = os.environ.get('APIFY_INPUT_SPIDER_NAME', spider_name)
    
    Actor.log.info(f'Running spider: {spider_name}')
    
    settings = get_project_settings()
    

    run_spider(spider_name, settings)
    reactor.run()
    
    Actor.log.info(f'Spider {spider_name} completed successfully')
    try:
        asyncio.run(Actor.exit())
    except Exception as e:
        try:
            Actor.log.warning(f'Could not shut down Apify Actor cleanly: {e}')
        except Exception:
            pass


if __name__ == '__main__':
    main()
