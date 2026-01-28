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


async def get_actor_input():
    try:
        input_data = await Actor.get_input() or {}
        return input_data
    except Exception as e:
        Actor.log.warning(f'Could not get Actor input: {e}')
        return {}


def main():

    Actor.log.info('Starting Scrapy spider on Apify...')

    spider_name = 'uke' 
    
    try:
        input_data = asyncio.run(get_actor_input())
        spider_name = input_data.get('spider_name', spider_name)
    except Exception as e:
        Actor.log.warning(f'Could not get input from Actor: {e}')
        spider_name = os.environ.get('APIFY_INPUT_SPIDER_NAME', spider_name)
    
    Actor.log.info(f'Running spider: {spider_name}')
    
    settings = get_project_settings()
    

    run_spider(spider_name, settings)
    reactor.run()
    
    Actor.log.info(f'Spider {spider_name} completed successfully')


if __name__ == '__main__':
    main()
