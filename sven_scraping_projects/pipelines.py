from itemadapter import ItemAdapter
from apify import Actor


class SvenScrapingProjectsPipeline:
    def process_item(self, item, spider):
        return item


class ApifyPipeline:

    
    def __init__(self):
        self.items = []
        self._apify_available = False
    
    def open_spider(self, spider):
        # Check if Apify Actor is available
        try:
            self._apify_available = True
            Actor.log.info(f'ApifyPipeline: Spider {spider.name} opened')
        except Exception:
            self._apify_available = False
            import logging
            logging.warning('ApifyPipeline: Apify Actor not available, items will be collected but not pushed')
        
        self.items = []
    
    def process_item(self, item, spider):

        if hasattr(item, 'asdict'):
            item_dict = item.asdict()
        elif isinstance(item, dict):
            item_dict = dict(item)
        else:
            adapter = ItemAdapter(item)
            item_dict = dict(adapter)

        # Add source so we can identify which spider produced each record
        item_dict['source'] = spider.name
        self.items.append(item_dict)

        return item
    
    def close_spider(self, spider):
        if not self.items:
            if self._apify_available:
                Actor.log.info(f'ApifyPipeline: No items to push for spider {spider.name}')
            return
        
        if not self._apify_available:
            import logging
            logging.warning(f'ApifyPipeline: {len(self.items)} items collected but Apify Actor not available')
            return
        
        Actor.log.info(f'ApifyPipeline: Pushing {len(self.items)} items to Apify dataset...')
        
        try:

            import asyncio
            
            async def push_all_items():
                """Push all items to Apify dataset."""
                for item in self.items:
                    await Actor.push_data(item)
            
            asyncio.run(push_all_items())
            
            Actor.log.info(f'ApifyPipeline: Successfully pushed {len(self.items)} items to dataset')
        except Exception as e:
            Actor.log.error(f'ApifyPipeline: Error pushing items to dataset: {e}')
            Actor.log.error(f'ApifyPipeline: Failed to push {len(self.items)} items')
        
        Actor.log.info(f'ApifyPipeline: Spider {spider.name} closed')
