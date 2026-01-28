import os

os.environ.setdefault('SCRAPY_SETTINGS_MODULE', 'sven_scraping_projects.settings')

from src.main import main

if __name__ == '__main__':
    main()
