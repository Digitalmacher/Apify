import { Actor } from 'apify';
import cheerio from 'cheerio';
import axios from 'axios';

export async function discoverKvhh() {
    const dataset = await Actor.openDataset('discover-kvhh');

    const sitemapUrl = 'https://www.kvhh.net/de/sitemap.xml';

    // 1. Fetch sitemap
    const res = await axios.get(sitemapUrl);
    const $ = cheerio.load(res.data, { xmlMode: true });

    // 2. Extract all <loc> URLs
    let allUrls = [];
    $('loc').each((i, el) => {
        allUrls.push($(el).text().trim());
    });

    // 3. Filter only doctor profile URLs
    const doctorUrls = allUrls.filter(url =>
        url.startsWith('https://www.kvhh.net/de/medicalregister/net-kvhh-physician-')
    );

    console.log(`Found ${doctorUrls.length} doctor profile URLs`);

    // 4. Store each one in dataset
    for (const url of doctorUrls) {
        await dataset.pushData({
            source: 'kvhh',
            scraping_profile_url: url
        });
    }

    console.log('Done saving KVHH doctor URLs.');
}