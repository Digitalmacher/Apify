import { Actor } from 'apify';
import { discoverKvhh } from '../kvhh.js';
import { profileKvhh } from '../profiles/kvhh.js';

export async function runKvhhPipeline() {
    const log = Actor.log;

    // 1. Run discovery → writes into default dataset for this run
    await discoverKvhh();

    // 2. Read ALL items from the discovery dataset (default)
    const discoveryDataset = await Actor.openDataset('discover-kvhh');
    const allDoctors = [];

    let offset = 0;
    const limit = 100; // CHANGE IN FUTURE

    while (true) {
        const { items, total } = await discoveryDataset.getData({ offset, limit });
        allDoctors.push(...items);
        offset += items.length;
        if (offset >= total || items.length === 0) break;
    }


    // 3. Collect unique profile URLs
    const urlSet = new Set(
        allDoctors
            .map((d) => d.scraping_profile_url)
            .filter(Boolean)
    );
    const urls = Array.from(urlSet);

    if (urls.length === 0) {
        return;
    }

    // 4. Run profile scraper on ALL URLs (with concurrency inside profileUKE)
    await profileKvhh({ urls: urls });

    return true;
}