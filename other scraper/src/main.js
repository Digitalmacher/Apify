import { Actor } from 'apify';
import { Dataset } from 'crawlee';

import { runKvhhPipeline } from './routes/pipelines/kvhh_pipeline.js';
import { runMarienkrankenhausPipeline } from './routes/pipelines/marienkrankenhaus_pipeline.js';
import { runUKEPipeline } from './routes/pipelines/uke_pipeline.js';

await Actor.init();

const DATASETS_TO_RESET = [
    'discover-marienkrankenhaus',
    'discover-kvhh',
    'final-doctors-dataset',
];

for (const dsName of DATASETS_TO_RESET) {
    const ds = await Dataset.open(dsName);
    await ds.drop();
}

await runUKEPipeline();
await runMarienkrankenhausPipeline();
await runKvhhPipeline();

await Actor.exit();