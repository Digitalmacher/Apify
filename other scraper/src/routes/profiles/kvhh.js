import { Actor } from 'apify';
import { CheerioCrawler } from 'crawlee';

import { extractDtDdLinesJoined, extractDtDdText, parseDoctorName } from '../../utils/kvhh_extractors.js';

export async function profileKvhh({ urls }) {
    const urlList = Array.isArray(urls) ? urls : [urls];
    const dataset = await Actor.openDataset('final-doctors-dataset');
    const crawler = new CheerioCrawler({
        maxRequestRetries: 0,
        maxConcurrency: 20,
        async requestHandler({
            $,
            request,
            log: _log,
        }) {
            const { title, firstName, lastName } = parseDoctorName($('h1').text().trim());
            const phone = $("a[href^='tel:']").first().text().trim();
            const email = $("a[href^='mailto:']").first().attr('href').trim().substring(7);
            const specialty = extractDtDdText($, 'Fachgebiet');

            await dataset.pushData({
                source: 'kvhh',
                url: request.url,
                title,
                firstName,
                lastName,
                position: specialty,
                areaOfWork: specialty,
                department: specialty,
                phone,
                email,
                languages: extractDtDdLinesJoined($, 'Fremdsprachen'),
                specialization: specialty,
                mainAreasOfActivity: extractDtDdLinesJoined($, 'Leistungen'),
                llmContent: '',
                fieldMembership: '',
            });

        },
    });

    await crawler.run(urlList);
}