import {
	Actor
} from 'apify';
import {
	CheerioCrawler
} from 'crawlee';
import cheerio from 'cheerio';

const TITLES = [
    "Dr. med.",
    "Dr.",
    "Prof. Dr. med.",
    "Prof. Dr.",
    "Dipl.-Psych.",
    "PD Dr. med.",
    "PD Dr.",
    "Med. pract.",
];

function parseDoctorName(rawName) {
    if (!rawName) return { title: null, firstName: null, lastName: null };

    let name = rawName.trim();
    let matchedTitle = null;

    // 1. Detect the correct title (longest first to avoid “Dr.” matching before “Dr. med.”)
    for (const t of TITLES) {
        if (name.startsWith(t)) {
            matchedTitle = t;
            name = name.replace(t, "").trim();  // Remove title from the string
            break;
        }
    }

    // 2. Split remaining full name
    const parts = name.split(/\s+/);

    let firstName = null;
    let lastName = null;

    if (parts.length === 1) {
        // Only one name given
        firstName = parts[0];
    } else {
        firstName = parts[0];
        lastName = parts.slice(1).join(" ");
    }

    return {
        title: matchedTitle,
        firstName,
        lastName
    };
}

function extractLanguages($) {
    const dt = $('dt').filter(function () {
        return $(this).text().trim().includes("Fremdsprachen");
    }).first();

    if (!dt.length) return null;

    // 2. The <dd> is the next sibling
    const dd = dt.next('dd');

    if (!dd.length) return null;

    // 3. Extract the languages, split by <br>, remove whitespace
    const languages = dd
        .html()                          // includes "<br>"
        .split(/<br\s*\/?>/i)            // split by <br> tags
        .map(l => cheerio.load(l).text().trim()) // remove tags + spaces
        .filter(Boolean);                // remove empty entries

    return languages.join(', ');
}

function extractMainAreasOfActivity($) {
    const dt = $('dt').filter(function () {
        return $(this).text().trim().includes("Leistungen");
    }).first();

    if (!dt.length) return null;

    // 2. The <dd> is the next sibling
    const dd = dt.next('dd');

    if (!dd.length) return null;

    // 3. Extract the languages, split by <br>, remove whitespace
    const mainAreasOfActivity = dd
        .html()                          // includes "<br>"
        .split(/<br\s*\/?>/i)            // split by <br> tags
        .map(l => cheerio.load(l).text().trim()) // remove tags + spaces
        .filter(Boolean);                // remove empty entries

    return mainAreasOfActivity.join(', ');
}

function extractSpecialty($) {
    const dt = $('dt').filter(function () {
        return $(this).text().trim().startsWith("Fachgebiet");
    }).first();

    if (!dt.length) return null;

    const dd = dt.next('dd');
    if (!dd.length) return null;

    return dd.text().trim();
}


export async function profileKvhh({urls}) {
    const urlList = Array.isArray(urls) ? urls : [urls];
    const dataset = await Actor.openDataset('final-doctors-dataset');
    const crawler = new CheerioCrawler({
        maxRequestRetries: 0,
        maxConcurrency: 20,
        async requestHandler({
            $,
            request,
            log
        }) {
            const parseName = parseDoctorName($('h1').text().trim());
            const title = parseName.title;
            const firstName = parseName.firstName;
            const lastName = parseName.lastName;
            const phone = $("a[href^='tel:']").first().text().trim();
            const email = $("a[href^='mailto:']").first().attr('href').trim().substring(7);

            await dataset.pushData({
                source: 'kvhh',
                url: request.url,
                title,
                firstName,
                lastName,
                position: extractSpecialty($),
                areaOfWork: extractSpecialty($),
                department: extractSpecialty($),
                phone,
                email,
                languages: extractLanguages($),
                specialization: extractSpecialty($),
                mainAreasOfActivity: extractMainAreasOfActivity($),
                llmContent: '',
                fieldMembership: ''
            });

        },
    });

    await crawler.run(urlList);
};