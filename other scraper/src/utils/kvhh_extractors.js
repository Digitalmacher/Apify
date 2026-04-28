import cheerio from 'cheerio';

const TITLES = [
    'Prof. Dr. med.',
    'Prof. Dr.',
    'PD Dr. med.',
    'PD Dr.',
    'Dr. med.',
    'Dr.',
    'Dipl.-Psych.',
    'Med. pract.',
];

export function parseDoctorName(rawName) {
    if (!rawName) return { title: null, firstName: null, lastName: null };

    let name = rawName.trim();
    let matchedTitle = null;

    // Longest first to avoid “Dr.” matching before “Dr. med.”
    for (const t of TITLES) {
        if (name.startsWith(t)) {
            matchedTitle = t;
            name = name.slice(t.length).trim();
            break;
        }
    }

    const parts = name.split(/\s+/).filter(Boolean);
    if (parts.length === 0) return { title: matchedTitle, firstName: null, lastName: null };
    if (parts.length === 1) return { title: matchedTitle, firstName: parts[0], lastName: null };

    return {
        title: matchedTitle,
        firstName: parts[0],
        lastName: parts.slice(1).join(' '),
    };
}

function findFirstDtContaining($, labelSubstring) {
    return $('dt')
        .filter(function () {
            return $(this).text().trim().includes(labelSubstring);
        })
        .first();
}

export function extractDtDdText($, labelSubstring) {
    const dt = findFirstDtContaining($, labelSubstring);
    if (!dt.length) return null;
    const dd = dt.next('dd');
    if (!dd.length) return null;
    const text = dd.text().trim();
    return text || null;
}

export function extractDtDdLinesJoined($, labelSubstring) {
    const dt = findFirstDtContaining($, labelSubstring);
    if (!dt.length) return null;
    const dd = dt.next('dd');
    if (!dd.length) return null;
    const html = dd.html();
    if (!html) return null;
    const parts = html
        .split(/<br\s*\/?>/i)
        .map((l) => cheerio.load(l).text().trim())
        .filter(Boolean);
    if (!parts.length) return null;
    return parts.join(', ');
}

