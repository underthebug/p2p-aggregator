// Link safety checks applied to every card link. Runs locally and never contacts the site.
(function (root) {
    const TRACKING_EXACT = new Set([
        'fbclid', 'gclid', 'dclid', 'msclkid', 'yclid', 'twclid', 'wbraid', 'gbraid', 'igshid',
        'mc_eid', 'mc_cid', 'mkt_tok', '_hsenc', '_hsmi', 'vero_id', 'oly_enc_id', 'li_fat_id',
        'ref', 'ref_src', 'ref_url', 'at_medium', 'at_campaign', 'cmpid', 'ocid', 'ncid', 'taid', 's_cid'
    ]);
    const TRACKING_PREFIXES = ['utm_', 'ns_', 'at_custom', 'pk_', 'hsa_'];
    const SHORTENERS = [
        'bit.ly', 't.co', 'tinyurl.com', 'goo.gl', 'ow.ly', 'is.gd', 'buff.ly', 'rebrand.ly', 'cutt.ly',
        't.ly', 'lnkd.in', 'fb.me', 'amzn.to', 'shorturl.at', 'rb.gy', 'tiny.cc', 'bl.ink', 'dlvr.it', 'trib.al'
    ];

    function isTrackingParam(name) {
        const lowered = name.toLowerCase();
        return TRACKING_EXACT.has(lowered) || TRACKING_PREFIXES.some(prefix => lowered.startsWith(prefix));
    }

    // Returns the link without tracking parameters, plus what was removed and any warning flags.
    function analyzeLink(raw) {
        let url;
        try {
            url = new URL(raw);
        } catch (e) {
            return { valid: false, original: raw, cleanUrl: raw, removed: [], flags: [] };
        }
        if (url.protocol !== 'http:' && url.protocol !== 'https:') {
            return { valid: false, original: raw, cleanUrl: raw, removed: [], flags: [] };
        }

        const removed = [];
        const kept = new URLSearchParams();
        for (const [name, value] of url.searchParams) {
            if (isTrackingParam(name)) removed.push(name);
            else kept.append(name, value);
        }
        const cleaned = new URL(url.href);
        cleaned.search = kept.toString();

        const host = url.hostname.toLowerCase();
        const flags = [];
        if (SHORTENERS.some(domain => host === domain || host.endsWith('.' + domain))) {
            flags.push('Shortened link: the real destination is hidden');
        }
        if (url.username || url.password) {
            flags.push('Contains a username or password before the host');
        }
        if (/^\d{1,3}(\.\d{1,3}){3}$/.test(host) || host.startsWith('[')) {
            flags.push('Points to a raw IP address');
        }
        if (host.split('.').some(label => label.startsWith('xn--'))) {
            flags.push('Look-alike (punycode) domain name');
        }

        return { valid: true, original: raw, cleanUrl: cleaned.href, removed, flags };
    }

    root.analyzeLink = analyzeLink;
    if (typeof module !== 'undefined' && module.exports) module.exports = { analyzeLink };
})(typeof window !== 'undefined' ? window : globalThis);
