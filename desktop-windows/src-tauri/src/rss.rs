//! RSS and Atom fetching, parsing, and OPML helpers shared by the desktop app and the daemon.

use serde::Serialize;
use quick_xml::events::Event;
use quick_xml::escape::resolve_predefined_entity;
use quick_xml::reader::NsReader;
use quick_xml::XmlVersion;
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

/// Upper bound on items read from a single feed.
pub const MAX_FEED_ITEMS: usize = 50;

#[derive(Debug, Clone, Serialize)]
pub struct FeedItem {
    pub title: String,
    pub link: String,
    pub pub_date: String,
}

pub fn is_http_url(url: &str) -> bool {
    url.starts_with("http://") || url.starts_with("https://")
}

/// Parses RSS 2.0, RDF/RSS 1.0, and Atom entries by local XML name.
pub fn parse_feed_items(text: &str) -> Result<Vec<FeedItem>, String> {
    let mut reader = NsReader::from_str(text);
    reader.config_mut().trim_text(false);
    let mut depth = 0usize;
    let mut entry_depth = None;
    let mut current: Option<FeedItem> = None;
    let mut capture: Option<(String, usize)> = None;
    let mut capture_text = String::new();
    let mut items = Vec::new();

    loop {
        let (_, event) = reader.read_resolved_event().map_err(|error| format!("Invalid feed XML: {error}"))?;
        match event {
            Event::Start(element) => {
                depth += 1;
                let local = element.local_name();
                if matches!(local.as_ref(), "item" | "entry") && current.is_none() {
                    current = Some(FeedItem { title: String::new(), link: String::new(), pub_date: String::new() });
                    entry_depth = Some(depth);
                    for attribute in element.attributes() {
                        let attribute = attribute.map_err(|error| format!("Invalid feed XML attribute: {error}"))?;
                        if attribute.key.local_name().as_ref() == "about" {
                            current.as_mut().unwrap().link = attribute
                                .normalized_value(XmlVersion::Implicit1_0)
                                .map_err(|error| error.to_string())?
                                .into_owned();
                        }
                    }
                    continue;
                }
                let Some(item) = current.as_mut() else { continue };
                if local.as_ref() == "link" {
                    let mut href = None;
                    let mut rel = None;
                    for attribute in element.attributes() {
                        let attribute = attribute.map_err(|error| format!("Invalid feed XML attribute: {error}"))?;
                        match attribute.key.local_name().as_ref() {
                            "href" => href = Some(attribute.normalized_value(XmlVersion::Implicit1_0).map_err(|error| error.to_string())?.into_owned()),
                            "rel" => rel = Some(attribute.normalized_value(XmlVersion::Implicit1_0).map_err(|error| error.to_string())?.into_owned()),
                            _ => {}
                        }
                    }
                    if let Some(href) = href {
                        if item.link.is_empty() || rel.as_deref().is_none_or(|value| value == "alternate") {
                            item.link = href;
                        }
                    } else if item.link.is_empty() && capture.is_none() {
                        capture = Some(("link".to_string(), depth));
                        capture_text.clear();
                    }
                } else if capture.is_none() {
                    let field = match local.as_ref() {
                        "title" if item.title.is_empty() => Some("title"),
                        "pubDate" | "published" | "updated" | "date" if item.pub_date.is_empty() => Some("pub_date"),
                        _ => None,
                    };
                    if let Some(field) = field {
                        capture = Some((field.to_string(), depth));
                        capture_text.clear();
                    }
                }
            }
            Event::Empty(element) => {
                let local = element.local_name();
                if local.as_ref() == "link" {
                    if let Some(item) = current.as_mut() {
                        let mut href = None;
                        let mut rel = None;
                        for attribute in element.attributes() {
                            let attribute = attribute.map_err(|error| format!("Invalid feed XML attribute: {error}"))?;
                            match attribute.key.local_name().as_ref() {
                                "href" => href = Some(attribute.normalized_value(XmlVersion::Implicit1_0).map_err(|error| error.to_string())?.into_owned()),
                                "rel" => rel = Some(attribute.normalized_value(XmlVersion::Implicit1_0).map_err(|error| error.to_string())?.into_owned()),
                                _ => {}
                            }
                        }
                        if let Some(href) = href {
                            if item.link.is_empty() || rel.as_deref().is_none_or(|value| value == "alternate") {
                                item.link = href;
                            }
                        }
                    }
                }
            }
            Event::Text(text_event) => {
                if capture.is_some() {
                    capture_text.push_str(&text_event.xml_content(XmlVersion::Implicit1_0));
                }
            }
            Event::CData(text_event) => {
                if capture.is_some() {
                    capture_text.push_str(&text_event.xml_content(XmlVersion::Implicit1_0));
                }
            }
            Event::GeneralRef(reference) => {
                if capture.is_some() {
                    if let Some(character) = reference.resolve_char_ref().map_err(|error| error.to_string())? {
                        capture_text.push(character);
                    } else if let Some(value) = resolve_predefined_entity(reference.as_ref()) {
                        capture_text.push_str(value);
                    } else {
                        return Err(format!("Unsupported XML entity &{};", reference.as_ref()));
                    }
                }
            }
            Event::End(element) => {
                if let Some((field, field_depth)) = capture.as_ref() {
                    if *field_depth == depth {
                        let value = capture_text.trim().to_string();
                        if let Some(item) = current.as_mut() {
                            match field.as_str() {
                                "title" => item.title = value,
                                "link" => item.link = value,
                                "pub_date" if item.pub_date.is_empty() => item.pub_date = value,
                                _ => {}
                            }
                        }
                        capture = None;
                        capture_text.clear();
                    }
                }
                if entry_depth == Some(depth) && matches!(element.local_name().as_ref(), "item" | "entry") {
                    if let Some(item) = current.take() {
                        if !item.title.is_empty() {
                            items.push(item);
                            if items.len() >= MAX_FEED_ITEMS {
                                break;
                            }
                        }
                    }
                    entry_depth = None;
                }
                depth = depth.saturating_sub(1);
            }
            Event::Eof => break,
            _ => {}
        }
    }
    Ok(items)
}

/// Hard cap on the response body so a hostile or broken server cannot exhaust memory.
const MAX_FEED_BYTES: usize = 5 * 1024 * 1024;

fn is_blocked_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            ip.is_private()
                || ip.is_loopback()
                || ip.is_link_local()
                || ip.is_unspecified()
                || ip.is_broadcast()
                || ip.is_multicast()
                || ip.octets()[0] == 0
        }
        IpAddr::V6(ip) => {
            ip.to_ipv4().is_some_and(|mapped| is_blocked_ip(IpAddr::V4(mapped)))
                || ip.is_loopback()
                || ip.is_unique_local()
                || ip.is_unicast_link_local()
                || ip.is_unspecified()
                || ip.is_multicast()
        }
    }
}

async fn validated_client(url: &str) -> Result<(reqwest::Client, reqwest::Url), String> {
    let url = reqwest::Url::parse(url.trim()).map_err(|_| "Feed URL is invalid".to_string())?;
    if !matches!(url.scheme(), "http" | "https") || !url.username().is_empty() || url.password().is_some() {
        return Err("Feed URL must use HTTP or HTTPS without embedded credentials".to_string());
    }

    let host = url.host_str().ok_or_else(|| "Feed URL must include a host".to_string())?;
    let normalized_host = host.trim_end_matches('.').to_ascii_lowercase();
    if normalized_host == "localhost" || normalized_host.ends_with(".localhost") {
        return Err("Local feed hosts are not allowed".to_string());
    }

    let pinned_addresses = if let Ok(ip) = host.parse::<IpAddr>() {
        if is_blocked_ip(ip) {
            return Err("Local or private feed addresses are not allowed".to_string());
        }
        None
    } else {
        let port = url.port_or_known_default().ok_or_else(|| "Feed URL has no valid port".to_string())?;
        let addresses = tokio::net::lookup_host((host, port))
            .await
            .map_err(|e| format!("Could not resolve feed host: {e}"))?
            .collect::<Vec<SocketAddr>>();
        if addresses.is_empty() || addresses.iter().any(|address| is_blocked_ip(address.ip())) {
            return Err("Feed host resolves to a local or private address".to_string());
        }
        Some(addresses)
    };

    let mut builder = reqwest::Client::builder()
        .user_agent("Mozilla/5.0 (compatible; p2p-desktop/0.1; +rss-reader)")
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::none());
    if let Some(addresses) = pinned_addresses {
        builder = builder.resolve_to_addrs(host, &addresses);
    }
    let client = builder
        .build()
        .map_err(|e| format!("Failed to build HTTP client (TLS init): {e}"))?;
    Ok((client, url))
}

/// Downloads a feed and returns its body. Only http and https URLs are accepted.
/// Every failure is logged and returned as a readable `Err` string for the frontend.
pub async fn fetch_feed_text(url: &str) -> Result<String, String> {
    let result = fetch_feed_text_inner(url).await;
    if let Err(err) = &result {
        eprintln!("[RSS] {url}: {err}");
    }
    result
}

async fn fetch_feed_text_inner(url: &str) -> Result<String, String> {
    let (client, url) = validated_client(url).await?;
    let mut resp = client
        .get(url)
        .header(
            reqwest::header::ACCEPT,
            "application/rss+xml, application/atom+xml, application/xml, text/xml;q=0.9, */*;q=0.8",
        )
        .send()
        .await
        .map_err(|e| {
            if e.is_timeout() {
                "Feed request timed out after 10s".to_string()
            } else if e.is_connect() {
                format!("Could not connect to feed host (DNS/TLS/network): {e}")
            } else {
                format!("Failed to fetch feed: {e}")
            }
        })?
        .error_for_status()
        .map_err(|e| format!("Feed server returned an error: {e}"))?;

    let mut body: Vec<u8> = Vec::new();
    while let Some(chunk) = resp
        .chunk()
        .await
        .map_err(|e| format!("Failed to read feed: {e}"))?
    {
        if body.len() + chunk.len() > MAX_FEED_BYTES {
            return Err(format!("Feed exceeds {} MB limit", MAX_FEED_BYTES / 1024 / 1024));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(String::from_utf8_lossy(&body).into_owned())
}

/// Fetches a feed and parses its items.
pub async fn fetch_items(url: &str) -> Result<Vec<FeedItem>, String> {
    let text = fetch_feed_text(url).await?;
    parse_feed_items(&text)
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn xml_unescape(value: &str) -> String {
    value
        .replace("&quot;", "\"")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&")
}

/// Builds an OPML document listing the given feed URLs.
pub fn build_opml(urls: &[String]) -> String {
    let outlines: String = urls
        .iter()
        .map(|url| {
            let escaped = xml_escape(url);
            format!("    <outline type=\"rss\" text=\"{escaped}\" xmlUrl=\"{escaped}\"/>\n")
        })
        .collect();
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<opml version=\"2.0\">\n  <head><title>Mesh RSS subscriptions</title></head>\n  <body>\n{outlines}  </body>\n</opml>\n"
    )
}

/// Extracts `xmlUrl` attributes from OPML outlines. Non-http entries and duplicates are skipped.
pub fn parse_opml_urls(text: &str) -> Vec<String> {
    let mut urls: Vec<String> = Vec::new();
    for part in text.split("xmlUrl=\"").skip(1) {
        if let Some(end) = part.find('"') {
            let url = xml_unescape(part[..end].trim());
            if is_http_url(&url) && !urls.contains(&url) {
                urls.push(url);
            }
        }
    }
    urls
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blocks_local_and_private_ip_addresses() {
        for address in ["127.0.0.1", "10.1.2.3", "172.20.0.1", "192.168.1.1", "169.254.169.254", "::1", "fd00::1"] {
            assert!(is_blocked_ip(address.parse().unwrap()), "{address} should be blocked");
        }
        assert!(!is_blocked_ip("8.8.8.8".parse().unwrap()));
    }

    #[tokio::test]
    async fn rejects_local_feed_urls() {
        for url in ["file:///etc/passwd", "http://localhost/rss", "http://127.0.0.1/feed"] {
            assert!(validated_client(url).await.is_err(), "{url} should be rejected");
        }
    }

    #[test]
    fn opml_round_trip_keeps_urls() {
        let urls = vec!["https://example.com/feed?a=1&b=2".to_string()];
        let opml = build_opml(&urls);
        assert_eq!(parse_opml_urls(&opml), urls);
    }

    #[test]
    fn opml_skips_non_http_entries() {
        let text = r#"<outline xmlUrl="file:///etc/passwd"/><outline xmlUrl="https://example.com/rss"/>"#;
        assert_eq!(parse_opml_urls(text), vec!["https://example.com/rss".to_string()]);
    }

    #[test]
    fn item_fields_do_not_leak_from_next_item() {
        let rss = "<item><title>A</title></item><item><title>B</title><pubDate>Mon</pubDate></item>";
        let items = parse_feed_items(rss).unwrap();
        assert_eq!(items[0].pub_date, "");
        assert_eq!(items[1].pub_date, "Mon");
    }

    #[test]
    fn parses_rdf_and_prefixed_atom_with_decoded_text() {
        let rdf = r#"<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:dc="http://purl.org/dc/elements/1.1/"><item rdf:about="https://example.test/rdf"><title>R&amp;D news</title><dc:date>2026-10-31</dc:date></item></rdf:RDF>"#;
        let rdf_items = parse_feed_items(rdf).unwrap();
        assert_eq!(rdf_items.len(), 1);
        assert_eq!(rdf_items[0].title, "R&D news");
        assert_eq!(rdf_items[0].link, "https://example.test/rdf");
        assert_eq!(rdf_items[0].pub_date, "2026-10-31");

        let atom = r#"<a:feed xmlns:a="http://www.w3.org/2005/Atom"><a:entry><a:title>Atom &amp; updates</a:title><a:link rel="alternate" href="https://example.test/atom?a=1&amp;b=2"/><a:updated>2026-11-01T10:00:00Z</a:updated></a:entry></a:feed>"#;
        let atom_items = parse_feed_items(atom).unwrap();
        assert_eq!(atom_items.len(), 1);
        assert_eq!(atom_items[0].title, "Atom & updates");
        assert_eq!(atom_items[0].link, "https://example.test/atom?a=1&b=2");
    }
}
