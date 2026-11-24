use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use anyhow::{Context, Result, bail};
use reqwest::{Client, StatusCode, Url, header};
use tokio::net::lookup_host;

use super::GatewayAttachment;

const MAX_REDIRECTS: usize = 3;
const MAX_ATTACHMENTS: usize = 16;
const MAX_ATTACHMENT_BYTES: usize = 20 * 1024 * 1024;
const MAX_TOTAL_ATTACHMENT_BYTES: usize = 50 * 1024 * 1024;

pub async fn resolve_attachments(
    attachments: Vec<GatewayAttachment>,
) -> Result<Vec<GatewayAttachment>> {
    if attachments.len() > MAX_ATTACHMENTS {
        bail!("a request can contain at most {MAX_ATTACHMENTS} attachments");
    }
    let mut total_bytes = 0usize;
    let mut resolved = Vec::with_capacity(attachments.len());

    for mut attachment in attachments {
        if attachment.data.is_some() && attachment.uri.is_some() {
            bail!("an attachment must provide either data or uri, not both");
        }

        if let Some(data) = &attachment.data {
            check_attachment_size(data.len())?;
        } else if let Some(uri) = attachment.uri.take() {
            let (data, content_type) = download_attachment(&uri).await?;
            if attachment.content_type.is_none() {
                attachment.content_type = content_type;
            }
            attachment.data = Some(data);
        } else {
            bail!("an attachment must provide data or uri");
        }

        let data_len = attachment.data.as_ref().map_or(0, Vec::len);
        total_bytes = total_bytes
            .checked_add(data_len)
            .context("attachment payload size overflow")?;
        if total_bytes > MAX_TOTAL_ATTACHMENT_BYTES {
            bail!("attachments exceed the 50 MiB total size limit");
        }
        resolved.push(attachment);
    }

    Ok(resolved)
}

pub async fn download_attachment(uri: &str) -> Result<(Vec<u8>, Option<String>)> {
    let mut url = Url::parse(uri).context("parse attachment uri")?;

    for redirect_count in 0..=MAX_REDIRECTS {
        let (host, addresses) = resolve_public_url(&url).await?;
        let client = Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .resolve_to_addrs(&host, &addresses)
            .build()
            .context("create attachment download client")?;
        let response = client
            .get(url.clone())
            .send()
            .await
            .with_context(|| format!("download attachment {url}"))?;

        if response.status().is_redirection() {
            if redirect_count == MAX_REDIRECTS {
                bail!("attachment URI exceeded the redirect limit");
            }
            let location = response
                .headers()
                .get(header::LOCATION)
                .context("attachment redirect has no location header")?
                .to_str()
                .context("attachment redirect location is not valid UTF-8")?;
            url = url.join(location).context("resolve attachment redirect")?;
            continue;
        }
        if response.status() != StatusCode::OK {
            bail!("attachment download returned HTTP {}", response.status());
        }
        if response
            .content_length()
            .is_some_and(|length| length > MAX_ATTACHMENT_BYTES as u64)
        {
            bail!("attachment exceeds the 20 MiB size limit");
        }

        let content_type = response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(';').next())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned);
        let mut data = Vec::new();
        let mut response = response;
        while let Some(chunk) = response.chunk().await.context("read attachment body")? {
            let new_len = data
                .len()
                .checked_add(chunk.len())
                .context("attachment payload size overflow")?;
            check_attachment_size(new_len)?;
            data.extend_from_slice(&chunk);
        }
        return Ok((data, content_type));
    }

    unreachable!("redirect loop returns or fails")
}

async fn resolve_public_url(url: &Url) -> Result<(String, Vec<SocketAddr>)> {
    match url.scheme() {
        "http" | "https" => {}
        scheme => bail!("attachment URI scheme {scheme:?} is not allowed"),
    }
    let host = url
        .host_str()
        .context("attachment URI has no host")?
        .to_owned();
    let port = url
        .port_or_known_default()
        .context("attachment URI has no port")?;
    let addresses = if let Ok(address) = host.parse::<IpAddr>() {
        vec![SocketAddr::new(address, port)]
    } else {
        lookup_host((host.as_str(), port))
            .await
            .with_context(|| format!("resolve attachment host {host}"))?
            .collect()
    };
    if addresses.is_empty() || addresses.iter().any(|address| !is_public_ip(address.ip())) {
        bail!("attachment URI host must resolve only to public IP addresses");
    }
    Ok((host, addresses))
}

fn check_attachment_size(size: usize) -> Result<()> {
    if size > MAX_ATTACHMENT_BYTES {
        bail!("attachment exceeds the 20 MiB size limit");
    }
    Ok(())
}

fn is_public_ip(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => is_public_ipv4(address),
        IpAddr::V6(address) => is_public_ipv6(address),
    }
}

fn is_public_ipv4(address: Ipv4Addr) -> bool {
    let octets = address.octets();
    !address.is_private()
        && !address.is_loopback()
        && !address.is_link_local()
        && !address.is_broadcast()
        && !address.is_unspecified()
        && !address.is_multicast()
        && octets[0] != 0
        && !(octets[0] == 100 && (64..=127).contains(&octets[1]))
        && !(octets[0] == 192 && octets[1] == 0 && octets[2] == 0)
        && !(octets[0] == 192 && octets[1] == 0 && octets[2] == 2)
        && !(octets[0] == 198 && (18..=19).contains(&octets[1]))
        && !(octets[0] == 198 && octets[1] == 51 && octets[2] == 100)
        && !(octets[0] == 203 && octets[1] == 0 && octets[2] == 113)
        && octets[0] < 224
}

fn is_public_ipv6(address: Ipv6Addr) -> bool {
    let segments = address.segments();
    !address.is_loopback()
        && !address.is_unspecified()
        && !address.is_multicast()
        && !address.is_unique_local()
        && !address.is_unicast_link_local()
        && !(segments[0] == 0x2001 && segments[1] == 0x0db8)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn rejects_non_public_addresses() {
        for address in [
            "127.0.0.1",
            "10.0.0.1",
            "192.168.0.1",
            "169.254.1.1",
            "100.64.0.1",
            "192.0.2.1",
            "::1",
            "fc00::1",
            "fe80::1",
            "2001:db8::1",
        ] {
            assert!(!is_public_ip(address.parse().unwrap()), "{address}");
        }
        assert!(is_public_ip("8.8.8.8".parse().unwrap()));
    }

    #[tokio::test]
    async fn accepts_inline_data_and_rejects_private_uris() {
        let inline = GatewayAttachment {
            attachment_id: None,
            content_type: Some("image/png".into()),
            file_name: None,
            data: Some(vec![1, 2, 3]),
            uri: None,
            metadata: HashMap::new(),
        };
        assert_eq!(
            resolve_attachments(vec![inline]).await.unwrap()[0].data,
            Some(vec![1, 2, 3])
        );

        let private_uri = GatewayAttachment {
            attachment_id: None,
            content_type: None,
            file_name: None,
            data: None,
            uri: Some("http://127.0.0.1/private.png".into()),
            metadata: HashMap::new(),
        };
        assert!(resolve_attachments(vec![private_uri]).await.is_err());
    }
}
