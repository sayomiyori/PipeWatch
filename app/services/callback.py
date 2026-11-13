from __future__ import annotations

import asyncio
import ipaddress
import socket
from urllib.parse import urlsplit

import httpx

from app.config import settings


def _translated_address(address) -> bool:
    # Translated/tunneled IPv6 addresses must not bypass the IPv4 destination policy.
    return isinstance(address, ipaddress.IPv6Address) and (
        address.ipv4_mapped is not None or address.sixtofour is not None
        or address.teredo is not None or address in ipaddress.ip_network("64:ff9b::/96")
        or address in ipaddress.ip_network("64:ff9b:1::/48")
    )


def _public_address(address) -> bool:
    return address.is_global and not address.is_multicast and not _translated_address(address)


def parse_target(target: str) -> tuple[httpx.URL, bool]:
    try:
        raw = urlsplit(target)
        url = httpx.URL(target)
        if (any(character.isspace() for character in target) or url.scheme not in ("http", "https") or not url.host or raw.username is not None
                or raw.password is not None or raw.fragment or "#" in target):
            raise ValueError("Invalid callback URL")
        if url.port is not None and not 1 <= url.port <= 65535:
            raise ValueError("Invalid callback port")
        origin = f"{url.scheme}://{url.netloc.decode('ascii')}"
        trusted = origin in {item.strip() for item in settings.CALLBACK_TRUSTED_ORIGINS.split(",") if item.strip()}
        if trusted:
            # Exceptions require a numeric address, never a hostname that can rebind.
            address = ipaddress.ip_address(url.host)
            if (_translated_address(address) or address.is_link_local or address.is_multicast or address.is_unspecified
                    or address.is_reserved or address == ipaddress.ip_address("169.254.169.254")):
                raise ValueError("Forbidden callback address")
            if not (address.is_loopback or address.is_private):
                trusted = False
        return url, trusted
    except (ValueError, httpx.InvalidURL, UnicodeError):
        raise ValueError("Invalid or forbidden callback URL") from None


async def resolve_target(target: str) -> tuple[httpx.URL, str]:
    url, trusted = parse_target(target)
    port = url.port or (443 if url.scheme == "https" else 80)
    answers = await asyncio.wait_for(
        asyncio.get_running_loop().getaddrinfo(url.host, port, type=socket.SOCK_STREAM), timeout=5,
    )
    addresses = [ipaddress.ip_address(answer[4][0]) for answer in answers]
    if not addresses or (not trusted and any(not _public_address(address) for address in addresses)):
        raise ValueError("Callback must resolve only to public unicast addresses")
    return url, str(addresses[0])


async def post_callback(target: str, payload: dict) -> None:
    # Fresh resolution and an IP URL eliminate a second hostname lookup at connect time.
    url, address = await resolve_target(target)
    async with httpx.AsyncClient(timeout=10, trust_env=False, follow_redirects=False) as client:
        async with client.stream(
            "POST", url.copy_with(host=address), headers={"Host": url.netloc.decode("ascii")},
            extensions={"sni_hostname": url.host}, json=payload,
        ) as response:
            if not 200 <= response.status_code < 300:
                raise ValueError("Callback returned a non-success status")
