"""Utilities for moving CKAN packages into ESS-DIVE."""

from __future__ import annotations

import logging
import pathlib
from typing import Any, Dict, List, Optional

import requests

try:
    from tapipy.tapis import Tapis  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    Tapis = None
    logging.warning("tapipy not available; Tapis-powered uploads will be skipped.")


USER_AGENT = "ckan-to-ess-dive-notebook"

REQUIRED_FIELDS = {
    "title": "Title",
    "description": "Description / abstract",
    "creators": "At least one creator",
    "contacts": "Primary contact / maintainer",
    "keywords": "Keywords / tags",
}


class CkanEssDiveClient:
    """Lightweight helper for CKAN ➜ ESS-DIVE flows."""

    def __init__(
        self,
        *,
        ckan_url: str,
        ckan_key: str = "",
        ess_url: str,
        ess_token: str = "",
        local_stage: str | pathlib.Path = "./staging",
        dry_run: bool = True,
    ) -> None:
        self.ckan_url = ckan_url.rstrip("/")
        self.ckan_key = ckan_key
        self.ess_url = ess_url.rstrip("/")
        self.ess_token = ess_token
        self.local_stage = pathlib.Path(local_stage).expanduser()
        self.dry_run = dry_run

    # ---- CKAN helpers ----
    @staticmethod
    def _headers(api_key: str = "") -> Dict[str, str]:
        headers = {"User-Agent": USER_AGENT}
        if api_key:
            headers["Authorization"] = api_key
        return headers

    def ckan_request(
        self, action: str, params: Dict[str, Any] | None = None
    ) -> Dict[str, Any]:
        url = f"{self.ckan_url}/api/3/action/{action}"
        resp = requests.get(
            url, headers=self._headers(self.ckan_key), params=params or {}, timeout=60
        )
        resp.raise_for_status()
        payload = resp.json()
        if not payload.get("success"):
            raise RuntimeError(f"CKAN call {action} failed: {payload}")
        return payload["result"]

    def list_ckan_packages(
        self, search: str | None = None, limit: int = 40
    ) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {"rows": limit}
        if search:
            params["q"] = search
        result = self.ckan_request("package_search", params=params)
        return result.get("results", [])

    def get_ckan_package(self, name_or_id: str) -> Dict[str, Any]:
        return self.ckan_request("package_show", params={"id": name_or_id})

    # ---- Mapping helpers ----
    @staticmethod
    def map_ckan_to_essdive(package: Dict[str, Any]) -> Dict[str, Any]:
        extras = {item.get("key"): item.get("value") for item in package.get("extras", [])}
        creators = []
        if package.get("author") or package.get("author_email"):
            creators.append({"name": package.get("author"), "email": package.get("author_email")})
        contacts = []
        if package.get("maintainer") or package.get("maintainer_email"):
            contacts.append(
                {"name": package.get("maintainer"), "email": package.get("maintainer_email")}
            )

        payload: Dict[str, Any] = {
            "title": package.get("title") or package.get("name"),
            "description": package.get("notes"),
            "keywords": [tag.get("display_name") for tag in package.get("tags", []) if tag.get("display_name")],
            "creators": creators,
            "contacts": contacts,
            "temporalCoverage": {
                "startDate": extras.get("temporal_start") or extras.get("time_start"),
                "endDate": extras.get("temporal_end") or extras.get("time_end"),
            },
            "spatialCoverage": extras.get("spatial") or extras.get("bbox"),
            "communities": [group.get("name") for group in package.get("groups", []) if group.get("name")],
            "sourceCkanId": package.get("id"),
            "sourceCkanName": package.get("name"),
            "resources": [
                {
                    "id": res.get("id"),
                    "name": res.get("name"),
                    "url": res.get("url"),
                    "format": res.get("format"),
                    "description": res.get("description"),
                    "size": res.get("size"),
                }
                for res in package.get("resources", [])
            ],
            "extras": extras,
        }
        return payload

    @staticmethod
    def find_missing_metadata(payload: Dict[str, Any]) -> List[str]:
        missing: List[str] = []
        for key, label in REQUIRED_FIELDS.items():
            value = payload.get(key)
            if not value:
                missing.append(label)
            elif isinstance(value, list) and not any(value):
                missing.append(label)
        temporal = payload.get("temporalCoverage") or {}
        if not temporal.get("startDate"):
            missing.append("Temporal start date")
        if not temporal.get("endDate"):
            missing.append("Temporal end date")
        return missing

    @staticmethod
    def summarize_payload(payload: Dict[str, Any]) -> str:
        lines = [
            f"Title: {payload.get('title')}",
            f"Keywords: {', '.join(payload.get('keywords', [])) or 'none'}",
            f"Creators: {', '.join([c.get('name') or '' for c in payload.get('creators', [])]) or 'none'}",
            f"Contacts: {', '.join([c.get('email') or c.get('name') or '' for c in payload.get('contacts', [])]) or 'none'}",
            f"Temporal: {payload.get('temporalCoverage', {})}",
            f"Resources: {len(payload.get('resources', []))}",
        ]
        return "\n".join(lines)

    # ---- Resource handling ----
    @staticmethod
    def _resource_filename(resource: Dict[str, Any]) -> str:
        name = resource.get("name") or resource.get("id") or "resource"
        url = resource.get("url") or ""
        suffix = pathlib.Path(url).suffix if url else ""
        if suffix and not name.endswith(suffix):
            return f"{name}{suffix}"
        return name

    def download_resource(self, resource: Dict[str, Any]) -> pathlib.Path:
        url = resource.get("url")
        if not url:
            raise ValueError("Resource has no URL to download")
        filename = self._resource_filename(resource)
        path = self.local_stage.expanduser().resolve() / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        with requests.get(url, headers=self._headers(self.ckan_key), stream=True, timeout=300) as resp:
            resp.raise_for_status()
            with open(path, "wb") as handle:
                for chunk in resp.iter_content(chunk_size=512 * 1024):
                    if chunk:
                        handle.write(chunk)
        return path

    def stage_resources(self, package: Dict[str, Any]) -> List[pathlib.Path]:
        saved: List[pathlib.Path] = []
        for res in package.get("resources", []):
            try:
                saved.append(self.download_resource(res))
            except Exception as exc:  # pragma: no cover - depends on remote endpoints
                logging.warning("Could not stage %s: %s", res.get("name"), exc)
        return saved

    # ---- ESS-DIVE submission ----
    def submit_to_essdive(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        if self.dry_run:
            return {"status": "skipped", "reason": "dry_run_enabled"}
        token = self.ess_token.strip()
        if not token:
            raise RuntimeError("ESS-DIVE token is required to write")
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        url = f"{self.ess_url}/datasets"
        resp = requests.post(url, headers=headers, json=payload, timeout=90)
        resp.raise_for_status()
        return resp.json()

    # ---- Auth helpers ----
    @staticmethod
    def get_ckan_token_via_tapis(
        username: str,
        password: str,
        base_url: str = "https://portals.tapis.io",
    ) -> str:
        if not Tapis:
            raise RuntimeError("tapipy is not installed; cannot fetch Tapis tokens")
        t = Tapis(base_url=base_url, username=username, password=password)
        t.get_tokens()
        return t.access_token.access_token

    def authenticate_ckan_with_tapis(
        self,
        username: str,
        password: str,
        base_url: str = "https://portals.tapis.io",
    ) -> str:
        token = self.get_ckan_token_via_tapis(username, password, base_url)
        self.ckan_key = token
        return token


def fetch_ckan_token_via_tapis(
    username: str,
    password: str,
    base_url: str = "https://portals.tapis.io",
) -> str:
    """Convenience wrapper for fetching a CKAN bearer token from Tapis credentials."""
    return CkanEssDiveClient.get_ckan_token_via_tapis(username=username, password=password, base_url=base_url)


def test_ckan_status(client: CkanEssDiveClient) -> Dict[str, Any]:
    """Ping CKAN status endpoint using the configured client."""
    return client.ckan_request("status")
