#!/usr/bin/env python3
"""Check a Data API OAuth resource and a database read with an existing token."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import getpass
import ipaddress
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
import warnings

PROTOCOL = "2026-07-28"
MAX_BODY = 2 * 1024 * 1024


class SmokeFailure(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise SmokeFailure(message)


def origin(url):
    try:
        parts = urllib.parse.urlsplit(url)
        host, port = parts.hostname, parts.port
        try:
            loopback = ipaddress.ip_address(host or "").is_loopback
        except ValueError:
            loopback = host == "localhost"
        require(host and (parts.scheme == "https" or parts.scheme == "http" and loopback)
                and parts.username is None and parts.password is None
                and not parts.query and not parts.fragment,
                "URLs must use HTTPS or loopback HTTP without credentials, queries, or fragments")
        return parts.scheme, host, port or (443 if parts.scheme == "https" else 80)
    except ValueError as error:
        if isinstance(error, SmokeFailure):
            raise
        raise SmokeFailure("Invalid URL") from None


def json_object(raw):
    try:
        value = json.loads(raw)
    except (ValueError, UnicodeDecodeError):
        raise SmokeFailure("Expected a JSON object") from None
    require(isinstance(value, dict), "Expected a JSON object")
    return value


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args):
        return None


@dataclass
class Response:
    status: int
    headers: object
    body: bytes

    def json(self):
        return json_object(self.body)


class Client:
    def __init__(self, endpoint):
        origin(endpoint)
        self.endpoint = endpoint
        self.sequence = 0
        self.opener = urllib.request.build_opener(NoRedirect())

    def request(self, url, data=None, headers=None):
        origin(url)
        request = urllib.request.Request(url, data=data, headers=headers or {})
        try:
            response = self.opener.open(request, timeout=20)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            body = response.read(MAX_BODY + 1)
            require(len(body) <= MAX_BODY, "Response exceeds size limit")
            return Response(response.code, response.headers, body)

    def rpc(self, method, params, token=None):
        self.sequence += 1
        payload = {"jsonrpc": "2.0", "id": self.sequence, "method": method,
                   "params": {**params, "_meta": {
                       "io.modelcontextprotocol/protocolVersion": PROTOCOL,
                       "io.modelcontextprotocol/clientCapabilities": {}}}}
        headers = {"Content-Type": "application/json", "Accept": "application/json, text/event-stream",
                   "MCP-Protocol-Version": PROTOCOL, "Mcp-Method": method}
        if method == "tools/call":
            headers["Mcp-Name"] = params["name"]
        if token:
            headers["Authorization"] = "Bearer " + token
        return self.request(self.endpoint, json.dumps(payload).encode(), headers)

    def result(self, method, params, token):
        response = self.rpc(method, params, token)
        require(response.status == 200, f"{method} returned HTTP {response.status}")
        body = response.json()
        require(body.get("jsonrpc") == "2.0" and body.get("id") == self.sequence,
                "JSON-RPC response did not match its request")
        require("error" not in body and isinstance(body.get("result"), dict),
                "MCP returned a protocol error or no result")
        return body["result"]


def smoke_test(endpoint, token, keyspace, collection, filter_value, expected_document,
               denied_keyspace=None):
    require(isinstance(token, str) and bool(token) and not any(c.isspace() for c in token),
            "A nonempty access token without whitespace is required")
    require(isinstance(filter_value, dict) and isinstance(expected_document, dict),
            "The filter and expected document must be JSON objects")
    client = Client(endpoint)
    challenge = client.rpc("tools/list", {})
    require(challenge.status == 401, f"Unauthenticated MCP returned HTTP {challenge.status}, expected 401")
    header = challenge.headers.get("WWW-Authenticate", "")
    metadata = re.search(r'\bresource_metadata="([^"]+)"', header)
    require(header.lower().startswith("bearer ") and metadata is not None,
            "The 401 challenge is missing Bearer resource metadata")
    metadata_url = metadata.group(1)
    require(origin(metadata_url) == origin(endpoint), "Resource metadata must use the MCP origin")
    response = client.request(metadata_url)
    require(response.status == 200, f"Resource metadata returned HTTP {response.status}")
    document = response.json()
    servers = document.get("authorization_servers")
    require(document.get("resource") == endpoint and isinstance(servers, list) and bool(servers)
            and all(isinstance(server, str) for server in servers)
            and isinstance(document.get("bearer_methods_supported"), list)
            and "header" in document["bearer_methods_supported"], "Resource metadata is inconsistent")
    for server in servers:
        origin(server)
    print("PASS  Unauthenticated challenge and protected-resource metadata")

    catalog = client.result("tools/list", {}, token)
    tools = catalog.get("tools")
    require(isinstance(tools, list) and bool(tools)
            and all(isinstance(tool, dict) and isinstance(tool.get("name"), str)
                    and bool(tool["name"]) for tool in tools), "Invalid tool catalog")
    names = [tool["name"] for tool in tools]
    require(len(names) == len(set(names)) and "findOne" in names and not catalog.get("nextCursor"),
            "Expected a complete, unique catalog containing findOne")
    print(f"PASS  Authenticated catalog ({len(names)} tools): "
          + json.dumps(names).replace(token, "[REDACTED]"))

    arguments = {"keyspace": keyspace, "collection": collection, "filter": filter_value}
    result = client.result("tools/call", {"name": "findOne", "arguments": arguments}, token)
    content = result.get("structuredContent")
    require(result.get("isError", False) is False and isinstance(content, dict)
            and not content.get("errors") and content.get("document") == expected_document,
            "Database read did not return the expected document without application errors")
    print("PASS  Database read returned the exact expected document")
    if denied_keyspace is not None:
        result = client.result("tools/call", {"name": "findOne", "arguments": {
            **arguments, "keyspace": denied_keyspace}}, token)
        content = result.get("structuredContent")
        require(result.get("isError") is True and isinstance(content, dict)
                and isinstance(content.get("errors"), list) and len(content["errors"]) == 1
                and isinstance(content["errors"][0], dict)
                and content["errors"][0].get("errorCode") == "UNAUTHORIZED_ACCESS",
                "Denied read did not return UNAUTHORIZED_ACCESS")
        print("PASS  Other keyspace read was denied with UNAUTHORIZED_ACCESS")
    return names


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("endpoint", "keyspace", "collection"):
        parser.add_argument("--" + name, required=True)

    def object_argument(value):
        try:
            return json_object(value)
        except SmokeFailure as error:
            raise argparse.ArgumentTypeError(str(error)) from None

    parser.add_argument("--filter", required=True, type=object_argument, dest="filter_value")
    parser.add_argument("--expect-document", required=True, type=object_argument, dest="expected_document")
    parser.add_argument("--denied-keyspace")
    args = parser.parse_args()
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", getpass.GetPassWarning)
            token = os.environ.get("MCP_ACCESS_TOKEN") or getpass.getpass("OAuth access token: ")
        smoke_test(token=token, **vars(args))
    except SmokeFailure as error:
        print("FAIL  " + str(error), file=sys.stderr)
        return 1
    except (OSError, ValueError, EOFError, getpass.GetPassWarning):
        print("FAIL  Request or token input failed", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
