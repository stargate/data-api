"""Run with python3 -B -m unittest discover -s docs/mcp -p 'test_*.py'."""

from contextlib import redirect_stderr, redirect_stdout
from http.server import BaseHTTPRequestHandler, HTTPServer
from io import StringIO
import json
import os
import sys
import threading
import unittest
from unittest.mock import patch
import warnings

import oauth_smoke as smoke


class OAuthSmokeTest(unittest.TestCase):
    def setUp(self):
        self.requests = []
        self.mutation = lambda method, body: body
        self.redirect = False
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def send(self, status, body, **headers):
                raw = json.dumps(body).encode()
                self.send_response(status)
                for key, value in headers.items():
                    self.send_header(key.replace("_", "-"), value)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            def do_GET(self):
                owner.requests.append((self.path, dict(self.headers), None))
                body = {"resource": owner.endpoint, "authorization_servers": [owner.base],
                        "bearer_methods_supported": ["header"]}
                self.send(200, owner.mutation("metadata", body))

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                owner.requests.append((self.path, dict(self.headers), body))
                if "Authorization" not in self.headers:
                    self.send(401, {}, WWW_Authenticate=f'Bearer resource_metadata="{owner.base}/metadata"')
                    return
                if owner.redirect:
                    self.send(302, {}, Location=owner.base + "/redirected")
                    return
                if body["method"] == "tools/list":
                    result = {"tools": [{"name": "findOne"}, {"name": "findKeyspaces"}]}
                elif body["params"]["arguments"]["keyspace"] == "denied":
                    result = {"isError": True, "structuredContent": {
                        "errors": [{"errorCode": "UNAUTHORIZED_ACCESS"}]}}
                else:
                    result = {"isError": False, "structuredContent": {"document": {"id": "hello"}}}
                response = {"jsonrpc": "2.0", "id": body["id"], "result": result}
                self.send(200, owner.mutation(body["method"], response))

        self.server = HTTPServer(("127.0.0.1", 0), Handler)
        self.base = "http://127.0.0.1:" + str(self.server.server_port)
        self.endpoint = self.base + "/v1/mcp"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.thread.join)
        self.addCleanup(self.server.shutdown)

    def run_smoke(self, **overrides):
        arguments = {"endpoint": self.endpoint, "token": "test-token", "keyspace": "allowed",
                     "collection": "messages", "filter_value": {"id": "hello"},
                     "expected_document": {"id": "hello"}}
        with redirect_stdout(StringIO()):
            return smoke.smoke_test(**{**arguments, **overrides})

    def test_read_and_denied_read_use_bearer_only_on_mcp(self):
        self.assertEqual(self.run_smoke(denied_keyspace="denied"), ["findOne", "findKeyspaces"])
        self.assertEqual(len(self.requests), 5)
        for path, headers, body in self.requests:
            self.assertNotIn("Token", headers)
            self.assertNotIn("X-Cassandra-Token", headers)
            if body is None or body["id"] == 1:
                self.assertNotIn("Authorization", headers)
            else:
                self.assertEqual(headers["Authorization"], "Bearer test-token")
                self.assertEqual(path, "/v1/mcp")
                self.assertEqual(headers["Mcp-Protocol-Version"], smoke.PROTOCOL)
                self.assertEqual(body["params"]["_meta"], {
                    "io.modelcontextprotocol/protocolVersion": smoke.PROTOCOL,
                    "io.modelcontextprotocol/clientCapabilities": {}})
        self.assertEqual(self.requests[-1][2]["params"]["arguments"], {
            "keyspace": "denied", "collection": "messages", "filter": {"id": "hello"}})

    def test_wrong_document_fails(self):
        with self.assertRaisesRegex(smoke.SmokeFailure, "expected document"):
            self.run_smoke(expected_document={"id": "different"})

    def test_setup_error_does_not_count_as_permission_denial(self):
        def mutation(method, body):
            if method == "tools/call" and body["result"].get("isError"):
                body["result"]["structuredContent"]["errors"][0]["errorCode"] = "UNAUTHENTICATED_REQUEST"
            return body
        self.mutation = mutation
        with self.assertRaisesRegex(smoke.SmokeFailure, "UNAUTHORIZED_ACCESS"):
            self.run_smoke(denied_keyspace="denied")

    def test_application_error_cannot_pass_with_matching_document(self):
        def mutation(method, body):
            if method == "tools/call":
                body["result"]["structuredContent"]["errors"] = [{"errorCode": "INVALID_QUERY"}]
            return body
        self.mutation = mutation
        with self.assertRaisesRegex(smoke.SmokeFailure, "application errors"):
            self.run_smoke()

    def test_rpc_id_must_match(self):
        def mutation(method, body):
            if method == "tools/list":
                body["id"] += 1
            return body
        self.mutation = mutation
        with self.assertRaisesRegex(smoke.SmokeFailure, "did not match"):
            self.run_smoke()

    def test_resource_must_match_endpoint(self):
        def mutation(method, body):
            if method == "metadata":
                body["resource"] += "/other"
            return body
        self.mutation = mutation
        with self.assertRaisesRegex(smoke.SmokeFailure, "metadata is inconsistent"):
            self.run_smoke()
        self.assertEqual(len(self.requests), 2)

    def test_authenticated_redirect_is_not_followed(self):
        self.redirect = True
        with self.assertRaisesRegex(smoke.SmokeFailure, "HTTP 302"):
            self.run_smoke()
        self.assertEqual([path for path, _, _ in self.requests], ["/v1/mcp", "/metadata", "/v1/mcp"])

    def test_cross_origin_metadata_is_not_requested(self):
        with patch.object(smoke.Client, "rpc", return_value=smoke.Response(401, {
            "WWW-Authenticate": 'Bearer resource_metadata="https://other.example/metadata"'}, b"")):
            with self.assertRaisesRegex(smoke.SmokeFailure, "MCP origin"):
                self.run_smoke()
        self.assertEqual(self.requests, [])

    def test_unsafe_urls_fail_before_network(self):
        for endpoint in ("http://api.example/v1/mcp", "https://user:pass@api.example/v1/mcp",
                         "https://api.example/v1/mcp?token=secret", "https://api.example/#fragment"):
            with self.subTest(endpoint=endpoint), self.assertRaises(smoke.SmokeFailure):
                self.run_smoke(endpoint=endpoint)
        self.assertEqual(self.requests, [])

    def test_prompt_fails_closed_when_hidden_input_is_unavailable(self):
        arguments = ["oauth_smoke.py", "--endpoint", self.endpoint, "--keyspace", "allowed",
                     "--collection", "messages", "--filter", '{}', "--expect-document", '{}']
        def unavailable_prompt(*args):
            warnings.warn("secret input must not be echoed", smoke.getpass.GetPassWarning)
            self.fail("The prompt continued after its hidden-input warning")
        output, error = StringIO(), StringIO()
        with patch.object(sys, "argv", arguments), patch.dict(os.environ, {"MCP_ACCESS_TOKEN": ""}), \
                patch.object(smoke.getpass, "getpass", side_effect=unavailable_prompt), \
                patch.object(smoke, "smoke_test") as run, redirect_stdout(output), redirect_stderr(error):
            self.assertEqual(smoke.main(), 1)
        run.assert_not_called()
        self.assertEqual(output.getvalue(), "")
        self.assertEqual(error.getvalue(), "FAIL  Request or token input failed\n")


if __name__ == "__main__":
    unittest.main()
