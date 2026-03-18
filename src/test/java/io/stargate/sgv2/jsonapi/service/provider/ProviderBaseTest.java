package io.stargate.sgv2.jsonapi.service.provider;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** Tests for {@link ProviderBase} URL routing logic */
public class ProviderBaseTest {

  @Test
  void testAstraCSTokenUsesDefaultPath() {
    String baseUrl = "https://api.example.com/nvidia/rerank";
    String astracsToken = "AstraCS:abc123def456";

    String result = ProviderBase.getUrlForTokenType(baseUrl, astracsToken);

    assertThat(result)
        .as("AstraCS tokens should use the default path without /portal/ prefix")
        .isEqualTo(baseUrl);
  }

  @Test
  void testJWTTokenUsesPortalPath() {
    String baseUrl = "https://api.example.com/nvidia/rerank";
    String jwtToken = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.abc";

    String result = ProviderBase.getUrlForTokenType(baseUrl, jwtToken);

    assertThat(result)
        .as("JWT tokens should use /portal/ prefixed path")
        .isEqualTo("https://api.example.com/portal/nvidia/rerank");
  }

  @Test
  void testJWTTokenWithRootPath() {
    String baseUrl = "https://api.example.com/";
    String jwtToken = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.abc";

    String result = ProviderBase.getUrlForTokenType(baseUrl, jwtToken);

    assertThat(result)
        .as("JWT tokens with root path should add /portal/")
        .isEqualTo("https://api.example.com/portal/");
  }

  @Test
  void testJWTTokenWithNestedPath() {
    String baseUrl = "https://api.example.com/v1/nvidia/embedding";
    String jwtToken = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.abc";

    String result = ProviderBase.getUrlForTokenType(baseUrl, jwtToken);

    assertThat(result)
        .as("JWT tokens should preserve nested paths with /portal/ prefix")
        .isEqualTo("https://api.example.com/portal/v1/nvidia/embedding");
  }

  @Test
  void testJWTTokenWithQueryParameters() {
    String baseUrl = "https://api.example.com/nvidia/rerank?version=1&debug=true";
    String jwtToken = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.abc";

    String result = ProviderBase.getUrlForTokenType(baseUrl, jwtToken);

    assertThat(result)
        .as("JWT tokens should add /portal/ prefix and preserve query parameters")
        .isEqualTo("https://api.example.com/portal/nvidia/rerank?version=1&debug=true");
  }

  @Test
  void testAstraCSTokenWithQueryParameters() {
    String baseUrl = "https://api.example.com/nvidia/rerank?version=1&debug=true";
    String astracsToken = "AstraCS:test123";

    String result = ProviderBase.getUrlForTokenType(baseUrl, astracsToken);

    assertThat(result)
        .as("AstraCS tokens should preserve original URL including query parameters")
        .isEqualTo(baseUrl);
  }

  @Test
  void testJWTTokenWithFragment() {
    String baseUrl = "https://api.example.com/nvidia/rerank#section1";
    String jwtToken = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.abc";

    String result = ProviderBase.getUrlForTokenType(baseUrl, jwtToken);

    assertThat(result)
        .as("JWT tokens should add /portal/ prefix and preserve fragment")
        .isEqualTo("https://api.example.com/portal/nvidia/rerank#section1");
  }

  @Test
  void testJWTTokenWithQueryAndFragment() {
    String baseUrl = "https://api.example.com/nvidia/rerank?version=1#section1";
    String jwtToken = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.abc";

    String result = ProviderBase.getUrlForTokenType(baseUrl, jwtToken);

    assertThat(result)
        .as("JWT tokens should add /portal/ prefix and preserve both query and fragment")
        .isEqualTo("https://api.example.com/portal/nvidia/rerank?version=1#section1");
  }

  @Test
  void testAstraCSTokenWithNestedPath() {
    String baseUrl = "https://api.example.com/v1/nvidia/embedding";
    String astracsToken = "AstraCS:xyz789";

    String result = ProviderBase.getUrlForTokenType(baseUrl, astracsToken);

    assertThat(result)
        .as("AstraCS tokens should always use original URL regardless of path complexity")
        .isEqualTo(baseUrl);
  }

  @Test
  void testEmptyApiKey() {
    String baseUrl = "https://api.example.com/nvidia/rerank";
    String emptyToken = "";

    String result = ProviderBase.getUrlForTokenType(baseUrl, emptyToken);

    assertThat(result)
        .as("Empty API key (not AstraCS) should use /portal/ path")
        .isEqualTo("https://api.example.com/portal/nvidia/rerank");
  }

  @Test
  void testNullApiKey() {
    String baseUrl = "https://api.example.com/nvidia/rerank";
    String nullToken = null;

    String result = ProviderBase.getUrlForTokenType(baseUrl, nullToken);

    assertThat(result)
        .as("Null API key should use /portal/ path (NPE safe)")
        .isEqualTo("https://api.example.com/portal/nvidia/rerank");
  }

  @Test
  void testDifferentPortWithJWT() {
    String baseUrl = "https://api.example.com:8443/nvidia/rerank";
    String jwtToken = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.abc";

    String result = ProviderBase.getUrlForTokenType(baseUrl, jwtToken);

    assertThat(result)
        .as("JWT tokens should preserve port number")
        .isEqualTo("https://api.example.com:8443/portal/nvidia/rerank");
  }

  @Test
  void testDifferentPortWithAstraCS() {
    String baseUrl = "https://api.example.com:8443/nvidia/rerank";
    String astracsToken = "AstraCS:test123";

    String result = ProviderBase.getUrlForTokenType(baseUrl, astracsToken);

    assertThat(result).as("AstraCS tokens should use original URL with port").isEqualTo(baseUrl);
  }

  @Test
  void testCaseInsensitiveAstraCS() {
    String baseUrl = "https://api.example.com/nvidia/rerank";
    String lowercaseToken = "astraCS:test123";

    String result = ProviderBase.getUrlForTokenType(baseUrl, lowercaseToken);

    assertThat(result)
        .as("Token check is case-sensitive, lowercase 'astraCS' should use /portal/ path")
        .isEqualTo("https://api.example.com/portal/nvidia/rerank");
  }

  @Test
  void testAstraCSPrefixInMiddleOfToken() {
    String baseUrl = "https://api.example.com/nvidia/rerank";
    String tokenWithAstraCSInMiddle = "Bearer_AstraCS:test123";

    String result = ProviderBase.getUrlForTokenType(baseUrl, tokenWithAstraCSInMiddle);

    assertThat(result)
        .as("AstraCS must be at the start of the token, not in the middle")
        .isEqualTo("https://api.example.com/portal/nvidia/rerank");
  }

  @Test
  void testHttpProtocol() {
    String baseUrl = "http://localhost:8080/nvidia/rerank";
    String jwtToken = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.abc";

    String result = ProviderBase.getUrlForTokenType(baseUrl, jwtToken);

    assertThat(result)
        .as("HTTP protocol should be preserved with JWT tokens")
        .isEqualTo("http://localhost:8080/portal/nvidia/rerank");
  }

  @Test
  void testPathWithoutLeadingSlash() {
    String baseUrl = "https://api.example.com";
    String jwtToken = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.abc";

    String result = ProviderBase.getUrlForTokenType(baseUrl, jwtToken);

    assertThat(result)
        .as("URL without path should get /portal/ added")
        .isEqualTo("https://api.example.com/portal/");
  }
}

// Made with Bob
