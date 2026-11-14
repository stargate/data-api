package io.stargate.sgv2.jsonapi.api.v1.mcp;

import io.vertx.core.json.JsonObject;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.spec.PKCS8EncodedKeySpec;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Objects;

final class McpOAuthTestSupport {
  static final String AUTH_SERVER = "https://auth.example.test";
  static final String PUBLIC_BASE = "https://data.example.test";
  static final String METADATA_PATH = "/.well-known/oauth-protected-resource";
  static final String FIRST_SUBJECT = "oauth-client-one";
  static final String SECOND_SUBJECT = "oauth-client-two";
  static final String FIRST_ORG = "11111111-1111-4111-8111-111111111111";
  static final String SECOND_ORG = "22222222-2222-4222-8222-222222222222";
  private static final Base64.Encoder BASE64 = Base64.getUrlEncoder().withoutPadding();

  private McpOAuthTestSupport() {}

  static String token(String subject, String org, String role, String audience) {
    return sign(claims(subject, org, role, audience), fixturePrivateKey());
  }

  static String invalidToken(InvalidToken kind, String audience) {
    if (kind == InvalidToken.MALFORMED) {
      return "malformed-access-token";
    }
    if (kind == InvalidToken.EMPTY) {
      return "";
    }
    var claims = claims(FIRST_SUBJECT, FIRST_ORG, "read-role", audience);
    switch (kind) {
      case UNSIGNED -> {
        return BASE64.encodeToString(
                "{\"alg\":\"none\",\"typ\":\"JWT\"}".getBytes(StandardCharsets.UTF_8))
            + "."
            + BASE64.encodeToString(claims.encode().getBytes(StandardCharsets.UTF_8))
            + ".";
      }
      case EXPIRED -> {
        long now = Instant.now().getEpochSecond();
        claims.put("iat", now - 900).put("nbf", now - 900).put("exp", now - 300);
      }
      case WRONG_AUDIENCE -> claims.put("aud", List.of("https://another-resource.example.test"));
      case MISSING_AUDIENCE -> claims.remove("aud");
      case MISSING_EXPIRATION -> claims.remove("exp");
      case WRONG_ISSUER -> claims.put("iss", "https://another-issuer.example.test");
      case NOT_YET_VALID -> claims.put("nbf", Instant.now().plusSeconds(300).getEpochSecond());
      case WRONG_SIGNATURE -> {
        try {
          var generator = KeyPairGenerator.getInstance("RSA");
          generator.initialize(2048);
          return sign(claims, generator.generateKeyPair().getPrivate());
        } catch (GeneralSecurityException exception) {
          throw new IllegalStateException("Unable to generate a synthetic signing key", exception);
        }
      }
      default -> throw new IllegalArgumentException("Unsupported invalid-token case: " + kind);
    }
    return sign(claims, fixturePrivateKey());
  }

  private static JsonObject claims(String subject, String org, String role, String audience) {
    long now = Instant.now().getEpochSecond();
    return new JsonObject()
        .put("iss", AUTH_SERVER)
        .put("sub", subject)
        .put("aud", List.of(audience))
        .put("iat", now - 60)
        .put("nbf", now - 60)
        .put("exp", now + 300)
        .put("org", org)
        .put("groups", List.of(role));
  }

  private static String sign(JsonObject claims, PrivateKey privateKey) {
    var header = new JsonObject().put("alg", "RS256").put("typ", "JWT");
    String input = encode(header.encode()) + "." + encode(claims.encode());
    try {
      var signer = Signature.getInstance("SHA256withRSA");
      signer.initSign(privateKey);
      signer.update(input.getBytes(StandardCharsets.US_ASCII));
      return input + "." + BASE64.encodeToString(signer.sign());
    } catch (GeneralSecurityException exception) {
      throw new IllegalStateException("Unable to sign a synthetic JWT", exception);
    }
  }

  private static String encode(String value) {
    return BASE64.encodeToString(value.getBytes(StandardCharsets.UTF_8));
  }

  private static PrivateKey fixturePrivateKey() {
    // These checked-in synthetic keys are only used by authentication tests.
    try (var stream =
        Objects.requireNonNull(
            McpOAuthTestSupport.class.getResourceAsStream("/mcp-oauth-private-key.pem"))) {
      String pem = new String(stream.readAllBytes(), StandardCharsets.US_ASCII);
      String contents =
          pem.replace("-----BEGIN PRIVATE KEY-----", "")
              .replace("-----END PRIVATE KEY-----", "")
              .replaceAll("\\s", "");
      return KeyFactory.getInstance("RSA")
          .generatePrivate(new PKCS8EncodedKeySpec(Base64.getDecoder().decode(contents)));
    } catch (IOException | GeneralSecurityException exception) {
      throw new IllegalStateException("Unable to read the synthetic JWT signing key", exception);
    }
  }

  enum InvalidToken {
    MALFORMED,
    UNSIGNED,
    EMPTY,
    EXPIRED,
    WRONG_AUDIENCE,
    MISSING_AUDIENCE,
    MISSING_EXPIRATION,
    WRONG_ISSUER,
    WRONG_SIGNATURE,
    NOT_YET_VALID
  }
}
