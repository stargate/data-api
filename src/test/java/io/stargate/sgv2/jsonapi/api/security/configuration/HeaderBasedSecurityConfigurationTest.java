package io.stargate.sgv2.jsonapi.api.security.configuration;

import static io.stargate.sgv2.jsonapi.api.security.configuration.HeaderBasedSecurityConfiguration.validateBearerConfiguration;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.smallrye.jwt.auth.principal.JWTAuthContextInfo;
import io.smallrye.jwt.config.JWTAuthContextInfoProvider;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class HeaderBasedSecurityConfigurationTest {

  @Test
  void verifiedBearerRequiresExplicitIssuerKeyAndAudience() {
    assertThatCode(() -> validateBearerConfiguration(jwtConfig())).doesNotThrowAnyException();
  }

  @Test
  void inlinePublicKeysAreSupported() {
    var config = jwtConfig();
    config.setPublicKeyLocation(null);
    config.setPublicKeyContent("inline-test-public-key");
    assertThatCode(() -> validateBearerConfiguration(config)).doesNotThrowAnyException();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "mp.jwt.verify.issuer",
        "mp.jwt.verify.publickey.location",
        "mp.jwt.verify.audiences"
      })
  void missingRequiredVerificationConfigurationFailsClosed(String missing) {
    var config = jwtConfig();
    switch (missing) {
      case "mp.jwt.verify.issuer" -> config.setIssuedBy(null);
      case "mp.jwt.verify.publickey.location" -> config.setPublicKeyLocation(null);
      case "mp.jwt.verify.audiences" -> config.setExpectedAudience(null);
      default -> throw new IllegalArgumentException(missing);
    }
    assertThatThrownBy(() -> validateBearerConfiguration(config))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(missing);
  }

  @Test
  void emptyAudienceListFailsClosed() {
    var config = jwtConfig();
    config.setExpectedAudience(Set.of("", " "));
    assertThatThrownBy(() -> validateBearerConfiguration(config))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("mp.jwt.verify.audiences");
  }

  @Test
  void disabledIssuerVerificationFailsClosed() {
    var config =
        JWTAuthContextInfoProvider.createWithKeyLocation("https://auth.example.test/jwks", "NONE")
            .getContextInfo();
    assertThatThrownBy(() -> validateBearerConfiguration(config))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("mp.jwt.verify.issuer");
  }

  private static JWTAuthContextInfo jwtConfig() {
    var config = new JWTAuthContextInfo();
    config.setIssuedBy("https://auth.example.test");
    config.setPublicKeyLocation("test-public-key.pem");
    config.setExpectedAudience(Set.of("https://data.example.test/v1/mcp"));
    return config;
  }
}
