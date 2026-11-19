package io.stargate.sgv2.jsonapi.api.request.token;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.eclipse.microprofile.jwt.JsonWebToken;
import org.junit.jupiter.api.Test;

class PrincipalTokenResolverTest {

  private final PrincipalTokenResolver resolver = new PrincipalTokenResolver();

  @Test
  void preservesRawJwtInsteadOfItsSubject() {
    var principal = mock(JsonWebToken.class);
    when(principal.getName()).thenReturn("client-id");
    when(principal.getRawToken()).thenReturn("signed-access-token");

    assertThat(resolver.resolve(null, principal)).isEqualTo("signed-access-token");
  }

  @Test
  void preservesLegacyPrincipalCredentials() {
    assertThat(resolver.resolve(null, () -> "legacy-token")).isEqualTo("legacy-token");
  }

  @Test
  void anonymousPrincipalHasNoCredential() {
    assertThat(resolver.resolve(null, null)).isNull();
  }
}
