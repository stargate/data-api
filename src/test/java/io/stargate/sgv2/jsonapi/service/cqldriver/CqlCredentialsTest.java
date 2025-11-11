package io.stargate.sgv2.jsonapi.service.cqldriver;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** Tests for {@link CqlCredentials}. */
public class CqlCredentialsTest {
  // [databind#2253]: triggered by logging
  @Test
  public void toStringOkForShort() {
    assertThat(new CqlCredentials.TokenCredentials("a").toString())
        .isEqualTo("TokenCredentials{token='a', isAnonymous=false}");
    assertThat(new CqlCredentials.TokenCredentials("abcdefgh").toString())
        .isEqualTo("TokenCredentials{token='abcd...', isAnonymous=false}");
  }
}
