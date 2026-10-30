package io.stargate.sgv2.jsonapi.api.request.token;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import org.junit.jupiter.api.Test;

/** Tests for {@link BearerJwtTokenResolver}. */
public class BearerJwtTokenResolverTest {

  private final BearerJwtTokenResolver resolver = new BearerJwtTokenResolver();

  private static RoutingContext routingContextWithAuthorization(String authorization) {
    var request = mock(HttpServerRequest.class);
    when(request.getHeader("Authorization")).thenReturn(authorization);
    var context = mock(RoutingContext.class);
    when(context.request()).thenReturn(request);
    return context;
  }

  @Test
  public void resolves_token_from_valid_bearer_header() {
    var context = routingContextWithAuthorization("Bearer mytoken123");

    assertThat(resolver.resolve(context, null)).isEqualTo("mytoken123");
  }

  @Test
  public void returns_null_when_no_authorization_header() {
    var context = routingContextWithAuthorization(null);

    assertThat(resolver.resolve(context, null)).isNull();
  }

  @Test
  public void returns_null_when_header_is_not_bearer() {
    var context = routingContextWithAuthorization("Token mytoken");

    assertThat(resolver.resolve(context, null)).isNull();
  }

  @Test
  public void returns_empty_string_when_bearer_prefix_only() {
    // "Bearer " starts with the prefix, so substring + trim yields an empty string (not null).
    var context = routingContextWithAuthorization("Bearer ");

    assertThat(resolver.resolve(context, null)).isEqualTo("");
  }

  @Test
  public void strips_whitespace_from_token() {
    var context = routingContextWithAuthorization("Bearer   mytoken  ");

    assertThat(resolver.resolve(context, null)).isEqualTo("mytoken");
  }
}
