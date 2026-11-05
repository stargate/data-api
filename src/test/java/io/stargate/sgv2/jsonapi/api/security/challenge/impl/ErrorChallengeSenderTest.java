package io.stargate.sgv2.jsonapi.api.security.challenge.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.vertx.http.runtime.security.ChallengeData;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import jakarta.ws.rs.core.HttpHeaders;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class ErrorChallengeSenderTest {
  @Test
  void rejectedBearerReportsInvalidCredentialsAndPreservesTheChallenge() throws Exception {
    String body = sendChallenge("Bearer rejected-credential");

    assertThat(new ObjectMapper().readTree(body).at("/errors/0/errorCode").asText())
        .isEqualTo("UNAUTHENTICATED_REQUEST");
    assertThat(body).contains("invalid token").doesNotContain("rejected-credential", "missing");
  }

  @Test
  void missingCredentialsKeepTheExistingMissingTokenResponse() throws Exception {
    String body = sendChallenge(null);

    assertThat(new ObjectMapper().readTree(body).at("/errors/0/errorCode").asText())
        .isEqualTo("MISSING_AUTHENTICATION_TOKEN");
    assertThat(body).contains("Token");
  }

  private static String sendChallenge(String authorization) {
    var request = mock(HttpServerRequest.class);
    when(request.getHeader(HttpHeaders.AUTHORIZATION)).thenReturn(authorization);
    var response = mock(HttpServerResponse.class);
    var headers = MultiMap.caseInsensitiveMultiMap();
    when(response.headers()).thenReturn(headers);
    when(response.write(anyString())).thenReturn(Future.succeededFuture());
    var context = mock(RoutingContext.class);
    when(context.request()).thenReturn(request);
    when(context.response()).thenReturn(response);
    String challenge = "Bearer resource_metadata=\"https://data.example.test/metadata\"";

    assertThat(
            new ErrorChallengeSender(new ObjectMapper())
                .apply(context, new ChallengeData(401, HttpHeaders.WWW_AUTHENTICATE, challenge))
                .await()
                .indefinitely())
        .isTrue();

    assertThat(headers.get(HttpHeaders.WWW_AUTHENTICATE)).isEqualTo(challenge);
    verify(response).setStatusCode(401);
    var body = ArgumentCaptor.forClass(String.class);
    verify(response).write(body.capture());
    return body.getValue();
  }
}
