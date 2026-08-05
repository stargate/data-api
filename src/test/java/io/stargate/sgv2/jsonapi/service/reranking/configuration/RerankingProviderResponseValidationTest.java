package io.stargate.sgv2.jsonapi.service.reranking.configuration;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.stargate.sgv2.jsonapi.exception.SchemaException;
import jakarta.ws.rs.client.ClientResponseContext;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.Test;

class RerankingProviderResponseValidationTest {

  private final RerankingProviderResponseValidation validation =
      new RerankingProviderResponseValidation();

  @Test
  void ignoresEmptyServerErrorResponse() {
    ClientResponseContext responseContext = responseContext(Response.Status.INTERNAL_SERVER_ERROR);
    when(responseContext.hasEntity()).thenReturn(false);

    assertThatCode(() -> validation.filter(null, responseContext)).doesNotThrowAnyException();

    verify(responseContext, never()).hasEntity();
  }

  @Test
  void ignoresNonJsonServerErrorResponse() {
    ClientResponseContext responseContext = responseContext(Response.Status.INTERNAL_SERVER_ERROR);
    when(responseContext.hasEntity()).thenReturn(true);
    when(responseContext.getMediaType()).thenReturn(MediaType.TEXT_PLAIN_TYPE);

    assertThatCode(() -> validation.filter(null, responseContext)).doesNotThrowAnyException();

    verify(responseContext, never()).getMediaType();
  }

  @Test
  void rejectsEmptySuccessfulResponse() {
    ClientResponseContext responseContext = responseContext(Response.Status.OK);
    when(responseContext.hasEntity()).thenReturn(false);

    assertThatThrownBy(() -> validation.filter(null, responseContext))
        .isInstanceOf(SchemaException.class)
        .hasMessageContaining("No response body from the reranking provider");
  }

  @Test
  void rejectsEmptyRedirectResponse() {
    ClientResponseContext responseContext = responseContext(Response.Status.TEMPORARY_REDIRECT);
    when(responseContext.hasEntity()).thenReturn(false);

    assertThatThrownBy(() -> validation.filter(null, responseContext))
        .isInstanceOf(SchemaException.class)
        .hasMessageContaining("No response body from the reranking provider");
  }

  private static ClientResponseContext responseContext(Response.Status status) {
    ClientResponseContext responseContext = mock(ClientResponseContext.class);
    when(responseContext.getStatus()).thenReturn(status.getStatusCode());
    when(responseContext.getStatusInfo()).thenReturn(status);
    return responseContext;
  }
}
