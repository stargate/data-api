package io.stargate.sgv2.jsonapi.service.provider;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.stargate.sgv2.jsonapi.exception.APIException;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import jakarta.ws.rs.client.ClientResponseContext;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

/**
 * Base of tests for the {@link ProviderContentTypeFilter}, see subtypes for what they are testing
 */
public abstract class ProviderContentTypeFilterTest<T extends APIException> {

  protected abstract ProviderContentTypeFilter instance();

  private final Class<T> errorClass;
  private final ErrorCode<T> expectedErrorCode;

  protected ProviderContentTypeFilterTest(Class<T> errorClass, ErrorCode<T> expectedErrorCode) {
    this.errorClass = errorClass;
    this.expectedErrorCode = expectedErrorCode;
  }

  @Test
  public void successValidJson() {

    // test both of the accepted json types
    runFilter(
        c -> {
          when(c.getMediaType()).thenReturn(MediaType.APPLICATION_JSON_TYPE);
          when(c.getEntityStream())
              .thenReturn(
                  new ByteArrayInputStream("{'foo' : 'bar'}".getBytes(StandardCharsets.UTF_8)));
        });

    // test both of the accepted json types
    runFilter(
        c -> {
          when(c.getMediaType()).thenReturn(new MediaType("text", "json"));
          when(c.getEntityStream())
              .thenReturn(
                  new ByteArrayInputStream("{'foo' : 'bar'}".getBytes(StandardCharsets.UTF_8)));
        });
  }

  @Test
  public void ignoreZeroStatus() {

    runFilter(
        c -> {
          // status is zero
          when(c.getStatus()).thenReturn(0);
          // flag no entity, this would be an error if status is success
          when(c.hasEntity()).thenReturn(false);
        });
    // nothing to do, test is not to throw
  }

  @Test
  public void ignoreNonSuccessStatus() {

    // with missing entity
    runFilter(
        c -> {
          Response.StatusType status = mock(Response.StatusType.class);
          when(status.getFamily()).thenReturn(Response.Status.Family.CLIENT_ERROR);
          when(c.getStatusInfo()).thenReturn(status);
          when(c.hasEntity()).thenReturn(false);
        });

    // with non JSON content type
    runFilter(
        c -> {
          Response.StatusType status = mock(Response.StatusType.class);
          when(status.getFamily()).thenReturn(Response.Status.Family.CLIENT_ERROR);
          when(c.getStatusInfo()).thenReturn(status);
          when(c.getMediaType()).thenReturn(MediaType.TEXT_PLAIN_TYPE);
        });
    // nothing to do, test is not to throw
  }

  @Test
  public void failMissingEntity() {

    var filter = instance();
    var responseContext =
        createClientResponseContext(
            c -> {
              when(c.hasEntity()).thenReturn(false);
            });

    assertThatThrownBy(() -> filter.filter(null, responseContext))
        .as("failMissingEntity")
        .isInstanceOfSatisfying(errorClass, e -> assertExpectedError(e, filter));
  }

  @Test
  public void failWrongContentType() {

    var filter = instance();
    var responseContext =
        createClientResponseContext(
            c -> {
              when(c.getMediaType()).thenReturn(MediaType.TEXT_PLAIN_TYPE);
              when(c.getEntityStream())
                  .thenReturn(
                      new ByteArrayInputStream("oops wrong data".getBytes(StandardCharsets.UTF_8)));
            });

    assertThatThrownBy(() -> filter.filter(null, responseContext))
        .as("failWrongContentType")
        .isInstanceOfSatisfying(
            errorClass,
            e -> {
              assertExpectedError(e, filter);
              assertThat(e).hasMessageContaining("oops wrong data");
            });
  }

  /**
   * Edge case that prob would not happen, docs say mediaType is only null if no entity. This test
   * set isEntity to true, but there is no content type
   */
  @Test
  public void failMissingContentType() {

    // this will actually fail because there is no stream to read
    var filter = instance();
    var responseContext =
        createClientResponseContext(
            c -> {
              when(c.getMediaType()).thenReturn(null);
            });

    assertThatThrownBy(() -> filter.filter(null, responseContext))
        .as("failMissingContentType")
        .isInstanceOfSatisfying(errorClass, e -> assertExpectedError(e, filter));
  }

  protected void assertExpectedError(T error, ProviderContentTypeFilter filter) {

    assertThat(error.code).as("code matches").isEqualTo(expectedErrorCode.name());
  }

  protected ProviderContentTypeFilter runFilter(Consumer<ClientResponseContext> consumer) {

    var filter = instance();
    var responseContext = createClientResponseContext(consumer);
    filter.filter(null, responseContext);
    return filter;
  }

  protected ClientResponseContext createClientResponseContext(
      Consumer<ClientResponseContext> consumer) {

    ClientResponseContext responseContext = mock(ClientResponseContext.class);

    // Setting this up for success, consumer should change things for failure
    when(responseContext.getStatus()).thenReturn(200);
    when(responseContext.hasEntity()).thenReturn(true);
    when(responseContext.getMediaType()).thenReturn(MediaType.APPLICATION_JSON_TYPE);

    Response.StatusType status = mock(Response.StatusType.class);
    when(status.getFamily()).thenReturn(Response.Status.Family.SUCCESSFUL);
    when(responseContext.getStatusInfo()).thenReturn(status);

    consumer.accept(responseContext);
    return responseContext;
  }
}
