package io.stargate.sgv2.jsonapi.service.provider;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.stargate.sgv2.jsonapi.exception.ProviderException;
import jakarta.ws.rs.core.Response;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Unit tests for {@link ProviderHttpResponseErrorMapper}.
 *
 * <p>These tests verify that HTTP responses from external providers are correctly mapped to the
 * appropriate {@link ProviderException} types based on HTTP status code and provider type. The
 * tests cover all error categories:
 *
 * <ul>
 *   <li>Timeout errors (408, 504)
 *   <li>Rate limiting errors (429)
 *   <li>Client errors (4xx)
 *   <li>Server errors (5xx)
 *   <li>Unexpected responses
 * </ul>
 */
class ProviderHttpResponseErrorMapperTest {

  private static final String PROVIDER_NAME = "TestProvider";
  private static final String ERROR_MESSAGE = "Test error message";
  private static final ProviderHttpResponseErrorMapper.ProviderType EMBEDDING =
      ProviderHttpResponseErrorMapper.ProviderType.EMBEDDING;
  private static final ProviderHttpResponseErrorMapper.ProviderType RERANKING =
      ProviderHttpResponseErrorMapper.ProviderType.RERANKING;

  /** Test for timeout errors (408 and 504) */
  static Stream<Arguments> timeoutStatusCodes() {
    return Stream.of(
        Arguments.of(EMBEDDING, Response.Status.REQUEST_TIMEOUT),
        Arguments.of(EMBEDDING, Response.Status.GATEWAY_TIMEOUT),
        Arguments.of(RERANKING, Response.Status.REQUEST_TIMEOUT),
        Arguments.of(RERANKING, Response.Status.GATEWAY_TIMEOUT));
  }

  @ParameterizedTest
  @MethodSource("timeoutStatusCodes")
  void shouldMapTimeoutErrors(
      ProviderHttpResponseErrorMapper.ProviderType providerType, Response.Status status) {
    // Setup
    Response response = mockResponse(status);
    // Execute
    RuntimeException exception =
        ProviderHttpResponseErrorMapper.mapToAPIException(
            providerType, PROVIDER_NAME, response, ERROR_MESSAGE);
    // Verify
    assertProviderException(providerType, status, exception, ProviderException.Code.TIMEOUT);
  }

  /** Test for rate limiting (429) */
  static Stream<Arguments> rateLimitStatusCodes() {
    return Stream.of(
        Arguments.of(EMBEDDING, Response.Status.TOO_MANY_REQUESTS),
        Arguments.of(RERANKING, Response.Status.TOO_MANY_REQUESTS));
  }

  @ParameterizedTest
  @MethodSource("rateLimitStatusCodes")
  void shouldMapRateLimitErrors(
      ProviderHttpResponseErrorMapper.ProviderType providerType, Response.Status status) {
    // Setup
    Response response = mockResponse(status);
    // Execute
    RuntimeException exception =
        ProviderHttpResponseErrorMapper.mapToAPIException(
            providerType, PROVIDER_NAME, response, ERROR_MESSAGE);
    // Verify
    assertProviderException(
        providerType, status, exception, ProviderException.Code.TOO_MANY_REQUESTS);
  }

  /** Test for client errors (4xx except those already handled) */
  static Stream<Arguments> clientErrorStatusCodes() {
    return Stream.of(
        Arguments.of(EMBEDDING, Response.Status.BAD_REQUEST),
        Arguments.of(EMBEDDING, Response.Status.UNAUTHORIZED),
        Arguments.of(EMBEDDING, Response.Status.FORBIDDEN),
        Arguments.of(EMBEDDING, Response.Status.NOT_FOUND),
        Arguments.of(RERANKING, Response.Status.BAD_REQUEST),
        Arguments.of(RERANKING, Response.Status.UNAUTHORIZED),
        Arguments.of(RERANKING, Response.Status.FORBIDDEN),
        Arguments.of(RERANKING, Response.Status.NOT_FOUND));
  }

  @ParameterizedTest
  @MethodSource("clientErrorStatusCodes")
  void shouldMapClientErrors(
      ProviderHttpResponseErrorMapper.ProviderType providerType, Response.Status status) {
    // Setup
    Response response = mockResponse(status);
    // Execute
    var exception =
        ProviderHttpResponseErrorMapper.mapToAPIException(
            providerType, PROVIDER_NAME, response, ERROR_MESSAGE);
    // Verify
    assertProviderException(providerType, status, exception, ProviderException.Code.CLIENT_ERROR);
  }

  /** Test for server errors (5xx) */
  static Stream<Arguments> serverErrorStatusCodes() {
    return Stream.of(
        Arguments.of(EMBEDDING, Response.Status.INTERNAL_SERVER_ERROR),
        Arguments.of(EMBEDDING, Response.Status.BAD_GATEWAY),
        Arguments.of(EMBEDDING, Response.Status.SERVICE_UNAVAILABLE),
        Arguments.of(RERANKING, Response.Status.INTERNAL_SERVER_ERROR),
        Arguments.of(RERANKING, Response.Status.BAD_GATEWAY),
        Arguments.of(RERANKING, Response.Status.SERVICE_UNAVAILABLE));
  }

  @ParameterizedTest
  @MethodSource("serverErrorStatusCodes")
  void shouldMapServerErrors(
      ProviderHttpResponseErrorMapper.ProviderType providerType, Response.Status status) {
    // Setup
    Response response = mockResponse(status);
    // Execute
    var exception =
        ProviderHttpResponseErrorMapper.mapToAPIException(
            providerType, PROVIDER_NAME, response, ERROR_MESSAGE);
    // Verify
    assertProviderException(providerType, status, exception, ProviderException.Code.SERVER_ERROR);
  }

  /** Test for unexpected responses */
  private static Stream<Arguments> unexpectedStatusCodes() {
    return Stream.of(
        Arguments.of(EMBEDDING, Response.Status.MOVED_PERMANENTLY),
        Arguments.of(RERANKING, Response.Status.MOVED_PERMANENTLY));
  }

  // Test for unexpected responses
  @ParameterizedTest
  @MethodSource("unexpectedStatusCodes")
  void shouldMapUnexpectedResponses(
      ProviderHttpResponseErrorMapper.ProviderType providerType, Response.Status status) {
    // Setup - using 200 OK as an unexpected response in this error mapper context
    Response response = mockResponse(status);
    // Execute
    var exception =
        ProviderHttpResponseErrorMapper.mapToAPIException(
            providerType, PROVIDER_NAME, response, ERROR_MESSAGE);
    // Verify
    assertProviderException(
        providerType, status, exception, ProviderException.Code.UNEXPECTED_RESPONSE);
  }

  /**
   * Creates a mock {@link Response} object with the specified HTTP status.
   *
   * <p>The mock is configured to return the provided status and status code when {@link
   * Response#getStatusInfo()} and {@link Response#getStatus()} are called.
   *
   * @param status the HTTP status to be returned by the mock
   * @return a configured mock Response object
   */
  private Response mockResponse(Response.Status status) {
    Response response = mock(Response.class);
    when(response.getStatusInfo()).thenReturn(status);
    when(response.getStatus()).thenReturn(status.getStatusCode());
    return response;
  }

  /**
   * Performs detailed assertions on the generated exception to verify it contains the expected
   * information.
   *
   * <p>Verifies that:
   *
   * <ul>
   *   <li>The exception is not null
   *   <li>The exception is of type ProviderException
   *   <li>The exception has the expected error code
   *   <li>The exception message contains the provider type name
   *   <li>The exception message contains the provider name
   *   <li>The exception message contains the HTTP status code
   *   <li>The exception message contains the original error message
   * </ul>
   *
   * @param providerType the provider type that was used (for error message verification)
   * @param status the HTTP status that was used (for error message verification)
   * @param exception the exception produced by the mapper
   * @param expectedCode the expected error code
   */
  private void assertProviderException(
      ProviderHttpResponseErrorMapper.ProviderType providerType,
      Response.Status status,
      RuntimeException exception,
      ProviderException.Code expectedCode) {
    // First verify the exception is not null
    assertNotNull(exception, "The mapped exception should not be null");

    // Then do detailed verification with descriptive messages
    assertThat(exception)
        .as("Exception should be of type ProviderException")
        .isInstanceOf(ProviderException.class)

        // Verify the error code
        .satisfies(
            e ->
                Assertions.assertThat(((ProviderException) e).code)
                    .as(
                        "Error code should be %s for status %d",
                        expectedCode.name(), status.getStatusCode())
                    .isEqualTo(expectedCode.name()))

        // Verify provider type is included in message
        .satisfies(
            t ->
                Assertions.assertThat(t.getMessage())
                    .as(
                        "Error message should indicate provider type (embedding or reranking): %s",
                        providerType.apiName())
                    .contains(providerType.apiName()))

        // Verify provider name is included in message
        .satisfies(
            t ->
                Assertions.assertThat(t.getMessage())
                    .as("Error message should contain provider name: %s", PROVIDER_NAME)
                    .contains(PROVIDER_NAME))

        // Verify status code is included in message
        .satisfies(
            t ->
                Assertions.assertThat(t.getMessage())
                    .as("Error message should contain status code: %d", status.getStatusCode())
                    .contains(String.valueOf(status.getStatusCode())))

        // Verify original error message is included
        .satisfies(
            t ->
                Assertions.assertThat(t.getMessage())
                    .as("Error message should contain original error message: '%s'", ERROR_MESSAGE)
                    .contains(ERROR_MESSAGE));
  }
}
