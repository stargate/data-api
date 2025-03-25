package io.stargate.sgv2.jsonapi.service.provider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.quarkus.test.junit.QuarkusTest;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import jakarta.ws.rs.core.Response;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@QuarkusTest
public class ProviderHttpResponseErrorMapperTest {

  private static final String PROVIDER_NAME = "TestProvider";
  private static final String ERROR_MESSAGE = "HTTP error message from provider";

  /** Test cases for timeout errors (408 and 504) */
  private static Stream<Arguments> timeoutTestCases() {
    return Stream.of(
        Arguments.of(
            ProviderHttpResponseErrorMapper.ProviderType.EMBEDDING,
            Response.Status.REQUEST_TIMEOUT.getStatusCode(),
            ErrorCodeV1.EMBEDDING_PROVIDER_TIMEOUT),
        Arguments.of(
            ProviderHttpResponseErrorMapper.ProviderType.EMBEDDING,
            Response.Status.GATEWAY_TIMEOUT.getStatusCode(),
            ErrorCodeV1.EMBEDDING_PROVIDER_TIMEOUT),
        Arguments.of(
            ProviderHttpResponseErrorMapper.ProviderType.RERANKING,
            Response.Status.REQUEST_TIMEOUT.getStatusCode(),
            ErrorCodeV1.RERANKING_PROVIDER_TIMEOUT),
        Arguments.of(
            ProviderHttpResponseErrorMapper.ProviderType.RERANKING,
            Response.Status.GATEWAY_TIMEOUT.getStatusCode(),
            ErrorCodeV1.RERANKING_PROVIDER_TIMEOUT));
  }

  @ParameterizedTest
  @MethodSource("timeoutTestCases")
  void testTimeoutErrors(
      ProviderHttpResponseErrorMapper.ProviderType providerType,
      int statusCode,
      ErrorCodeV1 expectedErrorCode) {
    // Setup
    Response response = mockResponse(statusCode, null);

    // Execute
    RuntimeException exception =
        ProviderHttpResponseErrorMapper.mapToAPIException(
            providerType, PROVIDER_NAME, response, ERROR_MESSAGE);

    // Verify
    JsonApiException apiException = assertApiExceptionType(exception);
    assertEquals(expectedErrorCode, apiException.getErrorCode());
    verifyErrorMessage(apiException, providerType, statusCode);
  }

  // Test cases for rate limit errors (429)
  static Stream<Arguments> rateLimitTestCases() {
    return Stream.of(
        Arguments.of(
            ProviderHttpResponseErrorMapper.ProviderType.EMBEDDING,
            ErrorCodeV1.EMBEDDING_PROVIDER_RATE_LIMITED),
        Arguments.of(
            ProviderHttpResponseErrorMapper.ProviderType.RERANKING,
            ErrorCodeV1.RERANKING_PROVIDER_RATE_LIMITED));
  }

  @ParameterizedTest
  @MethodSource("rateLimitTestCases")
  void testRateLimitErrors(
      ProviderHttpResponseErrorMapper.ProviderType providerType, ErrorCodeV1 expectedErrorCode) {
    // Setup
    Response response =
        mockResponse(
            Response.Status.TOO_MANY_REQUESTS.getStatusCode(), Response.Status.Family.CLIENT_ERROR);

    // Execute
    RuntimeException exception =
        ProviderHttpResponseErrorMapper.mapToAPIException(
            providerType, PROVIDER_NAME, response, ERROR_MESSAGE);

    // Verify
    JsonApiException apiException = assertApiExceptionType(exception);
    assertEquals(expectedErrorCode, apiException.getErrorCode());
    verifyErrorMessage(
        apiException, providerType, Response.Status.TOO_MANY_REQUESTS.getStatusCode());
  }

  // Test cases for client errors (4xx except 408, 429)
  static Stream<Arguments> clientErrorTestCases() {
    return Stream.of(
        Arguments.of(
            ProviderHttpResponseErrorMapper.ProviderType.EMBEDDING,
            Response.Status.BAD_REQUEST.getStatusCode(),
            ErrorCodeV1.EMBEDDING_PROVIDER_CLIENT_ERROR),
        Arguments.of(
            ProviderHttpResponseErrorMapper.ProviderType.EMBEDDING,
            Response.Status.UNAUTHORIZED.getStatusCode(),
            ErrorCodeV1.EMBEDDING_PROVIDER_CLIENT_ERROR),
        Arguments.of(
            ProviderHttpResponseErrorMapper.ProviderType.RERANKING,
            Response.Status.BAD_REQUEST.getStatusCode(),
            ErrorCodeV1.RERANKING_PROVIDER_CLIENT_ERROR),
        Arguments.of(
            ProviderHttpResponseErrorMapper.ProviderType.RERANKING,
            Response.Status.UNAUTHORIZED.getStatusCode(),
            ErrorCodeV1.RERANKING_PROVIDER_CLIENT_ERROR));
  }

  @ParameterizedTest
  @MethodSource("clientErrorTestCases")
  void testClientErrors(
      ProviderHttpResponseErrorMapper.ProviderType providerType,
      int statusCode,
      ErrorCodeV1 expectedErrorCode) {
    // Setup
    Response response = mockResponse(statusCode, Response.Status.Family.CLIENT_ERROR);

    // Execute
    RuntimeException exception =
        ProviderHttpResponseErrorMapper.mapToAPIException(
            providerType, PROVIDER_NAME, response, ERROR_MESSAGE);

    // Verify
    JsonApiException apiException = assertApiExceptionType(exception);
    assertEquals(expectedErrorCode, apiException.getErrorCode());
    verifyErrorMessage(apiException, providerType, statusCode);
  }

  // Test cases for server errors (5xx except 504)
  static Stream<Arguments> serverErrorTestCases() {
    return Stream.of(
        Arguments.of(
            ProviderHttpResponseErrorMapper.ProviderType.EMBEDDING,
            Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
            ErrorCodeV1.EMBEDDING_PROVIDER_SERVER_ERROR),
        Arguments.of(
            ProviderHttpResponseErrorMapper.ProviderType.EMBEDDING,
            Response.Status.BAD_GATEWAY.getStatusCode(),
            ErrorCodeV1.EMBEDDING_PROVIDER_SERVER_ERROR),
        Arguments.of(
            ProviderHttpResponseErrorMapper.ProviderType.RERANKING,
            Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
            ErrorCodeV1.RERANKING_PROVIDER_SERVER_ERROR),
        Arguments.of(
            ProviderHttpResponseErrorMapper.ProviderType.RERANKING,
            Response.Status.BAD_GATEWAY.getStatusCode(),
            ErrorCodeV1.RERANKING_PROVIDER_SERVER_ERROR));
  }

  @ParameterizedTest
  @MethodSource("serverErrorTestCases")
  void testServerErrors(
      ProviderHttpResponseErrorMapper.ProviderType providerType,
      int statusCode,
      ErrorCodeV1 expectedErrorCode) {
    // Setup
    Response response = mockResponse(statusCode, Response.Status.Family.SERVER_ERROR);

    // Execute
    RuntimeException exception =
        ProviderHttpResponseErrorMapper.mapToAPIException(
            providerType, PROVIDER_NAME, response, ERROR_MESSAGE);

    // Verify
    JsonApiException apiException = assertApiExceptionType(exception);
    assertEquals(expectedErrorCode, apiException.getErrorCode());
    verifyErrorMessage(apiException, providerType, statusCode);
  }

  // Test cases for unexpected responses
  @Test
  void testUnexpectedResponses() {
    // Setup - using 200 OK as an unexpected error scenario
    Response response =
        mockResponse(Response.Status.OK.getStatusCode(), Response.Status.Family.SUCCESSFUL);

    // Execute & Verify for EMBEDDING
    RuntimeException embeddingException =
        ProviderHttpResponseErrorMapper.mapToAPIException(
            ProviderHttpResponseErrorMapper.ProviderType.EMBEDDING,
            PROVIDER_NAME,
            response,
            ERROR_MESSAGE);
    JsonApiException embeddingApiException = assertApiExceptionType(embeddingException);
    assertEquals(
        ErrorCodeV1.EMBEDDING_PROVIDER_UNEXPECTED_RESPONSE, embeddingApiException.getErrorCode());

    // Execute & Verify for RERANKING
    RuntimeException rerankingException =
        ProviderHttpResponseErrorMapper.mapToAPIException(
            ProviderHttpResponseErrorMapper.ProviderType.RERANKING,
            PROVIDER_NAME,
            response,
            ERROR_MESSAGE);
    JsonApiException rerankingApiException = assertApiExceptionType(rerankingException);
    assertEquals(
        ErrorCodeV1.RERANKING_PROVIDER_UNEXPECTED_RESPONSE, rerankingApiException.getErrorCode());
  }

  @Test
  void testCustomProviderNameAndMessage() {
    // Setup
    String customProvider = "CustomAIProvider";
    String customMessage = "Detailed error information";
    Response response =
        mockResponse(
            Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
            Response.Status.Family.SERVER_ERROR);

    // Execute
    RuntimeException exception =
        ProviderHttpResponseErrorMapper.mapToAPIException(
            ProviderHttpResponseErrorMapper.ProviderType.EMBEDDING,
            customProvider,
            response,
            customMessage);

    // Verify
    JsonApiException apiException = assertApiExceptionType(exception);
    String errorMessage = apiException.getMessage();
    assertTrue(errorMessage.contains(customProvider));
    assertTrue(errorMessage.contains(customMessage));
  }

  // Helper method to create a mock Response
  private Response mockResponse(int statusCode, Response.Status.Family family) {
    Response response = mock(Response.class);
    Response.StatusType statusType = mock(Response.StatusType.class);

    when(response.getStatus()).thenReturn(statusCode);
    when(response.getStatusInfo()).thenReturn(statusType);
    if (family != null) {
      when(statusType.getFamily()).thenReturn(family);
    }

    return response;
  }

  // Helper method to assert exception type and cast
  private JsonApiException assertApiExceptionType(RuntimeException exception) {
    assertTrue(
        exception instanceof JsonApiException,
        "Exception should be of type ApiException but was " + exception.getClass().getName());
    return (JsonApiException) exception;
  }

  // Helper method to verify error message content
  private void verifyErrorMessage(
      JsonApiException exception,
      ProviderHttpResponseErrorMapper.ProviderType providerType,
      int statusCode) {
    String errorMessage = exception.getMessage();
    assertTrue(errorMessage.contains(providerType.apiName()));
    assertTrue(errorMessage.contains(PROVIDER_NAME));
    assertTrue(errorMessage.contains(String.valueOf(statusCode)));
    assertTrue(errorMessage.contains(ERROR_MESSAGE));
  }
}
