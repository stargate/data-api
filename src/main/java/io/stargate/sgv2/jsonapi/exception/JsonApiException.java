package io.stargate.sgv2.jsonapi.exception;

import static io.stargate.sgv2.jsonapi.exception.ErrorCode.*;

import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.config.DebugModeConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.config.constants.ApiConstants;
import io.stargate.sgv2.jsonapi.exception.mappers.ThrowableToErrorMapper;
import jakarta.ws.rs.core.Response;
import java.util.*;
import java.util.function.Supplier;
import org.eclipse.microprofile.config.ConfigProvider;

/**
 * Our own {@link RuntimeException} that uses {@link ErrorCode} to describe the exception cause.
 * Supports specification of the custom message.
 *
 * <p>Implements {@link Supplier< CommandResult >} so this exception can be mapped to command result
 * directly.
 */
public class JsonApiException extends RuntimeException implements Supplier<CommandResult> {
  private final OperationsConfig operationsConfig;

  private final ErrorCode errorCode;

  private final Response.Status httpStatus;

  private final String title;

  private final String errorFamily;

  private final String errorScope;

  protected JsonApiException(ErrorCode errorCode) {
    this(errorCode, errorCode.getMessage(), null);
  }

  // Still needed by EmbeddingGatewayClient for gRPC, needs to remain public
  public JsonApiException(ErrorCode errorCode, String message) {
    this(errorCode, message, null);
  }

  protected JsonApiException(ErrorCode errorCode, Throwable cause) {
    this(errorCode, null, cause);
  }

  protected JsonApiException(ErrorCode errorCode, String message, Throwable cause) {
    this(errorCode, message, cause, Response.Status.OK);
  }

  protected JsonApiException(
      ErrorCode errorCode, String message, Throwable cause, Response.Status httpStatus) {
    super(message, cause);
    this.errorCode = errorCode;
    this.httpStatus = httpStatus;
    this.title = errorCode.getMessage();
    String errorFamily = getErrorFamily();
    this.errorFamily = errorFamily;
    this.errorScope = getErrorScope(errorFamily);
    this.operationsConfig =
        ConfigProvider.getConfig()
            .unwrap(SmallRyeConfig.class)
            .getConfigMapping(OperationsConfig.class);
  }

  /** {@inheritDoc} */
  @Override
  public CommandResult get() {
    // resolve message
    String message = getMessage();
    if (message == null) {
      message = errorCode.getMessage();
    }

    // construct and return
    CommandResult.Error error = getCommandResultError(message, httpStatus);

    // handle cause as well
    Throwable cause = getCause();
    if (null == cause) {
      return new CommandResult(List.of(error));
    } else {
      CommandResult.Error causeError = ThrowableToErrorMapper.getMapperFunction().apply(cause);
      return new CommandResult(List.of(error, causeError));
    }
  }

  public CommandResult.Error getCommandResultError(String message, Response.Status status) {
    Map<String, Object> fieldsForMetricsTag =
        Map.of("errorCode", errorCode.name(), "exceptionClass", this.getClass().getSimpleName());
    // enable debug mode for unit tests, since it can not be injected
    SmallRyeConfig config;
    if (ApiConstants.isOffline()) {
      config = new SmallRyeConfigBuilder().withMapping(DebugModeConfig.class).build();
    } else {
      config = ConfigProvider.getConfig().unwrap(SmallRyeConfig.class);
    }
    DebugModeConfig debugModeConfig = config.getConfigMapping(DebugModeConfig.class);
    final boolean debugEnabled = debugModeConfig.enabled();
    Map<String, Object> fields = null;
    if (debugEnabled) {
      fields =
          Map.of("errorCode", errorCode.name(), "exceptionClass", this.getClass().getSimpleName());
    } else if (operationsConfig.extendError()) {
      fields =
          Map.of(
              "errorCode",
              errorCode.name(),
              "family",
              errorFamily,
              "scope",
              errorScope,
              "title",
              title);
    } else {
      fields = Map.of("errorCode", errorCode.name());
    }
    return new CommandResult.Error(message, fieldsForMetricsTag, fields, status);
  }

  public CommandResult.Error getCommandResultError(Response.Status status) {
    return getCommandResultError(getMessage(), status);
  }

  public CommandResult.Error getCommandResultError() {
    return getCommandResultError(getMessage(), httpStatus);
  }

  public ErrorCode getErrorCode() {
    return errorCode;
  }

  public Response.Status getHttpStatus() {
    return httpStatus;
  }

  private String getErrorFamily() {
    // some error codes should be classified as SERVER errors but do not have any pattern
    Set<ErrorCode> serverFamily =
        new HashSet<>() {
          {
            add(COUNT_READ_FAILED);
            add(CONCURRENCY_FAILURE);
            add(TOO_MANY_COLLECTIONS);
            add(VECTOR_SEARCH_NOT_AVAILABLE);
            add(VECTOR_SEARCH_NOT_SUPPORTED);
            add(VECTORIZE_FEATURE_NOT_AVAILABLE);
            add(VECTORIZE_SERVICE_NOT_REGISTERED);
            add(VECTORIZE_SERVICE_TYPE_UNAVAILABLE);
            add(VECTORIZE_INVALID_AUTHENTICATION_TYPE);
            add(VECTORIZE_CREDENTIAL_INVALID);
            add(VECTORIZECONFIG_CHECK_FAIL);
            add(UNAUTHENTICATED_REQUEST);
            add(COLLECTION_CREATION_ERROR);
          }
        };

    if (serverFamily.contains(errorCode)
        || errorCode.name().startsWith("SERVER")
        || errorCode.name().startsWith("EMBEDDING")) {
      return "SERVER";
    }
    return "REQUEST";
  }

  private String getErrorScope(String family) {
    Set<ErrorCode> schemeScope =
        new HashSet<>() {
          {
            add(INVALID_CREATE_COLLECTION_OPTIONS);
            add(INVALID_USAGE_OF_VECTORIZE);
            add(VECTOR_SEARCH_INVALID_FUNCTION_NAME);
            add(VECTOR_SEARCH_TOO_BIG_VALUE);
            add(INVALID_PARAMETER_VALIDATION_TYPE);
          }
        };
    Set<ErrorCode> embeddingScope =
        new HashSet<>() {
          {
            add(INVALID_REQUEST);
            add(SERVER_EMBEDDING_GATEWAY_NOT_AVAILABLE);
          }
        };
    Set<ErrorCode> filterScope =
        new HashSet<>() {
          {
            add(ID_NOT_INDEXED);
          }
        };
    Set<ErrorCode> sortScope =
        new HashSet<>() {
          {
            add(VECTOR_SEARCH_USAGE_ERROR);
            add(VECTORIZE_USAGE_ERROR);
          }
        };
    Set<ErrorCode> documentScope =
        new HashSet<>() {
          {
            add(INVALID_VECTORIZE_VALUE_TYPE);
          }
        };

    // first handle special cases
    if (errorCode.name().equals("SERVER_INTERNAL_ERROR")) {
      return "";
    }
    if (schemeScope.contains(errorCode)) {
      return "SCHEME";
    }
    if (embeddingScope.contains(errorCode)) {
      return "EMBEDDING";
    }
    if (filterScope.contains(errorCode)) {
      return "FILTER";
    }
    if (sortScope.contains(errorCode)) {
      return "SORT";
    }
    if (documentScope.contains(errorCode)) {
      return "DOCUMENT";
    }
    if (errorCode.name().contains("SCHEME")) {
      return "SCHEME";
    }

    // decide the scope based in error code pattern
    if (errorCode.name().startsWith("EMBEDDING") || errorCode.name().startsWith("VECTORIZE")) {
      return "EMBEDDING";
    }
    if (errorCode.name().contains("FILTER")) {
      return "FILTER";
    }
    if (errorCode.name().contains("SORT")) {
      return "SORT";
    }
    if (errorCode.name().contains("INDEX")) {
      return "INDEX";
    }
    if (errorCode.name().contains("UPDATE")) {
      return "UPDATE";
    }
    if (errorCode.name().contains("SHRED") || errorCode.name().contains("DOCUMENT")) {
      return "DOCUMENT";
    }
    if (errorCode.name().contains("PROJECTION")) {
      return "PROJECTION";
    }
    if (errorCode.name().contains("AUTHENTICATION")) {
      return "AUTHENTICATION";
    }
    if (errorCode.name().contains("OFFLINE")) {
      return "DATA_LOADER";
    }

    // decide the scope based on family
    if (family.equals("SERVER")) {
      return "DATABASE";
    }

    return "";
  }
}
