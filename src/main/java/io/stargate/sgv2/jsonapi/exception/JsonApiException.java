package io.stargate.sgv2.jsonapi.exception;

import static io.stargate.sgv2.jsonapi.exception.ErrorCodeV1.*;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.RequestTracing;
import io.stargate.sgv2.jsonapi.config.DebugConfigAccess;
import io.stargate.sgv2.jsonapi.exception.mappers.ThrowableToErrorMapper;
import jakarta.ws.rs.core.Response;
import java.util.*;
import java.util.function.Supplier;

/**
 * Our own {@link RuntimeException} that uses {@link ErrorCodeV1} to describe the exception cause.
 * Supports specification of the custom message.
 *
 * <p>Implements {@link Supplier< CommandResult >} so this exception can be mapped to command result
 * directly.
 */
public class JsonApiException extends RuntimeException implements Supplier<CommandResult> {
  private final UUID id;

  private final ErrorCodeV1 errorCode;

  private final Response.Status httpStatus;

  private final String title;

  private final ErrorFamily errorFamily;

  private final ErrorScope errorScope;

  // some error codes should be classified as "SERVER" family but do not have any pattern
  private static final Set<ErrorCodeV1> serverFamily =
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
          add(INVALID_QUERY);
          add(NO_INDEX_ERROR);
        }
      };

  // map of error codes to error scope
  private static final Map<Set<ErrorCodeV1>, ErrorScope> errorCodeScopeMap =
      Map.of(
          new HashSet<>() {
            {
              add(INVALID_CREATE_COLLECTION_OPTIONS);
              add(INVALID_USAGE_OF_VECTORIZE);
              add(VECTOR_SEARCH_INVALID_FUNCTION_NAME);
              add(VECTOR_SEARCH_TOO_BIG_VALUE);
              add(INVALID_PARAMETER_VALIDATION_TYPE);
              add(INVALID_ID_TYPE);
              add(INVALID_INDEXING_DEFINITION);
            }
          },
          ErrorScope.SCHEMA,
          new HashSet<>() {
            {
              add(INVALID_REQUEST);
              add(SERVER_EMBEDDING_GATEWAY_NOT_AVAILABLE);
            }
          },
          ErrorScope.EMBEDDING,
          new HashSet<>() {
            {
              add(ID_NOT_INDEXED);
            }
          },
          ErrorScope.FILTER,
          new HashSet<>() {
            {
              add(VECTOR_SEARCH_USAGE_ERROR);
              add(VECTORIZE_USAGE_ERROR);
            }
          },
          ErrorScope.SORT,
          new HashSet<>() {
            {
              add(INVALID_VECTORIZE_VALUE_TYPE);
            }
          },
          ErrorScope.DOCUMENT);

  protected JsonApiException(ErrorCodeV1 errorCode) {
    this(errorCode, errorCode.getMessage(), null);
  }

  // Still needed by EmbeddingGatewayClient for gRPC, needs to remain public
  public JsonApiException(ErrorCodeV1 errorCode, String message) {
    this(errorCode, message, null);
  }

  protected JsonApiException(ErrorCodeV1 errorCode, Throwable cause) {
    this(errorCode, null, cause);
  }

  protected JsonApiException(ErrorCodeV1 errorCode, String message, Throwable cause) {
    this(errorCode, message, cause, Response.Status.OK);
  }

  protected JsonApiException(
      ErrorCodeV1 errorCode, String message, Throwable cause, Response.Status httpStatus) {
    super(message, cause);
    this.id = UUID.randomUUID();
    this.errorCode = errorCode;
    this.httpStatus = httpStatus;
    this.title = errorCode.getMessage();
    this.errorFamily = getErrorFamily();
    this.errorScope = getErrorScope(errorFamily);
  }

  /** {@inheritDoc} */
  @Override
  public CommandResult get() {
    // resolve message
    String message = getMessage();
    if (message == null) {
      message = errorCode.getMessage();
    }

    var builder = CommandResult.statusOnlyBuilder(false, RequestTracing.NO_OP);

    // construct and return
    builder.addCommandResultError(getCommandResultError(message, httpStatus));
    // handle cause as well
    Throwable cause = getCause();
    if (null != cause) {
      builder.addCommandResultError(ThrowableToErrorMapper.getMapperFunction().apply(cause));
    }
    return builder.build();
  }

  public CommandResult.Error getCommandResultError(String message, Response.Status status) {
    Map<String, Object> fieldsForMetricsTag =
        Map.of("errorCode", errorCode.name(), "exceptionClass", this.getClass().getSimpleName());
    Map<String, Object> fields =
        new HashMap<>(
            Map.of(
                "id",
                id,
                "errorCode",
                errorCode.name(),
                "family",
                errorFamily,
                "scope",
                errorScope,
                "title",
                title));

    if (DebugConfigAccess.isDebugEnabled()) {
      fields.put("exceptionClass", this.getClass().getSimpleName());
    }

    return new CommandResult.Error(message, fieldsForMetricsTag, fields, status);
  }

  public CommandResult.Error getCommandResultError(Response.Status status) {
    return getCommandResultError(getMessage(), status);
  }

  public CommandResult.Error getCommandResultError() {
    return getCommandResultError(getMessage(), httpStatus);
  }

  public ErrorCodeV1 getErrorCode() {
    return errorCode;
  }

  public Response.Status getHttpStatus() {
    return httpStatus;
  }

  private ErrorFamily getErrorFamily() {
    if (serverFamily.contains(errorCode)
        || errorCode.name().startsWith("SERVER")
        || errorCode.name().startsWith("EMBEDDING")) {
      return ErrorFamily.SERVER;
    }
    return ErrorFamily.REQUEST;
  }

  private ErrorScope getErrorScope(ErrorFamily family) {
    for (Map.Entry<Set<ErrorCodeV1>, ErrorScope> entry : errorCodeScopeMap.entrySet()) {
      if (entry.getKey().contains(errorCode)) {
        return entry.getValue();
      }
    }

    // decide the scope based in error code pattern
    if (errorCode.name().contains("SCHEMA")) {
      return ErrorScope.SCHEMA;
    }
    if (errorCode.name().startsWith("EMBEDDING") || errorCode.name().startsWith("VECTORIZE")) {
      return ErrorScope.EMBEDDING;
    }
    if (errorCode.name().startsWith("RERANKING")) {
      return ErrorScope.RERANKING;
    }
    if (errorCode.name().contains("FILTER")) {
      return ErrorScope.FILTER;
    }
    if (errorCode.name().contains("SORT")) {
      return ErrorScope.SORT;
    }
    if (errorCode.name().contains("INDEX")) {
      return ErrorScope.INDEX;
    }
    if (errorCode.name().contains("UPDATE")) {
      return ErrorScope.UPDATE;
    }
    if (errorCode.name().contains("SHRED") || errorCode.name().contains("DOCUMENT")) {
      return ErrorScope.DOCUMENT;
    }
    if (errorCode.name().contains("PROJECTION")) {
      return ErrorScope.PROJECTION;
    }
    if (errorCode.name().contains("AUTHENTICATION")) {
      return ErrorScope.AUTHENTICATION;
    }
    if (errorCode.name().contains("OFFLINE")) {
      return ErrorScope.DATA_LOADER;
    }

    return ErrorScope.EMPTY;
  }

  enum ErrorFamily {
    SERVER,
    REQUEST
  }

  enum ErrorScope {
    AUTHENTICATION("AUTHENTICATION"),
    DATABASE("DATABASE"),
    DATA_LOADER("DATA_LOADER"),
    DOCUMENT("DOCUMENT"),
    EMBEDDING("EMBEDDING"),
    EMPTY(""),
    FILTER("FILTER"),
    INDEX("INDEX"),
    PROJECTION("PROJECTION"),
    RERANKING("RERANKING"),
    SCHEMA("SCHEMA"),
    SORT("SORT"),
    UPDATE("UPDATE");

    private final String scope;

    ErrorScope(String scope) {
      this.scope = scope;
    }

    @Override
    public String toString() {
      return this.scope;
    }
  }
}
