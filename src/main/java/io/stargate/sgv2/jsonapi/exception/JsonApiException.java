package io.stargate.sgv2.jsonapi.exception;

import static io.stargate.sgv2.jsonapi.exception.ErrorCodeV1.*;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import jakarta.ws.rs.core.Response;
import java.util.*;
import java.util.function.Supplier;

/**
 * Our older {@link RuntimeException} that uses {@link ErrorCodeV1} to describe the exception cause.
 * Supports specification of the custom message. Replaced by {@link APIException}.
 *
 * <p>Implements {@link Supplier< CommandResult >} so this exception can be mapped to command result
 * directly.
 */
public class JsonApiException extends RuntimeException {

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
          add(VECTORIZE_FEATURE_NOT_AVAILABLE);
          add(VECTORIZE_CREDENTIAL_INVALID);
        }
      };

  // map of error codes to error scope
  private static final Map<Set<ErrorCodeV1>, ErrorScope> errorCodeScopeMap =
      Map.of(
          new HashSet<>() {
            {
              add(VECTOR_SEARCH_TOO_BIG_VALUE);
            }
          },
          ErrorScope.SCHEMA);

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
    this.errorScope = getErrorScope();
  }

  public UUID getErrorId() {
    return id;
  }

  public String getTitle() {
    return title;
  }

  public ErrorCodeV1 getErrorCode() {
    return errorCode;
  }

  public String getFullyQualifiedErrorCode() {
    return getErrorFamily() + "_" + getErrorScope() + "_" + errorCode.name();
  }

  public Response.Status getHttpStatus() {
    return httpStatus;
  }

  public ErrorFamily getErrorFamily() {
    if (serverFamily.contains(errorCode)
        || errorCode.name().startsWith("SERVER")
        || errorCode.name().startsWith("EMBEDDING")) {
      return ErrorFamily.SERVER;
    }
    return ErrorFamily.REQUEST;
  }

  public ErrorScope getErrorScope() {
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

  public enum ErrorFamily {
    SERVER,
    REQUEST
  }

  public enum ErrorScope {
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
