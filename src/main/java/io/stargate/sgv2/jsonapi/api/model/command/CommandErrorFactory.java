package io.stargate.sgv2.jsonapi.api.model.command;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errVars;

import io.stargate.sgv2.jsonapi.config.DebugConfigAccess;
import io.stargate.sgv2.jsonapi.exception.APIException;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.exception.ServerException;
import io.stargate.sgv2.jsonapi.metrics.ExceptionMetrics;
import io.stargate.sgv2.jsonapi.service.shredding.DocRowIdentifer;
import jakarta.ws.rs.core.Response;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory that creates a {@link CommandError} from an {@link APIException} or the legacy {@link
 * io.stargate.sgv2.jsonapi.exception.JsonApiException}.
 *
 * <p><b>NOTE:</b> This holds some state on the DEBUG status,to decide if the exception class should
 * be in the output.
 *
 * <p>This class encapsulates the mapping between the APIException and the API tier to keep it out
 * of the core exception classes.
 */
public class CommandErrorFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(CommandErrorFactory.class);

  private final boolean debugEnabled;

  public CommandErrorFactory() {
    this.debugEnabled = DebugConfigAccess.isDebugEnabled();
  }

  /** See {@link #create(Throwable, List)}. */
  public CommandError create(Throwable throwable) {
    return create(throwable, Collections.emptyList());
  }

  /**
   * Create a {@link CommandError} from any throwable.
   *
   * <p>
   *
   * @param throwable Exception ofy any type. If not a {@link APIException} or the older {@link
   *     JsonApiException} it will be wrapped in a {@link
   *     ServerException.Code#UNEXPECTED_SERVER_ERROR} .
   * @param documentIds Nullable list of impacted documents or rows for the error.
   * @return New instance of {@link CommandError} representing the throwable.
   */
  public CommandError create(Throwable throwable, List<? extends DocRowIdentifer> documentIds) {
    Objects.requireNonNull(throwable, "throwable cannot be null");

    return switch (throwable) {
      case APIException apiException -> create(apiException, documentIds);
      case JsonApiException jsonApiException -> create(jsonApiException, documentIds);
      case Throwable t -> create(wrapThrowable(t), documentIds);
    };
  }

  private APIException wrapThrowable(Throwable throwable) {
    LOGGER.warn(
        "An unhandled Java exception was mapped to {}",
        ServerException.Code.UNEXPECTED_SERVER_ERROR.name(),
        throwable);
    return ServerException.Code.UNEXPECTED_SERVER_ERROR.get(errVars(throwable));
  }

  /** See {@link #create(Throwable, List)}. */
  public CommandError create(JsonApiException jsonApiException) {
    return create(jsonApiException, Collections.emptyList());
  }

  /** See {@link #create(Throwable, List)}. */
  public CommandError create(
      JsonApiException jsonApiException, List<? extends DocRowIdentifer> documentIds) {

    Objects.requireNonNull(jsonApiException, "jsonApiException cannot be null");
    var builder = CommandError.builder();

    if (debugEnabled) {
      builder.exceptionClass(jsonApiException.getClass().getSimpleName());
    }

    return builder
        .errorCode(jsonApiException.getErrorCode().name())
        .message(jsonApiException.getMessage())
        .httpStatus(jsonApiException.getHttpStatus())
        .metricsTags(ExceptionMetrics.tagsFor(jsonApiException))
        .family(jsonApiException.getErrorFamily().toString())
        .scope(jsonApiException.getErrorScope().toString())
        .title(jsonApiException.getTitle())
        .id(jsonApiException.getErrorId())
        .documentIds(documentIds == null ? Collections.emptyList() : documentIds)
        .build();
  }

  /** See {@link #create(Throwable, List)}. */
  public CommandError create(APIException apiException) {
    return create(apiException, Collections.emptyList());
  }

  /** See {@link #create(Throwable, List)}. */
  public CommandError create(
      APIException apiException, List<? extends DocRowIdentifer> documentIds) {

    Objects.requireNonNull(apiException, "apiException cannot be null");
    var builder = CommandError.builder();

    if (debugEnabled) {
      builder.exceptionClass(apiException.getClass().getSimpleName());
    }

    return builder
        .errorCode(apiException.code)
        .message(apiException.body)
        .httpStatus(Response.Status.fromStatusCode(apiException.httpStatus))
        .metricsTags(ExceptionMetrics.tagsFor(apiException))
        .family(apiException.family.name())
        .scope(apiException.scope)
        .title(apiException.title)
        .id(apiException.errorId)
        .documentIds(documentIds == null ? Collections.emptyList() : documentIds)
        .build();
  }
}
