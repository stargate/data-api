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
 * Builder that creates a {@link CommandErrorV2} from an {@link APIException} or the legacy {@link
 * io.stargate.sgv2.jsonapi.exception.JsonApiException}.
 *
 * <p>This class encapsulates the mapping between the APIException and the API tier to keep it out
 * of the core exception classes. <b>NOTE:</b> aaron 9-oct-2024 needed to tweak this class to work
 * with the new CommandErrorV2, once we have rolled out the use of CommandErrorV2 everywhere we can
 * remove the legacy CommandResult.Error
 */
public class CommandErrorFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(CommandErrorFactory.class);

  private final boolean debugEnabled;

  public CommandErrorFactory() {
    this.debugEnabled = DebugConfigAccess.isDebugEnabled();
  }

  public CommandErrorV2 create(Throwable throwable) {
    return create(throwable, Collections.emptyList());
  }

  public CommandErrorV2 create(Throwable throwable, List<? extends DocRowIdentifer> documentIds) {
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

  public CommandErrorV2 create(JsonApiException jsonApiException) {
    return create(jsonApiException, Collections.emptyList());
  }

  public CommandErrorV2 create(
      JsonApiException jsonApiException, List<? extends DocRowIdentifer> documentIds) {

    Objects.requireNonNull(jsonApiException, "jsonApiException cannot be null");
    var builder = CommandErrorV2.builder();

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

  public CommandErrorV2 create(APIException apiException) {
    return create(apiException, Collections.emptyList());
  }

  /**
   * Create a new instance that will create a {@link CommandErrorV2} that represents the <code>
   * apiException</code>.
   *
   * @param apiException the exception that is going to be returned.
   * @return a {@link CommandErrorV2} that represents the <code>apiException</code>.
   */
  public CommandErrorV2 create(
      APIException apiException, List<? extends DocRowIdentifer> documentIds) {

    Objects.requireNonNull(apiException, "apiException cannot be null");
    var builder = CommandErrorV2.builder();

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

  //  /**
  //   * Create a new instance that will create a {@link CommandResult.Error} that represents the
  // <code>
  //   * apiException</code>.
  //   *
  //   * @param apiException the exception that is going to be returned.
  //   * @return a {@link CommandResult.Error} that represents the <code>apiException</code>.
  //   */
  //  public CommandResult.Error buildLegacyCommandResultError(APIException apiException) {
  //    // Note, in the old JsonApiException the code also traverses the cause, we do not want to do
  //    // that in
  //    // error objects V2 because the proper error is created by the template etc.
  //
  //    // aaron - 28 aug 2024 - This should change when we improve the APi classes that handle
  // errors,
  //    // for now have to work with what we have
  //    Map<String, Object> errorFields = new HashMap<>();
  //    // AJM - 28 aug 2024 - for now, the CommandResult.Error checks thats message is not in the
  //    // fields we send
  //    // will fix this later, keeping this here so we can see all the things we expect to pass.
  //    // TODO: refactor the CommandResult.Error so it has the the V2 fields and then change how we
  //    // create it here
  //    // errorFields.put(ErrorObjectV2Constants.Fields.MESSAGE, apiException.body);
  //    errorFields.put(ErrorObjectV2Constants.Fields.CODE, apiException.code);
  //
  //    if (returnErrorObjectV2) {
  //      errorFields.put(ErrorObjectV2Constants.Fields.FAMILY, apiException.family.name());
  //      errorFields.put(ErrorObjectV2Constants.Fields.SCOPE, apiException.scope);
  //      errorFields.put(ErrorObjectV2Constants.Fields.TITLE, apiException.title);
  //      errorFields.put(ErrorObjectV2Constants.Fields.ID, apiException.errorId);
  //    }
  //    if (debugEnabled) {
  //      errorFields.put(
  //          ErrorObjectV2Constants.Fields.EXCEPTION_CLASS,
  // apiException.getClass().getSimpleName());
  //    }
  //
  //    return new CommandResult.Error(
  //        apiException.body,
  //        tagsForMetrics(apiException),
  //        errorFields,
  //        Response.Status.fromStatusCode(apiException.httpStatus));
  //  }

}
