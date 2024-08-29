package io.stargate.sgv2.jsonapi.exception.playing;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.config.constants.ErrorObjectV2Constants;
import jakarta.ws.rs.core.Response;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Supplier that creates a {@link CommandResult.Error} from an {@link APIException}.
 *
 * <p>This class encapsulates the mapping between the APIException and the API tier to keep it out
 * of the core exception classes.
 */
public class APIExceptionCommandResultSupplier implements Supplier<CommandResult> {

  private final APIException apiException;
  private final boolean debugEnabled;
  private boolean returnErrorObjectV2;

  public APIExceptionCommandResultSupplier(APIException apiException) {
    this(apiException, false, true);
  }

  public APIExceptionCommandResultSupplier(
      APIException apiException, boolean debugEnabled, boolean returnErrorObjectV2) {
    this.apiException = Objects.requireNonNull(apiException, "APIException must not be null");
    this.debugEnabled = debugEnabled;
    this.returnErrorObjectV2 = returnErrorObjectV2;
  }

  @Override
  public CommandResult get() {
    // Note, in the old JsonApiException the code also traverses the cause, we do not want to do
    // that in
    // error objects V2 because the proper error is created by the template etc.
    return new CommandResult(List.of(commandResultError()));
  }

  private CommandResult.Error commandResultError() {

    // aaron - 28 aug 2024 - This should change when we improve the APi classes that handle errors,
    // for now have
    // to work with what we have
    Map<String, Object> errorFields = new HashMap<>();
    // AJM - 28 aug 2024 - for now, the CommandResult.Error checks thats message is not in the
    // fields we send
    // will fix this later, keeping this here so we can see all the things we expect to pass.
    // TODO: refactor the CommandResult.Error so it has the the V2 fields and then change how we
    // create it here
    // errorFields.put(ErrorObjectV2Constants.Fields.MESSAGE, apiException.body);
    errorFields.put(ErrorObjectV2Constants.Fields.CODE, apiException.code);

    if (returnErrorObjectV2) {
      errorFields.put(ErrorObjectV2Constants.Fields.FAMILY, apiException.family.name());
      errorFields.put(ErrorObjectV2Constants.Fields.SCOPE, apiException.scope);
      errorFields.put(ErrorObjectV2Constants.Fields.TITLE, apiException.title);
    }
    if (debugEnabled) {
      errorFields.put(
          ErrorObjectV2Constants.Fields.EXCEPTION_CLASS, apiException.getClass().getSimpleName());
    }

    return new CommandResult.Error(
        apiException.body,
        tagsForMetrics(),
        errorFields,
        Response.Status.fromStatusCode(apiException.httpStatus));
  }

  private Map<String, Object> tagsForMetrics() {
    // These tags must be backwards compatible with how we tracked before
    return Map.of(
        ErrorObjectV2Constants.MetricTags.ERROR_CODE, apiException.fullyQualifiedCode(),
        ErrorObjectV2Constants.MetricTags.EXCEPTION_CLASS, apiException.getClass().getSimpleName());
  }
}
