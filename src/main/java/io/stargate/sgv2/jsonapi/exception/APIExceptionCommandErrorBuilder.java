package io.stargate.sgv2.jsonapi.exception;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.config.constants.ErrorObjectV2Constants;
import jakarta.ws.rs.core.Response;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Builder that creates a {@link CommandResult.Error} from an {@link APIException}.
 *
 * <p>This class encapsulates the mapping between the APIException and the API tier to keep it out
 * of the core exception classes.
 */
public class APIExceptionCommandErrorBuilder
    implements Function<APIException, CommandResult.Error> {

  private final boolean debugEnabled;
  private final boolean returnErrorObjectV2;

  /**
   * Create a new instance that will create a {@link CommandResult.Error} with the given options.
   *
   * @param debugEnabled if <code>true</code> the {@link CommandResult.Error} will include the
   *     {@link ErrorObjectV2Constants.Fields#EXCEPTION_CLASS} field.
   * @param returnErrorObjectV2 if <code>true</code> will include the fields for the V2 error object
   *     such as family etc
   */
  public APIExceptionCommandErrorBuilder(boolean debugEnabled, boolean returnErrorObjectV2) {

    this.debugEnabled = debugEnabled;
    this.returnErrorObjectV2 = returnErrorObjectV2;
  }

  /**
   * Create a new instance that will create a {@link CommandResult.Error} that represents the <code>
   * apiException</code>.
   *
   * @param apiException the exception that is going to be returned.
   * @return a {@link CommandResult.Error} that represents the <code>apiException</code>.
   */
  @Override
  public CommandResult.Error apply(APIException apiException) {
    // Note, in the old JsonApiException the code also traverses the cause, we do not want to do
    // that in
    // error objects V2 because the proper error is created by the template etc.

    // aaron - 28 aug 2024 - This should change when we improve the APi classes that handle errors,
    // for now have to work with what we have
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
        tagsForMetrics(apiException),
        errorFields,
        Response.Status.fromStatusCode(apiException.httpStatus));
  }

  private Map<String, Object> tagsForMetrics(APIException apiException) {
    // These tags must be backwards compatible with how we tracked before
    return Map.of(
        ErrorObjectV2Constants.MetricTags.ERROR_CODE, apiException.fullyQualifiedCode(),
        ErrorObjectV2Constants.MetricTags.EXCEPTION_CLASS, apiException.getClass().getSimpleName());
  }
}
