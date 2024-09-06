package io.stargate.sgv2.jsonapi.exception;

import jakarta.ws.rs.core.Response;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Base for all exceptions returned from the API for external use (as opposed to ones only used
 * internally)
 *
 * <p>All errors are of a {@link ErrorFamily}, this class should not be used directly, one of the
 * subclasses should be used. There are further categorised to be errors have an optional {@link
 * ErrorScope}, that groups errors of a similar source together. Finally, the error has an {@link
 * ErrorCode} that is unique within the family and scope.
 *
 * <p>To facilitate better error messages we template the messages in a {@link ErrorTemplate} that
 * is loaded from a properties file. The body for the error may change with each instance of the
 * exception, for example to include the number of filters that were included in a request.
 *
 * <p>The process of adding a new Error Code is:
 *
 * <p>
 *
 * <ul>
 *   <li>Decide what {@link ErrorFamily} the code belongs to.
 *   <li>Decide if the error has a {@link ErrorScope}, such as errors with Embedding Providers, if
 *       it does not then use {@link ErrorScope#NONE}.
 *   <li>Decide on the {@link ErrorCode}, it should be unique within the Family and Scope
 *       combination.
 *   <li>Add the error to file read by {@link ErrorTemplate} to define the title and templated body
 *       body.
 *   <li>Add the error code to the Code enum for the Exception class, such as {@link
 *       FilterException.Code} or {@link RequestException.Code} with the same name. When the enum is
 *       instantiated at JVM start the error template is loaded.
 *   <li>Create instances using the methods on {@link ErrorCode}.
 * </ul>
 *
 * To get the Error to be returned in the {@link
 * io.stargate.sgv2.jsonapi.api.model.command.CommandResult} use a {@link
 * APIExceptionCommandErrorBuilder} all the logic for mapping to the API is in there to keep it out
 * of the core exception classes.
 */
public abstract class APIException extends RuntimeException {

  // All errors default to 200 HTTP status code, because we have partial failure modes.
  // There are some overrides, e.g. a server timeout may be a 500, this is managed in the
  // error config. See ErrorTemplate.
  public static final int DEFAULT_HTTP_STATUS = Response.Status.OK.getStatusCode();

  /**
   * HTTP Response code for this error. NOTE: Not using enum from quarkus because do not want
   * references to the HTTP framework this deep into the command processing
   */
  public final int httpStatus;

  /** Unique identifier for this error instance. */
  public final UUID errorId;

  /** The family of the error. */
  public final ErrorFamily family;

  /**
   * Optional scope of the error, inside the family.
   *
   * <p>Never {@code null}, uses "" for no scope. See {@link ErrorScope}
   */
  public final String scope;

  /** Unique code for this error, codes should be unique within the Family and Scope combination. */
  public final String code;

  /** Title of this exception, the same title is used for all instances of the error code. */
  public final String title;

  /**
   * Message body for this instance of the error.
   *
   * <p>Messages may be unique for each instance of the error code, they are typically driven from
   * the {@link ErrorTemplate}.
   *
   * <p>This is the processed body to be returned to the client. NOT called body to avoid confusion
   * with getMessage() on the RuntimeException
   */
  public final String body;

  public APIException(ErrorInstance errorInstance) {
    Objects.requireNonNull(errorInstance, "ErrorInstance cannot be null");

    this.errorId = errorInstance.errorId();
    this.family = errorInstance.family();
    this.scope = errorInstance.scope().scope();
    this.code = errorInstance.code();
    this.title = errorInstance.title();
    this.body = errorInstance.body();
    Objects.requireNonNull(errorInstance.httpStatusOverride(), "httpStatusOverride cannot be null");
    this.httpStatus = errorInstance.httpStatusOverride().orElse(DEFAULT_HTTP_STATUS);
  }

  public APIException(
      ErrorFamily family, ErrorScope scope, String code, String title, String body) {
    this(new ErrorInstance(UUID.randomUUID(), family, scope, code, title, body, Optional.empty()));
  }

  /**
   * Gets the fully qualified code of FAMILY_SCOPE_CODE
   *
   * <p>This is somewhwat backwards compatible with previous error codes, and may be used to group
   * errors of the same "type". Note that when grouping errors, the body may still be different even
   * if the fully qualified code is the same.
   *
   * @return qualified code of FAMILY_SCOPE_CODE , or when SCOPE is empty will be FAMILY_CODE
   */
  public String fullyQualifiedCode() {
    if (scope.isEmpty()) {
      return family.name() + "_" + code;
    }
    return family.name() + "_" + scope + "_" + code;
  }

  /**
   * Overrides to return the {@link #body} of the error. Using the body as this is effectively the
   * message, the structure we want to return to the in the API JSON comes from {@link
   * APIExceptionCommandErrorBuilder}
   *
   * @return
   */
  @Override
  public String getMessage() {
    return body;
  }

  @Override
  public String toString() {
    return String.format(
        "%s{errorId=%s, family=%s, scope='%s', code='%s', title='%s', body='%s'}",
        getClass().getSimpleName(), errorId, family, scope, code, title, body);
  }
}
