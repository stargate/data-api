package io.stargate.sgv2.jsonapi.exception.playing;

import java.util.UUID;
import java.util.function.Supplier;

/**
 * Base for any exceptions from the API.
 *
 * <p>All errors are of a {@link ErrorFamily}, this class should not be used directly, one of the
 * subclasses should be used. There are further categorised to be errors have an optional {@link
 * ErrorScope}, that groups errors of a similar source together. Finally, the error has an {@link
 * ErrorCode} that is unique within the family and scope.
 *
 * <p>To facilitate better error messages we template the messages in a {@link ErrorTemplate} that
 * is loaded from a properties file. The message for the error may change with each instance of the
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
 *   <li>Add the error to file read by {@link ErrorTemplate} to define the title and templated
 *       message body.
 *   <li>Add the error code to the Code enum for the Exception class, such as {@link
 *       FilterException.Code} or {@link RequestException.Code} with the same name. When the enum is
 *       instantiated at JVM start the error template is loaded.
 *   <li>Create instances using the methods on {@link ErrorCode}.
 * </ul>
 *
 * To get the Error to be returned in the {@link
 * io.stargate.sgv2.jsonapi.api.model.command.CommandResult} call the {@link #get()} method to get a
 * {@link CommandResponseError} that contains the subset of information we want to return.
 */
public abstract class APIException extends RuntimeException
    implements Supplier<CommandResponseError> {

  /** Unique identifier for this error instance. */
  public final UUID errorId;

  /** The family of the error. */
  public final ErrorFamily family;

  /**
   * Optional scope of the error, inside the family.
   *
   * <p>Always none null, uses "" for no scope. See {@link ErrorScope}
   */
  public final String scope;

  /** Unique code for this error, codes should be unique within the Family and Scope combination. */
  public final String code;

  /** Title of this exception, the same title is used for all instances of the error code. */
  public final String title;

  /**
   * Message for this instance of the error.
   *
   * <p>Messages may be unique for each instance of the error code, they are typically driven from
   * the {@link ErrorTemplate}.
   *
   * <p>This is the processed message to be returned to the client.
   */
  public final String message;

  public APIException(ErrorInstance errorInstance) {
    this.errorId = errorInstance.errorId();
    this.family = errorInstance.family();
    this.scope = errorInstance.scope().safeScope();
    this.code = errorInstance.code();
    this.title = errorInstance.title();
    this.message = errorInstance.message();
  }

  public APIException(
      ErrorFamily family, ErrorScope scope, String code, String title, String message) {
    this(new ErrorInstance(UUID.randomUUID(), family, scope, code, title, message));
  }

  @Override
  public CommandResponseError get() {
    return null;
  }

  @Override
  public String getMessage() {
    // TODO: work out the message, is it just the message or a formatted string of all the
    // properties ?
    return super.getMessage();
  }

  @Override
  public String toString() {
    // TODO: make a nice string for error logging etc.
    return super.toString();
  }
}
