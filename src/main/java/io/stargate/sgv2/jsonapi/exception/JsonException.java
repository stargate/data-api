package io.stargate.sgv2.jsonapi.exception;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.exception.mappers.ThrowableToErrorMapper;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Our own {@link RuntimeException} that uses {@link ErrorCode} to describe the exception cause.
 * Supports specification of the custom message.
 *
 * <p>Implements {@link Supplier< CommandResult >} so this exception can be mapped to command result
 * directly.
 */
public class JsonException extends RuntimeException implements Supplier<CommandResult> {

  private final ErrorCode errorCode;

  public JsonException(ErrorCode errorCode) {
    this(errorCode, errorCode.getMessage(), null);
  }

  public JsonException(ErrorCode errorCode, String message) {
    this(errorCode, message, null);
  }

  public JsonException(ErrorCode errorCode, Throwable cause) {
    this(errorCode, null, cause);
  }

  public JsonException(ErrorCode errorCode, String message, Throwable cause) {
    super(message, cause);
    this.errorCode = errorCode;
  }

  /** {@inheritDoc} */
  @Override
  public CommandResult get() {
    // resolve message
    String message = getMessage();
    if (message == null) {
      message = errorCode.getMessage();
    }

    // add error code as error field
    Map<String, Object> fields = Map.of("errorCode", errorCode.name());

    // construct and return
    CommandResult.Error error = new CommandResult.Error(message, fields);

    // handle cause as well
    Throwable cause = getCause();
    if (null == cause) {
      return new CommandResult(List.of(error));
    } else {
      CommandResult.Error causeError = ThrowableToErrorMapper.getMapperFunction().apply(cause);
      return new CommandResult(List.of(error, causeError));
    }
  }
}
