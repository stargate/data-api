package io.stargate.sgv2.jsonapi.exception;

import io.smallrye.config.SmallRyeConfig;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.config.DebugModeConfig;
import io.stargate.sgv2.jsonapi.exception.mappers.ThrowableToErrorMapper;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
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

  private final ErrorCode errorCode;

  public JsonApiException(ErrorCode errorCode) {
    this(errorCode, errorCode.getMessage(), null);
  }

  public JsonApiException(ErrorCode errorCode, String message) {
    this(errorCode, message, null);
  }

  public JsonApiException(ErrorCode errorCode, Throwable cause) {
    this(errorCode, null, cause);
  }

  public JsonApiException(ErrorCode errorCode, String message, Throwable cause) {
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

    // construct and return
    CommandResult.Error error = getCommandResultError(message);

    // handle cause as well
    Throwable cause = getCause();
    if (null == cause) {
      return new CommandResult(List.of(error));
    } else {
      CommandResult.Error causeError = ThrowableToErrorMapper.getMapperFunction().apply(cause);
      return new CommandResult(List.of(error, causeError));
    }
  }

  public CommandResult.Error getCommandResultError(String message) {
    Map<String, Object> fieldsForMetricsTag =
        Map.of("errorCode", errorCode.name(), "exceptionClass", this.getClass().getSimpleName());
    Map<String, Object> fields = null;
    SmallRyeConfig config = ConfigProvider.getConfig().unwrap(SmallRyeConfig.class);
    // enable debug mode for unit tests, since it can not be injected
    DebugModeConfig debugModeConfig = config.getConfigMapping(DebugModeConfig.class);
    final boolean debugEnabled = debugModeConfig.enabled();
    if (debugEnabled) {
      fields =
          Map.of("errorCode", errorCode.name(), "exceptionClass", this.getClass().getSimpleName());
    } else {
      fields = Map.of("errorCode", errorCode.name());
    }
    return new CommandResult.Error(message, fields, fieldsForMetricsTag, Response.Status.OK);
  }

  public ErrorCode getErrorCode() {
    return errorCode;
  }
}
