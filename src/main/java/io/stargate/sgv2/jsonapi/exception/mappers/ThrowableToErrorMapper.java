package io.stargate.sgv2.jsonapi.exception.mappers;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Simple mapper for mapping {@link Throwable}s to {@link CommandResult.Error}, with a default
 * implementation.
 */
public final class ThrowableToErrorMapper {

  private static final BiFunction<Throwable, String, CommandResult.Error> MAPPER_WITH_MESSAGE =
      (throwable, message) -> {
        // if our own exception, shortcut
        if (throwable instanceof JsonApiException jae) {
          return jae.getCommandResultError(message);
        }
        // Override response error code
        if (throwable instanceof StatusRuntimeException sre) {
          Map<String, Object> fields =
              Map.of("exceptionClass", throwable.getClass().getSimpleName());
          if (sre.getStatus().getCode() == Status.Code.UNAUTHENTICATED) {
            return new CommandResult.Error(
                message, fields, CommandResult.Error.StatusCode.UNAUTHORIZED);
          } else if (sre.getStatus().getCode() == Status.Code.INTERNAL) {
            return new CommandResult.Error(
                message, fields, CommandResult.Error.StatusCode.INTERNAL_SERVER_ERROR);
          } else if (sre.getStatus().getCode() == Status.Code.UNAVAILABLE) {
            return new CommandResult.Error(
                message, fields, CommandResult.Error.StatusCode.BAD_GATEWAY);
          } else if (sre.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED) {
            return new CommandResult.Error(
                message, fields, CommandResult.Error.StatusCode.GATEWAY_TIMEOUT);
          }
        }
        // add error code as error field
        Map<String, Object> fields = Map.of("exceptionClass", throwable.getClass().getSimpleName());
        return new CommandResult.Error(message, fields, CommandResult.Error.StatusCode.OK);
      };

  private static final Function<Throwable, CommandResult.Error> MAPPER =
      throwable -> {
        String message = throwable.getMessage();
        if (message == null) {
          message = "Unexpected exception occurred.";
        }

        return MAPPER_WITH_MESSAGE.apply(throwable, message);
      };

  private ThrowableToErrorMapper() {}

  public static Function<Throwable, CommandResult.Error> getMapperFunction() {
    return MAPPER;
  }

  public static BiFunction<Throwable, String, CommandResult.Error> getMapperWithMessageFunction() {
    return MAPPER_WITH_MESSAGE;
  }
}
