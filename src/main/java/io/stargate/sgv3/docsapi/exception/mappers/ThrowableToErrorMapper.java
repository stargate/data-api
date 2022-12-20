package io.stargate.sgv3.docsapi.exception.mappers;

import io.stargate.sgv3.docsapi.api.model.command.CommandResult;
import java.util.Map;
import java.util.function.Function;

/**
 * Simple mapper for mapping {@link Throwable}s to {@link CommandResult.Error}, with a default
 * implementation.
 */
public final class ThrowableToErrorMapper {

  private static final Function<Throwable, CommandResult.Error> MAPPER =
      throwable -> {
        String message = throwable.getMessage();
        if (message == null) {
          message = "Unexpected exception occurred.";
        }

        // add error code as error field
        Map<String, Object> fields = Map.of("exceptionClass", throwable.getClass().getSimpleName());
        return new CommandResult.Error(message, fields);
      };

  private ThrowableToErrorMapper() {}

  public static Function<Throwable, CommandResult.Error> getMapperFunction() {
    return MAPPER;
  }
}
