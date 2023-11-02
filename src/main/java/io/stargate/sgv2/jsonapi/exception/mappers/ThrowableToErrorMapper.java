package io.stargate.sgv2.jsonapi.exception.mappers;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.NoNodeAvailableException;
import com.datastax.oss.driver.api.core.NodeUnavailableException;
import com.datastax.oss.driver.api.core.servererrors.QueryValidationException;
import com.datastax.oss.driver.api.core.servererrors.WriteTimeoutException;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.security.UnauthorizedException;
import io.smallrye.config.SmallRyeConfig;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.config.DebugModeConfig;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import jakarta.ws.rs.core.Response;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.eclipse.microprofile.config.ConfigProvider;

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
        SmallRyeConfig config = ConfigProvider.getConfig().unwrap(SmallRyeConfig.class);
        DebugModeConfig debugModeConfig = config.getConfigMapping(DebugModeConfig.class);
        final boolean debugEnabled = debugModeConfig.enabled();
        final Map<String, Object> fields =
            debugEnabled ? Map.of("exceptionClass", throwable.getClass().getSimpleName()) : null;
        final Map<String, Object> fieldsForMetricsTag =
            Map.of("exceptionClass", throwable.getClass().getSimpleName());
        if (throwable instanceof StatusRuntimeException sre) {
          if (sre.getStatus().getCode() == Status.Code.UNAUTHENTICATED) {
            return new CommandResult.Error(
                message, fieldsForMetricsTag, fields, Response.Status.UNAUTHORIZED);
          } else if (sre.getStatus().getCode() == Status.Code.INTERNAL) {
            return new CommandResult.Error(
                message, fieldsForMetricsTag, fields, Response.Status.INTERNAL_SERVER_ERROR);
          } else if (sre.getStatus().getCode() == Status.Code.UNAVAILABLE) {
            return new CommandResult.Error(
                message, fieldsForMetricsTag, fields, Response.Status.BAD_GATEWAY);
          } else if (sre.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED) {
            return new CommandResult.Error(
                message, fieldsForMetricsTag, fields, Response.Status.GATEWAY_TIMEOUT);
          }
        }
        if (throwable instanceof UnauthorizedException
            || throwable
                instanceof com.datastax.oss.driver.api.core.servererrors.UnauthorizedException) {
          return new CommandResult.Error(
              "UNAUTHENTICATED: Invalid token",
              fieldsForMetricsTag,
              fields,
              Response.Status.UNAUTHORIZED);
        } else if (throwable instanceof QueryValidationException) {
          if (message.contains("vector<float,")) { // TODO is there a better way?
            message = "Mismatched vector dimension";
          }
          return new CommandResult.Error(message, fieldsForMetricsTag, fields, Response.Status.OK);
        } else if (throwable instanceof NodeUnavailableException
            || throwable instanceof DriverException
            || throwable instanceof AllNodesFailedException
            || throwable instanceof NoNodeAvailableException) {
          return new CommandResult.Error(
              message, fieldsForMetricsTag, fields, Response.Status.INTERNAL_SERVER_ERROR);
        } else if (throwable instanceof DriverTimeoutException
            || throwable instanceof WriteTimeoutException) {
          return new CommandResult.Error(
              message, fieldsForMetricsTag, fields, Response.Status.GATEWAY_TIMEOUT);
        }
        return new CommandResult.Error(message, fieldsForMetricsTag, fields, Response.Status.OK);
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
