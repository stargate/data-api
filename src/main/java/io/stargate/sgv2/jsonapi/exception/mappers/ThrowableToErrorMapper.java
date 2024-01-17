package io.stargate.sgv2.jsonapi.exception.mappers;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.auth.AuthenticationException;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.QueryValidationException;
import com.datastax.oss.driver.api.core.servererrors.ReadTimeoutException;
import com.datastax.oss.driver.api.core.servererrors.WriteTimeoutException;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.security.UnauthorizedException;
import io.smallrye.config.SmallRyeConfig;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.config.DebugModeConfig;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import jakarta.ws.rs.core.Response;
import java.util.List;
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
        Map<String, Object> fields =
            debugEnabled ? Map.of("exceptionClass", throwable.getClass().getSimpleName()) : null;
        final Map<String, Object> fieldsForMetricsTag =
            Map.of("exceptionClass", throwable.getClass().getSimpleName());
        if (throwable instanceof StatusRuntimeException sre) {
          if (sre.getStatus().getCode() == Status.Code.UNAUTHENTICATED) {
            return new CommandResult.Error(
                "UNAUTHENTICATED: Invalid token",
                fieldsForMetricsTag,
                fields,
                Response.Status.UNAUTHORIZED);
          } else if (sre.getStatus().getCode() == Status.Code.INTERNAL) {
            return new CommandResult.Error(
                message, fieldsForMetricsTag, fields, Response.Status.INTERNAL_SERVER_ERROR);
          } else if (sre.getStatus().getCode() == Status.Code.UNAVAILABLE) {
            return new CommandResult.Error(
                message, fieldsForMetricsTag, fields, Response.Status.BAD_GATEWAY);
          } else if (sre.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED) {
            return new CommandResult.Error(
                message, fieldsForMetricsTag, fields, Response.Status.OK);
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
          fields = Map.of("errorCode", ErrorCode.INVALID_REQUST.name());
          return new CommandResult.Error(message, fieldsForMetricsTag, fields, Response.Status.OK);
        } else if (throwable instanceof DriverTimeoutException
            || throwable instanceof WriteTimeoutException
            || throwable instanceof ReadTimeoutException) {
          return new CommandResult.Error(
              message, fieldsForMetricsTag, fields, Response.Status.OK);
        } else if (throwable instanceof DriverException) {
          if (throwable instanceof AllNodesFailedException) {
            Map<Node, List<Throwable>> nodewiseErrors =
                ((AllNodesFailedException) throwable).getAllErrors();
            if (!nodewiseErrors.isEmpty()) {
              List<Throwable> errors = nodewiseErrors.values().iterator().next();
              if (errors != null && !errors.isEmpty()) {
                Throwable error =
                    errors.stream()
                        .findAny()
                        .filter(
                            t ->
                                t instanceof AuthenticationException
                                    || t instanceof IllegalArgumentException)
                        .orElse(null);
                // connecting to oss cassandra throws AuthenticationException for invalid
                // credentials connecting to AstraDB throws IllegalArgumentException for invalid
                // token/credentials
                if (error instanceof AuthenticationException
                    || (error instanceof IllegalArgumentException
                        && (error.getMessage().contains("AUTHENTICATION ERROR")
                            || error
                                .getMessage()
                                .contains(
                                    "Provided username token and/or password are incorrect")))) {
                  return new CommandResult.Error(
                      "UNAUTHENTICATED: Invalid token",
                      fieldsForMetricsTag,
                      fields,
                      Response.Status.UNAUTHORIZED);
                }
              }
            }
          }
          return new CommandResult.Error(
              message, fieldsForMetricsTag, fields, Response.Status.INTERNAL_SERVER_ERROR);
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
