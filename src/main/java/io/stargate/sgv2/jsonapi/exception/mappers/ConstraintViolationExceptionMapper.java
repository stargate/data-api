package io.stargate.sgv2.jsonapi.exception.mappers;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.RequestTracing;
import io.stargate.sgv2.jsonapi.exception.RequestException;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import java.util.Map;
import java.util.Set;
import org.jboss.resteasy.reactive.RestResponse;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;

/** Simple exception mapper for the {@link ConstraintViolationException}. */
public class ConstraintViolationExceptionMapper {
  private static final String PREFIX_POST_COMMAND = "postCommand.";

  /**
   * Maximum length of "value" to include without truncation: values longer than this will be
   * truncated to this length (plus marker)
   */
  private static final int MAX_VALUE_LENGTH_TO_INCLUDE = 1000;

  @ServerExceptionMapper
  public RestResponse<CommandResult> constraintViolationException(
      ConstraintViolationException exception) {
    // map all violations to errors
    Set<ConstraintViolation<?>> violations = exception.getConstraintViolations();
    var builder = CommandResult.statusOnlyBuilder(false, RequestTracing.NO_OP);
    violations.stream()
        .map(ConstraintViolationExceptionMapper::getError)
        .distinct()
        .forEach(builder::addCommandResultError);

    // return result
    return builder.build().toRestResponse();
  }

  private static CommandResult.Error getError(ConstraintViolation<?> violation) {
    String message = violation.getMessage();
    String propertyPath = violation.getPropertyPath().toString();
    // Let's remove useless "postCommand." prefix if seen
    if (propertyPath.startsWith(PREFIX_POST_COMMAND)) {
      propertyPath = propertyPath.substring(PREFIX_POST_COMMAND.length());
    }

    String propertyValueDesc = valueDescription(violation.getInvalidValue());
    RequestException ex =
        RequestException.Code.COMMAND_FIELD_VALUE_INVALID.get(
            Map.of("field", propertyPath, "value", propertyValueDesc, "message", message));
    return ex.getCommandResultError();
  }

  /** Helper method for construction description of value that caused the constraint violation. */
  private static String valueDescription(Object rawValue) {
    if (rawValue == null) {
      return "`null`";
    }
    // Some value types never truncated: Numbers, Booleans.
    if (rawValue instanceof Number
        || rawValue instanceof Boolean
        || rawValue.getClass().isPrimitive()) {
      return rawValue.toString();
    }

    // JSON values: do not include the whole value, just description
    if (rawValue instanceof JsonNode) {
      return "<JSON value of " + rawValue.toString().length() + " characters>";
    }

    String valueDesc = rawValue.toString();
    if (valueDesc.length() <= MAX_VALUE_LENGTH_TO_INCLUDE) {
      return String.format("\"%s\"", valueDesc);
    }
    return String.format(
        "\"%s\"...[TRUNCATED from %d to %d characters]",
        valueDesc, valueDesc.length(), MAX_VALUE_LENGTH_TO_INCLUDE);
  }
}
