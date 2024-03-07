package io.stargate.sgv2.jsonapi.exception.mappers;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCommand;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import jakarta.ws.rs.core.Response;
import java.util.List;
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

  // constant error fields
  public static final Map<String, Object> ERROR_FIELDS =
      Map.of("exceptionClass", ConstraintViolationException.class.getSimpleName());

  public static final Map<String, Object> ERROR_FIELDS_METRICS_TAG =
      Map.of("exceptionClass", ConstraintViolationException.class.getSimpleName());

  @ServerExceptionMapper
  public RestResponse<CommandResult> constraintViolationException(
      ConstraintViolationException exception) {
    // map all violations to errors
    Set<ConstraintViolation<?>> violations = exception.getConstraintViolations();
    List<CommandResult.Error> errors =
        violations.stream().map(ConstraintViolationExceptionMapper::getError).distinct().toList();

    // return result
    CommandResult commandResult = new CommandResult(errors);
    return RestResponse.ok(commandResult);
  }

  private static CommandResult.Error getError(ConstraintViolation<?> violation) {
    String message = violation.getMessage();
    String propertyPath = violation.getPropertyPath().toString();
    // Let's remove useless "postCommand." prefix if seen
    if (propertyPath.startsWith(PREFIX_POST_COMMAND)) {
      propertyPath = propertyPath.substring(PREFIX_POST_COMMAND.length());
    }

    String propertyValueDesc = valueDescription(violation.getInvalidValue(), propertyPath);
    JsonApiException ex =
        ErrorCode.COMMAND_FIELD_INVALID.toApiException(
            "field '%s' value %s not valid. Problem: %s.",
            propertyPath, propertyValueDesc, message);
    return ex.getCommandResultError(ex.getMessage(), Response.Status.OK);
  }

  /** Helper method for construction description of value that caused the constraint violation. */
  private static String valueDescription(Object rawValue, String propertyPath) {
    if (rawValue == null) {
      return "'null'";
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

    if (rawValue instanceof FindCommand) {
      if (propertyPath.contains("options")) {
        return String.format("'%s'", ((FindCommand) rawValue).options());
      }
    }

    String valueDesc = rawValue.toString();
    if (valueDesc.length() <= MAX_VALUE_LENGTH_TO_INCLUDE) {
      return String.format("'%s'", valueDesc);
    }
    return String.format(
        "'%s'...[TRUNCATED from %d to %d characters]",
        valueDesc, valueDesc.length(), MAX_VALUE_LENGTH_TO_INCLUDE);
  }
}
