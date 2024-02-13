package io.stargate.sgv2.jsonapi.exception.mappers;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import jakarta.validation.Path;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jboss.resteasy.reactive.RestResponse;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;

/** Simple exception mapper for the {@link ConstraintViolationException}. */
public class ConstraintViolationExceptionMapper {

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
    Path propertyPath = violation.getPropertyPath();
    Object propertyValue = violation.getInvalidValue();
    String msg =
        "Request invalid, the field '%s' (value '%s') not valid: %s."
            .formatted(propertyPath, propertyValue, message);
    return new CommandResult.Error(msg, ERROR_FIELDS, ERROR_FIELDS_METRICS_TAG, Response.Status.OK);
  }
}
