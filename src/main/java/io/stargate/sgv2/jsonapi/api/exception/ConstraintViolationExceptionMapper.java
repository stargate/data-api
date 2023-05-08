package io.stargate.sgv2.jsonapi.api.exception;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;
import javax.validation.Path;
import org.jboss.resteasy.reactive.RestResponse;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;

/** Simple exception mapper for the {@link ConstraintViolationException}. */
public class ConstraintViolationExceptionMapper {

  // constant error fields
  public static final Map<String, Object> ERROR_FIELDS =
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
    String msg = "Request invalid, the field %s not valid: %s.".formatted(propertyPath, message);
    return new CommandResult.Error(msg, ERROR_FIELDS, CommandResult.Error.StatusCode.OK);
  }
}
