/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.stargate.sgv3.docsapi.api.exception;

import io.stargate.sgv3.docsapi.api.model.command.CommandResult;
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
    return new CommandResult.Error(msg, ERROR_FIELDS);
  }
}
