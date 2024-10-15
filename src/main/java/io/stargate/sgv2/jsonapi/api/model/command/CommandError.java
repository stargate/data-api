package io.stargate.sgv2.jsonapi.api.model.command;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import jakarta.ws.rs.core.Response;
import java.util.*;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * Intended to replace the {@link CommandResult.Error} with something that has all the fields in an
 * error.
 *
 * <p>This super class is the legacy V1 structure, and the subclass {@link CommandErrorV2} is the
 * new V2 structure. Once we have fully removed the {@link CommandResult.Error} we can remove this
 * class and just have the V2 structure.
 *
 * <p>This object is intended to be used in the CommandResult, and is serialised into the JSON
 * response.
 *
 * <p>aaron - 9 -oct-2024 - This is initially only used with the Warnings in the CommandResult,
 * further work needed to use it in all places. Warnings are in the status, and not described on the
 * swagger for the CommandResult. This is why all the decorators from the original class are not
 * here yet, but they have things to ignore flagged just incase
 */
public class CommandError {

  private final String errorCode;
  private final String message;
  private final Response.Status httpStatus;
  private final String errorClass;
  private final Map<String, Object> metricsTags;

  public CommandError(
      String errorCode,
      String message,
      Response.Status httpStatus,
      String errorClass,
      Map<String, Object> metricsTags) {

    this.errorCode = requireNoNullOrBlank(errorCode, "errorCode cannot be null or blank");
    this.message = requireNoNullOrBlank(message, "message cannot be null or blank");
    this.httpStatus = Objects.requireNonNull(httpStatus, "httpStatus cannot be null");

    this.metricsTags = metricsTags == null ? Collections.emptyMap() : Map.copyOf(metricsTags);
    // errorClass is not required, it is only passed when we are in debug mode
    // normalise to null if blank
    this.errorClass = errorClass == null || errorClass.isBlank() ? null : errorClass;
  }

  protected static String requireNoNullOrBlank(String value, String message) {
    if (Objects.isNull(value) || value.isBlank()) {
      throw new IllegalArgumentException(message);
    }
    return value;
  }

  /**
   * @return
   */
  public String getErrorCode() {
    return errorCode;
  }

  public String getMessage() {
    return message;
  }

  @JsonIgnore
  public Response.Status httpStatus() {
    return httpStatus;
  }

  @JsonIgnore
  @Schema(hidden = true)
  public Map<String, Object> metricsTags() {
    return metricsTags;
  }

  @Schema(hidden = true)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String exceptionClass() {
    return errorClass;
  }
}
