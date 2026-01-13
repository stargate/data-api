package io.stargate.sgv2.jsonapi.api.model.command;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.RequestTracing;
import io.stargate.sgv2.jsonapi.exception.ServerException;
import jakarta.ws.rs.core.Response;
import java.util.*;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.media.SchemaProperty;
import org.jboss.resteasy.reactive.RestResponse;

/**
 * Use the {@link CommandResultBuilder} to create a {@link CommandResult} for a command response,
 * for creation see {@link #singleDocumentBuilder} and the other factory methods.
 *
 * <p>Comments on {@link CommandResultBuilder} explain future work here.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record CommandResult(
    @Schema(
            description =
                "A response data holding documents that were returned as the result of a command.",
            nullable = true,
            oneOf = {ResponseData.MultiResponseData.class, ResponseData.SingleResponseData.class})
        ResponseData data,
    // **
    @Schema(
            description =
                "Status objects, generally describe the side effects of commands, such as the number of updated or inserted documents.",
            nullable = true,
            minProperties = 1,
            properties = {
              @SchemaProperty(
                  name = "insertedIds",
                  description = "IDs of inserted documents for an insert command.",
                  type = SchemaType.ARRAY,
                  implementation = String.class,
                  nullable = true)
            })
        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        Map<CommandStatus, Object> status,
    // **
    @JsonInclude(JsonInclude.Include.NON_EMPTY) @Schema(nullable = true) List<CommandErrorV2> errors) {

  public CommandResult {

    // make both of these not null, so they can be mutated if needed
    // the decorators on the record fields tell Jackson to exclude when they are null or empty
    if (null == status) {
      status = new HashMap<>();
    }
    if (null == errors) {
      errors = new ArrayList<>();
    }
  }

  /**
   * Get a builder for the {@link CommandResult} for a single document response, see {@link
   * CommandResultBuilder}
   *
   * <p><b>NOTE:</b> aaron 9-oct-2024 I kept the errorObjectV2 param to make it clear how
   * inconsistency we are configuring these settings. Ultimately useErrorObjectV2 will go away. I
   * will create ticket so that we create the builder in resolver or similar and then pass it around
   * rather than creating in many places. Also the {@link
   * io.stargate.sgv2.jsonapi.service.operation.OperationAttemptPageBuilder} is how things will turn
   * out.
   */
  public static CommandResultBuilder singleDocumentBuilder(RequestTracing requestTracing) {
    return new CommandResultBuilder(
        CommandResultBuilder.ResponseType.SINGLE_DOCUMENT, requestTracing);
  }

  /** See {@link #singleDocumentBuilder(RequestTracing)} */
  public static CommandResultBuilder multiDocumentBuilder(RequestTracing requestTracing) {
    return new CommandResultBuilder(
        CommandResultBuilder.ResponseType.MULTI_DOCUMENT, requestTracing);
  }

  /** See {@link #singleDocumentBuilder(RequestTracing)} */
  public static CommandResultBuilder statusOnlyBuilder(RequestTracing requestTracing) {
    return new CommandResultBuilder(
        CommandResultBuilder.ResponseType.STATUS_ONLY, requestTracing);
  }

//  /**
//   * @param message Error message.
//   * @param fields Error fields. Note that they are serialized at the same level as the message.
//   */
//  @Schema(
//      type = SchemaType.OBJECT,
//      description =
//          "List of errors that occurred during a command execution. Can include additional properties besides the message that is always provided, like `errorCode`, `exceptionClass`, etc.",
//      properties = {
//        @SchemaProperty(
//            name = "message",
//            description = "Human-readable error message.",
//            implementation = String.class)
//      })
//
//  public record Error(
//      String message,
//      @JsonIgnore @Schema(hidden = true) Map<String, Object> fieldsForMetricsTag,
//      @JsonAnyGetter @Schema(hidden = true) Map<String, Object> fields,
//      // Http status to be used in the response, defaulted to 200
//      @JsonIgnore Response.Status httpStatus) {
//
//    // this is a compact constructor for records
//    // ensure message is not set in the fields key
//    public Error {
//      if (null != fields && fields.containsKey("message")) {
//        throw ServerException.internalServerError(
//            "Error fields can not contain the reserved key 'message'");
//      }
//    }
//  }

  /**
   * Create the {@link RestResponse} Maps CommandResult to RestResponse. Except for few selective
   * errors, all errors are mapped to http status 200. In case of 401, 500, 502 and 504 response is
   * sent with appropriate status code.
   *
   */
  public RestResponse<CommandResult> toRestResponse() {

    Optional<RestResponse<CommandResult>> maybeErrorResponse = errors == null ?
        Optional.empty()
        :
        errors().stream()
            .filter(error -> error.httpStatus() != Response.Status.OK)
            .findFirst()
            .map(firstError -> RestResponse.ResponseBuilder.create(firstError.httpStatus(), this).build());

    return maybeErrorResponse.orElseGet(() -> RestResponse.ok(this));
  }

  /**
   * returned a new CommandResult with warning message added in status map
   *
   * @param warning message
   */
  public void addWarning(CommandErrorV2 warning) {
    List<CommandErrorV2> warnings =
        (List<CommandErrorV2>)
            status.computeIfAbsent(CommandStatus.WARNINGS, k -> new ArrayList<>());
    warnings.add(warning);
  }
}
