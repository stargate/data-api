package io.stargate.sgv2.jsonapi.api.model.command;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import jakarta.validation.constraints.NotNull;
import jakarta.ws.rs.core.Response;
import java.util.*;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.media.SchemaProperty;
import org.jboss.resteasy.reactive.RestResponse;

/**
 * POJO object (data no behavior) that has the result of running a command, either documents, list
 * of documents modified, or errors.
 *
 * <p>This class is part of the Command layer and is the bridge from the internal Command back to
 * the Message layer.
 *
 * <p>Because it is in the Command layer this is where we de-shred and do the Projection of what
 * fields we want from the document.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record CommandResult(
    @Schema(
            description =
                "A response data holding documents that were returned as the result of a command.",
            nullable = true,
            oneOf = {CommandResult.MultiResponseData.class, CommandResult.SingleResponseData.class})
        ResponseData data,
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
        Map<CommandStatus, Object> status,
    @JsonInclude(JsonInclude.Include.NON_EMPTY) @Schema(nullable = true) List<Error> errors) {

  /**
   * Constructor for only specifying the {@link MultiResponseData}.
   *
   * @param responseData {@link MultiResponseData}
   */
  public CommandResult(ResponseData responseData) {
    this(responseData, null, null);
  }

  /**
   * Constructor for specifying the {@link MultiResponseData} and statuses.
   *
   * @param responseData {@link MultiResponseData}
   * @param status Map of status information.
   */
  public CommandResult(ResponseData responseData, Map<CommandStatus, Object> status) {
    this(responseData, status, null);
  }

  /**
   * Constructor for only specifying the status.
   *
   * @param status Map of status information.
   */
  public CommandResult(Map<CommandStatus, Object> status) {
    this(null, status, null);
  }

  /**
   * Constructor for only specifying the errors.
   *
   * @param errors List of errors.
   */
  public CommandResult(List<Error> errors) {
    this(null, null, errors);
  }

  public interface ResponseData {

    /**
     * @return Simple shared method to get the response documents. Usually used only in tests,
     *     ignored in JSON response.
     */
    @JsonIgnore
    List<JsonNode> getResponseDocuments();
  }

  /**
   * Response data object that's included in the {@link CommandResult}, for a single document
   * responses.
   *
   * @param document Document.
   */
  @Schema(description = "Response data for a single document commands.")
  public record SingleResponseData(
      @NotNull
          @Schema(
              description = "Document that resulted from a command.",
              type = SchemaType.OBJECT,
              implementation = Object.class,
              nullable = true)
          JsonNode document)
      implements ResponseData {

    /** {@inheritDoc} */
    @Override
    public List<JsonNode> getResponseDocuments() {
      return List.of(document);
    }
  }

  /**
   * Response data object that's included in the {@link CommandResult}, for multi document
   * responses.
   *
   * @param documents Documents.
   * @param nextPageState Optional next page state.
   */
  @Schema(description = "Response data for multiple documents commands.")
  public record MultiResponseData(
      @NotNull
          @Schema(
              description = "Documents that resulted from a command.",
              type = SchemaType.ARRAY,
              implementation = Object.class,
              minItems = 0)
          List<JsonNode> documents,
      @Schema(description = "Next page state for pagination.", nullable = true)
          String nextPageState)
      implements ResponseData {

    /** {@inheritDoc} */
    @Override
    public List<JsonNode> getResponseDocuments() {
      return documents;
    }
  }

  /**
   * @param message Error message.
   * @param fields Error fields. Note that they are serialized at the same level as the message.
   */
  @Schema(
      type = SchemaType.OBJECT,
      description =
          "List of errors that occurred during a command execution. Can include additional properties besides the message that is always provided, like `errorCode`, `exceptionClass`, etc.",
      properties = {
        @SchemaProperty(
            name = "message",
            description = "Human-readable error message.",
            implementation = String.class)
      })
  public record Error(
      String message,
      @JsonIgnore @Schema(hidden = true) Map<String, Object> fieldsForMetricsTag,
      @JsonAnyGetter @Schema(hidden = true) Map<String, Object> fields,
      // Http status to be used in the response, defaulted to 200
      @JsonIgnore Response.Status httpStatus) {

    // this is a compact constructor for records
    // ensure message is not set in the fields key
    public Error {
      if (null != fields && fields.containsKey("message")) {
        throw ErrorCodeV1.SERVER_INTERNAL_ERROR.toApiException(
            "Error fields can not contain the reserved key 'message'");
      }
    }
  }

  /**
   * Create the {@link RestResponse} Maps CommandResult to RestResponse. Except for few selective
   * errors, all errors are mapped to http status 200. In case of 401, 500, 502 and 504 response is
   * sent with appropriate status code.
   *
   * @return
   */
  public RestResponse<CommandResult> toRestResponse() {

    if (null != this.errors()) {
      final Optional<Error> first =
          this.errors().stream()
              .filter(error -> error.httpStatus() != Response.Status.OK)
              .findFirst();

      if (first.isPresent()) {
        return RestResponse.ResponseBuilder.create(first.get().httpStatus(), this).build();
      }
    }
    return RestResponse.ok(this);
  }

  /**
   * returned a new CommandResult with warning message added in status map
   *
   * @param warning message
   * @return CommandResult
   */
  public CommandResult withWarning(String warning) {
    Map<CommandStatus, Object> newStatus = new HashMap<>();
    if (status != null) {
      newStatus.putAll(status);
    }
    List<String> newWarnings =
        newStatus.get(CommandStatus.WARNINGS) != null
            ? new ArrayList<>((List<String>) newStatus.get(CommandStatus.WARNINGS))
            : new ArrayList<>();
    newWarnings.add(warning);
    newStatus.put(CommandStatus.WARNINGS, newWarnings);
    return new CommandResult(this.data, newStatus, errors);
  }
}
