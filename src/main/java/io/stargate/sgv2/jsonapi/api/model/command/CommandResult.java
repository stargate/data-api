package io.stargate.sgv2.jsonapi.api.model.command;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.validation.constraints.NotNull;
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
    @Schema(nullable = true) List<Error> errors) {

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
   * @param nextPageState Optional next paging state.
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

    /**
     * Constructor that sets documents without next paging state.
     *
     * @param documents Documents, must not be <code>null</code>.
     */
    public MultiResponseData(List<JsonNode> documents) {
      this(documents, null);
    }

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
      @JsonAnyGetter @Schema(hidden = true) Map<String, Object> fields,
      // Http status code to be used in the response, defaulted to 200
      @JsonIgnore StatusCode statusCode) {

    // this is a compact constructor for records
    // ensure message is not set in the fields key
    public Error {
      if (null != fields && fields.get("message") != null) {
        throw new IllegalArgumentException(
            "Error fields can not contain the reserved message key.");
      }
    }

    /**
     * Constructor that sets documents only the message.
     *
     * @param message Error message.
     */
    public Error(String message) {
      this(message, Collections.emptyMap(), StatusCode.OK);
    }

    public enum StatusCode {
      OK(200),
      UNAUTHORIZED(401),
      NOT_FOUND(404),
      METHOD_NOT_ALLOWED(405),
      INTERNAL_SERVER_ERROR(500),
      BAD_GATEWAY(502),
      GATEWAY_TIMEOUT(504);

      private final int code;

      StatusCode(int code) {
        this.code = code;
      }

      public int getCode() {
        return code;
      }
    }
  }

  /**
   * Maps CommandResult to RestResponse. Except for few selective errors, all errors are mapped to
   * http status 200. In case of 401, 500, 502 and 504 response is sent with appropriate status
   * code.
   *
   * @return
   */
  public RestResponse map() {
    if (null != this.errors() && !this.errors().isEmpty()) {
      final Optional<Error> first =
          this.errors().stream()
              .filter(error -> error.statusCode() != Error.StatusCode.OK)
              .findFirst();
      if (first.isPresent()) {
        final RestResponse.Status status =
            RestResponse.Status.fromStatusCode(first.get().statusCode().getCode());
        return RestResponse.ResponseBuilder.create(status, this).build();
      }
    }
    return RestResponse.ok(this);
  }
}
