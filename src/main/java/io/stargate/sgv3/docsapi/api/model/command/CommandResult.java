package io.stargate.sgv3.docsapi.api.model.command;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.stargate.sgv3.docsapi.api.model.command.serializers.ErrorSerializer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.validation.constraints.NotNull;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.media.SchemaProperty;

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
            nullable = true)
        ResponseData data,
    @Schema(
            description =
                "Status objects, generally describe the side effects of commands, such as the number of updated or inserted documents.",
            nullable = true)
        Map<CommandStatus, Object> status,
    @Schema(
            description = "List of errors that occurred during a command execution.",
            nullable = true)
        List<Error> errors) {

  /**
   * Constructor for only specifying the {@link ResponseData}.
   *
   * @param responseData {@link ResponseData}
   */
  public CommandResult(ResponseData responseData) {
    this(responseData, null, null);
  }

  /**
   * Constructor for specifying the {@link ResponseData} and statuses.
   *
   * @param responseData {@link ResponseData}
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

  /**
   * Response data object that's included in the {@link CommandResult}.
   *
   * @param docs Documents.
   * @param nextPageState
   * @param count
   */
  public record ResponseData(
      @NotNull
          @Schema(
              description = "Documents that resulted from a command.",
              type = SchemaType.ARRAY,
              minItems = 0)
          List<JsonNode> docs,
      @Schema(description = "Next page state for pagination.", nullable = true)
          String nextPageState,
      @Schema(description = "Count of returned documents.") int count) {

    /**
     * Constructor that sets documents without next paging state.
     *
     * @param docs Documents, must not be <code>null</code>.
     */
    public ResponseData(List<JsonNode> docs) {
      this(docs, null);
    }

    /**
     * Constructor that sets documents with next paging state.
     *
     * @param docs Documents, must not be <code>null</code>.
     * @param nextPageState Paging state
     */
    public ResponseData(List<JsonNode> docs, String nextPageState) {
      // TODO Aaron&Team is this correct, is count always relating to the number of documents in the
      // docs?
      this(docs, nextPageState, docs.size());
    }
  }

  /**
   * @param message Error message.
   * @param fields Error fields. Note that they are serialized at the same level as the message.
   */
  @JsonSerialize(using = ErrorSerializer.class)
  @Schema(
      type = SchemaType.OBJECT,
      properties = {
        @SchemaProperty(
            name = "message",
            description = "Human-readable error message.",
            implementation = String.class)
      })
  public record Error(String message, Map<String, Object> fields) {

    /**
     * Constructor that sets documents only the message.
     *
     * @param message Error message.
     */
    public Error(String message) {
      this(message, Collections.emptyMap());
    }
  }
}
