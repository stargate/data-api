package io.stargate.sgv2.jsonapi.api.model.command;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

public interface ResponseData {

  /**
   * @return Simple shared method to get the response documents. Usually used only in tests, ignored
   *     in JSON response.
   */
  @JsonIgnore
  List<JsonNode> getResponseDocuments();

  /**
   * Response data object that's included in the {@link CommandResult}, for a single document
   * responses.
   *
   * @param document Document.
   */
  @Schema(description = "Response data for a single document commands.")
  record SingleResponseData(
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
  record MultiResponseData(
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
}
