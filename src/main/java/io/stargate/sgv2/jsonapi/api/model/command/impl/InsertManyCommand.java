package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.ModifyCommand;
import io.stargate.sgv2.jsonapi.api.model.command.validation.MaxInsertManyDocuments;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import javax.annotation.Nullable;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * Representation of the insertMany API {@link Command}.
 *
 * @param documents The document to insert.
 * @param options Options for this command.
 */
@Schema(description = "Command that inserts multiple JSON document to a collection.")
@JsonTypeName("insertMany")
public record InsertManyCommand(
    @NotNull
        @NotEmpty
        @MaxInsertManyDocuments
        @Schema(
            description = "JSON document to insert.",
            implementation = Object.class,
            type = SchemaType.ARRAY)
        List<JsonNode> documents,
    @Nullable Options options)
    implements ModifyCommand {

  @Schema(name = "InsertManyCommand.Options", description = "Options for inserting many documents.")
  public record Options(
      @Schema(
              description =
                  "When `true` the server will insert the documents in sequential order, ensuring each document is successfully inserted before starting the next. Additionally the command will \"fail fast\", failing the first document that fails to insert. When `false` the server is free to re-order the inserts and parallelize them for performance. In this mode more than one document may fail to be inserted (aka \"fail silently\" mode).",
              defaultValue = "false")
          boolean ordered,
      @Schema(
              description =
                      "When `true`, response will contain 2 additional fields: 'insertedDocuments' and 'failedDocuments'" +
                      " both arrays of 2-element arrays; inner array containing document index (number) as the first element"+
                      " and the document id (if known) as the second element. If the document id is not known, the second element will be null.",
              defaultValue = "false")
      boolean returnDocumentPositions
  ) {}
}
