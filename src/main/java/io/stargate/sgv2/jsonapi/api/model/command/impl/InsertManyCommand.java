package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.ModifyCommand;
import java.util.List;
import javax.annotation.Nullable;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
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
                  "When `true` the server will insert the documents in sequential order, otherwise when `false` the server is free to re-order the inserts and parallelize them for performance. See specifications for more info on failure modes.",
              defaultValue = "true")
          Boolean ordered) {}
}
