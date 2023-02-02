package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.ModifyCommand;
import java.util.List;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * Representation of the insertMany API {@link Command}.
 *
 * @param document The document to insert.
 */
@Schema(description = "Command that inserts multiple JSON document to a collection.")
@JsonTypeName("insertMany")
public record InsertManyCommand(
    @NotNull
        @Schema(
            description = "JSON document to insert.",
            implementation = Object.class,
            type = SchemaType.ARRAY)
        List<JsonNode> documents,
    @Nullable Options options)
    implements ModifyCommand {
  public record Options() {}
}
