package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.SchemaChangeCommand;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(description = "Command that creates a collection.")
@JsonTypeName("createCollection")
public record CreateCollectionCommand(
    @NotNull
        @Schema(
            description = "Name of the collection",
            implementation = Object.class,
            type = SchemaType.OBJECT)
        String name,
    @Nullable Options options)
    implements SchemaChangeCommand {
  public record Options() {}
}
