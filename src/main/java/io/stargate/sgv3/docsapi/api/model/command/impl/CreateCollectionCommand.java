package io.stargate.sgv3.docsapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv3.docsapi.api.model.command.SchemaChangeCommand;
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
        String name)
    implements SchemaChangeCommand {}
