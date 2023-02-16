package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.NamespaceCommand;
import javax.validation.constraints.NotBlank;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * Command for deleting a collection.
 *
 * @param name Name of the collection
 */
@Schema(description = "Command that deletes a collection if one exists.")
@JsonTypeName("deleteCollection")
public record DeleteCollectionCommand(
    @NotBlank
        @Schema(
            description = "Name of the collection",
            implementation = Object.class,
            type = SchemaType.OBJECT)
        String name)
    implements NamespaceCommand {}
