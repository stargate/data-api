package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import javax.annotation.Nullable;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * Representation of the offline begin-writer API {@link Command}.
 *
 * @param createCollectionCommand The document to insert.
 */
@Schema(description = "Command that initializes the offline writer.")
@JsonTypeName("offlineBeginWriter")
public record OfflineBeginWriterCommand(
    CreateCollectionCommand createCollectionCommand,
    @Nullable
        @Schema(
            description = "The SSTable output directory.",
            type = SchemaType.STRING,
            implementation = String.class)
        String ssTableOutputDirectory)
    implements Command {}
