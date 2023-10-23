package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.NamespaceCommand;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(description = "Command that lists all available collections in a namespace.")
@JsonTypeName("findCollections")
public record FindCollectionsCommand(Options options) implements NamespaceCommand {
  public record Options(
      // include create collection options
      @Schema(
              description = "include collection properties.",
              type = SchemaType.BOOLEAN,
              implementation = Boolean.class)
          boolean explain) {}
}
