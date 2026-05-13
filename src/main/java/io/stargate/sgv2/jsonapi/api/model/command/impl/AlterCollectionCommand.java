package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.CollectionCommand;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import jakarta.validation.Valid;
import javax.annotation.Nullable;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(
    description =
        "Command that alters mutable settings of an existing collection. Currently supports enabling the 'lexical' feature.")
@JsonTypeName(CommandName.Names.ALTER_COLLECTION)
public record AlterCollectionCommand(
    @Valid
        @JsonInclude(JsonInclude.Include.NON_NULL)
        @Nullable
        @Schema(
            description =
                "Lexical configuration to apply. Currently only enabling is supported ('enabled' must be true).",
            type = SchemaType.OBJECT,
            implementation = CreateCollectionCommand.Options.LexicalConfigDefinition.class)
        CreateCollectionCommand.Options.LexicalConfigDefinition lexical)
    implements CollectionCommand {

  @Override
  public CommandName commandName() {
    return CommandName.ALTER_COLLECTION;
  }

  @Override
  public boolean isForceSchemaRefresh() {
    return true;
  }
}
