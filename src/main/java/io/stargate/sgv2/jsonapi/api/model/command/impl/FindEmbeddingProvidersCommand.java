package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.api.model.command.GeneralCommand;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Pattern;
import javax.annotation.Nullable;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(description = "Lists the available Embedding Providers for this database.")
@JsonTypeName(CommandName.Names.FIND_EMBEDDING_PROVIDERS)
public record FindEmbeddingProvidersCommand(
    @Valid @Nullable @Schema(type = SchemaType.OBJECT, implementation = Options.class)
        Options options)
    implements GeneralCommand {

  public record Options(
      @Nullable
          @Schema(
              description =
                  "Optional filter to select models by required support status. If omitted, only SUPPORTED models are returned (ones which can be used when creating a new Collection or Table). Available values are SUPPORTED, DEPRECATED, and END_OF_LIFE (case-insensitive).",
              type = SchemaType.STRING,
              implementation = String.class)
          @Pattern(regexp = "(?i)^(SUPPORTED|DEPRECATED|END_OF_LIFE)?$")
          String filterModelStatus) {}

  /** {@inheritDoc} */
  @Override
  public CommandName commandName() {
    return CommandName.FIND_EMBEDDING_PROVIDERS;
  }
}
