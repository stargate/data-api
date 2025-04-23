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
                  "If not provided, only SUPPORTED models will be returned."
                      + " If provided with \"\" empty string, all SUPPORTED, DEPRECATED, END_OF_LIFE model will be returned."
                      + " If provided with specified SUPPORTED or DEPRECATED or END_OF_LIFE, only models matched the status will be returned.",
              type = SchemaType.STRING,
              implementation = String.class)
          @Pattern(
              regexp = "^(SUPPORTED|DEPRECATED|END_OF_LIFE|supported|deprecated|end_of_life|)$")
          String includeModelStatus) {}

  /** {@inheritDoc} */
  @Override
  public CommandName commandName() {
    return CommandName.FIND_EMBEDDING_PROVIDERS;
  }
}
