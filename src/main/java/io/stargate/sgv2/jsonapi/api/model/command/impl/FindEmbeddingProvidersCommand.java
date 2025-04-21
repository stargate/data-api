package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.api.model.command.GeneralCommand;
import io.stargate.sgv2.jsonapi.service.provider.ModelSupport;
import jakarta.validation.Valid;
import java.util.EnumSet;
import javax.annotation.Nullable;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(description = "Lists the available Embedding Providers for this database.")
@JsonTypeName(CommandName.Names.FIND_EMBEDDING_PROVIDERS)
public record FindEmbeddingProvidersCommand(
    @Valid @Nullable @Schema(type = SchemaType.OBJECT, implementation = Options.class)
        Options options)
    implements GeneralCommand {

  /**
   * By default, if includeModelStatus is not provided, only model in supported status will be
   * returned.
   */
  public record Options(
      @Schema(
              description = "Use the option to include models as in target support status.",
              type = SchemaType.OBJECT,
              implementation = ModelSupport.SupportStatus.class)
          EnumSet<ModelSupport.SupportStatus> includeModelStatus) {}

  /** {@inheritDoc} */
  @Override
  public CommandName commandName() {
    return CommandName.FIND_EMBEDDING_PROVIDERS;
  }
}
