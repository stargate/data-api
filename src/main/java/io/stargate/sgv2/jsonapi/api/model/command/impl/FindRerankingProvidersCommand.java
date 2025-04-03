package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.api.model.command.GeneralCommand;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfig;
import jakarta.validation.Valid;
import java.util.Set;
import javax.annotation.Nullable;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(description = "Lists the available Reranking Providers for this database.")
@JsonTypeName(CommandName.Names.FIND_RERANKING_PROVIDERS)
public record FindRerankingProvidersCommand(
    @Valid @Nullable @Schema(type = SchemaType.OBJECT, implementation = Options.class)
        Options options)
    implements GeneralCommand {

  public record Options(
      // By default, if includeModelStatus is not provided, only model in supporting status will be
      // returned.
      @Schema(
              description = "Use the option to include models as in target support status.",
              type = SchemaType.ARRAY)
          Set<
                  RerankingProvidersConfig.RerankingProviderConfig.ModelConfig.ModelSupport
                      .SupportStatus>
              includeModelStatus) {}

  /** {@inheritDoc} */
  @Override
  public CommandName commandName() {
    return CommandName.FIND_RERANKING_PROVIDERS;
  }
}
