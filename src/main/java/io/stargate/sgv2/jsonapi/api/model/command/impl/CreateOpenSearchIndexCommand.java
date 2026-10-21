package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.CollectionCommand;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.api.model.command.IndexCreationCommand;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.indexes.OpenSearchIndexDefinitionDesc;
import io.stargate.sgv2.jsonapi.config.constants.TableDescConstants;
import io.stargate.sgv2.jsonapi.config.constants.TableDescDefaults;
import jakarta.annotation.Nullable;
import jakarta.validation.Valid;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * Command that creates an OpenSearch custom index on a table. The SAI index name is the {@code
 * name} field at the top level — it becomes the Cassandra custom index identifier and is used in
 * every subsequent {@code expr(<name>, ...)} CQL predicate.
 */
@Schema(
    description =
        "Creates an OpenSearch custom index on a table for full-text and aggregation queries.")
@JsonTypeName(CommandName.Names.CREATE_OPEN_SEARCH_INDEX)
@JsonPropertyOrder({
  TableDescConstants.IndexDesc.NAME,
  TableDescConstants.IndexDesc.DEFINITION,
  TableDescConstants.IndexDesc.OPTIONS
})
public record CreateOpenSearchIndexCommand(
    @Schema(description = "SAI index name (Cassandra custom index identifier).")
        @JsonProperty(TableDescConstants.IndexDesc.NAME)
        String name,
    @Nullable
        @Valid
        @JsonInclude(JsonInclude.Include.NON_NULL)
        @Schema(description = "Definition of the OpenSearch index.", type = SchemaType.OBJECT)
        @JsonProperty(TableDescConstants.IndexDesc.DEFINITION)
        OpenSearchIndexDefinitionDesc definition,
    @Nullable
        @JsonInclude(JsonInclude.Include.NON_NULL)
        @Schema(description = "Command-level options.", type = SchemaType.OBJECT)
        @JsonProperty(TableDescConstants.IndexDesc.OPTIONS)
        CommandOptions options)
    implements CollectionCommand, IndexCreationCommand {

  /** Command-level options for {@code createOpenSearchIndex}. */
  public record CommandOptions(
      @Nullable
          @Schema(
              description = "Do not error if an index with the same SAI name already exists.",
              defaultValue =
                  TableDescDefaults.CreateTextIndexOptionsDefaults.Constants.IF_NOT_EXISTS,
              type = SchemaType.BOOLEAN,
              implementation = Boolean.class)
          Boolean ifNotExists) {}

  @Override
  public CommandName commandName() {
    return CommandName.CREATE_OPEN_SEARCH_INDEX;
  }
}
