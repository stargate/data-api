package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.CollectionCommand;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.api.model.command.IndexCreationCommand;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.indexes.RegularIndexDefinitionDesc;
import io.stargate.sgv2.jsonapi.config.constants.TableDescConstants;
import io.stargate.sgv2.jsonapi.config.constants.TableDescDefaults;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiIndexType;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(description = "Creates a regular index for a column in a table.")
@JsonTypeName(CommandName.Names.CREATE_INDEX)
@JsonPropertyOrder({
  TableDescConstants.IndexDesc.NAME,
  TableDescConstants.IndexDesc.DEFINITION,
  TableDescConstants.IndexDesc.INDEX_TYPE,
  TableDescConstants.IndexDesc.OPTIONS
})
public record CreateIndexCommand(
    @NotNull
        @Schema(description = "Name of the index.")
        @JsonProperty(TableDescConstants.IndexDesc.NAME)
        String name,
    //
    @NotNull
        @Schema(description = "Definition of the index to create.", type = SchemaType.OBJECT)
        @JsonProperty(TableDescConstants.IndexDesc.DEFINITION)
        RegularIndexDefinitionDesc definition,
    //
    @Nullable
        @Schema(
            description =
                "Optional type of the index to create. The only supported value is '"
                    + ApiIndexType.Constants.REGULAR
                    + "'.",
            type = SchemaType.STRING,
            defaultValue = ApiIndexType.Constants.REGULAR)
        @JsonProperty(TableDescConstants.IndexDesc.INDEX_TYPE)
        String indexType,
    //
    @Nullable
        @JsonInclude(JsonInclude.Include.NON_NULL)
        @Schema(description = "Options for the command.", type = SchemaType.OBJECT)
        @JsonProperty(TableDescConstants.IndexDesc.OPTIONS)
        CreateIndexCommandOptions options)
    implements CollectionCommand, IndexCreationCommand {

  /** Options for the command */
  public record CreateIndexCommandOptions(
      @Nullable
          @Schema(
              description = "True to ignore if index with the same name already exists.",
              defaultValue = TableDescDefaults.CreateIndexOptionsDefaults.Constants.IF_NOT_EXISTS,
              type = SchemaType.BOOLEAN,
              implementation = Boolean.class)
          Boolean ifNotExists) {}

  /** {@inheritDoc} */
  @Override
  public CommandName commandName() {
    return CommandName.CREATE_INDEX;
  }
}
