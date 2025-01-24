package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.model.command.*;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterSpec;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import jakarta.validation.Valid;
import java.util.Optional;
import javax.annotation.Nullable;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(description = "Command that finds a single JSON document from a collection.")
@JsonTypeName(CommandName.Names.FIND_ONE)
public record FindOneCommand(
    @Valid @JsonProperty("filter") FilterSpec filterSpec,
    @JsonProperty("projection") JsonNode projectionDefinition,
    @Valid @JsonProperty("sort") SortClause sortClause,
    @Valid @Nullable Options options)
    implements ReadCommand, Filterable, Projectable, Sortable, Windowable, VectorSortable {

  public record Options(

      // include similarity function score
      @Schema(
              description = "Include similarity function score in response.",
              type = SchemaType.BOOLEAN)
          boolean includeSimilarity,

      // return vector embedding used for ANN sorting
      @Schema(
              description = "Return vector embedding used for ANN sorting.",
              type = SchemaType.BOOLEAN)
          boolean includeSortVector) {}

  /** {@inheritDoc} */
  @Override
  public CommandName commandName() {
    return CommandName.FIND_ONE;
  }

  @Override
  public Optional<Integer> limit() {
    return Optional.of(1);
  }

  @Override
  public Optional<Boolean> includeSimilarityScore() {
    return options() == null ? Optional.empty() : Optional.of(options().includeSimilarity);
  }

  @Override
  public Optional<Boolean> includeSortVector() {
    return options() == null ? Optional.empty() : Optional.of(options().includeSortVector);
  }
}
