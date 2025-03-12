package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.model.command.*;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterSpec;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.FindAndRerankSort;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.validation.CheckFindOption;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

import javax.annotation.Nullable;
import java.util.Optional;

@Schema(description = "Finds documents using using vector and lexical sorting, then reranks the results.")
@JsonTypeName(CommandName.Names.FIND_AND_RERANK)
@CheckFindOption
public record FindAndRerankCommand(
    @Valid @JsonProperty("filter") FilterSpec filterSpec,
    @JsonProperty("projection") JsonNode projectionDefinition,
    @Valid @JsonProperty("sort") SortClause sortClause,
    @Valid @Nullable Options options)
    implements ReadCommand, Filterable, Projectable, Windowable, VectorSortable {

  public record Options(

      @Positive(message = "limit should be greater than `0`")
          @Schema(
              description =
                  "The maximum number of documents to return after the reranking service has ranked them.",
              type = SchemaType.INTEGER,
              implementation = Integer.class)
          Integer limit,

      @Positive(message = "limit should be greater than `0`")
      @Schema(
          description =
              "The maximum number of documents to read for the vector and lexical queries that feed into the reranking.",
          type = SchemaType.INTEGER,
          implementation = Integer.class)
      Integer hybridLimit,

      @Schema(
              description = "Include the scores from vectors and reranking in the response.",
              type = SchemaType.BOOLEAN)
          boolean includeScores,

      @Schema(
              description = "Return vector embedding used for ANN sorting.",
              type = SchemaType.BOOLEAN)
          boolean includeSortVector) {}

  /** {@inheritDoc} */
  @Override
  public CommandName commandName() {
    return CommandName.FIND;
  }

  @Override
  public Optional<Integer> limit() {
    return options() == null ? Optional.empty() : Optional.of(options().limit());
  }

  @Override
  public Optional<Boolean> includeSortVector() {
    return options() == null ? Optional.empty() : Optional.of(options().includeSortVector);
  }
}
