package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.model.command.*;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterSpec;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.SortSpec;
import io.stargate.sgv2.jsonapi.api.model.command.validation.CheckFindOption;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import java.util.Optional;
import javax.annotation.Nullable;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(description = "Command that finds a single JSON document from a collection.")
@JsonTypeName(CommandName.Names.FIND)
@CheckFindOption
public record FindCommand(
    @Valid @JsonProperty("filter") FilterSpec filterSpec,
    @JsonProperty("projection") JsonNode projectionDefinition,
    @Valid @JsonProperty("sort") SortSpec sortSpec,
    @Valid @Nullable Options options)
    implements ReadCommand, Filterable, Projectable, Sortable, Windowable, VectorSortable {

  public record Options(

      // limit of returned documents
      @Positive(message = "limit should be greater than `0`")
          @Schema(
              description =
                  "Maximum number of document that can be fetched for the command. If value is higher than the default page size, amount of returned documents will be limited to the default page size and page state will be returned in the response, so a caller can to continue paging through documents.",
              type = SchemaType.INTEGER,
              implementation = Integer.class)
          Integer limit,

      // amount of documents to skip
      @PositiveOrZero(message = "skip should be greater than or equal to `0`")
          @Schema(
              description = "Skips provided number of documents before returning sorted documents.",
              type = SchemaType.INTEGER,
              implementation = Integer.class)
          Integer skip,

      // page state for pagination
      @Schema(
              description = "Next page state for pagination.",
              type = SchemaType.STRING,
              implementation = String.class)
          @JsonProperty("pageState")
          @JsonAlias("pagingState") // old name, 1.0.0-BETA-3 and prior
          String pageState,

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
    return CommandName.FIND;
  }

  @Override
  public Optional<Integer> limit() {
    return Optional.ofNullable(options()).map(Options::limit);
  }

  @Override
  public Optional<Integer> skip() {
    return Optional.ofNullable(options()).map(Options::skip).filter(skip -> skip > 0);
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
