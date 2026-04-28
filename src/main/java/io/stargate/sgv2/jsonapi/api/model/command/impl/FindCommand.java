package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.model.command.*;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterDefinition;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.SortDefinition;
import io.stargate.sgv2.jsonapi.api.model.command.validation.CheckFindOption;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import java.util.Optional;
import javax.annotation.Nullable;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(description = "Command that finds JSON documents from a collection.")
@JsonTypeName(CommandName.Names.FIND)
@CheckFindOption
public record FindCommand(
    @Valid @JsonProperty("filter") FilterDefinition filterDefinition,
    @JsonProperty("projection") JsonNode projectionDefinition,
    @Valid @JsonProperty("sort") SortDefinition sortDefinition,
    @Valid @Nullable Options options)
    implements ReadCommand, Filterable, Projectable, Sortable, Windowable, VectorSortable {

  public record Options(

      // limit of returned documents
      @Positive(message = "limit should be greater than `0`")
          @Schema(
              description =
                  "Maximum number of documents to return. Defaults to the default page size, "
                      + OperationsConfig.DEFAULT_PAGE_SIZE
                      + ". If the limit is higher than the default page size, the response will include up to the default page size of documents and a page state so the caller can continue paging through documents.",
              type = SchemaType.INTEGER,
              implementation = Integer.class,
              defaultValue = "" + OperationsConfig.DEFAULT_PAGE_SIZE)
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
