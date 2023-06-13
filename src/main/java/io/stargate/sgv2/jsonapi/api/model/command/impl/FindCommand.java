package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.model.command.Filterable;
import io.stargate.sgv2.jsonapi.api.model.command.Projectable;
import io.stargate.sgv2.jsonapi.api.model.command.ReadCommand;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.validation.CheckFindOption;
import jakarta.validation.Valid;
import jakarta.validation.constraints.PositiveOrZero;
import javax.annotation.Nullable;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(description = "Command that finds a single JSON document from a collection.")
@JsonTypeName("find")
@CheckFindOption
public record FindCommand(
    @Valid @JsonProperty("filter") FilterClause filterClause,
    @JsonProperty("projection") JsonNode projectionDefinition,
    @Valid @JsonProperty("sort") SortClause sortClause,
    @Valid @Nullable Options options)
    implements ReadCommand, Filterable, Projectable {

  public record Options(

      // limit of returned documents
      @PositiveOrZero(message = "limit should be greater than or equal to `0`")
          // TODO this must have a limit, currently it's not enforced, I can ask for 1000 docs
          @Schema(
              description = "Maximum number of document that can be fetched for the command.",
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

      // paging state for pagination
      @Schema(
              description = "Next page state for pagination.",
              type = SchemaType.STRING,
              implementation = String.class)
          String pagingState) {}
}
