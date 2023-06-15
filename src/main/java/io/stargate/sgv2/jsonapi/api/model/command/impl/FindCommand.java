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
import jakarta.validation.constraints.Positive;
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
      @Positive(message = "limit should be greater than `0`")
          @Schema(
              description =
                  "Maximum number of document that can be fetched for the command. If value is higher than the default page size, amount of returned documents will be limited to the default page size and paging state will be returned in the response, so a caller can to continue paging through documents.",
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
