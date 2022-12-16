package io.stargate.sgv3.docsapi.commands.serializers;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv3.docsapi.commands.FindCommand;
import io.stargate.sgv3.docsapi.commands.FindCommand.Options;
import io.stargate.sgv3.docsapi.commands.clauses.FilterClause;
import io.stargate.sgv3.docsapi.commands.clauses.ProjectionClause;
import io.stargate.sgv3.docsapi.commands.clauses.SortClause;

/**
 * Jackson Mixin for {@link FindCommand}
 *
 * <p>see {@link CommandSerializer} for details
 */
@JsonTypeName("find")
public abstract class FindCommandMixin {

  @JsonCreator
  public FindCommandMixin(
      @JsonProperty("filter") FilterClause filter,
      @JsonProperty("projection") ProjectionClause projection,
      @JsonProperty("sort") SortClause sort,
      @JsonProperty("options") Options options) {

    throw new UnsupportedOperationException();
  }
}
