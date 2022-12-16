package io.stargate.sgv3.docsapi.commands.serializers;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv3.docsapi.commands.FindOneCommand;
import io.stargate.sgv3.docsapi.commands.FindOneCommand.Options;
import io.stargate.sgv3.docsapi.commands.clauses.FilterClause;
import io.stargate.sgv3.docsapi.commands.clauses.ProjectionClause;
import io.stargate.sgv3.docsapi.commands.clauses.SortClause;

/**
 * Jackson Mixin for {@link FindOneCommand}
 *
 * <p>see {@link CommandSerializer} for details
 */
@JsonTypeName("findOne")
public abstract class FindOneCommandMixin {

  @JsonCreator
  public FindOneCommandMixin(
      @JsonProperty("filter") FilterClause filter,
      @JsonProperty("projection") ProjectionClause projection,
      @JsonProperty("sort") SortClause sort,
      @JsonProperty("options") Options options) {

    throw new UnsupportedOperationException();
  }
}
