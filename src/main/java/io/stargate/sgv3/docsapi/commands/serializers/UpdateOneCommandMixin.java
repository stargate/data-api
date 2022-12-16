package io.stargate.sgv3.docsapi.commands.serializers;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv3.docsapi.commands.UpdateOneCommand.Options;
import io.stargate.sgv3.docsapi.commands.clauses.FilterClause;
import io.stargate.sgv3.docsapi.commands.clauses.UpdateClause;

/**
 * Jackson Mixin for {@link UpdateOneCommand}
 *
 * <p>see {@link CommandSerializer} for details
 */
@JsonTypeName("updateOne")
public abstract class UpdateOneCommandMixin {

  @JsonCreator
  public UpdateOneCommandMixin(
      @JsonProperty("filter") FilterClause filter,
      @JsonProperty("update") UpdateClause update,
      @JsonProperty("options") Options options) {

    throw new UnsupportedOperationException();
  }
}
