package io.stargate.sgv3.docsapi.commands.serializers;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv3.docsapi.commands.FindOneAndUpdateCommand.Options;
import io.stargate.sgv3.docsapi.commands.FindOneAndUpdateCommand.Options.ReturnDocumentOption;
import io.stargate.sgv3.docsapi.commands.clauses.FilterClause;
import io.stargate.sgv3.docsapi.commands.clauses.ProjectionClause;
import io.stargate.sgv3.docsapi.commands.clauses.UpdateClause;

/**
 * Jackson Mixin for {@link FindOneAndUpdateCommand}
 *
 * <p>see {@link CommandSerializer} for details
 */
@JsonTypeName("findOneAndUpdate")
public abstract class FindOneAndUpdateCommandMixin {

  @JsonCreator
  public FindOneAndUpdateCommandMixin(
      @JsonProperty("filter") FilterClause filter,
      @JsonProperty("update") UpdateClause update,
      @JsonProperty("projection") ProjectionClause projection,
      @JsonProperty("options") Options options) {

    throw new UnsupportedOperationException();
  }

  public abstract static class OptionsMixin {

    @JsonCreator
    public OptionsMixin(
        @JsonProperty("upsert") boolean upsert,
        @JsonProperty("returnDocument") ReturnDocumentOption returnDocument) {

      throw new UnsupportedOperationException();
    }
  }
}
