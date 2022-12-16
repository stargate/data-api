package io.stargate.sgv3.docsapi.commands.serializers;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonCreator.Mode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.stargate.sgv3.docsapi.commands.clauses.update.UpdateClauseOperation;
import java.util.List;

/**
 * Jackson Mixin for {@link UpdateClause}
 *
 * <p>see {@link CommandSerializer} for details
 */
public abstract class UpdateClauseMixin {

  @JsonCreator(mode = Mode.DELEGATING)
  public UpdateClauseMixin(
      @JsonDeserialize(using = UpdateClauseOperationListDeserializer.class)
          List<UpdateClauseOperation> operations) {

    throw new UnsupportedOperationException();
  }
}
