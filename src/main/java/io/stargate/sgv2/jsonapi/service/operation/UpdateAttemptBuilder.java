package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.querybuilder.update.Update;
import io.stargate.sgv2.jsonapi.exception.FilterException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableBasedSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.UpdateAttempt;
import io.stargate.sgv2.jsonapi.service.operation.query.UpdateValuesCQLClause;
import io.stargate.sgv2.jsonapi.service.operation.query.WhereCQLClause;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds an attempt to delete a row from an API Table, create a single instance and then call
 * {@link #build(WhereCQLClause)} for each different where clause the command creates.
 */
public class UpdateAttemptBuilder<SchemaT extends TableBasedSchemaObject> {

  private static final Logger LOGGER = LoggerFactory.getLogger(UpdateAttemptBuilder.class);

  // first value is zero, but we increment before we use it
  private int readPosition = -1;

  private final SchemaT tableBasedSchema;
  private final WhereCQLClauseAnalyzer whereCQLClauseAnalyzer;
  private final UpdateValuesCQLClause updateCQLClause;

  public UpdateAttemptBuilder(SchemaT tableBasedSchema, UpdateValuesCQLClause updateCQLClause) {

    this.tableBasedSchema = tableBasedSchema;
    this.whereCQLClauseAnalyzer = new WhereCQLClauseAnalyzer(tableBasedSchema);
    this.updateCQLClause = updateCQLClause;
  }

  public UpdateAttempt<SchemaT> build(WhereCQLClause<Update> whereCQLClause) {

    readPosition += 1;

    // TODO: this may be common for creating a read / delete / where attempt will look at how to
    // refactor once all done
    WhereCQLClauseAnalyzer.WhereClauseAnalysis analyzedResult = null;
    Exception exception = null;
    try {
      analyzedResult = whereCQLClauseAnalyzer.analyse(whereCQLClause);
    } catch (FilterException filterException) {
      exception = filterException;
    }

    var attempt =
        new UpdateAttempt<>(readPosition, tableBasedSchema, updateCQLClause, whereCQLClause);

    // ok to pass null exception, will be ignored
    attempt.maybeAddFailure(exception);

    // There should not be any warnings, we cannot turn on allow filtering for delete
    // and we should not be turning on  allow filtering for delete
    // sanity check
    if (analyzedResult != null && !analyzedResult.isEmpty()) {
      throw new IllegalStateException(
          "Where clause analysis for update was not empty, analysis:%s".formatted(analyzedResult));
    }

    return attempt;
  }
}
