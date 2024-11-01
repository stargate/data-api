package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import io.stargate.sgv2.jsonapi.exception.FilterException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.DeleteAttempt;
import io.stargate.sgv2.jsonapi.service.operation.query.WhereCQLClause;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds an attempt to delete a row from an API Table, create a single instance and then call
 * {@link #build(WhereCQLClause)} for each different where clause the command creates.
 */
public class DeleteAttemptBuilder<SchemaT extends TableSchemaObject> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DeleteAttemptBuilder.class);

  // first value is zero, but we increment before we use it
  private int attemptPosition = -1;

  private final SchemaT tableBasedSchema;
  private final WhereCQLClauseAnalyzer whereCQLClauseAnalyzer;

  /**
   * @param tableBasedSchema Table based schema object
   * @param deleteOne Set TRUE is this is a delete one operation, if true the builder will make sure
   *     the where clause has the full PK specified, otherwise it can be partial.
   */
  public DeleteAttemptBuilder(SchemaT tableBasedSchema, boolean deleteOne) {

    this.tableBasedSchema = tableBasedSchema;
    this.whereCQLClauseAnalyzer =
        new WhereCQLClauseAnalyzer(
            tableBasedSchema,
            deleteOne
                ? WhereCQLClauseAnalyzer.StatementType.DELETE_ONE
                : WhereCQLClauseAnalyzer.StatementType.DELETE_MANY);
  }

  public DeleteAttempt<SchemaT> build(WhereCQLClause<Delete> whereCQLClause) {

    attemptPosition += 1;

    // TODO: this may be common for creating a read / delete / where attempt will look at how to
    // refactor once all done
    WhereCQLClauseAnalyzer.WhereClauseWithWarnings analyzedResult = null;
    Exception exception = null;
    try {
      analyzedResult = whereCQLClauseAnalyzer.analyse(whereCQLClause);
    } catch (FilterException filterException) {
      exception = filterException;
    }

    var attempt = new DeleteAttempt<>(attemptPosition, tableBasedSchema, whereCQLClause);

    // ok to pass null exception, will be ignored
    attempt.maybeAddFailure(exception);

    // There should not be any warnings, we cannot turn on allow filtering for delete
    // and we should not be turning on  allow filtering for delete
    // sanity check
    if (analyzedResult != null && !analyzedResult.isEmpty()) {
      throw new IllegalStateException(
          "Where clause analysis for delete was not empty, analysis:%s".formatted(analyzedResult));
    }

    return attempt;
  }
}
