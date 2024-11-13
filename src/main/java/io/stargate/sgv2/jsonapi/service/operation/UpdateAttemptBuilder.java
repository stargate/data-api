package io.stargate.sgv2.jsonapi.service.operation;

import com.datastax.oss.driver.api.querybuilder.update.Update;
import io.stargate.sgv2.jsonapi.exception.FilterException;
import io.stargate.sgv2.jsonapi.exception.WithWarnings;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.query.UpdateValuesCQLClause;
import io.stargate.sgv2.jsonapi.service.operation.query.WhereCQLClause;
import io.stargate.sgv2.jsonapi.service.operation.tables.WhereCQLClauseAnalyzer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds an attempt to delete a row from an API Table, create a single instance and then call
 * {@link #build(WhereCQLClause)} for each different where clause the command creates.
 */
public class UpdateAttemptBuilder<SchemaT extends TableSchemaObject> {

  private static final Logger LOGGER = LoggerFactory.getLogger(UpdateAttemptBuilder.class);

  // first value is zero, but we increment before we use it
  private int readPosition = -1;

  private final SchemaT tableBasedSchema;
  private final WhereCQLClauseAnalyzer whereCQLClauseAnalyzer;

  public UpdateAttemptBuilder(SchemaT tableBasedSchema, boolean updateOne) {

    this.tableBasedSchema = tableBasedSchema;
    this.whereCQLClauseAnalyzer =
        new WhereCQLClauseAnalyzer(
            tableBasedSchema, WhereCQLClauseAnalyzer.StatementType.UPDATE_ONE);
  }

  public UpdateAttempt<SchemaT> build(
      WhereCQLClause<Update> whereCQLClause, WithWarnings<UpdateValuesCQLClause> updateCQLClause) {

    readPosition += 1;

    // TODO: this may be common for creating a read / delete / where attempt will look at how to
    // refactor once all done
    WhereCQLClauseAnalyzer.WhereClauseWithWarnings whereClauseWithWarnings = null;
    Exception exception = null;
    try {
      whereClauseWithWarnings = whereCQLClauseAnalyzer.analyse(whereCQLClause);
    } catch (FilterException filterException) {
      exception = filterException;
    }

    var attempt =
        new UpdateAttempt<>(
            readPosition, tableBasedSchema, updateCQLClause.target(), whereCQLClause);

    // ok to pass null exception, will be ignored
    attempt.maybeAddFailure(exception);

    // There should not be any warnings, we cannot turn on allow filtering for delete
    // and we should not be turning on  allow filtering for delete
    // sanity check
    if (whereClauseWithWarnings != null
        && (whereClauseWithWarnings.requiresAllowFiltering()
            || !whereClauseWithWarnings.isEmpty())) {
      throw new IllegalStateException(
          "Where clause analysis for update was not empty, analysis:%s"
              .formatted(whereClauseWithWarnings));
    }

    // add warnings from the CQL clauses to the attempt
    updateCQLClause.accept(attempt);
    return attempt;
  }
}
