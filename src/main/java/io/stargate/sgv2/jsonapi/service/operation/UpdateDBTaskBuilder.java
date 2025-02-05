package io.stargate.sgv2.jsonapi.service.operation;

import com.datastax.oss.driver.api.querybuilder.update.Update;
import io.stargate.sgv2.jsonapi.exception.FilterException;
import io.stargate.sgv2.jsonapi.exception.WithWarnings;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableBasedSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.query.UpdateValuesCQLClause;
import io.stargate.sgv2.jsonapi.service.operation.query.WhereCQLClause;
import io.stargate.sgv2.jsonapi.service.operation.tables.WhereCQLClauseAnalyzer;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Builds an attempt to delete a row from an API Table, create a single instance and then call
 * {@link #build(WhereCQLClause)} for each different where clause the command creates.
 */
public class UpdateDBTaskBuilder extends TaskBuilder<UpdateDBTask<TableSchemaObject>, TableSchemaObject> {

  private static final Logger LOGGER = LoggerFactory.getLogger(UpdateDBTaskBuilder.class);

  private Boolean updateOne = null;

  public UpdateDBTaskBuilder( TableSchemaObject schemaObject) {
    super(schemaObject);
  }

  public UpdateDBTaskBuilder withUpdateOne(Boolean updateOne) {
    this.updateOne = updateOne;
    return this;
  }

  public UpdateDBTask<TableSchemaObject> build(
      WhereCQLClause<Update> whereCQLClause, WithWarnings<UpdateValuesCQLClause> updateCQLClause) {

    Objects.requireNonNull(updateOne, "updateOne must be set");

    // If we are a table not a collection, then we only support update one
    // and we onl have tables in this class for now.
    if (!updateOne) {
      throw new IllegalStateException("Update many is not supported for tables");
    }
    var whereCQLClauseAnalyzer = new WhereCQLClauseAnalyzer(
            schemaObject, WhereCQLClauseAnalyzer.StatementType.UPDATE_ONE);

    WhereCQLClauseAnalyzer.WhereClauseWithWarnings whereClauseWithWarnings = null;
    Exception exception = null;
    try {
      whereClauseWithWarnings = whereCQLClauseAnalyzer.analyse(whereCQLClause);
    } catch (FilterException filterException) {
      exception = filterException;
    }

    var task =
        new UpdateDBTask<>(
            nextPosition(), schemaObject, getExceptionHandlerFactory(), updateCQLClause.target(), whereCQLClause);

    // ok to pass null exception, will be ignored
    task.maybeAddFailure(exception);

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
    updateCQLClause.accept(task);
    return task;
  }
}
