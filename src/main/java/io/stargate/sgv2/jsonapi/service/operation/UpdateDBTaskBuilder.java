package io.stargate.sgv2.jsonapi.service.operation;

import com.datastax.oss.driver.api.querybuilder.update.Update;
import io.stargate.sgv2.jsonapi.exception.FilterException;
import io.stargate.sgv2.jsonapi.exception.WithWarnings;
import io.stargate.sgv2.jsonapi.service.operation.query.UpdateValuesCQLClause;
import io.stargate.sgv2.jsonapi.service.operation.query.WhereCQLClause;
import io.stargate.sgv2.jsonapi.service.operation.tables.WhereCQLClauseAnalyzer;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskBuilder;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds an attempt to delete a row from an API Table, create a single instance and then call
 * {@link #build(WhereCQLClause)} for each different where clause the command creates.
 */
public class UpdateDBTaskBuilder<SchemaT extends TableSchemaObject>
    extends TaskBuilder<UpdateDBTask<SchemaT>, SchemaT, UpdateDBTaskBuilder<SchemaT>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(UpdateDBTaskBuilder.class);

  private Boolean updateOne = null;
  private WithWarnings<? extends WhereCQLClause<Update>> whereCQLClauseWithWarnings;
  private WithWarnings<UpdateValuesCQLClause> updateValuesCQLClauseWithWarnings;

  protected UpdateDBTaskBuilder(SchemaT schemaObject) {
    super(schemaObject);
  }

  public UpdateDBTaskBuilder<SchemaT> withUpdateOne(Boolean updateOne) {
    this.updateOne = updateOne;
    return this;
  }

  public UpdateDBTaskBuilder<SchemaT> withWhereCQLClause(
      WithWarnings<? extends WhereCQLClause<Update>> whereCQLClauseWithWarnings) {
    this.whereCQLClauseWithWarnings = whereCQLClauseWithWarnings;
    return this;
  }

  public UpdateDBTaskBuilder<SchemaT> withUpdateValuesCQLClause(
      WithWarnings<UpdateValuesCQLClause> updateValuesCQLClauseWithWarnings) {
    this.updateValuesCQLClauseWithWarnings = updateValuesCQLClauseWithWarnings;
    return this;
  }

  public UpdateDBTask<SchemaT> build() {

    Objects.requireNonNull(updateOne, "updateOne must be set");
    Objects.requireNonNull(whereCQLClauseWithWarnings, "whereCQLClauseWithWarnings must be set");
    Objects.requireNonNull(
        updateValuesCQLClauseWithWarnings, "updateValuesCQLClauseWithWarnings must be set");

    // If we are a table not a collection, then we only support update one
    // and we onl have tables in this class for now.
    if (!updateOne) {
      throw new IllegalStateException("Update many is not supported for tables");
    }
    var whereCQLClauseAnalyzer =
        new WhereCQLClauseAnalyzer(schemaObject, WhereCQLClauseAnalyzer.StatementType.UPDATE_ONE);

    WhereCQLClauseAnalyzer.WhereClauseWithWarnings analysisWarnings = null;
    Exception exception = null;
    try {
      analysisWarnings = whereCQLClauseAnalyzer.analyse(whereCQLClauseWithWarnings.target());
    } catch (FilterException filterException) {
      exception = filterException;
    }

    var task =
        new UpdateDBTask<>(
            nextPosition(),
            schemaObject,
            getExceptionHandlerFactory(),
            updateValuesCQLClauseWithWarnings.target(),
            whereCQLClauseWithWarnings.target());

    // ok to pass null exception, will be ignored
    task.maybeAddFailure(exception);

    // There should not be any warnings, we cannot turn on allow filtering for delete
    // and we should not be turning on  allow filtering for delete
    // sanity check
    if (analysisWarnings != null
        && (analysisWarnings.requiresAllowFiltering() || !analysisWarnings.isEmpty())) {
      throw new IllegalStateException(
          "Where clause analysis for update was not empty, analysis:%s"
              .formatted(analysisWarnings));
    }

    // add warnings from the CQL clauses to the attempt
    updateValuesCQLClauseWithWarnings.accept(task);
    return task;
  }
}
