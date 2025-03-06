package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import io.stargate.sgv2.jsonapi.exception.FilterException;
import io.stargate.sgv2.jsonapi.service.operation.DeleteDBTask;
import io.stargate.sgv2.jsonapi.service.operation.query.WhereCQLClause;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskBuilder;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Builds a {@link DeleteDBTask} to delete from a {@link TableSchemaObject} */
public class TableDeleteDBTaskBuilder
    extends TaskBuilder<
        DeleteDBTask<TableSchemaObject>, TableSchemaObject, TableDeleteDBTaskBuilder> {

  private static final Logger LOGGER = LoggerFactory.getLogger(TableDeleteDBTaskBuilder.class);

  private Boolean deleteOne = null;

  /**
   * @param tableSchemaObject The table to delete from.
   */
  public TableDeleteDBTaskBuilder(TableSchemaObject tableSchemaObject) {
    super(tableSchemaObject);
  }

  /**
   * @param deleteOne Set TRUE is this is a deleteOne command, if true the builder will make sure
   *     the where clause has the full PK specified, otherwise it can be partial. This is particular
   *     to tables. Defaults to null so must be set.
   * @return this builder
   */
  public TableDeleteDBTaskBuilder withDeleteOne(boolean deleteOne) {
    this.deleteOne = deleteOne;
    return this;
  }

  public DeleteDBTask<TableSchemaObject> build(WhereCQLClause<Delete> whereCQLClause) {

    Objects.requireNonNull(deleteOne, "deleteOne must be set");

    var whereCQLClauseAnalyzer =
        new WhereCQLClauseAnalyzer(
            schemaObject,
            deleteOne
                ? WhereCQLClauseAnalyzer.StatementType.DELETE_ONE
                : WhereCQLClauseAnalyzer.StatementType.DELETE_MANY);

    WhereCQLClauseAnalyzer.WhereClauseWithWarnings whereClauseWithWarnings = null;
    Exception exception = null;
    try {
      whereClauseWithWarnings = whereCQLClauseAnalyzer.analyse(whereCQLClause);
    } catch (FilterException filterException) {
      exception = filterException;
    }

    var task =
        new DeleteDBTask<>(
            nextPosition(), schemaObject, getExceptionHandlerFactory(), whereCQLClause);

    // ok to pass null exception, will be ignored
    task.maybeAddFailure(exception);

    // There should not be any warnings, we cannot turn on allow filtering for delete
    // and we should not be turning on  allow filtering for delete
    // sanity check
    if (whereClauseWithWarnings != null && !whereClauseWithWarnings.isEmpty()) {
      throw new IllegalStateException(
          "Where clause analysis for delete was not empty, whereClauseWithWarnings: %s"
              .formatted(whereClauseWithWarnings));
    }

    return task;
  }
}
