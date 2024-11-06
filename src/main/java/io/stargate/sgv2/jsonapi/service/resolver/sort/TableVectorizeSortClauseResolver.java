package io.stargate.sgv2.jsonapi.service.resolver.sort;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.*;
import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmtJoin;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.Sortable;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression;
import io.stargate.sgv2.jsonapi.exception.SortException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiTypeName;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiVectorType;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Resolves a sort clause to ONE single vectorize sort expression. <p>
 * There are several rules: <br>
 * Note, current only one vectorize sort expression can be supported in update clause.
 *
 */
public class TableVectorizeSortClauseResolver {
  // public class TableVectorizeSortClauseResolver<CmdT extends Command & Sortable>
  //    extends TableSortClauseResolver<CmdT, TableSchemaObject, SortExpression> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(TableVectorizeSortClauseResolver.class);

  //  public TableVectorizeSortClauseResolver(
  //      OperationsConfig operationsConfig) {
  //    super(operationsConfig);
  //  }

  //  @Override
  //  public WithWarnings<SortExpression> resolve(
  //          CommandContext<TableSchemaObject> commandContext, CmdT command) {
  public Optional<SortExpression> resolve(
      CommandContext<TableSchemaObject> commandContext, Sortable command) {
    Objects.requireNonNull(commandContext, "commandContext is required");
    Objects.requireNonNull(command, "command is required");

    var vectorizeSorts = command.sortClause().tableVectorizeSorts();
    if (vectorizeSorts.isEmpty()) {
      return Optional.empty();
    }

    var tableSchemaObject = commandContext.schemaObject();

    // Only one vectorize can be specified in sort clause
    if (vectorizeSorts.size() > 1) {
      throw SortException.Code.MORE_THAN_ONE_VECTORIZE_SORT.get(
          errVars(
              tableSchemaObject,
              map -> {
                map.put(
                    "sortVectorizeOnVectorColumns",
                    errFmtJoin(vectorizeSorts.stream().map(SortExpression::path).toList()));
              }));
    }

    // Only allowed to have 1 sort expression when vectorizing / vector sorting
    if (command.sortClause().sortExpressions().size() > 1) {
      // TODO: AARON YUQI same as below, if we can leave this for the TableSortClauseResolver to
      // check
      // we can get most of the sort validation into that class.
      throw SortException.Code.VECTORIZE_SORT_WITH_OTHER_SORT_EXPRESSION.get(
          errVars(tableSchemaObject, map -> {}));
    }

    var vectorizeSortExpression = vectorizeSorts.getFirst();
    var apiTableDef = tableSchemaObject.apiTableDef();
    var vectorColumnDef =
        apiTableDef.allColumns().get(vectorizeSortExpression.pathAsCqlIdentifier());

    if (vectorColumnDef == null) {
      // TODO AARON YUQI we can throw the SORT code for unknown column OR leave this and make the
      // TableSortClauseResolver
      // detect this and throw the error, I THINK that will work after the in memory sort PR and we
      // should check that
      throw SortException.Code.CANNOT_SORT_UNKNOWN_COLUMNS.get(
          errVars(
              tableSchemaObject,
              map -> {
                map.put(
                    "allColumns", errFmtApiColumnDef(tableSchemaObject.apiTableDef().allColumns()));
                map.put(
                    "unknownColumns",
                    errFmtCqlIdentifier(List.of(vectorizeSortExpression.pathAsCqlIdentifier())));
              }));
    }

    if (vectorColumnDef.type().typeName() != ApiTypeName.VECTOR) {
      // TODO AARON YUQI again can we leave this to be checked in TableSortClauseResolver with the
      // way it works in the
      // in memory sort PR ? So we have it in one place ? If we do that for this and above this
      // function would return
      // List.of()
      throw SortException.Code.VECTORIZE_SORT_ON_NON_VECTOR_COLUMN.get(
          errVars(
              tableSchemaObject,
              map -> {
                map.put(
                    "sortVectorizeOnNonVectorColumn", errFmtApiColumnDef(List.of(vectorColumnDef)));
              }));
    }

    var vectorTypeDef = (ApiVectorType) vectorColumnDef.type();
    if (vectorTypeDef.getVectorizeDefinition() == null) {
      // TODO AARON YUQI need a better error, this can be a new V2 error from the SORT scope about
      // not being able
      // to vectorize a column that does not have a vectorize definition. Similar to the TODO for
      // inserts above
      throw SortException.Code.VECTORIZE_SORT_ON_VECTOR_COLUMN_WITHOUT_VECTORIZE_DEFINITION.get(
          errVars(
              tableSchemaObject,
              map -> {
                map.put(
                    "vectorColumnWithoutVectorizeDefinition",
                    errFmtApiColumnDef(List.of(vectorColumnDef)));
              }));
    }

    return Optional.of(vectorizeSorts.getFirst());
  }
}
