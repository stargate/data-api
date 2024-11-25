package io.stargate.sgv2.jsonapi.service.resolver.update;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.*;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.Updatable;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonLiteral;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonType;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperator;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.UpdateException;
import io.stargate.sgv2.jsonapi.exception.WithWarnings;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.query.ColumnAssignment;
import io.stargate.sgv2.jsonapi.service.operation.query.DefaultUpdateValuesCQLClause;
import io.stargate.sgv2.jsonapi.service.operation.query.UpdateValuesCQLClause;
import io.stargate.sgv2.jsonapi.service.shredding.tables.RowShredder;
import io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil;
import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * Resolves the update clause for a command when the target schema object is a {@link
 * TableSchemaObject}
 *
 * <p>Resolving the update clause creates a {@link UpdateValuesCQLClause} that has the changes to be
 * applied to the CQL table.
 *
 * <p>
 *
 * @param <CmdT> The type of the command that has the update clause.
 */
public class TableUpdateResolver<CmdT extends Command & Updatable>
    extends UpdateResolver<CmdT, TableSchemaObject> {

  // Using map here so we can expose the list of supported operators for validation to check.
  // Keep this immutable, we return the key set in a property below.
  private static final Map<
          UpdateOperator, BiFunction<TableSchemaObject, ObjectNode, List<ColumnAssignment>>>
      supportedOperatorsMap =
          Map.of(
              UpdateOperator.SET, TableUpdateResolver::resolveSet,
              UpdateOperator.UNSET, TableUpdateResolver::resolveUnset);

  private static final List<String> supportedOperatorsStringList =
      ImmutableList.of(UpdateOperator.SET.operator(), UpdateOperator.UNSET.operator());

  public TableUpdateResolver(OperationsConfig operationsConfig) {
    super(operationsConfig);
  }

  @Override
  public WithWarnings<UpdateValuesCQLClause> resolve(
      CommandContext<TableSchemaObject> commandContext, CmdT command) {

    var updateClause = command.updateClause();

    if (updateClause == null || updateClause.updateOperationDefs().isEmpty()) {
      throw UpdateException.Code.MISSING_UPDATE_OPERATIONS.get(
          errVars(
              commandContext.schemaObject(),
              map -> {
                map.put("supportedUpdateOperations", errFmtJoin(supportedOperatorsStringList));
              }));
    }

    List<ColumnAssignment> assignments = new ArrayList<>();
    List<String> usedUnsupportedOperators = new ArrayList<>();

    for (var updateOperationDef : updateClause.updateOperationDefs().entrySet()) {

      UpdateOperator updateOperator = updateOperationDef.getKey();
      ObjectNode arguments = updateOperationDef.getValue();

      var resolverFunction = supportedOperatorsMap.get(updateOperator);
      if (resolverFunction == null) {
        usedUnsupportedOperators.add(updateOperator.operator());
        continue;
      }

      assignments.addAll(resolverFunction.apply(commandContext.schemaObject(), arguments));
    }

    // Collect all used unsupported operator and throw Update exception
    if (!usedUnsupportedOperators.isEmpty()) {
      throw UpdateException.Code.UNSUPPORTED_UPDATE_OPERATIONS_FOR_TABLE.get(
          errVars(
              commandContext.schemaObject(),
              map -> {
                map.put("usedUnsupportedUpdateOperations", errFmtJoin(usedUnsupportedOperators));
                map.put("supportedUpdateOperations", errFmtJoin(supportedOperatorsStringList));
              }));
    }

    // Duplicate column assignments is invalid
    // e.g. {"update": {"$set": {"description": "123"},"$unset": {"description": "456"}}}
    List<CqlIdentifier> assignmentColumns =
        assignments.stream().map(columnAssignment -> columnAssignment.column).toList();
    Set<CqlIdentifier> allItems = new HashSet<>();
    Set<CqlIdentifier> duplicates =
        assignmentColumns.stream()
            .filter(column -> !allItems.add(column))
            .collect(Collectors.toSet());
    if (!duplicates.isEmpty()) {
      throw UpdateException.Code.DUPLICATE_UPDATE_OPERATION_ASSIGNMENTS.get(
          errVars(
              commandContext.schemaObject(),
              map -> {
                map.put(
                    "duplicateAssignmentColumns",
                    errFmtJoin(
                        duplicates.stream()
                            .map(CqlIdentifierUtil::cqlIdentifierToMessageString)
                            .toList()));
              }));
    }

    // Analyze table update columnAssignments before create CQLClause
    new TableUpdateAnalyzer(commandContext.schemaObject()).analyze(assignments);

    return WithWarnings.of(new DefaultUpdateValuesCQLClause(assignments));
  }

  /**
   * Resolve the {@link UpdateOperator#SET} operation
   *
   * <p>Example:
   *
   * <pre>
   *    {"$set" : { "age" : 51 , "human" : false}}
   * </pre>
   *
   * @param table
   * @param arguments
   * @return
   */
  private static List<ColumnAssignment> resolveSet(TableSchemaObject table, ObjectNode arguments) {
    // Checking if the columns exist in the table should be validated in the clause validation
    return arguments.properties().stream()
        .map(
            entry ->
                new ColumnAssignment(
                    table.tableMetadata(),
                    CqlIdentifierUtil.cqlIdentifierFromUserInput(entry.getKey()),
                    RowShredder.shredValue(entry.getValue())))
        .toList();
  }

  /**
   * Resolve the {@link UpdateOperator#UNSET} operation
   *
   * <p>Example:
   *
   * <pre>
   *    {"$unset" : { "country" : ""}}
   * </pre>
   *
   * @param table
   * @param arguments
   * @return
   */
  private static List<ColumnAssignment> resolveUnset(
      TableSchemaObject table, ObjectNode arguments) {
    // Checking if the columns exist in the table should be validated in the clause validation
    // we ignore the value, API spec says it should be empty string and we should validate that in
    // the API tier
    return arguments.properties().stream()
        .map(
            entry ->
                new ColumnAssignment(
                    table.tableMetadata(),
                    CqlIdentifierUtil.cqlIdentifierFromUserInput(entry.getKey()),
                    new JsonLiteral<>(null, JsonType.NULL)))
        .toList();
  }
}
