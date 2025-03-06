package io.stargate.sgv2.jsonapi.service.resolver.update;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.*;
import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.CQL_IDENTIFIER_COMPARATOR;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.Updatable;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperator;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.UpdateException;
import io.stargate.sgv2.jsonapi.exception.WithWarnings;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.query.ColumnAssignment;
import io.stargate.sgv2.jsonapi.service.operation.query.DefaultUpdateValuesCQLClause;
import io.stargate.sgv2.jsonapi.service.operation.query.UpdateValuesCQLClause;
import io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil;
import java.util.*;
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
  private static final Map<UpdateOperator, TableUpdateOperatorResolver> SUPPORTED_OPERATORS_MAP =
      Map.of(
          UpdateOperator.SET, new TableUpdateSetResolver(),
          UpdateOperator.UNSET, new TableUpdateUnsetResolver(),
          UpdateOperator.PUSH, new TableUpdatePushResolver(),
          UpdateOperator.PULL_ALL, new TableUpdatePullAllResolver());

  private static final List<String> SUPPORTED_OPERATORS_STRING_LIST =
      SUPPORTED_OPERATORS_MAP.keySet().stream().map(UpdateOperator::operator).toList();

  public TableUpdateResolver(OperationsConfig operationsConfig) {
    super(operationsConfig);
  }

  @Override
  public WithWarnings<UpdateValuesCQLClause> resolve(
      CommandContext<TableSchemaObject> commandContext, CmdT command) {

    var updateClause = command.updateClause();

    List<ColumnAssignment> assignments = new ArrayList<>();
    List<String> usedUnsupportedOperators = new ArrayList<>();
    // we check if there are no operations below, so the check also looks at empty operations
    EnumMap<UpdateOperator, ObjectNode> updateOperationDefs =
        updateClause != null
            ? updateClause.updateOperationDefs()
            : new EnumMap<>(UpdateOperator.class);

    for (var updateOperationDef : updateOperationDefs.entrySet()) {
      UpdateOperator updateOperator = updateOperationDef.getKey();
      ObjectNode arguments = updateOperationDef.getValue();

      var operatorResolver = SUPPORTED_OPERATORS_MAP.get(updateOperator);
      if (operatorResolver == null) {
        usedUnsupportedOperators.add(updateOperator.operator());
      } else if (!arguments.isEmpty()) {
        // For empty assignment operator, we won't add it to assignments result list
        // e.g. "$set": {}, "$unset": {}
        assignments.addAll(operatorResolver.resolve(commandContext.schemaObject(), arguments));
      }
    }
    // Collect all used unsupported operator and throw Update exception
    if (!usedUnsupportedOperators.isEmpty()) {
      throw UpdateException.Code.UNSUPPORTED_UPDATE_OPERATIONS_FOR_TABLE.get(
          errVars(
              commandContext.schemaObject(),
              map -> {
                map.put("usedUnsupportedUpdateOperations", errFmtJoin(usedUnsupportedOperators));
                map.put("supportedUpdateOperations", errFmtJoin(SUPPORTED_OPERATORS_STRING_LIST));
              }));
    }

    // Assignments list is empty meaning user does not specify any non-empty assignment operator
    // Data API does not have task to do, error out.
    if (assignments.isEmpty()) {
      throw UpdateException.Code.MISSING_UPDATE_OPERATIONS.get(
          errVars(
              commandContext.schemaObject(),
              map -> {
                map.put("supportedUpdateOperations", errFmtJoin(SUPPORTED_OPERATORS_STRING_LIST));
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
      throw UpdateException.Code.UNSUPPORTED_OVERLAPPING_UPDATE_OPERATIONS.get(
          errVars(
              commandContext.schemaObject(),
              map -> {
                map.put(
                    "duplicateAssignmentColumns",
                    errFmtJoin(
                        duplicates.stream()
                            .sorted(CQL_IDENTIFIER_COMPARATOR)
                            .map(CqlIdentifierUtil::cqlIdentifierToMessageString)
                            .toList()));
              }));
    }

    // Analyze table update columnAssignments before create CQLClause
    new TableUpdateAnalyzer(commandContext.schemaObject()).analyze(assignments);

    return WithWarnings.of(new DefaultUpdateValuesCQLClause(assignments));
  }
}
