package io.stargate.sgv2.jsonapi.service.resolver.update;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.*;
import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.CQL_IDENTIFIER_COMPARATOR;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.Updatable;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperator;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.UpdateException;
import io.stargate.sgv2.jsonapi.exception.WithWarnings;
import io.stargate.sgv2.jsonapi.service.schema.tables.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.query.ColumnAssignment;
import io.stargate.sgv2.jsonapi.service.operation.query.DefaultUpdateValuesCQLClause;
import io.stargate.sgv2.jsonapi.service.operation.query.UpdateValuesCQLClause;
import io.stargate.sgv2.jsonapi.service.shredding.CqlNamedValue;
import io.stargate.sgv2.jsonapi.service.shredding.CqlNamedValueContainer;
import io.stargate.sgv2.jsonapi.service.shredding.tables.CqlNamedValueContainerFactory;
import io.stargate.sgv2.jsonapi.util.recordable.PrettyPrintable;
import java.util.*;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final Logger LOGGER = LoggerFactory.getLogger(TableUpdateResolver.class);

  private static final Map<UpdateOperator, TableUpdateOperatorResolver>
      SUPPORTED_OPERATION_RESOLVERS =
          Map.of(
              UpdateOperator.SET, new TableUpdateSetResolver(),
              UpdateOperator.UNSET, new TableUpdateUnsetResolver(),
              UpdateOperator.PUSH, new TableUpdatePushResolver(),
              UpdateOperator.PULL_ALL, new TableUpdatePullAllResolver());

  private static final List<String> SUPPORTED_OPERATION_API_NAMES =
      SUPPORTED_OPERATION_RESOLVERS.keySet().stream().map(UpdateOperator::apiName).toList();

  /**
   * Creates a new resolver that will use the given config.
   *
   * @param operationsConfig the config to use
   */
  public TableUpdateResolver(OperationsConfig operationsConfig) {
    super(operationsConfig);
  }

  /** {@inheritDoc} */
  @Override
  public WithWarnings<UpdateValuesCQLClause> resolve(
      CommandContext<TableSchemaObject> commandContext, CmdT command) {

    var updateClause = command.updateClause();

    List<ColumnAssignment> assignments = new ArrayList<>();
    List<String> usedUnsupportedOperators = new ArrayList<>();

    // we check if there are no operations below, so the check also looks at empty operations
    EnumMap<UpdateOperator, ObjectNode> updateOperations =
        updateClause != null
            ? updateClause.updateOperationDefs()
            : new EnumMap<>(UpdateOperator.class);

    for (var updateOperationDef : updateOperations.entrySet()) {

      UpdateOperator updateOperator = updateOperationDef.getKey();
      ObjectNode arguments = updateOperationDef.getValue();

      var operatorResolver = SUPPORTED_OPERATION_RESOLVERS.get(updateOperator);
      if (operatorResolver == null) {
        usedUnsupportedOperators.add(updateOperator.apiName());
      } else if (!arguments.isEmpty()) {
        // For empty assignment operator, we won't add it to assignments result list
        // e.g. "$set": {}, "$unset": {}
        assignments.addAll(
            operatorResolver.resolve(
                commandContext.schemaObject(), new ErrorStrategy(updateOperator), arguments));
      }
    }

    // Collect all used unsupported operator and throw Update exception
    if (!usedUnsupportedOperators.isEmpty()) {
      throw UpdateException.Code.UNSUPPORTED_UPDATE_OPERATIONS_FOR_TABLE.get(
          errVars(
              commandContext.schemaObject(),
              map -> {
                map.put("usedUnsupportedUpdateOperations", errFmtJoin(usedUnsupportedOperators));
                map.put("supportedUpdateOperations", errFmtJoin(SUPPORTED_OPERATION_API_NAMES));
              }));
    }

    // Assignments list is empty meaning user does not specify any non-empty assignment operator
    // Data API does not have task to do, error out.
    if (assignments.isEmpty()) {
      throw UpdateException.Code.MISSING_UPDATE_OPERATIONS.get(
          errVars(
              commandContext.schemaObject(),
              map -> {
                map.put("supportedUpdateOperations", errFmtJoin(SUPPORTED_OPERATION_API_NAMES));
              }));
    }

    // Duplicate column assignments is invalid
    // e.g. {"update": {"$set": {"description": "123"},"$unset": {"description": "456"}}}
    Set<CqlIdentifier> allAssignments = new HashSet<>();
    Set<CqlIdentifier> duplicates =
        assignments.stream()
            .map(ColumnAssignment::name)
            .filter(column -> !allAssignments.add(column))
            .collect(Collectors.toSet());

    if (!duplicates.isEmpty()) {
      throw UpdateException.Code.UNSUPPORTED_OVERLAPPING_UPDATE_OPERATIONS.get(
          errVars(
              commandContext.schemaObject(),
              map -> {
                map.put(
                    "duplicateAssignmentColumns",
                    errFmtCqlIdentifier(
                        duplicates.stream().sorted(CQL_IDENTIFIER_COMPARATOR).toList()));
              }));
    }

    // Analyze table update columnAssignments before create CQLClause
    // this checks if the update is doing things it should not be
    new TableUpdateAnalyzer(commandContext.schemaObject()).analyze(assignments);

    return WithWarnings.of(new DefaultUpdateValuesCQLClause(assignments));
  }

  /**
   * Error strategy to use with {@link CqlNamedValueContainerFactory} for updates.
   *
   * <p>
   */
  @VisibleForTesting
  static class ErrorStrategy implements CqlNamedValue.ErrorStrategy<UpdateException> {

    private final UpdateOperator updateOperator;

    @VisibleForTesting
    ErrorStrategy(UpdateOperator updateOperator) {
      this.updateOperator = updateOperator;
    }

    @Override
    public ErrorCode<UpdateException> codeForUnknownColumn() {
      return UpdateException.Code.UNKNOWN_TABLE_COLUMNS;
    }

    @Override
    public ErrorCode<UpdateException> codeForMissingVectorize() {
      return UpdateException.Code.UNSUPPORTED_VECTORIZE_WHEN_MISSING_VECTORIZE_DEFINITION;
    }

    @Override
    public ErrorCode<UpdateException> codeForMissingCodec() {
      return UpdateException.Code.UNSUPPORTED_COLUMN_TYPES;
    }

    @Override
    public ErrorCode<UpdateException> codeForCodecError() {
      return UpdateException.Code.INVALID_UPDATE_COLUMN_VALUES;
    }

    @Override
    public void allChecks(TableSchemaObject tableSchemaObject, CqlNamedValueContainer allColumns) {
      checkUnknownColumns(tableSchemaObject, allColumns);
      checkApiSupport(tableSchemaObject, allColumns);
      checkMissingCodec(tableSchemaObject, allColumns);
      checkCodecError(tableSchemaObject, allColumns);
      checkMissingVectorize(tableSchemaObject, allColumns);
    }

    private void checkApiSupport(
        TableSchemaObject tableSchemaObject, CqlNamedValueContainer allColumns) {

      var unsupportedColumns =
          allColumns.values().stream()
              .filter(
                  namedValue ->
                      !namedValue
                          .apiColumnDef()
                          .type()
                          .apiSupport()
                          .update()
                          .supports(updateOperator))
              .sorted(CqlNamedValue.NAME_COMPARATOR)
              .toList();

      if (!unsupportedColumns.isEmpty()) {
        if (LOGGER.isTraceEnabled()) {
          LOGGER.trace(
              "checkApiSupport() - operator not supported by all columns, table:{},  updateOperator: {} unsupportedColumns: {}",
              tableSchemaObject.tableMetadata().getName(),
              updateOperator.apiName(),
              String.join(",", unsupportedColumns.stream().map(PrettyPrintable::print).toList()));
        }

        throw UpdateException.Code.UNSUPPORTED_UPDATE_OPERATOR.get(
            errVars(
                tableSchemaObject,
                map -> {
                  map.put(
                      "allColumns",
                      errFmtColumnMetadata(
                          tableSchemaObject.tableMetadata().getColumns().values()));
                  map.put("operator", updateOperator.apiName());
                  map.put("unsupportedColumns", errFmtCqlNamedValue(unsupportedColumns));
                }));
      }
    }
  }
  ;
}
