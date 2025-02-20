package io.stargate.sgv2.jsonapi.service.resolver.update;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.*;
import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.CQL_IDENTIFIER_COMPARATOR;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.Updatable;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonLiteral;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonType;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperator;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.UpdateException;
import io.stargate.sgv2.jsonapi.exception.WithWarnings;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.JSONCodecRegistries;
import io.stargate.sgv2.jsonapi.service.operation.query.ColumnAssignment;
import io.stargate.sgv2.jsonapi.service.operation.query.DefaultUpdateValuesCQLClause;
import io.stargate.sgv2.jsonapi.service.operation.query.UpdateValuesCQLClause;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiSupportDef;
import io.stargate.sgv2.jsonapi.service.shredding.CqlNamedValue;
import io.stargate.sgv2.jsonapi.service.shredding.CqlNamedValueContainer;
import io.stargate.sgv2.jsonapi.service.shredding.JsonNodeDecoder;
import io.stargate.sgv2.jsonapi.service.shredding.tables.CqlNamedValueFactory;
import io.stargate.sgv2.jsonapi.service.shredding.tables.JsonNamedValueFactory;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Predicate;
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

  /**
   * Error strategy to use with {@link CqlNamedValueFactory} for updates.
   *
   * <p>used for both Set and Unset operations
   */
  public static final CqlNamedValue.ErrorStrategy<UpdateException> ERROR_STRATEGY =
      new CqlNamedValue.ErrorStrategy<>() {

        private static final Predicate<ApiSupportDef> MATCH_INSERT_UNSUPPORTED =
            (apiSupportDef) -> !apiSupportDef.insert();

        @Override
        public ErrorCode<UpdateException> codeForNoApiSupport() {
          // the WritableTableRowBuilder did the same thing - re-using the unsupported column types
          // error
          return UpdateException.Code.UNSUPPORTED_COLUMN_TYPES;
        }

        @Override
        public ErrorCode<UpdateException> codeForUnknownColumn() {
          return UpdateException.Code.UNKNOWN_TABLE_COLUMNS;
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
        public void allChecks(
            TableSchemaObject tableSchemaObject, CqlNamedValueContainer allColumns) {
          checkUnknownColumns(tableSchemaObject, allColumns);
          checkApiSupport(tableSchemaObject, allColumns, MATCH_INSERT_UNSUPPORTED);
          checkMissingCodec(tableSchemaObject, allColumns);
          checkCodecError(tableSchemaObject, allColumns);
        }
      };

  // Operations we support on a table, and the function to resolve them
  private static final Map<
          UpdateOperator, BiFunction<TableSchemaObject, ObjectNode, List<ColumnAssignment>>>
      OPERATION_RESOLVERS =
          Map.of(
              UpdateOperator.SET, TableUpdateResolver::resolveSet,
              UpdateOperator.UNSET, TableUpdateResolver::resolveUnset);

  // String name sof the operators we support, used for error messages
  private static final List<String> SUPPORTED_OPERATION_NAMES =
      OPERATION_RESOLVERS.keySet().stream()
          .map(UpdateOperator::operator)
          .sorted(String::compareTo)
          .toList();

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
    EnumMap<UpdateOperator, ObjectNode> updateOperationDefs =
        updateClause != null
            ? updateClause.updateOperationDefs()
            : new EnumMap<>(UpdateOperator.class);

    for (var updateOperationDef : updateOperationDefs.entrySet()) {

      UpdateOperator updateOperator = updateOperationDef.getKey();
      ObjectNode arguments = updateOperationDef.getValue();

      var resolverFunction = OPERATION_RESOLVERS.get(updateOperator);
      if (resolverFunction == null) {
        usedUnsupportedOperators.add(updateOperator.operator());
      } else if (!arguments.isEmpty()) {
        // For empty assignment operator, we won't add it to assignments result list, e.g. "$set":
        // {} or "$unset": {}
        assignments.addAll(resolverFunction.apply(commandContext.schemaObject(), arguments));
      }
    }

    // Collect all used unsupported operator and throw Update exception
    if (!usedUnsupportedOperators.isEmpty()) {
      throw UpdateException.Code.UNSUPPORTED_UPDATE_OPERATIONS_FOR_TABLE.get(
          errVars(
              commandContext.schemaObject(),
              map -> {
                map.put("usedUnsupportedUpdateOperations", errFmtJoin(usedUnsupportedOperators));
                map.put("supportedUpdateOperations", errFmtJoin(SUPPORTED_OPERATION_NAMES));
              }));
    }

    // Assignments list is empty meaning user does not specify any non-empty assignment operator
    // Data API does not have task to do, error out.
    if (assignments.isEmpty()) {
      throw UpdateException.Code.MISSING_UPDATE_OPERATIONS.get(
          errVars(
              commandContext.schemaObject(),
              map -> {
                map.put("supportedUpdateOperations", errFmtJoin(SUPPORTED_OPERATION_NAMES));
              }));
    }

    // Duplicate column assignments is invalid
    // e.g. {"update": {"$set": {"description": "123"},"$unset": {"description": "456"}}}
    Set<CqlIdentifier> allItems = new HashSet<>();
    Set<CqlIdentifier> duplicates =
        assignments.stream()
            .map(ColumnAssignment::name)
            .filter(column -> !allItems.add(column))
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
   * @param tableSchemaObject
   * @param arguments the value of the `$set` in the example above.
   * @return
   */
  private static List<ColumnAssignment> resolveSet(
      TableSchemaObject tableSchemaObject, ObjectNode arguments) {

    // decode the JSON objects into our Java objects
    var jsonNamedValues =
        new JsonNamedValueFactory(tableSchemaObject, JsonNodeDecoder.DEFAULT).create(arguments);

    // now create the CQL values, this will include running codex to convert the values into the
    // correct CQL types
    var allColumns =
        new CqlNamedValueFactory(
                tableSchemaObject, JSONCodecRegistries.DEFAULT_REGISTRY, ERROR_STRATEGY)
            .create(jsonNamedValues);

    ERROR_STRATEGY.allChecks(tableSchemaObject, allColumns);
    // TODO: AARON - what about deferred values ?
    return allColumns.values().stream().map(ColumnAssignment::new).toList();
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
   * @param tableSchemaObject
   * @param arguments
   * @return
   */
  private static List<ColumnAssignment> resolveUnset(
      TableSchemaObject tableSchemaObject, ObjectNode arguments) {

    // decode the JSON objects into our Java objects
    // but this time, every value will be a JSON NULL this is how we do the UNSET
    var jsonNamedValues =
        new JsonNamedValueFactory(
                tableSchemaObject, (jsonNode) -> new JsonLiteral<>(null, JsonType.NULL))
            .create(arguments);

    // now create the CQL values, this will include running codex to convert the values into the
    // correct CQL types
    var allColumns =
        new CqlNamedValueFactory(
                tableSchemaObject, JSONCodecRegistries.DEFAULT_REGISTRY, ERROR_STRATEGY)
            .create(jsonNamedValues);

    ERROR_STRATEGY.allChecks(tableSchemaObject, allColumns);
    // TODO: AARON - what about deferred values ?

    return allColumns.values().stream().map(ColumnAssignment::new).toList();
  }
}
