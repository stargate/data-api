package io.stargate.sgv2.jsonapi.service.resolver.update;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.Updatable;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperator;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.query.ColumnAssignment;
import io.stargate.sgv2.jsonapi.service.operation.query.DefaultUpdateValuesCQLClause;
import io.stargate.sgv2.jsonapi.service.operation.query.UpdateValuesCQLClause;
import io.stargate.sgv2.jsonapi.service.resolver.UnvalidatedClauseException;
import io.stargate.sgv2.jsonapi.service.shredding.tables.RowShredder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

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
      supportedOperators =
          Map.of(
              UpdateOperator.SET, TableUpdateResolver::resolveSet,
              UpdateOperator.UNSET, TableUpdateResolver::resolveUnset);

  public TableUpdateResolver(OperationsConfig operationsConfig) {
    super(operationsConfig);
  }

  /**
   * The set of {@link UpdateOperator} 's that are supported by Tables, may be referenced for
   * validation of a clause.
   *
   * @return
   */
  public static Set<UpdateOperator> supportedOperators() {
    return supportedOperators.keySet();
  }

  @Override
  public UpdateValuesCQLClause resolve(
      CommandContext<TableSchemaObject> commandContext, CmdT command) {

    var updateClause = command.updateClause();

    // All validation should be done in the API tier, any invalid query here is unexpected
    if (updateClause == null || updateClause.updateOperationDefs().isEmpty()) {
      // TODO: better messages, see the exception class
      throw new UnvalidatedClauseException("Update clause is required");
    }

    List<ColumnAssignment> assignments = new ArrayList<>();
    for (var updateOperationDef : updateClause.updateOperationDefs().entrySet()) {

      UpdateOperator updateOperator = updateOperationDef.getKey();
      ObjectNode arguments = updateOperationDef.getValue();

      var resolverFunction = supportedOperators.get(updateOperator);
      if (resolverFunction == null) {
        throw new UnvalidatedClauseException("Unsupported update operator: " + updateOperator);
      }

      // TODO : should we assert the list of operations  is non empty here ?
      assignments.addAll(resolverFunction.apply(commandContext.schemaObject(), arguments));
    }
    return new DefaultUpdateValuesCQLClause(assignments);
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
                    table.tableMetadata,
                    CqlIdentifier.fromCql(entry.getKey()),
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
                    table.tableMetadata, CqlIdentifier.fromCql(entry.getKey()), null))
        .toList();
  }
}
