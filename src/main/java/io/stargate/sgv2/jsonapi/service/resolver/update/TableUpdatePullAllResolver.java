package io.stargate.sgv2.jsonapi.service.resolver.update;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errVars;

import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonLiteral;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperator;
import io.stargate.sgv2.jsonapi.exception.UpdateException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.query.ColumnAssignment;
import io.stargate.sgv2.jsonapi.service.operation.query.ColumnRemoveToAssignment;
import io.stargate.sgv2.jsonapi.service.shredding.tables.RowShredder;
import io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil;
import java.util.List;

/** Resolver to resolve $pullAll argument to List of ColumnAssignment. */
public class TableUpdatePullAllResolver implements TableUpdateOperatorResolver {

  /**
   * Resolve the {@link UpdateOperator#PULL_ALL} operation for a table update
   *
   * <p>Push operator can only be used for collection columns (list, set, map).
   *
   * <p>Example:
   *
   * <ul>
   *   <li>list column<code>{"$pullAll" : { "listColumn1" : [1,2], "listColumn2" : ["a","b"]}}
   *       </code>
   *   <li>set column<code>{"$pullAll" : { "setColumn2" : [1,2], "setColumn2" : ["a","b"]}}</code>
   *   <li>map column, pull from map key<code>
   *       {"$pullAll" : { "mapColumn" : [1,2], "mapColumn" : ["abc","def"]}}</code>
   * </ul>
   *
   * @param table tableSchemaObject
   * @param arguments arguments objectNode for the $pullAll
   * @return list of columnAssignments for all the $pullAll column updates
   */
  @Override
  public List<ColumnAssignment> resolve(TableSchemaObject table, ObjectNode arguments) {
    return arguments.properties().stream()
        .map(
            entry -> {
              var column = entry.getKey();
              var apiColumnDef = checkUpdateColumnExists(table, column);
              var inputValue = entry.getValue();
              JsonLiteral<?> shreddedValue = null;

              // $pullAll only works for map/set/list columns
              checkUpdateOperatorSupportOnColumn(apiColumnDef, table, UpdateOperator.PULL_ALL);

              // $pullAll value node must be an array
              if (inputValue.getNodeType() != JsonNodeType.ARRAY) {
                throw UpdateException.Code.INVALID_UPDATE_OPERATOR_PULL_VALUE.get(
                    errVars(
                        table,
                        map -> {
                          map.put("providedValue", inputValue.toString());
                        }));
              }

              shreddedValue = RowShredder.shredValue(inputValue);

              return new ColumnAssignment(
                  new ColumnRemoveToAssignment(),
                  table.tableMetadata(),
                  CqlIdentifierUtil.cqlIdentifierFromUserInput(column),
                  shreddedValue);
            })
        .toList();
  }
}
