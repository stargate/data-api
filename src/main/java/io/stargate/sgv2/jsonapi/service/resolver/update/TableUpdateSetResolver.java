package io.stargate.sgv2.jsonapi.service.resolver.update;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperator;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.query.ColumnAssignment;
import io.stargate.sgv2.jsonapi.service.shredding.tables.RowShredder;
import io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil;
import java.util.List;

public class TableUpdateSetResolver implements TableUpdateOperatorResolver {

  /**
   * Resolve the {@link UpdateOperator#SET} operation for a table update
   *
   * <p>Example:
   *
   * <ul>
   *   <li>primitive column<code>{"$set" : { "age" : 51 , "human" : false}}</code>
   *   <li>list column<code>{"$set" : { "listColumn" : [1,2]}}</code>
   *   <li>set column<code>{"$set" : { "setColumn" : { "key1": "value1", "key2": "value2"}}</code>
   *   <li>map column with string key(object format)<code>
   *       {"$set" : { "mapColumn" : { "key1": "value1", "key2": "value2"}}</code>
   *   <li>map column with string key(object format)<code>
   *       {"$set" : { "mapColumn" : [["key1","value1"], ["key2","value2"]]}}</code>
   *   <li>map column with non-string key(tuple format)<code>
   *       {"$set" : { "mapColumn" : [[123,"value1"], [456,"value2"]]}}</code>
   * </ul>
   *
   * @param table tableSchemaObject
   * @param arguments arguments objectNode for the $set
   * @return list of columnAssignments for all the $set column updates
   */
  @Override
  public List<ColumnAssignment> resolve(TableSchemaObject table, ObjectNode arguments) {
    return arguments.properties().stream()
        .map(
            entry ->
                new ColumnAssignment(
                    UpdateOperator.SET,
                    table.tableMetadata(),
                    CqlIdentifierUtil.cqlIdentifierFromUserInput(entry.getKey()),
                    RowShredder.shredValue(entry.getValue())))
        .toList();
  }
}
