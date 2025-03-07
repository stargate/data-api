package io.stargate.sgv2.jsonapi.service.resolver.update;


import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperator;
import io.stargate.sgv2.jsonapi.exception.UpdateException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.JSONCodecRegistries;
import io.stargate.sgv2.jsonapi.service.operation.query.ColumnAssignment;
import io.stargate.sgv2.jsonapi.service.operation.query.ColumnRemoveToAssignment;
import io.stargate.sgv2.jsonapi.service.shredding.CqlNamedValue;
import io.stargate.sgv2.jsonapi.service.shredding.JsonNodeDecoder;
import java.util.List;

/** Resolver to resolve $pullAll argument to List of ColumnAssignment. */
public class TableUpdatePullAllResolver extends TableUpdateOperatorResolver {

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
  public List<ColumnAssignment> resolve(
      TableSchemaObject tableSchemaObject,
      CqlNamedValue.ErrorStrategy<UpdateException> errorStrategy,
      ObjectNode arguments) {

    // we do not need to normalise the arguments to $pullAll, the RHS op is
    // already a key-value object where the values are an array of the values to remove from the
    // keys
    // However, for map columns this is just a list of the keys not map entries so we need to use
    // a JsonCodec that can handle this
    return createColumnAssignments(
        tableSchemaObject,
        arguments,
        errorStrategy,
        UpdateOperator.PULL_ALL,
        ColumnRemoveToAssignment::new,
        JsonNodeDecoder.DEFAULT,
        JSONCodecRegistries.MAP_KEY_REGISTRY);

    //    return arguments.properties().stream()
    //        .map(
    //            entry -> {
    //              var column = entry.getKey();
    //              var apiColumnDef = checkUpdateColumnExists(table, column);
    //              var inputValue = entry.getValue();
    //              JsonLiteral<?> shreddedValue = null;
    //
    //              // $pullAll only works for map/set/list columns
    //              checkUpdateOperatorSupportOnColumn(apiColumnDef, table,
    // UpdateOperator.PULL_ALL);
    //
    //              // $pullAll value node must be an array
    //              if (inputValue.getNodeType() != JsonNodeType.ARRAY) {
    //                throw UpdateException.Code.INVALID_UPDATE_OPERATOR_PULL_ALL_VALUE.get(
    //                    errVars(
    //                        table,
    //                        map -> {
    //                          map.put("providedValue", inputValue.toString());
    //                        }));
    //              }
    //
    //              shreddedValue = RowShredder.shredValue(inputValue);
    //
    //              return new ColumnAssignment(
    //                  new ColumnRemoveToAssignment(),
    //                  table.tableMetadata(),
    //                  CqlIdentifierUtil.cqlIdentifierFromUserInput(column),
    //                  shreddedValue);
    //            })
    //        .toList();
  }
}
