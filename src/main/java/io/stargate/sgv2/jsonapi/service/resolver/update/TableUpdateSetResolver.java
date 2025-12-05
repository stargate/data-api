package io.stargate.sgv2.jsonapi.service.resolver.update;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperator;
import io.stargate.sgv2.jsonapi.exception.UpdateException;
import io.stargate.sgv2.jsonapi.service.schema.tables.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.query.ColumnAssignment;
import io.stargate.sgv2.jsonapi.service.operation.query.ColumnSetToAssignment;
import io.stargate.sgv2.jsonapi.service.shredding.CqlNamedValue;
import java.util.List;

/** Resolves the {@link UpdateOperator#SET} operation for a table update. */
public class TableUpdateSetResolver extends TableUpdateOperatorResolver {

  /**
   * Resolve the {@link UpdateOperator#SET} operation for a table update
   *
   * <p>Example:
   *
   * <pre>
   * // primitive column
   * {"$set" : { "age" : 51 , "human" : false}}
   *
   * // list column
   * {"$set" : { "listColumn" : [1,2]}}
   *
   * // set column
   * {"$set" : { "setColumn" : { "key1": "value1", "key2": "value2"}}}
   *
   * // map column with string key (object format)
   * {"$set" : { "mapColumn" : { "key1": "value1", "key2": "value2"}}}
   *
   * // map column with string key (array-of-arrays format)
   * {"$set" : { "mapColumn" : [["key1","value1"], ["key2","value2"]]}}
   *
   * // map column with non-string key (array-of-arrays format)
   *  {"$set" : { "mapColumn" : [[123,"value1"], [456,"value2"]]}}
   * </pre>
   */
  @Override
  public List<ColumnAssignment> resolve(
      TableSchemaObject tableSchemaObject,
      CqlNamedValue.ErrorStrategy<UpdateException> errorStrategy,
      ObjectNode arguments) {

    // the arguments to the $set are just an insert document with the values to set
    // no need to normalise the arguments to $set, and we use the default codecs
    return createColumnAssignments(
        tableSchemaObject,
        arguments,
        errorStrategy,
        UpdateOperator.SET,
        ColumnSetToAssignment::new);
  }
}
