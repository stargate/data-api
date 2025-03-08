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

/** Resolves the {@link UpdateOperator#PULL_ALL} operation for a table update. */
public class TableUpdatePullAllResolver extends TableUpdateOperatorResolver {

  /**
   * Resolve the {@link UpdateOperator#PULL_ALL} operation for a table update
   *
   * <p>Push operator can only be used for collection columns (list, set, map).
   *
   * <p>Examples:
   *
   * <pre>
   * // list column
   * {"$pullAll" : { "listColumn1" : [1,2], "listColumn2" : ["a","b"]}}
   *
   * // set column
   * {"$pullAll" : { "setColumn2" : [1,2], "setColumn2" : ["a","b"]}}
   *
   * // map column (pull from map key)
   * {"$pullAll" : { "mapColumn" : [1,2], "mapColumn" : ["abc","def"]}}
   * </pre>
   */
  @Override
  public List<ColumnAssignment> resolve(
      TableSchemaObject tableSchemaObject,
      CqlNamedValue.ErrorStrategy<UpdateException> errorStrategy,
      ObjectNode arguments) {

    // we do not need to normalise the arguments to $pullAll, the op arguments are
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
  }
}
