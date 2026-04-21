package io.stargate.sgv2.jsonapi.service.resolver.update;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonLiteral;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonType;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperator;
import io.stargate.sgv2.jsonapi.exception.UpdateException;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.JSONCodecRegistries;
import io.stargate.sgv2.jsonapi.service.operation.query.ColumnAssignment;
import io.stargate.sgv2.jsonapi.service.operation.query.ColumnSetToAssignment;
import io.stargate.sgv2.jsonapi.service.schema.tables.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.shredding.CqlNamedValue;
import java.util.List;

/** Resolves the {@link UpdateOperator#UNSET} operation for a table update. */
public class TableUpdateUnsetResolver extends TableUpdateOperatorResolver {

  /**
   * Resolve the {@link UpdateOperator#UNSET} operation.
   *
   * <p>Example: (Note, it does not matter what the unset value is, NULL will be set)
   *
   * <pre>
   * // primitive column
   * {"$unset" : { "age" : 1 , "human" : false}}
   *
   * // list column
   * {"$unset" : { "listColumn" : "abc"}}
   *
   * // set column
   * {"$unset" : { "setColumn" : {"random":"random"}}}
   *
   * // map column
   * {"$unset" : { "mapColumn" : []}}
   * </pre>
   */
  @Override
  public List<ColumnAssignment> resolve(
      TableSchemaObject tableSchemaObject,
      CqlNamedValue.ErrorStrategy<UpdateException> errorStrategy,
      ObjectNode arguments) {

    // the arguments to the $unset are just an insert document with the values ignored and turned
    // into a null no need to normalise the arguments to unset,
    // we use a Json Decoder that will turn every value into a JSON NULL
    return createColumnAssignments(
        tableSchemaObject,
        arguments,
        errorStrategy,
        UpdateOperator.UNSET,
        ColumnSetToAssignment::new,
        (jsonNode) -> new JsonLiteral<>(null, JsonType.NULL),
        JSONCodecRegistries.DEFAULT_REGISTRY);
  }
}
