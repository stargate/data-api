package io.stargate.sgv2.jsonapi.service.resolver.update;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperator;
import io.stargate.sgv2.jsonapi.exception.UpdateException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.JSONCodecRegistries;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.JSONCodecRegistry;
import io.stargate.sgv2.jsonapi.service.operation.query.ColumnAssignment;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiColumnDef;
import io.stargate.sgv2.jsonapi.service.shredding.CqlNamedValue;
import io.stargate.sgv2.jsonapi.service.shredding.CqlNamedValueContainer;
import io.stargate.sgv2.jsonapi.service.shredding.JsonNodeDecoder;
import io.stargate.sgv2.jsonapi.service.shredding.NamedValue;
import io.stargate.sgv2.jsonapi.service.shredding.tables.CqlNamedValueFactory;
import io.stargate.sgv2.jsonapi.service.shredding.tables.JsonNamedValueFactory;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Base for classes that resolve a {@link UpdateOperator} for a table.
 *
 * <p>Implementations should normalise the update documents into a document that looks like an
 * insert doc, that is they have a key-value structure:
 *
 * <pre>
 *   {
 *     "column1": "value1",
 *   }
 * </pre>
 *
 * Then call an overload of createColumnAssignments to create the column assignments, using either
 * the default codecs or custom.
 */
public abstract class TableUpdateOperatorResolver {

  /**
   * Called to resolve an update operation.
   *
   * @param tableSchemaObject The table schema object
   * @param errorStrategy Error strategy to use with the {@link CqlNamedValueFactory}
   * @param arguments The right hand side arguments to the update operation, from the request. If
   *     the update clause has <code>{"$set" : { "age" : 51 , "human" : false}}</code> this is
   *     <code>{ "age" : 51 , "human" : false}</code>
   * @return A list of the assignments to make for this update operation.
   */
  public abstract List<ColumnAssignment> resolve(
      TableSchemaObject tableSchemaObject,
      CqlNamedValue.ErrorStrategy<UpdateException> errorStrategy,
      ObjectNode arguments);

  /**
   * Uses the {@link JsonNodeDecoder#DEFAULT} and {@link JSONCodecRegistries#DEFAULT_REGISTRY}, see
   * {@link #createColumnAssignments(TableSchemaObject, JsonNode, CqlNamedValue.ErrorStrategy,
   * UpdateOperator, Function, JsonNodeDecoder, JSONCodecRegistry)}
   */
  protected List<ColumnAssignment> createColumnAssignments(
      TableSchemaObject tableSchemaObject,
      JsonNode normalisedUpdateDoc,
      CqlNamedValue.ErrorStrategy<UpdateException> errorStrategy,
      UpdateOperator updateOperator,
      Function<CqlNamedValue, ColumnAssignment> assignmentSupplier) {
    return createColumnAssignments(
        tableSchemaObject,
        normalisedUpdateDoc,
        errorStrategy,
        updateOperator,
        assignmentSupplier,
        JsonNodeDecoder.DEFAULT,
        JSONCodecRegistries.DEFAULT_REGISTRY);
  }

  /**
   * Factory to create the column assignments from the normalised document of the update that can be
   * processed by the supplied {@link JsonNodeDecoder} and {@link JSONCodecRegistries}.
   *
   * <p>Implementations should call this method to create the column assignments after they have
   * normalised the update document.
   *
   * @param tableSchemaObject The table schema object
   * @param normalisedUpdateDoc Normalised document of key-value pairs that can be procesed by the
   *     decoder and the codecs.
   * @param errorStrategy Error strategy to use with the {@link CqlNamedValueFactory}
   * @param updateOperator The operators that is being resolved, used to check that all columns
   *     support the operator.
   * @param assignmentSupplier Function to build the assignment from a {@link CqlNamedValue}
   * @param jsonDecoder Decoder to use with {@link JsonNamedValueFactory} to process the JSON doc in
   *     Java objects
   * @param codecRegistry Codec to use with {@link CqlNamedValueFactory} to create the CQL
   *     parameters
   * @return List of assignments for the update operation
   */
  protected List<ColumnAssignment> createColumnAssignments(
      TableSchemaObject tableSchemaObject,
      JsonNode normalisedUpdateDoc,
      CqlNamedValue.ErrorStrategy<UpdateException> errorStrategy,
      UpdateOperator updateOperator,
      Function<CqlNamedValue, ColumnAssignment> assignmentSupplier,
      JsonNodeDecoder jsonDecoder,
      JSONCodecRegistry codecRegistry) {

    // decode the JSON objects into our Java objects
    var jsonNamedValues =
        new JsonNamedValueFactory(tableSchemaObject, jsonDecoder).create(normalisedUpdateDoc);

    // now create the CQL values, this will include running codec to convert the values into the
    // correct CQL types
    var allColumns =
        new CqlNamedValueFactory(tableSchemaObject, codecRegistry, errorStrategy)
            .create(jsonNamedValues);

    checkUpdateOperatorSupported(tableSchemaObject, allColumns, updateOperator);

    return allColumns.values().stream().map(assignmentSupplier).collect(Collectors.toList());
  }

  protected void checkUpdateOperatorSupported(
      TableSchemaObject tableSchemaObject,
      CqlNamedValueContainer allColumns,
      UpdateOperator operator) {

    var unsupportedColumns =
        allColumns.values().stream()
            .map(NamedValue::apiColumnDef)
            .filter(columnDef -> !columnDef.type().apiSupport().update().supports(operator))
            .sorted(ApiColumnDef.NAME_COMPARATOR)
            .toList();

    if (!unsupportedColumns.isEmpty()) {
      throw UpdateException.Code.UNSUPPORTED_UPDATE_OPERATOR.get(
          errVars(
              tableSchemaObject,
              map -> {
                map.put(
                    "allColumns", errFmtApiColumnDef(tableSchemaObject.apiTableDef().allColumns()));
                map.put("operator", operator.apiName());
                map.put("unsupportedColumns", errFmtApiColumnDef(unsupportedColumns));
              }));
    }
  }
}
