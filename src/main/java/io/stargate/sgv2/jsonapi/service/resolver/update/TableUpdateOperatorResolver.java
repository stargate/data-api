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
import io.stargate.sgv2.jsonapi.service.shredding.CqlNamedValue;
import io.stargate.sgv2.jsonapi.service.shredding.JsonNodeDecoder;
import io.stargate.sgv2.jsonapi.service.shredding.tables.CqlNamedValueContainerFactory;
import io.stargate.sgv2.jsonapi.service.shredding.tables.JsonNamedValueContainerFactory;
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
   * @param errorStrategy Error strategy to use with the {@link CqlNamedValueContainerFactory}
   * @param arguments The right hand side arguments to the update operation, from the request. If
   *     the update clause has:
   *     <pre>
   *     {"$set" : { "age" : 51 , "human" : false}}
   *     </pre>
   *     this is:
   *     <pre>
   *     { "age" : 51 , "human" : false}
   *     </pre>
   *
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
   * @param errorStrategy Error strategy to use with the {@link CqlNamedValueContainerFactory}
   * @param updateOperator The operators that is being resolved, used to check that all columns
   *     support the operator.
   * @param assignmentSupplier Function to build the assignment from a {@link CqlNamedValue}
   * @param jsonDecoder Decoder to use with {@link JsonNamedValueContainerFactory} to process the
   *     JSON doc in Java objects
   * @param codecRegistry Codec to use with {@link CqlNamedValueContainerFactory} to create the CQL
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
        new JsonNamedValueContainerFactory(tableSchemaObject, jsonDecoder)
            .create(normalisedUpdateDoc);

    // now create the CQL values, this will include running codec to convert the values into the
    // correct CQL types and run the error strategy to check for the operator support
    var allColumns =
        new CqlNamedValueContainerFactory(tableSchemaObject, codecRegistry, errorStrategy)
            .create(jsonNamedValues);

    return allColumns.values().stream().map(assignmentSupplier).collect(Collectors.toList());
  }
}
