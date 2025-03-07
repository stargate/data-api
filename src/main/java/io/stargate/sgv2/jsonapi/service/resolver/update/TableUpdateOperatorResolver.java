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

public abstract class TableUpdateOperatorResolver {

  public abstract List<ColumnAssignment> resolve(
      TableSchemaObject table,
      CqlNamedValue.ErrorStrategy<UpdateException> errorStrategy,
      ObjectNode arguments);

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

    // now create the CQL values, this will include running codex to convert the values into the
    // correct CQL types
    var allColumns =
        new CqlNamedValueFactory(tableSchemaObject, codecRegistry, errorStrategy)
            .create(jsonNamedValues);

    checkUpdateOperatorSupported(tableSchemaObject, allColumns, updateOperator);

    return allColumns.values().stream().map(assignmentSupplier).collect(Collectors.toList());
  }

  protected void checkUpdateOperatorSupported(
      TableSchemaObject table, CqlNamedValueContainer allColumns, UpdateOperator operator) {

    var unsupportedColumns =
        allColumns.values().stream()
            .map(NamedValue::apiColumnDef)
            .filter(columnDef -> columnDef.type().apiSupport().update().supports(operator))
            .sorted(ApiColumnDef.NAME_COMPARATOR)
            .toList();

    if (!unsupportedColumns.isEmpty()) {
      throw UpdateException.Code.UNSUPPORTED_UPDATE_OPERATOR.get(
          errVars(
              table,
              map -> {
                map.put("operator", operator.operator());
                map.put("unsupportedColumns", errFmtApiColumnDef(unsupportedColumns));
              }));
    }
  }
}
