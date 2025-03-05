package io.stargate.sgv2.jsonapi.service.shredding;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierFromUserInput;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonLiteral;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiColumnDef;
import io.stargate.sgv2.jsonapi.service.shredding.collections.JsonPath;
import java.util.Objects;

/**
 * A value that has come from Jackson or can be used to create a Jackson object.
 *
 * <p>The value is a {@link JsonLiteral} where the value is a Java object, not a Jackson node, which
 * is standard behaviour for the {@link JsonLiteral}.
 */
public class JsonNamedValue extends NamedValue<JsonPath, JsonLiteral<?>, JsonNode> {

  private final JsonNodeDecoder jsonNodeDecoder;

  public JsonNamedValue(JsonPath name, JsonNodeDecoder jsonNodeDecoder) {
    super(name);
    this.jsonNodeDecoder =
        Objects.requireNonNull(jsonNodeDecoder, "jsonNodeDecoder must not be null");
  }

  @Override
  protected ApiColumnDef bindToColumn(TableSchemaObject tableSchemaObject) {
    var apiColumnDef =
        tableSchemaObject
            .apiTableDef()
            .allColumns()
            .get(cqlIdentifierFromUserInput(name().toString()));
    if (apiColumnDef == null) {
      setState(NamedValueState.BIND_ERROR);
    }
    return apiColumnDef;
  }

  @Override
  protected DecodeResult<JsonLiteral<?>> decodeValue(JsonNode rawValue) {

    // First we need to check if the name can be found in the target table, we need to do this
    // because we need to
    // know for e.g. that the column is a vector with vectorize so we can defer the value until
    // later.
    // we can do this by checking we are bound
    checkIsState(NamedValueState.BOUND, "decodeValue");

    // we do not check for vectorize tasks here, we do that when preparing the CqlNamedValues
    // here we just want to go from JSON to Java types
    // so we never have a deferred decode result
    return new DecodeResult<>(jsonNodeDecoder.apply(rawValue), null);
  }

  //  @Override
  //  public String toString() {
  //    return new StringBuilder(getClass().getSimpleName())
  //        .append("{name=")
  //        .append(name())
  //        .append(", value=")
  //        .append(value())
  //        .append("}")
  //        .toString();
  //  }
}
