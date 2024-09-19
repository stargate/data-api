package io.stargate.sgv2.jsonapi.service.shredding.tables;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.EJSONWrapper;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonLiteral;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonType;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonProcessingMetricsReporter;
import io.stargate.sgv2.jsonapi.config.DocumentLimitsConfig;
import io.stargate.sgv2.jsonapi.service.shredding.JsonNamedValue;
import io.stargate.sgv2.jsonapi.service.shredding.JsonNamedValueContainer;
import io.stargate.sgv2.jsonapi.service.shredding.collections.JsonPath;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Shreds a document transforming it from a {@link JsonNode} to a {@link
 * io.stargate.sgv2.jsonapi.service.shredding.JsonNamedValueContainer}, extracting the values from
 * the Jackson document ready to be later converted into values for the CQL Driver.
 *
 * <p>Note: logic in {@link #shredValue(JsonNode)} and {@link #shredNumber} needs to be kept in sync
 * with code in {@link io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.JSONCodec}:
 * types shredded here must be supported by the codec.
 */
@ApplicationScoped
public class RowShredder {

  private final DocumentLimitsConfig documentLimits;

  private final JsonProcessingMetricsReporter jsonProcessingMetricsReporter;

  @Inject
  public RowShredder(
      DocumentLimitsConfig documentLimits,
      JsonProcessingMetricsReporter jsonProcessingMetricsReporter) {
    this.documentLimits = documentLimits;
    this.jsonProcessingMetricsReporter = jsonProcessingMetricsReporter;
  }

  /**
   * Shreds a document into the {@link JsonNamedValue}'s by extracting the Java value from the
   * Jackson document
   *
   * @param document the document to shred
   * @return A {@link JsonNamedValueContainer} of the values found in the document
   */
  public JsonNamedValueContainer shred(JsonNode document) {

    var container = new JsonNamedValueContainer();
    document
        .fields()
        .forEachRemaining(
            entry -> {
              var namedValue =
                  new JsonNamedValue(
                      JsonPath.rootBuilder().property(entry.getKey()).build(),
                      shredValue(entry.getValue()));
              container.put(namedValue);
            });
    return container;
  }

  /**
   * Function that will convert a JSONNode value, e.g. '1.25' into plain Java type expected when
   * processing tables, e.g. {@link String}, {@link Boolean}, {@link java.math.BigDecimal} and so
   * on.
   *
   * <p>The types returned here are types that are expected by the {@link
   * io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.JSONCodecRegistry} so we know
   * how to convert them into the correct Java types expected by the CQL driver.
   *
   * <p>The main difference here is that we convert all numbers to one of 3 types ({@link
   * java.math.BigDecimal}, {@link java.lang.Long}, {@link java.math.BigInteger}) and then defer
   * conversion into the type defined by the CQL Column until we are building the CQL statement
   * (e.g. insert, or select) where we bind to the column in the table and use the codec to sort it
   * out.
   *
   * @param value JSON value to convert ("shred")
   * @return the value as a "plain" Java type
   */
  public static JsonLiteral<?> shredValue(JsonNode value) {

    return switch (value.getNodeType()) {
      case NUMBER -> shredNumber(value);
      case STRING -> new JsonLiteral<>(value.textValue(), JsonType.STRING);
      case BOOLEAN -> new JsonLiteral<>(value.booleanValue(), JsonType.BOOLEAN);
      case NULL -> new JsonLiteral<>(null, JsonType.NULL);
      case ARRAY -> {
        ArrayNode arrayNode = (ArrayNode) value;
        List<JsonLiteral<?>> list = new ArrayList<>();
        for (JsonNode node : arrayNode) {
          list.add(shredValue(node));
        }
        yield new JsonLiteral<>(list, JsonType.ARRAY);
      }
      case OBJECT -> {
        ObjectNode objectNode = (ObjectNode) value;
        // EJSON-wrapper: single entry with key starting with "$TYPE"?
        if (objectNode.size() == 1) {
          Map.Entry<String, JsonNode> entry = objectNode.fields().next();
          EJSONWrapper wrapper = EJSONWrapper.from(entry.getKey(), entry.getValue());
          if (wrapper != null) {
            yield new JsonLiteral<>(wrapper, JsonType.EJSON_WRAPPER);
          }
        }
        // If not, treat as a regular sub-document
        Map<JsonPath, JsonLiteral<?>> map = new HashMap<>();
        for (var entry : objectNode.properties()) {
          map.put(
              JsonPath.rootBuilder().property(entry.getKey()).build(),
              shredValue(entry.getValue()));
        }
        yield new JsonLiteral<>(map, JsonType.SUB_DOC);
      }
      default ->
          throw new IllegalArgumentException("Unsupported JsonNode type " + value.getNodeType());
    };
  }

  // NOTE! This method must be kept in sync with the logic in {@code JSONCodecRegistry}:
  // specifically,
  // types shredded here must be supported by the codec.
  private static JsonLiteral<?> shredNumber(JsonNode number) {
    if (number.isIntegralNumber()) {
      // Return as BigInteger if one required (won't fit in 64-bit Long)
      if (number.isBigInteger()) {
        return new JsonLiteral<>(number.bigIntegerValue(), JsonType.NUMBER);
      }
      // Otherwise as Long (possibly upgrading from Integer)
      return new JsonLiteral<>(number.longValue(), JsonType.NUMBER);
    }
    // But all FPs are returned as BigDecimal
    return new JsonLiteral<>(number.decimalValue(), JsonType.NUMBER);
  }
}
