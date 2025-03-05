package io.stargate.sgv2.jsonapi.service.shredding.tables;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.shredding.JsonNamedValue;
import io.stargate.sgv2.jsonapi.service.shredding.JsonNamedValueContainer;
import io.stargate.sgv2.jsonapi.service.shredding.JsonNodeDecoder;
import io.stargate.sgv2.jsonapi.service.shredding.collections.JsonPath;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.util.*;

/**
 * Shreds a document transforming it from a {@link JsonNode} to a {@link
 * io.stargate.sgv2.jsonapi.service.shredding.JsonNamedValueContainer}, extracting the values from
 * the Jackson document ready to be later converted into values for the CQL Driver.
 *
 * <p>Note: logic in {@link #shredValue(JsonNode)} and {@link #shredNumber} needs to be kept in sync
 * with code in {@link io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.JSONCodec}:
 * types shredded here must be supported by the codec.
 */
// @ApplicationScoped
public class JsonNamedValueFactory {
  //  private static final JsonLiteral<Object> NULL_LITERAL = new JsonLiteral<>(null,
  // JsonType.NULL);
  //
  //  private static final JsonLiteral<Boolean> BOOLEAN_FALSE_LITERAL =
  //      new JsonLiteral<>(Boolean.FALSE, JsonType.BOOLEAN);
  //  private static final JsonLiteral<Boolean> BOOLEAN_TRUE_LITERAL =
  //      new JsonLiteral<>(Boolean.TRUE, JsonType.BOOLEAN);
  //
  //  private final DocumentLimitsConfig documentLimits;
  //
  //  private final JsonProcessingMetricsReporter jsonProcessingMetricsReporter;

  //  @Inject

  private final TableSchemaObject tableSchemaObject;
  private final JsonNodeDecoder jsonNodeDecoder;

  public JsonNamedValueFactory(
      TableSchemaObject tableSchemaObject, JsonNodeDecoder jsonNodeDecoder) {
    this.tableSchemaObject =
        Objects.requireNonNull(tableSchemaObject, "tableSchemaObject must not be null");
    this.jsonNodeDecoder =
        Objects.requireNonNull(jsonNodeDecoder, "jsonNodeDecoder must not be null");
  }

  /**
   * Shreds a document into the {@link JsonNamedValue}'s by extracting the Java value from the
   * Jackson document
   *
   * @param document the document to shred
   * @return A {@link JsonNamedValueContainer} of the values found in the document
   */
  public JsonNamedValueContainer create(JsonNode document) {

    var container = new JsonNamedValueContainer();
    document
        .fields()
        .forEachRemaining(
            entry -> {
              var namedValue =
                  new JsonNamedValue(
                      JsonPath.rootBuilder().property(entry.getKey()).build(), jsonNodeDecoder);
              if (namedValue.bind(tableSchemaObject)) {
                namedValue.prepare(entry.getValue());
              }
              // even if there was an error, we still want to put the named value into the container
              // so other code can see what was in the document
              container.put(namedValue);
            });
    return container;
  }

  public List<ParsedJsonDocument> create(List<JsonNode> documents) {

    List<ParsedJsonDocument> result = new ArrayList<>(documents.size());
    for (int i = 0; i < documents.size(); i++) {
      result.add(new ParsedJsonDocument(i, create(documents.get(i))));
    }
    return result;
  }

  public record ParsedJsonDocument(int offset, JsonNamedValueContainer namedValues)
      implements Recordable {

    @Override
    public DataRecorder recordTo(DataRecorder dataRecorder) {
      return dataRecorder.append("offset", offset).append("namedValues", namedValues);
    }
  }

  //  public static class ParsedJsonDocuments extends ArrayList<ParsedJsonDocument>
  //      implements Recordable {
  //
  //    public ParsedJsonDocuments(int initialCapacity) {
  //      super(initialCapacity);
  //    }
  //
  //    @Override
  //    public DataRecorder recordTo(DataRecorder dataRecorder) {
  //      // if we pass this, it will recursive call into this method
  //      return dataRecorder.append("items", List.copyOf(this));
  //    }
  //  }

  //
  //  /**
  //   * Function that will convert a JSONNode value, e.g. '1.25' into plain Java type expected when
  //   * processing tables, e.g. {@link String}, {@link Boolean}, {@link java.math.BigDecimal} and
  // so
  //   * on.
  //   *
  //   * <p>The types returned here are types that are expected by the {@link
  //   * io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.JSONCodecRegistry} so we
  // know
  //   * how to convert them into the correct Java types expected by the CQL driver.
  //   *
  //   * <p>The main difference here is that we convert all numbers to one of 3 types ({@link
  //   * java.math.BigDecimal}, {@link java.lang.Long}, {@link java.math.BigInteger}) and then defer
  //   * conversion into the type defined by the CQL Column until we are building the CQL statement
  //   * (e.g. insert, or select) where we bind to the column in the table and use the codec to sort
  // it
  //   * out.
  //   *
  //   * @param value JSON value to convert ("shred")
  //   * @return the value as a "plain" Java type
  //   */
  //  public static JsonLiteral<?> shredValue(JsonNode value) {
  //
  //    return switch (value.getNodeType()) {
  //      case NUMBER -> shredNumber(value);
  //      case STRING -> new JsonLiteral<>(value.textValue(), JsonType.STRING);
  //      case BOOLEAN -> value.booleanValue() ? BOOLEAN_TRUE_LITERAL : BOOLEAN_FALSE_LITERAL;
  //      case NULL -> NULL_LITERAL;
  //      case ARRAY -> {
  //        ArrayNode arrayNode = (ArrayNode) value;
  //        List<JsonLiteral<?>> list = new ArrayList<>();
  //        for (JsonNode node : arrayNode) {
  //          // Leave JsonLiteral wrapping as-is; removed by JsonCodec
  //          list.add(shredValue(node));
  //        }
  //        yield new JsonLiteral<>(list, JsonType.ARRAY);
  //      }
  //      case OBJECT -> {
  //        ObjectNode objectNode = (ObjectNode) value;
  //        EJSONWrapper wrapper = EJSONWrapper.maybeFrom(objectNode);
  //        if (wrapper != null) {
  //          yield new JsonLiteral<>(wrapper, JsonType.EJSON_WRAPPER);
  //        }
  //        // If not, treat as a regular sub-document
  //        Map<String, JsonLiteral<?>> map = new HashMap<>();
  //        for (var entry : objectNode.properties()) {
  //          map.put(entry.getKey(), shredValue(entry.getValue()));
  //        }
  //        yield new JsonLiteral<>(map, JsonType.SUB_DOC);
  //      }
  //      default ->
  //          throw new IllegalArgumentException("Unsupported JsonNode type " +
  // value.getNodeType());
  //    };
  //  }
  //
  //  // NOTE! This method must be kept in sync with the logic in {@code JSONCodecRegistry}:
  //  // specifically,
  //  // types shredded here must be supported by the codec.
  //  private static JsonLiteral<?> shredNumber(JsonNode number) {
  //    if (number.isIntegralNumber()) {
  //      // Return as BigInteger if one required (won't fit in 64-bit Long)
  //      if (number.isBigInteger()) {
  //        return new JsonLiteral<>(number.bigIntegerValue(), JsonType.NUMBER);
  //      }
  //      // Otherwise as Long (possibly upgrading from Integer)
  //      return new JsonLiteral<>(number.longValue(), JsonType.NUMBER);
  //    }
  //    // But all FPs are returned as BigDecimal
  //    return new JsonLiteral<>(number.decimalValue(), JsonType.NUMBER);
  //  }
}
