package io.stargate.sgv2.jsonapi.service.shredding;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.EJSONWrapper;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonLiteral;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonType;
import java.util.*;
import java.util.function.Function;

/**
 * TODO: DOCS: this was the RowShredder, docs copier below.
 *
 * <p>See the {@link
 * io.stargate.sgv2.jsonapi.service.shredding.tables.JsonNamedValueContainerFactory} for use.
 *
 * <p>Shreds a document transforming it from a {@link JsonNode} to a {@link
 * io.stargate.sgv2.jsonapi.service.shredding.JsonNamedValueContainer}, extracting the values from
 * the Jackson document ready to be later converted into values for the CQL Driver.
 *
 * <p>Note: logic in {@link #shredValue(JsonNode)} and {@link #shredNumber} needs to be kept in sync
 * with code in {@link io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.JSONCodec}:
 * types shredded here must be supported by the codec.
 */
@FunctionalInterface
public interface JsonNodeDecoder extends Function<JsonNode, JsonLiteral<?>> {

  JsonNodeDecoder DEFAULT =
      new JsonNodeDecoder() {
        private static final JsonLiteral<Object> NULL_LITERAL =
            new JsonLiteral<>(null, JsonType.NULL);

        private static final JsonLiteral<Boolean> BOOLEAN_FALSE_LITERAL =
            new JsonLiteral<>(Boolean.FALSE, JsonType.BOOLEAN);

        private static final JsonLiteral<Boolean> BOOLEAN_TRUE_LITERAL =
            new JsonLiteral<>(Boolean.TRUE, JsonType.BOOLEAN);

        /**
         * Function that will convert a JSONNode value, e.g. '1.25' into plain Java type expected
         * when processing tables, e.g. {@link String}, {@link Boolean}, {@link
         * java.math.BigDecimal} and so on.
         *
         * <p>The types returned here are types that are expected by the {@link
         * io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.JSONCodecRegistry} so we
         * know how to convert them into the correct Java types expected by the CQL driver.
         *
         * <p>The main difference here is that we convert all numbers to one of 3 types ({@link
         * java.math.BigDecimal}, {@link java.lang.Long}, {@link java.math.BigInteger}) and then
         * defer conversion into the type defined by the CQL Column until we are building the CQL
         * statement (e.g. insert, or select) where we bind to the column in the table and use the
         * codec to sort it out.
         *
         * @param value JSON value to convert ("shred")
         * @return the value as a "plain" Java type
         */
        @Override
        public JsonLiteral<?> apply(JsonNode value) {

          return switch (value.getNodeType()) {
            case NUMBER -> shredNumber(value);
            case STRING -> new JsonLiteral<>(value.textValue(), JsonType.STRING);
            case BOOLEAN -> value.booleanValue() ? BOOLEAN_TRUE_LITERAL : BOOLEAN_FALSE_LITERAL;
            case NULL -> NULL_LITERAL;
            case ARRAY -> {
              ArrayNode arrayNode = (ArrayNode) value;

              // Check if the array looks like tuple map entries, then shred to object format
              // JsonLiteral.
              Optional<JsonLiteral<?>> optionalMapFromTupleFormat = tupleToObject(arrayNode);
              if (optionalMapFromTupleFormat.isPresent()) {
                yield optionalMapFromTupleFormat.get();
              }

              List<JsonLiteral<?>> list = new ArrayList<>();
              for (JsonNode node : arrayNode) {
                // Leave JsonLiteral wrapping as-is; removed by JsonCodec
                list.add(apply(node));
              }
              yield new JsonLiteral<>(list, JsonType.ARRAY);
            }
            case OBJECT -> {
              ObjectNode objectNode = (ObjectNode) value;
              EJSONWrapper wrapper = EJSONWrapper.maybeFrom(objectNode);
              if (wrapper != null) {
                yield new JsonLiteral<>(wrapper, JsonType.EJSON_WRAPPER);
              }
              // If not, treat as a regular sub-document
              Map<String, JsonLiteral<?>> map = new HashMap<>();
              for (var entry : objectNode.properties()) {
                map.put(entry.getKey(), apply(entry.getValue()));
              }
              yield new JsonLiteral<>(map, JsonType.SUB_DOC);
            }
            default ->
                throw new IllegalArgumentException(
                    "Unsupported JsonNode type " + value.getNodeType());
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

        /**
         * For non-string key maps, public API uses tuple format to represent map entries. This
         * method will detect if the given array looks like tuple map entries, then shred to object
         * format JsonLiteral.
         *
         * <p>Tuple map entries are represented as an array of arrays where each inner array has two
         * elements.
         *
         * <p>E.G.
         *
         * <ul>
         *   <li>"intKeyMapColumn": [[1,"value1"],[2,"value2"]]
         *   <li>"textKeyMapColumn": [["1","value1"],["2","value2"]]
         * </ul>
         *
         * TODO: This method needs to be improved when we can decode jsonNode with schema context.
         * Currently, it is simply just inferring tupleFormat from the arrayNode, for tables feature
         * of map support. But this method could lead to false decoding of tupleFormat for other
         * similar format, such as list arrays.
         *
         * @param arrayNode array jsonNode for which we want to check if it looks like tuple map
         *     entries, then shred to object format JsonLiteral.
         */
        private Optional<JsonLiteral<?>> tupleToObject(ArrayNode arrayNode) {

          // this is to avoid the case where an empty array is considered as list/set
          // so to insert an empty map, user should use {} instead of []
          if (arrayNode.isEmpty()) {
            return Optional.empty();
          }

          // Tuple map entries are represented as an array of arrays where each inner array has two
          // elements.
          for (JsonNode entry : arrayNode) {
            if (entry.getNodeType() != JsonNodeType.ARRAY || entry.size() != 2) {
              return Optional.empty();
            }
          }

          Map<JsonLiteral<?>, JsonLiteral<?>> map = new HashMap<>();
          for (JsonNode entry : arrayNode) {
            JsonLiteral<?> key = apply(entry.get(0));
            JsonLiteral<?> value = apply(entry.get(1));
            map.put(key, value);
          }
          return Optional.of(new JsonLiteral<>(map, JsonType.SUB_DOC));
        }
      };
}
