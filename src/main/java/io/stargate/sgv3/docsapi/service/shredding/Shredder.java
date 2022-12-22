package io.stargate.sgv3.docsapi.service.shredding;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import io.stargate.sgv3.docsapi.service.shredding.model.JsonPath;
import io.stargate.sgv3.docsapi.service.shredding.model.JsonType;
import io.stargate.sgv3.docsapi.service.shredding.model.WritableShreddedDocument;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterators;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.enterprise.context.ApplicationScoped;
import javax.validation.constraints.NotNull;
import org.javatuples.Pair;

/**
 * Shred an incoming JSON document into the data we need to store in the DB, and then de-shred.
 *
 * <p>This will be based on the ideas in the python lab, and extended to do things like make better
 * decisions about when to use a hash and when to use the actual value. i.e. a hash of "a" is a lot
 * longer than "a".
 */
@ApplicationScoped
public class Shredder {
  private static final Charset CHARSET = Charset.forName("UTF-8");
  /**
   * Shreds a single JSON node into a {@link WritableShreddedDocument} representation.
   *
   * @param document {@link JsonNode} to shred.
   * @return WritableShreddedDocument
   */
  public WritableShreddedDocument shred(@NotNull JsonNode document) {

    // This is where we do all the shredding based on the python code

    // TODO HACK HACK HACK- this is not the correct shredding, only handles top level keys of
    // string, number, bool, and null
    // also the code is kind of shit - signed, the author

    JsonNode idNode = document.get("_id");
    Optional<UUID> txId = Optional.empty();
    if (idNode == null || idNode.getNodeType() != JsonNodeType.STRING) {
      // TODO Need to confirm the _id type and handling
      throw new RuntimeException("Bad _id type or missing");
    }
    String id = idNode.asText();

    // couple of helper functions to build the streams
    // whe we get smarter can do more in a single stream, for now still working out what we need to
    // do for each shredded field
    // its inefficient for now, but am focused on the features
    Function<Set<JsonNodeType>, Stream<Map.Entry<String, JsonNode>>> streamNodes =
        (Set<JsonNodeType> nodeTypes) -> {
          return StreamSupport.stream(
                  Spliterators.spliteratorUnknownSize(document.fields(), 0), false)
              .filter(entry -> nodeTypes.contains(entry.getValue().getNodeType()));
        };

    Supplier<Stream<String>> streamFieldNames =
        () -> {
          return StreamSupport.stream(
              Spliterators.spliteratorUnknownSize(document.fieldNames(), 0), false);
        };

    // Building the fields of the shredded doc

    Map<JsonPath, Integer> properties = Map.of();

    // HACK only expects to find top level keys, no sub docs or arrays
    Set<JsonPath> existKeys =
        streamFieldNames.get().map(JsonPath::from).collect(Collectors.toSet());

    Map<JsonPath, String> subDocEquals = Map.of(); // TODO
    Map<JsonPath, Integer> arraySize = Map.of(); // TODO
    Map<JsonPath, String> arrayEquals = Map.of(); // TODO
    Map<JsonPath, String> arrayContains = Map.of(); // TODO

    Map<JsonPath, Boolean> queryBoolValues =
        streamNodes
            .apply(EnumSet.of(JsonNodeType.BOOLEAN))
            .collect(
                Collectors.toMap(
                    entry -> JsonPath.from(entry.getKey()), entry -> entry.getValue().asBoolean()));

    Map<JsonPath, BigDecimal> queryNumberValues =
        streamNodes
            .apply(EnumSet.of(JsonNodeType.NUMBER))
            .collect(
                Collectors.toMap(
                    entry -> JsonPath.from(entry.getKey()),
                    entry -> BigDecimal.valueOf(entry.getValue().asDouble()).stripTrailingZeros()));

    Map<JsonPath, String> queryTextValues =
        streamNodes
            .apply(EnumSet.of(JsonNodeType.STRING))
            .collect(
                Collectors.toMap(
                    entry -> JsonPath.from(entry.getKey()), entry -> entry.getValue().asText()));

    Set<JsonPath> queryNullValues =
        streamNodes
            .apply(EnumSet.of(JsonNodeType.NULL))
            .map(Map.Entry::getKey)
            .map(JsonPath::from)
            .collect(Collectors.toSet());

    List<JsonPath> docFieldOrder =
        streamFieldNames.get().map(JsonPath::from).collect(Collectors.toList());

    Map<JsonPath, Pair<JsonType, ByteBuffer>> docAtomicFields =
        streamNodes
            .apply(EnumSet.of(JsonNodeType.BOOLEAN, JsonNodeType.NUMBER, JsonNodeType.STRING))
            .collect(
                Collectors.toMap(
                    entry -> JsonPath.from(entry.getKey()),
                    entry -> encodeAtomicValue(entry.getValue())));

    return new WritableShreddedDocument(
        id,
        txId,
        docFieldOrder,
        docAtomicFields,
        properties,
        existKeys,
        subDocEquals,
        arraySize,
        arrayEquals,
        arrayContains,
        queryBoolValues,
        queryNumberValues,
        queryTextValues,
        queryNullValues);
  }

  public static Pair<JsonType, ByteBuffer> encodeAtomicValue(JsonNode node) {

    JsonType jsonType = JsonType.fromJacksonType(node.getNodeType());

    switch (jsonType) {
      case STRING:
        return Pair.with(jsonType, CHARSET.encode(node.asText()));
      case BOOLEAN:
        return Pair.with(
            jsonType,
            node.asBoolean() ? ByteBuffer.wrap(new byte[] {1}) : ByteBuffer.wrap(new byte[] {0}));
      case NUMBER:
        // TODO HACK - better decimal encoding
        BigDecimal decimal = BigDecimal.valueOf(node.asDouble());
        return Pair.with(jsonType, CHARSET.encode(node.asText()));
      default:
        throw new RuntimeException("Unknown jsonType to encode buffer " + jsonType.toString());
    }
  }
}
