package io.stargate.sgv3.docsapi.shredding;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import io.stargate.sgv3.docsapi.commands.clauses.ProjectionClause;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
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
import org.javatuples.Pair;

/**
 * Shred an incoming JSON document into the data we need to store in the DB, and then de-shred.
 *
 * <p>This will be based on the ideas in the python lab, and extended to do things like make better
 * decisions about when to use a hash and when to use the actual value. i.e. a hash of "a" is a lot
 * longer than "a"
 */
public class Shredder {

  private static final Charset CHARSET = Charset.forName("UTF-8");

  public WritableShreddedDocument shred(JsonNode document) {
    return shred(document, Optional.empty());
  }

  public List<WritableShreddedDocument> shred(List<JsonNode> documents) {
    return documents.stream().map(doc -> shred(doc, Optional.empty())).toList();
  }

  /**
   * Re-shredding is when we are shredding a document we already read from the DB, when this happens
   * we we will want copy over the {@link ReadableShreddedDocument#txID} because this is used to
   * detect changes.
   *
   * @param documents
   * @return
   */
  public List<WritableShreddedDocument> reshred(
      List<Pair<ReadableShreddedDocument, JsonNode>> documents) {
    return documents.stream().map(pair -> shred(pair.getValue1(), pair.getValue0().txID)).toList();
  }

  private WritableShreddedDocument shred(JsonNode doc, Optional<UUID> txId) {

    // This is where we do all the shredding based on the python code

    // TODO HACK HACK HACK- this is not the correct shredding, only handles top level keys of
    // string, number, bool, and null
    // also the code is kind of shit - signed, the author

    var idNode = doc.get("_id");
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
          return StreamSupport.stream(Spliterators.spliteratorUnknownSize(doc.fields(), 0), false)
              .filter(entry -> nodeTypes.contains(entry.getValue().getNodeType()));
        };

    Supplier<Stream<String>> streamFieldNames =
        () -> {
          return StreamSupport.stream(
              Spliterators.spliteratorUnknownSize(doc.fieldNames(), 0), false);
        };

    // Building the fields of the shredded doc

    Map<JSONPath, Integer> properties = Map.of();

    // HACK only expects to find top level keys, no sub docs or arrays
    Set<JSONPath> existKeys =
        streamFieldNames.get().map(JSONPath::from).collect(Collectors.toSet());

    Map<JSONPath, String> subDocEquals = Map.of(); // TODO
    Map<JSONPath, Integer> arraySize = Map.of(); // TODO
    Map<JSONPath, String> arrayEquals = Map.of(); // TODO
    Map<JSONPath, String> arrayContains = Map.of(); // TODO

    Map<JSONPath, Boolean> queryBoolValues =
        streamNodes
            .apply(EnumSet.of(JsonNodeType.BOOLEAN))
            .collect(
                Collectors.toMap(
                    entry -> JSONPath.from(entry.getKey()), entry -> entry.getValue().asBoolean()));

    Map<JSONPath, BigDecimal> queryNumberValues =
        streamNodes
            .apply(EnumSet.of(JsonNodeType.NUMBER))
            .collect(
                Collectors.toMap(
                    entry -> JSONPath.from(entry.getKey()),
                    entry -> BigDecimal.valueOf(entry.getValue().asDouble()).stripTrailingZeros()));

    Map<JSONPath, String> queryTextValues =
        streamNodes
            .apply(EnumSet.of(JsonNodeType.STRING))
            .collect(
                Collectors.toMap(
                    entry -> JSONPath.from(entry.getKey()), entry -> entry.getValue().asText()));

    Set<JSONPath> queryNullValues =
        streamNodes
            .apply(EnumSet.of(JsonNodeType.NULL))
            .map(Map.Entry::getKey)
            .map(JSONPath::from)
            .collect(Collectors.toSet());

    List<JSONPath> docFieldOrder =
        streamFieldNames.get().map(JSONPath::from).collect(Collectors.toList());

    Map<JSONPath, Pair<JsonType, ByteBuffer>> docAtomicFields =
        streamNodes
            .apply(EnumSet.of(JsonNodeType.BOOLEAN, JsonNodeType.NUMBER, JsonNodeType.STRING))
            .collect(
                Collectors.toMap(
                    entry -> JSONPath.from(entry.getKey()),
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

  public List<JsonNode> deshred(List<ReadableShreddedDocument> docs) {
    return deshred(ProjectionClause.ALL, docs);
  }

  public List<JsonNode> deshred(
      ProjectionClause projection, List<? extends ReadableShreddedDocument> docs) {

    // TODO - HACK , deshredding only handles string and top level keys and does not handle doc
    // order
    // TODO - HACK - Does not rebuild the document based on the field order, it should do this
    // TODO handle a list of docs to deshred

    assert docs.size() == 1;
    List<JsonNode> responseDocs = new ArrayList<>(docs.size());

    for (ReadableShreddedDocument shredDoc : docs) {
      // This is a terrible way to deshred and build the JSON, just keeping it simple so we read the
      // correct stuff
      // onh
      Map<String, Object> data =
          shredDoc.docAtomicFields.entrySet().stream()
              .map(
                  entry -> {
                    // map from  <path, value> to <string, object> for top level keys and values
                    // only
                    // need to get the type
                    Pair<JsonType, ByteBuffer> atomicFieldValue = entry.getValue();
                    Object value =
                        decodeAtomicFieldValue(
                            atomicFieldValue.getValue0(), atomicFieldValue.getValue1());
                    return Pair.with(entry.getKey().getPath(), value);
                  })
              .collect(Collectors.toMap(Pair::getValue0, Pair::getValue1));

      ObjectMapper mapper = new ObjectMapper();
      JsonNode doc = mapper.valueToTree(data);
      responseDocs.add(doc);
    }

    return responseDocs;
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
  /**
   * Here now so we can code the _id when reading a row
   *
   * @param <T>
   * @param atomicFieldValue
   * @return
   */
  public static <T> T decodeAtomicFieldValue(Pair<JsonType, ByteBuffer> atomicFieldValue) {
    return decodeAtomicFieldValue(atomicFieldValue.getValue0(), atomicFieldValue.getValue1());
  }

  public static <T> T decodeAtomicFieldValue(JsonType jsonType, ByteBuffer bytes) {

    switch (jsonType) {
      case STRING:
        String str = CHARSET.decode(bytes).toString();
        // HACK : Need to rewind, so the java driver will read correctly.
        // aaron - i wrote this and dont understand it, but we read from the buffer before sending
        // it to the driver.
        bytes.rewind();
        return (T) str;
      case BOOLEAN:
        Boolean bool = bytes.get() == 0 ? Boolean.FALSE : Boolean.TRUE;
        // only doing this here to be consistent, see above
        bytes.rewind();
        return (T) bool;
      case NUMBER:
        // TODO HACK - better decimal encoding
        BigDecimal decimal = new BigDecimal(CHARSET.decode(bytes).toString());
        // only doing this here to be consistent, see above
        bytes.rewind();
        return (T) decimal;
      default:
        throw new RuntimeException("Unknown jsonType to decode buffer " + jsonType.toString());
    }
  }
}
