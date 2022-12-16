package io.stargate.sgv3.docsapi.commands.serializers;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Takes a JSON object and turns it into a Map<String, JsonNode> for situations where
 * the API is a dynamic map. e.g.
 *
 * The {@code $set} maps to {@link SetUpdateOperation} below:
 *
 * <pre>
 * {
 *      "updateOne": {
 *          "filter" : {"_id": "doc1"},
 *          "update" : {"$set" : {"updated_field" : "System.currentTimeMillis = %s", "field2" : "new value"}}
 *       }
 * }
 * </pre>
 *
 * So the SetUpdateOperation needs to get a map of {@code {"updated_field" : "System.currentTimeMillis = %s", "field2" : "new value"}}
 * and we want to get it at creation time, so it can be validated in the ctor.
 *
 * So configure the Mixin {@link SetUpdateOperationMixin} as:
 *
 * <pre>
 *@JsonCreator(mode= Mode.DELEGATING)
 *public static SetUpdateOperation create(
 *  @JsonDeserialize(using = MapStringJsonDeserializer.class) Map<String, JsonNode> updates) {
 *  return new SetUpdateOperation(updates);
 *}
 * </code>
 *
 * NOTE: I had to make sure there was no polymorphism configured for SetUpdateOperation or it would kickin
 * and get confused.
 */
public class MapStringJsonDeserializer extends StdDeserializer<Map<String, JsonNode>> {

  public MapStringJsonDeserializer() {
    this(null);
  }

  public MapStringJsonDeserializer(Class<?> vc) {
    super(vc);
  }

  @Override
  public Map<String, JsonNode> deserialize(JsonParser parser, DeserializationContext ctxt)
      throws IOException, JacksonException {

    Map<String, JsonNode> entries = new HashMap<>();

    JsonNode mapObject = ctxt.readTree(parser);
    var iter = mapObject.fields();
    while (iter.hasNext()) {
      var entry = iter.next();
      entries.put(entry.getKey(), entry.getValue());
    }
    return entries;
  }
}
