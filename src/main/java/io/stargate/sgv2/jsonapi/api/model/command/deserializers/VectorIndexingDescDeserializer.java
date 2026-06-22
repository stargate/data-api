package io.stargate.sgv2.jsonapi.api.model.command.deserializers;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.indexes.VectorIndexDefinitionDesc.VectorIndexingDesc;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Deserializes the overloaded {@code vectorIndexing} value, which is either:
 *
 * <ul>
 *   <li>a JSON string &rarr; a named profile the API expands into SAI options, e.g. <code>
 *       "vectorIndexing": "small-high-recall"</code>
 *   <li>a JSON object &rarr; raw Cassandra SAI tuning options set directly, e.g. <code>
 *       "vectorIndexing": { "maximum_node_connections": 32 }</code>
 * </ul>
 *
 * Anything else (number, boolean, array, null token) is a request error. This mirrors the design in
 * <a href="https://github.com/stargate/data-api/issues/2508">#2508</a>: the field is overloaded by
 * JSON type rather than carrying separate {@code profile} / {@code options} sub-keys, so a profile
 * and raw options are mutually exclusive in a single request.
 */
public class VectorIndexingDescDeserializer extends StdDeserializer<VectorIndexingDesc> {

  private static final TypeReference<LinkedHashMap<String, Object>> OPTIONS_TYPE =
      new TypeReference<>() {};

  public VectorIndexingDescDeserializer() {
    super(VectorIndexingDesc.class);
  }

  @Override
  public VectorIndexingDesc deserialize(
      JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
    JsonNode node = deserializationContext.readTree(jsonParser);

    if (node.isTextual()) {
      // "vectorIndexing": "small-high-recall" -> a named profile (validated at apply time).
      return VectorIndexingDesc.ofProfile(node.textValue());
    }
    if (node.isObject()) {
      // "vectorIndexing": { "maximum_node_connections": 32 } -> raw SAI options. convertValue
      // honours the mapper config (e.g. float handling) just as a Map<String, Object> field would.
      Map<String, Object> options =
          ((ObjectMapper) jsonParser.getCodec()).convertValue(node, OPTIONS_TYPE);
      return VectorIndexingDesc.ofOptions(options);
    }

    throw SchemaException.Code.INVALID_VECTOR_INDEXING_OPTIONS.get(
        Map.of(
            "reason",
            "`vectorIndexing` must be either a profile name (string) or an object of indexing "
                + "options, but was: "
                + node.getNodeType().name().toLowerCase()
                + "."));
  }
}
