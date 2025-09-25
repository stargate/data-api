package io.stargate.sgv2.jsonapi.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.POJONode;
import com.fasterxml.jackson.databind.node.ValueNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;

/**
 * Utility for deep-merging YAML documents with simple, predictable semantics:
 *
 * <ul>
 *   <li>Objects: recursively merged; fields from patch override or extend base
 *   <li>Arrays: replaced entirely when present in patch
 *   <li>Scalars (string/number/boolean/null): replaced by patch value
 * </ul>
 *
 * This class is independent and can be used without other framework pieces.
 */
public final class YamlMerger {

  private final ObjectMapper yamlMapper;

  public YamlMerger() {
    this.yamlMapper = new ObjectMapper(new YAMLFactory());
    // Register commonly used modules; harmless if not needed
    this.yamlMapper.registerModule(new Jdk8Module());
  }

  /** Merge two YAML strings and return the merged YAML string. */
  public String mergeYamlStrings(String baseYaml, String patchYaml) {
    try {
      JsonNode base = yamlMapper.readTree(baseYaml);
      JsonNode patch = yamlMapper.readTree(patchYaml);
      JsonNode merged = mergeNodes(base, patch);
      return yamlMapper.writeValueAsString(merged);
    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to merge YAML", e);
    }
  }

  /** Merge two YAML input streams and return the merged YAML string. */
  public String mergeYamlStreams(InputStream baseYaml, InputStream patchYaml) {
    try {
      JsonNode base = yamlMapper.readTree(baseYaml);
      JsonNode patch = yamlMapper.readTree(patchYaml);
      JsonNode merged = mergeNodes(base, patch);
      return yamlMapper.writeValueAsString(merged);
    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to merge YAML streams", e);
    }
  }

  /** Merge two YAML strings and return the merged node. */
  public JsonNode mergeToNode(String baseYaml, String patchYaml) {
    try {
      JsonNode base = yamlMapper.readTree(baseYaml);
      JsonNode patch = yamlMapper.readTree(patchYaml);
      return mergeNodes(base, patch);
    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to merge YAML", e);
    }
  }

  /** Serialize a node back to a YAML string. */
  public String toYaml(JsonNode node) {
    try {
      return yamlMapper.writeValueAsString(node);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to write YAML", e);
    }
  }

  /** Core merge logic following the documented semantics. */
  public JsonNode mergeNodes(JsonNode base, JsonNode patch) {
    if (base == null || base.isNull()) {
      return deepCopy(patch);
    }
    if (patch == null) {
      return deepCopy(base);
    }

    // If both are objects, merge field-by-field
    if (base.isObject() && patch.isObject()) {
      ObjectNode result = ((ObjectNode) base.deepCopy());
      Iterator<Map.Entry<String, JsonNode>> fields = patch.fields();
      while (fields.hasNext()) {
        Map.Entry<String, JsonNode> entry = fields.next();
        String fieldName = entry.getKey();
        JsonNode patchValue = entry.getValue();
        JsonNode baseValue = result.get(fieldName);
        if (baseValue != null) {
          JsonNode mergedChild = mergeNodes(baseValue, patchValue);
          result.set(fieldName, mergedChild);
        } else {
          result.set(fieldName, deepCopy(patchValue));
        }
      }
      return result;
    }

    // If both are arrays, replace entirely with patch (no element-wise merging)
    if (base.isArray() && patch.isArray()) {
      return ((ArrayNode) patch).deepCopy();
    }

    // Otherwise scalars or differing types: patch overrides
    return deepCopy(patch);
  }

  private JsonNode deepCopy(JsonNode node) {
    if (node == null) {
      return null;
    }
    if (node.isPojo()) {
      // Ensure POJOs remain intact, but wrapped safely
      return new POJONode(((POJONode) node).getPojo());
    }
    if (node.isObject() || node.isArray()) {
      return node.deepCopy();
    }
    // ValueNode (scalar) - immutable, return as is; but make a trivial copy by re-parsing to avoid
    // shared refs
    if (node instanceof ValueNode) {
      try {
        String yaml = yamlMapper.writeValueAsString(node);
        return yamlMapper.readTree(yaml);
      } catch (IOException e) {
        // Fallback to returning the same instance; scalars are immutable
        return node;
      }
    }
    // Default path
    return node.deepCopy();
  }
}
