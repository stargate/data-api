package io.stargate.sgv2.jsonapi.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ValueNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
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
 */
public final class YamlMerger {

  private final ObjectMapper yamlMapper;

  public YamlMerger() {
    this.yamlMapper = new YAMLMapper();
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
      ObjectNode result = base.deepCopy();
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
    if (node.isObject() || node.isArray()) {
      return node.deepCopy();
    }
    if (node instanceof ValueNode) {
      return node;
    }
    // Default path
    return node.deepCopy();
  }
}
