package io.stargate.sgv2.jsonapi.service.operation.filters.collection;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.util.Iterator;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Rewrites {@code _id} values inside OpenSearch Query DSL trees before sending to HCD.
 *
 * <p>The Data API {@code _id} field is excluded from OpenSearch {@code _source} body fields, and
 * exists in OpenSearch metadata as the HCD composite key format ({@code {_0=1, _1=<id>}}).
 *
 * <p>This rewriter walks the DSL JSON tree recursively and transforms:
 *
 * <ul>
 *   <li>{@code {"term": {"_id": "<value>"}}} &rarr; {@code {"term": {"_id": "{_0=1, _1=<value>}"}}}
 *   <li>{@code {"terms": {"_id": ["<v1>", "<v2>"]}}} &rarr; {@code {"terms": {"_id": ["{_0=1,
 *       _1=<v1>}", "{_0=1, _1=<v2>}"]}}}
 *   <li>{@code {"ids": {"values": ["<v1>", "<v2>"]}}} &rarr; {@code {"ids": {"values": ["{_0=1,
 *       _1=<v1>}", "{_0=1, _1=<v2>}"]}}}
 * </ul>
 *
 * <p>{@code search_after} array values are not rewritten.
 */
public final class OpenSearchDslIdRewriter {

  private static final Logger LOGGER = LoggerFactory.getLogger(OpenSearchDslIdRewriter.class);

  private OpenSearchDslIdRewriter() {}

  /**
   * Returns a rewritten copy of the given DSL node with {@code _id} values encoded. If the node is
   * not an ObjectNode or ArrayNode, or if no rewrites are needed, returns a deep copy (or modified
   * copy).
   */
  public static JsonNode rewrite(JsonNode dslNode) {
    if (dslNode == null) {
      LOGGER.info("[OpenSearchDslIdRewriter] rewrite() - input is null, returning null");
      return null;
    }
    LOGGER.info("[OpenSearchDslIdRewriter] rewrite() - input DSL: {}", dslNode);
    JsonNode cloned = dslNode.deepCopy();
    rewriteInPlace(cloned);
    LOGGER.info("[OpenSearchDslIdRewriter] rewrite() - output DSL (after _id rewrite): {}", cloned);
    return cloned;
  }

  private static void rewriteInPlace(JsonNode node) {
    if (node instanceof ObjectNode objectNode) {
      Iterator<Map.Entry<String, JsonNode>> fields = objectNode.properties().iterator();
      while (fields.hasNext()) {
        Map.Entry<String, JsonNode> entry = fields.next();
        String fieldName = entry.getKey();
        JsonNode child = entry.getValue();

        if ("search_after".equals(fieldName)) {
          // Do NOT rewrite search_after values — they are returned from OpenSearch already encoded
          continue;
        }

        if ("term".equals(fieldName) && child instanceof ObjectNode termObj) {
          rewriteTerm(termObj);
        } else if ("terms".equals(fieldName) && child instanceof ObjectNode termsObj) {
          rewriteTerms(termsObj);
        } else if ("ids".equals(fieldName) && child instanceof ObjectNode idsObj) {
          rewriteIds(idsObj);
        } else {
          rewriteInPlace(child);
        }
      }
    } else if (node instanceof ArrayNode arrayNode) {
      for (JsonNode elem : arrayNode) {
        rewriteInPlace(elem);
      }
    }
  }

  private static void rewriteTerm(ObjectNode termObj) {
    if (termObj.has("_id")) {
      JsonNode idNode = termObj.get("_id");
      if (idNode.isTextual()) {
        termObj.put("_id", OpenSearchIdEncoder.encode(idNode.asText()));
      } else if (idNode instanceof ObjectNode idObj
          && idObj.has("value")
          && idObj.get("value").isTextual()) {
        idObj.put("value", OpenSearchIdEncoder.encode(idObj.get("value").asText()));
      }
    }
  }

  private static void rewriteTerms(ObjectNode termsObj) {
    if (termsObj.has("_id")) {
      JsonNode idNode = termsObj.get("_id");
      if (idNode instanceof ArrayNode arrayNode) {
        for (int i = 0; i < arrayNode.size(); i++) {
          JsonNode elem = arrayNode.get(i);
          if (elem.isTextual()) {
            arrayNode.set(i, TextNode.valueOf(OpenSearchIdEncoder.encode(elem.asText())));
          }
        }
      }
    }
  }

  private static void rewriteIds(ObjectNode idsObj) {
    if (idsObj.has("values")) {
      JsonNode valuesNode = idsObj.get("values");
      if (valuesNode instanceof ArrayNode arrayNode) {
        for (int i = 0; i < arrayNode.size(); i++) {
          JsonNode elem = arrayNode.get(i);
          if (elem.isTextual()) {
            arrayNode.set(i, TextNode.valueOf(OpenSearchIdEncoder.encode(elem.asText())));
          }
        }
      }
    }
  }
}
