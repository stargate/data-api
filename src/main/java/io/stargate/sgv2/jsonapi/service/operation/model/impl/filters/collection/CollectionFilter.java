package io.stargate.sgv2.jsonapi.service.operation.model.impl.filters.collection;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.SetOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.filters.DBFilterBase;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocValueHasher;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import io.stargate.sgv2.jsonapi.util.JsonUtil;
import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public abstract class CollectionFilter extends DBFilterBase {
  protected CollectionFilter(String path) {
    super(path);
  }

  /**
   * Optionally returns a <link>SetOperation</link> that should be used to update a new document
   * created during an updset operation with the filter condition.
   *
   * @return
   */
  public Optional<SetOperation> updateForNewDocument(JsonNodeFactory nodeFactory) {
    return jsonNodeForNewDocument(nodeFactory)
        .map(jsonNode -> SetOperation.constructSet(path, jsonNode, true));
  }

  /**
   * Subclasses must implement this to return the JsonNode that should be used to update a new
   * document created
   *
   * @param nodeFactory
   * @return
   */
  protected abstract Optional<JsonNode> jsonNodeForNewDocument(JsonNodeFactory nodeFactory);

  /**
   * @param hasher
   * @param path Path value is prefixed to the hash value of arrays.
   * @param arrayValue
   * @return
   */
  protected static String getHashValue(DocValueHasher hasher, String path, Object arrayValue) {
    return path + " " + getHash(hasher, arrayValue);
  }

  protected static String getHash(DocValueHasher hasher, Object arrayValue) {
    return hasher.getHash(arrayValue).hash();
  }

  /** Helper functions to Java objects into the appropriate JSON representation. */
  protected static JsonNode toJsonNode(JsonNodeFactory nodeFactory, DocumentId value) {
    return value.asJson(nodeFactory);
  }

  protected static JsonNode toJsonNode(JsonNodeFactory nodeFactory, String value) {
    return nodeFactory.textNode(value);
  }

  protected static JsonNode toJsonNode(JsonNodeFactory nodeFactory, BigDecimal value) {
    return nodeFactory.numberNode(value);
  }

  protected static JsonNode toJsonNode(JsonNodeFactory nodeFactory, Boolean value) {
    return nodeFactory.booleanNode(value);
  }

  protected static JsonNode toJsonNode(JsonNodeFactory nodeFactory, Date value) {
    return JsonUtil.createEJSonDate(nodeFactory, value);
  }

  /**
   * Special case for returning NULL node
   *
   * @param nodeFactory
   * @return
   */
  protected static JsonNode toJsonNode(JsonNodeFactory nodeFactory) {
    return nodeFactory.nullNode();
  }

  protected static JsonNode toJsonNode(JsonNodeFactory nodeFactory, Object value) {

    // TODO: this is a lot easier using switch pattern matching
    if (value instanceof DocumentId) {
      return toJsonNode(nodeFactory, (DocumentId) value);
    } else if (value instanceof String) {
      return toJsonNode(nodeFactory, (String) value);
    } else if (value instanceof BigDecimal) {
      return toJsonNode(nodeFactory, (BigDecimal) value);
    } else if (value instanceof Boolean) {
      return toJsonNode(nodeFactory, (Boolean) value);
    } else if (value instanceof Date) {
      return toJsonNode(nodeFactory, (Date) value);
    } else if (value == null) {
      return nodeFactory.nullNode();
    } else {
      throw new IllegalArgumentException("Unexpected object class: " + value.getClass().getName());
    }
  }

  protected static JsonNode toJsonNode(JsonNodeFactory nodeFactory, List<Object> listValues) {
    final ArrayNode arrayNode = nodeFactory.arrayNode(listValues.size());
    listValues.forEach(listValue -> arrayNode.add(toJsonNode(nodeFactory, listValue)));
    return arrayNode;
  }

  protected static JsonNode toJsonNode(JsonNodeFactory nodeFactory, Map<String, Object> mapValues) {
    final ObjectNode objectNode = nodeFactory.objectNode();
    mapValues.forEach((k, v) -> objectNode.put(k, toJsonNode(nodeFactory, v)));
    return objectNode;
  }
}
