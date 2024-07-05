package io.stargate.sgv2.jsonapi.service.operation.model.impl.filters;

import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.service.cql.builder.BuiltCondition;
import io.stargate.sgv2.jsonapi.service.cql.builder.Predicate;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.IndexUsage;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.JsonTerm;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocValueHasher;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import io.stargate.sgv2.jsonapi.util.JsonUtil;
import java.math.BigDecimal;
import java.util.*;
import java.util.function.Supplier;

/** Base for the DB filters / conditions that we want to use with queries */
public abstract class DBFilterBase implements Supplier<BuiltCondition> {

  /** Tracks the index column usage */
  public final IndexUsage indexUsage = new IndexUsage();

  /** Filter condition element path. */
  private final String path;

  protected DBFilterBase(String path) {
    this.path = path;
  }

  /**
   * Get JsonNode for the representing filter condition value.
   *
   * @param nodeFactory
   * @return
   */
  public abstract JsonNode asJson(JsonNodeFactory nodeFactory);

  /**
   * Returns filter condition element path.
   *
   * @return
   */
  // HACK aaron - referenced from FindOperation, Needs to be fixed
  public String getPath() {
    return path;
  }

  /**
   * Returns `true` if the filter condition should be added to upsert row
   *
   * @return
   */
  public abstract boolean canAddField();

  /**
   * Return JsonNode for a filter conditions value, used to set in new document created for upsert.
   *
   * @param nodeFactory
   * @param value
   * @return
   */
  protected static JsonNode getJsonNode(JsonNodeFactory nodeFactory, Object value) {
    if (value == null) return nodeFactory.nullNode();
    if (value instanceof DocumentId) {
      return ((DocumentId) value).asJson(nodeFactory);
    } else if (value instanceof String) {
      return nodeFactory.textNode((String) value);
    } else if (value instanceof BigDecimal) {
      return nodeFactory.numberNode((BigDecimal) value);
    } else if (value instanceof Boolean) {
      return nodeFactory.booleanNode((Boolean) value);
    } else if (value instanceof Date) {
      return JsonUtil.createEJSonDate(nodeFactory, (Date) value);
    } else if (value instanceof List) {
      List<Object> listValues = (List<Object>) value;
      final ArrayNode arrayNode = nodeFactory.arrayNode(listValues.size());
      listValues.forEach(listValue -> arrayNode.add(getJsonNode(nodeFactory, listValue)));
      return arrayNode;
    } else if (value instanceof Map) {
      Map<String, Object> mapValues = (Map<String, Object>) value;
      final ObjectNode objectNode = nodeFactory.objectNode();
      mapValues
          .entrySet()
          .forEach(kv -> objectNode.put(kv.getKey(), getJsonNode(nodeFactory, kv.getValue())));
      return objectNode;
    }
    return nodeFactory.nullNode();
  }

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
}
