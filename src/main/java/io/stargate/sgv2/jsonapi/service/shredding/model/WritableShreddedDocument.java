package io.stargate.sgv2.jsonapi.service.shredding.model;

import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.operation.model.DocRowIdentifer;
import io.stargate.sgv2.jsonapi.service.operation.model.WritableDocRow;
import io.stargate.sgv2.jsonapi.service.shredding.JsonPath;
import io.stargate.sgv2.jsonapi.service.shredding.ShredListener;
import io.stargate.sgv2.jsonapi.util.JsonUtil;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * The fully shredded document, everything we need to write the document. Override of hashcode and
 * equals methods is needed to handle queryVectorValues array field.
 */
public record WritableShreddedDocument(
    /**
     * Unique id of this document: may be {@code null} when inserting; if so, will be
     * auto-generated.
     */
    DocumentId id,
    /** Optional transaction id used for optimistic locking */
    UUID txID,
    String docJson,
    JsonNode docJsonNode,
    Set<JsonPath> existKeys,
    Map<JsonPath, Integer> arraySize,
    Set<String> arrayContains,
    Map<JsonPath, Boolean> queryBoolValues,
    Map<JsonPath, BigDecimal> queryNumberValues,
    Map<JsonPath, String> queryTextValues,
    Map<JsonPath, Date> queryTimestampValues,
    Set<JsonPath> queryNullValues,
    float[] queryVectorValues,
    UUID nextTxID)
    implements WritableDocRow {

  @Override
  public boolean equals(Object o) {
    // TODO: why do we have a customer equals and has for a record ?
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    WritableShreddedDocument that = (WritableShreddedDocument) o;
    return Objects.equals(id, that.id)
        && Objects.equals(txID, that.txID)
        && Objects.equals(docJson, that.docJson)
        && Objects.equals(docJsonNode, that.docJsonNode)
        && Objects.equals(existKeys, that.existKeys)
        && Objects.equals(arraySize, that.arraySize)
        && Objects.equals(arrayContains, that.arrayContains)
        && Objects.equals(queryBoolValues, that.queryBoolValues)
        && Objects.equals(queryNumberValues, that.queryNumberValues)
        && Objects.equals(queryTextValues, that.queryTextValues)
        && Objects.equals(queryTimestampValues, that.queryTimestampValues)
        && Objects.equals(queryNullValues, that.queryNullValues)
        && Arrays.equals(queryVectorValues, that.queryVectorValues);
  }

  @Override
  public int hashCode() {
    int result =
        Objects.hash(
            id,
            txID,
            docJson,
            docJsonNode,
            existKeys,
            arraySize,
            arrayContains,
            queryBoolValues,
            queryNumberValues,
            queryTextValues,
            queryTimestampValues,
            queryNullValues);
    result = 31 * result + Arrays.hashCode(queryVectorValues);
    return result;
  }

  @Override
  public DocRowIdentifer docRowID() {
    return id();
  }

  public static Builder builder(DocumentId id, UUID txID, String docJson, JsonNode docJsonNode) {
    return new Builder(id, txID, docJson, docJsonNode);
  }

  /**
   * Due to large number of fields we need a Builder to construct instances. Builder also implements
   * {@link ShredListener} for automated construction when traversing a Document.
   */
  public static class Builder implements ShredListener {
    /**
     * We use helper object for efficient calculation of content value hashes we need for shredded
     * results.
     */
    private final DocValueHasher hasher;

    private final DocumentId id;
    private final UUID txID;

    private final String docJson;
    private final JsonNode docJsonNode;

    private final Set<JsonPath> existKeys;

    private Map<JsonPath, Integer> arraySize;
    private Set<String> arrayContains;

    private Map<JsonPath, Boolean> queryBoolValues;
    private Map<JsonPath, BigDecimal> queryNumberValues;
    private Map<JsonPath, String> queryTextValues;
    private Map<JsonPath, Date> queryTimestampValues;
    private Set<JsonPath> queryNullValues;

    private float[] queryVectorValues;

    public Builder(DocumentId id, UUID txID, String docJson, JsonNode docJsonNode) {
      hasher = new DocValueHasher();
      this.id = id;
      this.txID = txID;
      this.docJson = Objects.requireNonNull(docJson);
      this.docJsonNode = docJsonNode;
      existKeys = new LinkedHashSet<>(); // retain document order
    }

    /**
     * Method called once all shred information has been collected, to produce immutable instance.
     *
     * @return WritableShreddedDocument built from collected information
     */
    public WritableShreddedDocument build() {
      return new WritableShreddedDocument(
          id,
          txID,
          docJson,
          docJsonNode,
          existKeys,
          _nonNull(arraySize),
          _nonNull(arrayContains),
          _nonNull(queryBoolValues),
          _nonNull(queryNumberValues),
          _nonNull(queryTextValues),
          _nonNull(queryTimestampValues),
          _nonNull(queryNullValues),
          queryVectorValues,
          Uuids.timeBased());
    }

    private <T> Map<JsonPath, T> _nonNull(Map<JsonPath, T> map) {
      return (map == null) ? Collections.emptyMap() : map;
    }

    private <T> Set<T> _nonNull(Set<T> set) {
      return (set == null) ? Collections.emptySet() : set;
    }

    /*
    /**********************************************************************
    /* ShredCallback for populating builder state via traversal callbacks
    /**********************************************************************
     */

    @Override
    public boolean shredObject(JsonPath path, ObjectNode obj) {
      // Either Sub-doc or EJSON-encoded Date/timestamp or other extension value:
      if (JsonUtil.looksLikeEJsonValue(obj)) {
        JsonExtensionType type = JsonUtil.findJsonExtensionType(obj);
        if (type != null) {
          switch (type) {
            case EJSON_DATE:
              Date dtValue = JsonUtil.extractEJsonDate(obj, path);
              if (dtValue != null) {
                shredTimestamp(path, dtValue);
                return false; // we are done
              }
              break;
            default:
              Object extValue = JsonUtil.tryExtractExtendedValue(type, obj);
              if (extValue != null) {
                shredExtendedType(path, type, extValue);
                return false;
              }
              break;
          }
          throw ErrorCode.SHRED_BAD_EJSON_VALUE.toApiException(
              "invalid value (%s) for extended JSON type '%s' (path '%s')",
              obj.iterator().next(), obj.fieldNames().next(), path);
        }
        // Otherwise it's either unsupported of malformed EJSON-encoded value; fail
        throw ErrorCode.SHRED_BAD_EJSON_VALUE.toApiException(
            "unrecognized extended JSON type '%s' (path '%s')", obj.fieldNames().next(), path);
      }

      addKey(path);

      // User text column
      if (queryTextValues == null) {
        queryTextValues = new HashMap<>();
      }
      queryTextValues.put(path, hasher.hash(obj).hash());

      return true; // proceed to shred individual entries too
    }

    private void shredTimestamp(JsonPath path, Date dtValue) {
      addKey(path);
      if (queryTimestampValues == null) {
        queryTimestampValues = new HashMap<>();
      }
      queryTimestampValues.put(path, dtValue);
      addArrayContains(path, hasher.timestampValue(dtValue).hash());
    }

    @Override
    public void shredArray(JsonPath path, ArrayNode arr) {
      addKey(path);
      if (arraySize == null) { // all initialized the first time one needed
        arraySize = new HashMap<>();
      }
      if (queryTextValues == null) {
        queryTextValues = new HashMap<>();
      }
      // arrayEquals (full array contents hash) and arraySize are simple to generate
      arraySize.put(path, arr.size());

      DocValueHash arrHasher = hasher.hash(arr);
      queryTextValues.put(path, arrHasher.hash());

      // But arrayContains is bit different: must use path to array (not elements);
      // and for atomics need to avoid generating twice
      for (JsonNode element : arr) {
        addArrayContains(path, hasher.hash(element));
      }
    }

    @Override
    public void shredText(JsonPath path, String text) {
      addKey(path);
      if (queryTextValues == null) {
        queryTextValues = new HashMap<>();
      }
      queryTextValues.put(path, text);
      // Only add if NOT directly in array (because if so, containing array has already added)
      // if (!path.isArrayElement()) {
      addArrayContains(path, hasher.stringValue(text).hash());
      // }
    }

    @Override
    public void shredNumber(JsonPath path, BigDecimal number) {
      addKey(path);
      if (queryNumberValues == null) {
        queryNumberValues = new HashMap<>();
      }
      queryNumberValues.put(path, number);
      // Only add if NOT directly in array (because if so, containing array has already added)
      // if (!path.isArrayElement()) {
      addArrayContains(path, hasher.numberValue(number).hash());
      // }
    }

    @Override
    public void shredBoolean(JsonPath path, boolean value) {
      addKey(path);
      if (queryBoolValues == null) {
        queryBoolValues = new HashMap<>();
      }
      queryBoolValues.put(path, value);
      // Only add if NOT directly in array (because if so, containing array has already added)
      // if (!path.isArrayElement()) {
      addArrayContains(path, hasher.booleanValue(value).hash());
      // }
    }

    @Override
    public void shredNull(JsonPath path) {
      addKey(path);
      if (queryNullValues == null) {
        queryNullValues = new HashSet<>();
      }
      queryNullValues.add(path);
      // Only add if NOT directly in array (because if so, containing array has already added)
      // if (!path.isArrayElement()) {
      addArrayContains(path, hasher.nullValue().hash());
      // }
    }

    @Override
    public void shredVector(JsonPath path, ArrayNode vector) {
      // vector data is added only to queryVectorValues and exists keys index
      addKey(path);
      float[] arrayVals = new float[vector.size()];
      for (int i = 0; i < vector.size(); i++) {
        JsonNode element = vector.get(i);
        if (!element.isNumber()) {
          throw new JsonApiException(
              ErrorCode.SHRED_BAD_VECTOR_VALUE, ErrorCode.SHRED_BAD_VECTOR_VALUE.getMessage());
        }
        arrayVals[i] = element.floatValue();
      }
      queryVectorValues = arrayVals;
    }

    public void shredExtendedType(JsonPath path, JsonExtensionType type, Object extensionValue) {
      shredText(path, String.valueOf(extensionValue));
    }

    /**
     * Index vectorize field for $exists support
     *
     * @param path
     * @param vector
     */
    @Override
    public void shredVectorize(JsonPath path) {
      // vector data is added only to queryVectorValues and exists keys index
      addKey(path);
    }

    /*
    /**********************************************************************
    /* Internal methods for actual information accumulation
    /**********************************************************************
     */

    /**
     * Method for indicating there is an addressable path in document being shredded: it must be one
     * of:
     *
     * <ol>
     *   <li>Document root which is atomic value (instead of Object or Array)
     *   <li>Object property (at any nesting level)
     *   <li>Array element (at any nesting level)
     * </ol>
     *
     * <p>Method will add path to {@link #existKeys}.
     *
     * @param key
     */
    private void addKey(JsonPath key) {
      existKeys.add(key);
    }

    /**
     * Helper method used to add a single entry in "arrayCantains" Set: this is either an actual
     * Array element OR single atomic value. Both cases are needed to support Mongo's
     * "atomic-or-array-element" filtering.
     *
     * @param path Path to either Array that contains Element (but not index!) OR to an Atomic value
     *     not directly enclosed in an array.
     * @param elementHash Hash of value matching the path
     */
    private void addArrayContains(JsonPath path, DocValueHash elementHash) {
      // Do not add doc id field (we do not support Structured doc ids)
      if (!path.isDocumentId()) {
        if (arrayContains == null) {
          arrayContains = new HashSet<>();
        }
        arrayContains.add(path + " " + elementHash.hash());
      }
    }
  }
}
