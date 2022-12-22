package io.stargate.sgv3.docsapi.service.shredding.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv3.docsapi.service.shredding.JsonPath;
import io.stargate.sgv3.docsapi.service.shredding.ShredListener;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/** The fully shredded document, everything we need to write the document. */
public record WritableShreddedDocument(
    /**
     * Unique id of this document: may be {@code null} when inserting; if so, will be
     * auto-generated.
     */
    String id,
    /** Optional transaction id used for optimistic locking */
    Optional<UUID> txID,
    List<JsonPath> docFieldOrder,
    Map<JsonPath, String> docAtomicFields,
    /** !!! TODO: what purpose does this field serve? */
    Map<JsonPath, Integer> properties,
    Set<JsonPath> existKeys,
    Map<JsonPath, String> subDocEquals,
    Map<JsonPath, Integer> arraySize,
    Map<JsonPath, String> arrayEquals,
    Set<String> arrayContains,
    Map<JsonPath, Boolean> queryBoolValues,
    Map<JsonPath, BigDecimal> queryNumberValues,
    Map<JsonPath, String> queryTextValues,
    Set<JsonPath> queryNullValues) {
  public static Builder builder(DocValueHasher hasher, String id, Optional<UUID> txID) {
    return new Builder(hasher, id, txID);
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

    private final String id;
    private final Optional<UUID> txID;

    private final List<JsonPath> docFieldOrder;

    private final Map<JsonPath, String> docAtomicFields;

    private Map<JsonPath, Integer> properties;

    private final Set<JsonPath> existKeys;

    private Map<JsonPath, String> subDocEquals;

    private Map<JsonPath, Integer> arraySize;
    private Map<JsonPath, String> arrayEquals;
    private Set<String> arrayContains;

    private Map<JsonPath, Boolean> queryBoolValues;
    private Map<JsonPath, BigDecimal> queryNumberValues;
    private Map<JsonPath, String> queryTextValues;
    private Set<JsonPath> queryNullValues;

    public Builder(DocValueHasher hasher, String id, Optional<UUID> txID) {
      this.hasher = hasher;
      this.id = id;
      this.txID = txID;

      // Many fields left as null to avoid allocation but some common ones that are
      // expected to be always needed are pre-allocated

      docFieldOrder = new ArrayList<>();
      docAtomicFields = new HashMap<>();
      existKeys = new HashSet<>();
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
          docFieldOrder,
          docAtomicFields,
          _nonNull(properties),
          existKeys,
          _nonNull(subDocEquals),
          _nonNull(arraySize),
          _nonNull(arrayEquals),
          _nonNull(arrayContains),
          _nonNull(queryBoolValues),
          _nonNull(queryNumberValues),
          _nonNull(queryTextValues),
          _nonNull(queryNullValues));
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
    public void shredObject(JsonPath.Builder pathBuilder, ObjectNode obj) {
      final JsonPath path = pathBuilder.build();
      addKey(path);

      if (subDocEquals == null) {
        subDocEquals = new HashMap<>();
      }
      subDocEquals.put(path, hasher.hash(obj).hash());
    }

    @Override
    public void shredArray(JsonPath.Builder pathBuilder, ArrayNode arr) {
      final JsonPath path = pathBuilder.build();
      addKey(path);
      if (arraySize == null) { // all initialized the first time one needed
        arraySize = new HashMap<>();
        arrayEquals = new HashMap<>();
        arrayContains = new HashSet<>();
      }
      arraySize.put(path, arr.size());

      // 16-Dec-2022, tatu: "arrayEquals" is easy, but definition of "arrayContains"
      //    is quite unclear: older documents claim it's "JsonPath + content hash" (per
      //    element presumably). For now will use that, with space as separator; probably
      //    not what we want but...

      arrayEquals.put(path, hasher.hash(arr).hash());
      for (JsonNode element : arr) {
        // Assuming it's path to array, NOT index, since otherwise containment tricky.
        // Plus atomic values contains entries for in-array atomics anyway
        arrayContains.add(path + " " + hasher.hash(element).hash());
      }
    }

    @Override
    public void shredText(JsonPath path, String text) {
      addKey(path);
      if (queryTextValues == null) {
        queryTextValues = new HashMap<>();
      }
      queryTextValues.put(path, text);
      docAtomicFields.put(path, hasher.stringValue(text).typedFullValue());
    }

    @Override
    public void shredNumber(JsonPath path, BigDecimal number) {
      addKey(path);
      if (queryNumberValues == null) {
        queryNumberValues = new HashMap<>();
      }
      queryNumberValues.put(path, number);
      docAtomicFields.put(path, hasher.numberValue(number).typedFullValue());
    }

    @Override
    public void shredBoolean(JsonPath path, boolean value) {
      addKey(path);
      if (queryBoolValues == null) {
        queryBoolValues = new HashMap<>();
      }
      queryBoolValues.put(path, value);
      docAtomicFields.put(path, hasher.booleanValue(value).typedFullValue());
    }

    @Override
    public void shredNull(JsonPath path) {
      addKey(path);
      if (queryNullValues == null) {
        queryNullValues = new HashSet<>();
      }
      queryNullValues.add(path);
      docAtomicFields.put(path, hasher.nullValue().typedFullValue());
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
     * <p>Method will add path to both {@link #existKeys} and {@link #docFieldOrder}.
     *
     * @param key
     */
    private void addKey(JsonPath key) {
      docFieldOrder.add(key);
      existKeys.add(key);
    }
  }
}
