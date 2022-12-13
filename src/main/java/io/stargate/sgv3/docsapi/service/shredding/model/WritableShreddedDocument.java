package io.stargate.sgv3.docsapi.service.shredding.model;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv3.docsapi.service.shredding.JSONPath;
import io.stargate.sgv3.docsapi.service.shredding.ShredCallback;
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
    String id,
    Optional<UUID> txID,
    List<JSONPath> docFieldOrder,
    Set<JSONPath> existKeys,
    Map<JSONPath, String> subDocEquals,
    Map<JSONPath, Integer> arraySize,
    Map<JSONPath, String> arrayEquals,
    Map<JSONPath, String> arrayContents,
    Map<JSONPath, Boolean> queryBoolValues,
    Map<JSONPath, BigDecimal> queryNumberValues,
    Map<JSONPath, String> queryTextValues,
    Set<JSONPath> queryNullValues) {
  public static Builder builder(String id, Optional<UUID> txID) {
    return new Builder(id, txID);
  }

  /**
   * Due to large number of fields we need a Builder to construct instances. Builder also implements
   * {@link ShredCallback} for automated construction when traversing a Document.
   */
  public static class Builder implements ShredCallback {
    private final String id;
    private final Optional<UUID> txID;

    private final List<JSONPath> docFieldOrder;

    private final Set<JSONPath> existKeys;

    private Map<JSONPath, String> subDocEquals;

    private Map<JSONPath, Integer> arraySize;
    private Map<JSONPath, String> arrayEquals;
    private Map<JSONPath, String> arrayContents;

    private Map<JSONPath, Boolean> queryBoolValues;
    private Map<JSONPath, BigDecimal> queryNumberValues;
    private Map<JSONPath, String> queryTextValues;
    private Set<JSONPath> queryNullValues;

    public Builder(String id, Optional<UUID> txID) {
      this.id = id;
      this.txID = txID;

      // Many fields left as null to avoid allocation but some common ones that are
      // expected to be always needed are pre-allocated

      docFieldOrder = new ArrayList<>();
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
          existKeys,
          _nonNull(subDocEquals),
          _nonNull(arraySize),
          _nonNull(arrayEquals),
          _nonNull(arrayContents),
          _nonNull(queryBoolValues),
          _nonNull(queryNumberValues),
          _nonNull(queryTextValues),
          _nonNull(queryNullValues));
    }

    private <T> Map<JSONPath, T> _nonNull(Map<JSONPath, T> map) {
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
    public void shredObject(JSONPath.Builder pathBuilder, ObjectNode obj) {
      final JSONPath path = pathBuilder.build();
      addKey(path);

      // !!! TODO: subDocEquals
    }

    @Override
    public void shredArray(JSONPath.Builder pathBuilder, ArrayNode arr) {
      final JSONPath path = pathBuilder.build();
      addKey(path);
      if (arraySize == null) {
        arraySize = new HashMap<>();
      }
      arraySize.put(path, arr.size());

      // !!! TODO: arrayEquals, arrayContains
    }

    @Override
    public void shredText(JSONPath path, String text) {
      addKey(path);
      if (queryTextValues == null) {
        queryTextValues = new HashMap<>();
      }
      queryTextValues.put(path, text);
    }

    @Override
    public void shredNumber(JSONPath path, BigDecimal number) {
      addKey(path);
      if (queryNumberValues == null) {
        queryNumberValues = new HashMap<>();
      }
      queryNumberValues.put(path, number);
    }

    @Override
    public void shredBoolean(JSONPath path, boolean value) {
      addKey(path);
      if (queryBoolValues == null) {
        queryBoolValues = new HashMap<>();
      }
      queryBoolValues.put(path, value);
    }

    @Override
    public void shredNull(JSONPath path) {
      addKey(path);
      if (queryNullValues == null) {
        queryNullValues = new HashSet<>();
      }
      queryNullValues.add(path);
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
    private void addKey(JSONPath key) {
      docFieldOrder.add(key);
      existKeys.add(key);
    }
  }
}
