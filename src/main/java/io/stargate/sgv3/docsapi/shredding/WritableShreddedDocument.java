package io.stargate.sgv3.docsapi.shredding;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.javatuples.Pair;

/** The fully shredded document, everything we need to write the document. */
public class WritableShreddedDocument extends ReadableShreddedDocument {

  public Map<JSONPath, Integer> properties;
  public Set<JSONPath> existKeys;
  public Map<JSONPath, String> subDocEquals;
  public Map<JSONPath, Integer> arraySize;
  public Map<JSONPath, String> arrayEquals;
  public Map<JSONPath, String> arrayContains;
  public Map<JSONPath, Boolean> queryBoolValues;
  public Map<JSONPath, BigDecimal> queryNumberValues;
  public Map<JSONPath, String> queryTextValues;
  public Set<JSONPath> queryNullValues;

  public WritableShreddedDocument(
      String key,
      Optional<UUID> tx_id,
      List<JSONPath> docFieldOrder,
      Map<JSONPath, Pair<JsonType, ByteBuffer>> docAtomicFields,
      Map<JSONPath, Integer> properties,
      Set<JSONPath> existKeys,
      Map<JSONPath, String> subDocEquals,
      Map<JSONPath, Integer> arraySize,
      Map<JSONPath, String> arrayEquals,
      Map<JSONPath, String> arrayContains,
      Map<JSONPath, Boolean> queryBoolValues,
      Map<JSONPath, BigDecimal> queryNumberValues,
      Map<JSONPath, String> queryTextValues,
      Set<JSONPath> queryNullValues) {
    super(key, tx_id, docFieldOrder, docAtomicFields);
    this.properties = properties;
    this.existKeys = existKeys;
    this.subDocEquals = subDocEquals;
    this.arraySize = arraySize;
    this.arrayEquals = arrayEquals;
    this.arrayContains = arrayContains;
    this.queryBoolValues = queryBoolValues;
    this.queryNumberValues = queryNumberValues;
    this.queryTextValues = queryTextValues;
    this.queryNullValues = queryNullValues;
  }
}
