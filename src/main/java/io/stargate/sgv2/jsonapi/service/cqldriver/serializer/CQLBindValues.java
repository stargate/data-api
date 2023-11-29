package io.stargate.sgv2.jsonapi.service.cqldriver.serializer;

import com.datastax.oss.driver.api.core.data.CqlVector;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.TupleType;
import io.stargate.sgv2.jsonapi.service.shredding.JsonPath;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class CQLBindValues {

  public static Map<String, Integer> getIntegerMapValues(Map<JsonPath, Integer> from) {
    final Map<String, Integer> to = new HashMap<>(from.size());
    for (Map.Entry<JsonPath, Integer> entry : from.entrySet()) {
      to.put(entry.getKey().toString(), entry.getValue());
    }
    return to;
  }

  public static Set<String> getSetValue(Set<JsonPath> from) {
    return from.stream().map(val -> val.toString()).collect(Collectors.toSet());
  }

  public static Set<String> getStringSetValue(Set<?> from) {
    return from.stream().map(val -> val.toString()).collect(Collectors.toSet());
  }

  public static List<String> getListValue(List<?> from) {
    return from.stream().map(val -> val.toString()).collect(Collectors.toList());
  }

  public static Map<String, String> getStringMapValues(Map<JsonPath, String> from) {
    final Map<String, String> to = new HashMap<>(from.size());
    for (Map.Entry<JsonPath, String> entry : from.entrySet()) {
      to.put(entry.getKey().toString(), entry.getValue());
    }
    return to;
  }

  public static Map<String, Byte> getBooleanMapValues(Map<JsonPath, Boolean> from) {
    final Map<String, Byte> to = new HashMap<>(from.size());
    for (Map.Entry<JsonPath, Boolean> entry : from.entrySet()) {
      to.put(entry.getKey().toString(), (byte) (entry.getValue() ? 1 : 0));
    }
    return to;
  }

  public static Map<String, BigDecimal> getDoubleMapValues(Map<JsonPath, BigDecimal> from) {
    final Map<String, BigDecimal> to = new HashMap<>(from.size());
    for (Map.Entry<JsonPath, BigDecimal> entry : from.entrySet()) {
      to.put(entry.getKey().toString(), entry.getValue());
    }
    return to;
  }

  public static Map<String, Instant> getTimestampMapValues(Map<JsonPath, Date> from) {
    final Map<String, Instant> to = new HashMap<>(from.size());
    for (Map.Entry<JsonPath, Date> entry : from.entrySet()) {
      to.put(entry.getKey().toString(), Instant.ofEpochMilli(entry.getValue().getTime()));
    }
    return to;
  }

  private static TupleType tupleType = DataTypes.tupleOf(DataTypes.TINYINT, DataTypes.TEXT);

  public static TupleValue getDocumentIdValue(DocumentId documentId) {
    // Temporary implementation until we convert it to Tuple in DB
    final TupleValue tupleValue =
        tupleType.newValue((byte) documentId.typeId(), documentId.asDBKey());
    return tupleValue;
  }

  public static CqlVector<Float> getVectorValue(float[] vectors) {
    if (vectors == null || vectors.length == 0) {
      return null;
    }

    List<Float> vectorValues = new ArrayList<>(vectors.length);
    for (float vectorValue : vectors) vectorValues.add(vectorValue);
    return CqlVector.newInstance(vectorValues);
  }
}
