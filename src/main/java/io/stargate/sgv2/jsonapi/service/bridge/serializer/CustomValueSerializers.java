package io.stargate.sgv2.jsonapi.service.bridge.serializer;

import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.jsonapi.service.shredding.JsonPath;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class CustomValueSerializers {

  public static Map<QueryOuterClass.Value, QueryOuterClass.Value> getIntegerMapValues(
      Map<JsonPath, Integer> from) {
    final Map<QueryOuterClass.Value, QueryOuterClass.Value> to = new HashMap<>(from.size());
    for (Map.Entry<JsonPath, Integer> entry : from.entrySet()) {
      to.put(Values.of(entry.getKey().toString()), Values.of(entry.getValue()));
    }
    return to;
  }

  public static Set<QueryOuterClass.Value> getSetValue(Set<JsonPath> from) {
    return from.stream().map(val -> Values.of(val.toString())).collect(Collectors.toSet());
  }

  public static Set<QueryOuterClass.Value> getStringSetValue(Set<String> from) {
    return from.stream().map(val -> Values.of(val)).collect(Collectors.toSet());
  }

  public static List<QueryOuterClass.Value> getListValue(List<JsonPath> from) {
    return from.stream().map(val -> Values.of(val.toString())).collect(Collectors.toList());
  }

  public static Map<QueryOuterClass.Value, QueryOuterClass.Value> getStringMapValues(
      Map<JsonPath, String> from) {
    final Map<QueryOuterClass.Value, QueryOuterClass.Value> to = new HashMap<>(from.size());
    for (Map.Entry<JsonPath, String> entry : from.entrySet()) {
      to.put(Values.of(entry.getKey().toString()), Values.of(entry.getValue()));
    }
    return to;
  }

  public static Map<QueryOuterClass.Value, QueryOuterClass.Value> getTimestampMapValues(
      Map<JsonPath, Date> from) {
    final Map<QueryOuterClass.Value, QueryOuterClass.Value> to = new HashMap<>(from.size());
    for (Map.Entry<JsonPath, Date> entry : from.entrySet()) {
      // We can pass Timestamp value as 64-bit long
      to.put(Values.of(entry.getKey().toString()), Values.of(entry.getValue().getTime()));
    }
    return to;
  }

  public static Map<QueryOuterClass.Value, QueryOuterClass.Value> getBooleanMapValues(
      Map<JsonPath, Boolean> from) {
    final Map<QueryOuterClass.Value, QueryOuterClass.Value> to = new HashMap<>(from.size());
    for (Map.Entry<JsonPath, Boolean> entry : from.entrySet()) {
      to.put(Values.of(entry.getKey().toString()), Values.of((byte) (entry.getValue() ? 1 : 0)));
    }
    return to;
  }

  public static Map<QueryOuterClass.Value, QueryOuterClass.Value> getDoubleMapValues(
      Map<JsonPath, BigDecimal> from) {
    final Map<QueryOuterClass.Value, QueryOuterClass.Value> to = new HashMap<>(from.size());
    for (Map.Entry<JsonPath, BigDecimal> entry : from.entrySet()) {
      to.put(Values.of(entry.getKey().toString()), Values.of(entry.getValue()));
    }
    return to;
  }

  public static List<QueryOuterClass.Value> getDocumentIdValue(DocumentId documentId) {
    // Temporary implementation until we convert it to Tuple in DB
    List<QueryOuterClass.Value> tupleValues =
        List.of(Values.of(documentId.typeId()), Values.of(documentId.asDBKey()));
    return tupleValues;
  }
}
