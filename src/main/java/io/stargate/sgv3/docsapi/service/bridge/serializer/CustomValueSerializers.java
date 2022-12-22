package io.stargate.sgv3.docsapi.service.bridge.serializer;

import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv3.docsapi.service.shredding.model.JsonPath;
import io.stargate.sgv3.docsapi.service.shredding.model.JsonType;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.javatuples.Pair;

public class CustomValueSerializers {
  public static Map<QueryOuterClass.Value, QueryOuterClass.Value> getRawDataValue(
      Map<JsonPath, Pair<JsonType, ByteBuffer>> from) {
    final Map<QueryOuterClass.Value, QueryOuterClass.Value> to = new HashMap<>(from.size());
    for (Map.Entry<JsonPath, Pair<JsonType, ByteBuffer>> entry : from.entrySet()) {
      QueryOuterClass.Value key = Values.of(entry.getKey().getPath());
      QueryOuterClass.Value valueTuple = getTupleValue(entry.getValue());
      to.put(key, valueTuple);
    }
    return to;
  }

  private static QueryOuterClass.Value getTupleValue(Pair<JsonType, ByteBuffer> value) {
    List<QueryOuterClass.Value> decoded = new ArrayList<>();
    decoded.add(Values.of(value.getValue0().value));
    decoded.add(Values.of(value.getValue1()));
    return Values.of(decoded);
  }

  public static Map<QueryOuterClass.Value, QueryOuterClass.Value> getIntegerMapValues(
      Map<JsonPath, Integer> from) {
    final Map<QueryOuterClass.Value, QueryOuterClass.Value> to = new HashMap<>(from.size());
    for (Map.Entry<JsonPath, Integer> entry : from.entrySet()) {
      to.put(Values.of(entry.getKey().getPath()), Values.of(entry.getValue()));
    }
    return to;
  }

  public static Set<QueryOuterClass.Value> getSetValue(Set<JsonPath> from) {
    return from.stream().map(val -> Values.of(val.getPath())).collect(Collectors.toSet());
  }

  public static List<QueryOuterClass.Value> getListValue(List<JsonPath> from) {
    return from.stream().map(val -> Values.of(val.getPath())).collect(Collectors.toList());
  }

  public static Map<QueryOuterClass.Value, QueryOuterClass.Value> getStringMapValues(
      Map<JsonPath, String> from) {
    final Map<QueryOuterClass.Value, QueryOuterClass.Value> to = new HashMap<>(from.size());
    for (Map.Entry<JsonPath, String> entry : from.entrySet()) {
      to.put(Values.of(entry.getKey().getPath()), Values.of(entry.getValue()));
    }
    return to;
  }

  public static Map<QueryOuterClass.Value, QueryOuterClass.Value> getBooleanMapValues(
      Map<JsonPath, Boolean> from) {
    final Map<QueryOuterClass.Value, QueryOuterClass.Value> to = new HashMap<>(from.size());
    for (Map.Entry<JsonPath, Boolean> entry : from.entrySet()) {
      to.put(Values.of(entry.getKey().getPath()), Values.of(entry.getValue()));
    }
    return to;
  }

  public static Map<QueryOuterClass.Value, QueryOuterClass.Value> getDoubleMapValues(
      Map<JsonPath, BigDecimal> from) {
    final Map<QueryOuterClass.Value, QueryOuterClass.Value> to = new HashMap<>(from.size());
    for (Map.Entry<JsonPath, BigDecimal> entry : from.entrySet()) {
      to.put(Values.of(entry.getKey().getPath()), Values.of(entry.getValue()));
    }
    return to;
  }
}
