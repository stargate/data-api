package io.stargate.sgv2.jsonapi.service.bridge.serializer;

import static org.assertj.core.api.Assertions.assertThat;

import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.jsonapi.service.cqldriver.serializer.CustomValueSerializers;
import io.stargate.sgv2.jsonapi.service.shredding.JsonPath;
import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

public class CustomValueSerializersTest {
  @Nested
  class CustomValues {

    @Test
    public void getIntegerMapValues() {
      Map<JsonPath, Integer> from = Map.of(JsonPath.from("field1"), 10);
      final Map<QueryOuterClass.Value, QueryOuterClass.Value> to =
          CustomValueSerializers.getIntegerMapValues(from);
      final Map.Entry<QueryOuterClass.Value, QueryOuterClass.Value> next =
          to.entrySet().iterator().next();
      assertThat(Values.string(next.getKey())).isEqualTo("field1");
      assertThat(Values.int_(next.getValue())).isEqualTo(10);
    }

    @Test
    public void getStringMapValues() {
      Map<JsonPath, String> from = Map.of(JsonPath.from("field1"), "data1");
      final Map<QueryOuterClass.Value, QueryOuterClass.Value> to =
          CustomValueSerializers.getStringMapValues(from);
      final Map.Entry<QueryOuterClass.Value, QueryOuterClass.Value> next =
          to.entrySet().iterator().next();
      assertThat(Values.string(next.getKey())).isEqualTo("field1");
      assertThat(Values.string(next.getValue())).isEqualTo("data1");
    }

    @Test
    public void getBooleanMapValues() {
      Map<JsonPath, Boolean> from = Map.of(JsonPath.from("field1"), true);
      final Map<QueryOuterClass.Value, QueryOuterClass.Value> to =
          CustomValueSerializers.getBooleanMapValues(from);
      final Map.Entry<QueryOuterClass.Value, QueryOuterClass.Value> next =
          to.entrySet().iterator().next();
      assertThat(Values.string(next.getKey())).isEqualTo("field1");
      assertThat(Values.tinyint(next.getValue())).isEqualTo((byte) 1);
    }

    @Test
    public void getDoubleMapValues() {
      Map<JsonPath, BigDecimal> from = Map.of(JsonPath.from("field1"), new BigDecimal(10));
      final Map<QueryOuterClass.Value, QueryOuterClass.Value> to =
          CustomValueSerializers.getDoubleMapValues(from);
      final Map.Entry<QueryOuterClass.Value, QueryOuterClass.Value> next =
          to.entrySet().iterator().next();
      assertThat(Values.string(next.getKey())).isEqualTo("field1");
      assertThat(Values.decimal(next.getValue())).isEqualTo(new BigDecimal(10));
    }

    @Test
    public void getSetValue() {
      Set<JsonPath> from = Set.of(JsonPath.from("field1"));
      final Set<QueryOuterClass.Value> to = CustomValueSerializers.getSetValue(from);
      final QueryOuterClass.Value next = to.iterator().next();
      assertThat(Values.string(next)).isEqualTo("field1");
    }

    @Test
    public void getSetValueForString() {
      Set<String> from = Set.of("field1");
      final Set<QueryOuterClass.Value> to = CustomValueSerializers.getStringSetValue(from);
      final QueryOuterClass.Value next = to.iterator().next();
      assertThat(Values.string(next)).isEqualTo("field1");
    }

    @Test
    public void getListValue() {
      List<JsonPath> from = List.of(JsonPath.from("field1"));
      final List<QueryOuterClass.Value> to = CustomValueSerializers.getListValue(from);
      final QueryOuterClass.Value next = to.get(0);
      assertThat(Values.string(next)).isEqualTo("field1");
    }

    @Test
    public void getTimestampMapValues() {
      Map<JsonPath, Date> from = Map.of(JsonPath.from("field1"), new Date(10L));
      final Map<QueryOuterClass.Value, QueryOuterClass.Value> to =
          CustomValueSerializers.getTimestampMapValues(from);
      final Map.Entry<QueryOuterClass.Value, QueryOuterClass.Value> next =
          to.entrySet().iterator().next();
      assertThat(Values.string(next.getKey())).isEqualTo("field1");
      assertThat(Values.bigint(next.getValue())).isEqualTo(10L);
    }

    @Test
    public void getVectorValue() {
      float[] from = new float[] {0.11f, 0.22f};
      final QueryOuterClass.Value to = CustomValueSerializers.getVectorValue(from);
      assertThat(to.getCollection().getElementsCount()).isEqualTo(2);
      assertThat(Values.float_(to.getCollection().getElements(0))).isEqualTo(0.11f);
      assertThat(Values.float_(to.getCollection().getElements(1))).isEqualTo(0.22f);
    }
  }
}
