package io.stargate.sgv2.jsonapi.service.schema.model;

import static io.stargate.bridge.proto.QueryOuterClass.ColumnSpec;
import static org.assertj.core.api.Assertions.assertThat;

import io.stargate.bridge.proto.QueryOuterClass.TypeSpec;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class CqlColumnMatcherTest {

  @Nested
  class BasicType {

    @Test
    public void happyPath() {
      ColumnSpec spec =
          ColumnSpec.newBuilder()
              .setName("column")
              .setType(TypeSpec.newBuilder().setBasic(TypeSpec.Basic.VARCHAR))
              .build();

      CqlColumnMatcher.BasicType matcher =
          new CqlColumnMatcher.BasicType("column", TypeSpec.Basic.VARCHAR);
      boolean result = matcher.test(spec);

      assertThat(result).isTrue();
    }

    @Test
    public void wrongType() {
      ColumnSpec spec =
          ColumnSpec.newBuilder()
              .setName("column")
              .setType(TypeSpec.newBuilder().setBasic(TypeSpec.Basic.INT))
              .build();

      CqlColumnMatcher.BasicType matcher =
          new CqlColumnMatcher.BasicType("column", TypeSpec.Basic.VARCHAR);
      boolean result = matcher.test(spec);

      assertThat(result).isFalse();
    }

    @Test
    public void notBasicType() {
      ColumnSpec spec =
          ColumnSpec.newBuilder()
              .setName("column")
              .setType(TypeSpec.newBuilder().setMap(TypeSpec.Map.newBuilder()))
              .build();

      CqlColumnMatcher.BasicType matcher =
          new CqlColumnMatcher.BasicType("column", TypeSpec.Basic.VARCHAR);
      boolean result = matcher.test(spec);

      assertThat(result).isFalse();
    }

    @Test
    public void wrongName() {
      ColumnSpec spec =
          ColumnSpec.newBuilder()
              .setName("column")
              .setType(TypeSpec.newBuilder().setBasic(TypeSpec.Basic.VARCHAR))
              .build();

      CqlColumnMatcher.BasicType matcher =
          new CqlColumnMatcher.BasicType("wrong", TypeSpec.Basic.VARCHAR);
      boolean result = matcher.test(spec);

      assertThat(result).isFalse();
    }
  }

  @Nested
  class Tuple {

    @Test
    public void happyPath() {
      TypeSpec.Builder type1 = TypeSpec.newBuilder().setBasic(TypeSpec.Basic.VARCHAR);
      TypeSpec.Builder type2 = TypeSpec.newBuilder().setBasic(TypeSpec.Basic.INT);
      ColumnSpec spec =
          ColumnSpec.newBuilder()
              .setName("column")
              .setType(
                  TypeSpec.newBuilder()
                      .setTuple(TypeSpec.Tuple.newBuilder().addElements(type1).addElements(type2)))
              .build();

      CqlColumnMatcher.Tuple matcher =
          new CqlColumnMatcher.Tuple("column", TypeSpec.Basic.VARCHAR, TypeSpec.Basic.INT);
      boolean result = matcher.test(spec);

      assertThat(result).isTrue();
    }

    @Test
    public void wrongOrder() {
      TypeSpec.Builder type1 = TypeSpec.newBuilder().setBasic(TypeSpec.Basic.VARCHAR);
      TypeSpec.Builder type2 = TypeSpec.newBuilder().setBasic(TypeSpec.Basic.INT);
      ColumnSpec spec =
          ColumnSpec.newBuilder()
              .setName("column")
              .setType(
                  TypeSpec.newBuilder()
                      .setTuple(TypeSpec.Tuple.newBuilder().addElements(type1).addElements(type2)))
              .build();

      CqlColumnMatcher.Tuple matcher =
          new CqlColumnMatcher.Tuple("column", TypeSpec.Basic.INT, TypeSpec.Basic.VARCHAR);
      boolean result = matcher.test(spec);

      assertThat(result).isFalse();
    }

    @Test
    public void wrongTuple() {
      TypeSpec.Builder type1 = TypeSpec.newBuilder().setBasic(TypeSpec.Basic.VARCHAR);
      TypeSpec.Builder type2 = TypeSpec.newBuilder().setBasic(TypeSpec.Basic.INT);
      ColumnSpec spec =
          ColumnSpec.newBuilder()
              .setName("column")
              .setType(
                  TypeSpec.newBuilder()
                      .setTuple(TypeSpec.Tuple.newBuilder().addElements(type1).addElements(type2)))
              .build();

      CqlColumnMatcher.Tuple matcher = new CqlColumnMatcher.Tuple("column", TypeSpec.Basic.INT);
      boolean result = matcher.test(spec);

      assertThat(result).isFalse();
    }

    @Test
    public void notTuple() {
      TypeSpec.Builder type1 = TypeSpec.newBuilder().setBasic(TypeSpec.Basic.VARCHAR);
      ColumnSpec spec = ColumnSpec.newBuilder().setName("column").setType(type1).build();

      CqlColumnMatcher.Tuple matcher = new CqlColumnMatcher.Tuple("column", TypeSpec.Basic.VARCHAR);
      boolean result = matcher.test(spec);

      assertThat(result).isFalse();
    }

    @Test
    public void wrongColumn() {
      TypeSpec.Builder type1 = TypeSpec.newBuilder().setBasic(TypeSpec.Basic.VARCHAR);
      TypeSpec.Builder type2 = TypeSpec.newBuilder().setBasic(TypeSpec.Basic.INT);
      ColumnSpec spec =
          ColumnSpec.newBuilder()
              .setName("column")
              .setType(
                  TypeSpec.newBuilder()
                      .setTuple(TypeSpec.Tuple.newBuilder().addElements(type1).addElements(type2)))
              .build();

      CqlColumnMatcher.Tuple matcher =
          new CqlColumnMatcher.Tuple("wrong", TypeSpec.Basic.VARCHAR, TypeSpec.Basic.INT);
      boolean result = matcher.test(spec);

      assertThat(result).isFalse();
    }
  }

  @Nested
  class Map {

    @Test
    public void happyPath() {
      TypeSpec.Builder key = TypeSpec.newBuilder().setBasic(TypeSpec.Basic.VARCHAR);
      TypeSpec.Builder value = TypeSpec.newBuilder().setBasic(TypeSpec.Basic.INT);
      ColumnSpec spec =
          ColumnSpec.newBuilder()
              .setName("column")
              .setType(
                  TypeSpec.newBuilder()
                      .setMap(TypeSpec.Map.newBuilder().setKey(key).setValue(value)))
              .build();

      CqlColumnMatcher.Map matcher =
          new CqlColumnMatcher.Map("column", TypeSpec.Basic.VARCHAR, TypeSpec.Basic.INT);
      boolean result = matcher.test(spec);

      assertThat(result).isTrue();
    }

    @Test
    public void wrongValue() {
      TypeSpec.Builder key = TypeSpec.newBuilder().setBasic(TypeSpec.Basic.VARCHAR);
      TypeSpec.Builder value = TypeSpec.newBuilder().setBasic(TypeSpec.Basic.INT);
      ColumnSpec spec =
          ColumnSpec.newBuilder()
              .setName("column")
              .setType(
                  TypeSpec.newBuilder()
                      .setMap(TypeSpec.Map.newBuilder().setKey(key).setValue(value)))
              .build();

      CqlColumnMatcher.Map matcher =
          new CqlColumnMatcher.Map("column", TypeSpec.Basic.VARCHAR, TypeSpec.Basic.FLOAT);
      boolean result = matcher.test(spec);

      assertThat(result).isFalse();
    }

    @Test
    public void wrongKey() {
      TypeSpec.Builder key = TypeSpec.newBuilder().setBasic(TypeSpec.Basic.VARCHAR);
      TypeSpec.Builder value = TypeSpec.newBuilder().setBasic(TypeSpec.Basic.INT);
      ColumnSpec spec =
          ColumnSpec.newBuilder()
              .setName("column")
              .setType(
                  TypeSpec.newBuilder()
                      .setMap(TypeSpec.Map.newBuilder().setKey(key).setValue(value)))
              .build();

      CqlColumnMatcher.Map matcher =
          new CqlColumnMatcher.Map("column", TypeSpec.Basic.INT, TypeSpec.Basic.INT);
      boolean result = matcher.test(spec);

      assertThat(result).isFalse();
    }

    @Test
    public void notMap() {
      TypeSpec.Builder type = TypeSpec.newBuilder().setBasic(TypeSpec.Basic.VARCHAR);
      ColumnSpec spec = ColumnSpec.newBuilder().setName("column").setType(type).build();

      CqlColumnMatcher.Map matcher =
          new CqlColumnMatcher.Map("column", TypeSpec.Basic.VARCHAR, TypeSpec.Basic.INT);
      boolean result = matcher.test(spec);

      assertThat(result).isFalse();
    }

    @Test
    public void wrongColumn() {
      TypeSpec.Builder key = TypeSpec.newBuilder().setBasic(TypeSpec.Basic.VARCHAR);
      TypeSpec.Builder value = TypeSpec.newBuilder().setBasic(TypeSpec.Basic.INT);
      ColumnSpec spec =
          ColumnSpec.newBuilder()
              .setName("column")
              .setType(
                  TypeSpec.newBuilder()
                      .setMap(TypeSpec.Map.newBuilder().setKey(key).setValue(value)))
              .build();

      CqlColumnMatcher.Map matcher =
          new CqlColumnMatcher.Map("wrong", TypeSpec.Basic.VARCHAR, TypeSpec.Basic.INT);
      boolean result = matcher.test(spec);

      assertThat(result).isFalse();
    }
  }

  @Nested
  class Set {

    @Test
    public void happyPath() {
      TypeSpec.Builder type = TypeSpec.newBuilder().setBasic(TypeSpec.Basic.VARCHAR);
      ColumnSpec spec =
          ColumnSpec.newBuilder()
              .setName("column")
              .setType(TypeSpec.newBuilder().setSet(TypeSpec.Set.newBuilder().setElement(type)))
              .build();

      CqlColumnMatcher.Set matcher = new CqlColumnMatcher.Set("column", TypeSpec.Basic.VARCHAR);
      boolean result = matcher.test(spec);

      assertThat(result).isTrue();
    }

    @Test
    public void wrongType() {
      TypeSpec.Builder type = TypeSpec.newBuilder().setBasic(TypeSpec.Basic.VARCHAR);
      ColumnSpec spec =
          ColumnSpec.newBuilder()
              .setName("column")
              .setType(TypeSpec.newBuilder().setSet(TypeSpec.Set.newBuilder().setElement(type)))
              .build();

      CqlColumnMatcher.Set matcher = new CqlColumnMatcher.Set("column", TypeSpec.Basic.INT);
      boolean result = matcher.test(spec);

      assertThat(result).isFalse();
    }

    @Test
    public void notSet() {
      TypeSpec.Builder type = TypeSpec.newBuilder().setBasic(TypeSpec.Basic.VARCHAR);
      ColumnSpec spec = ColumnSpec.newBuilder().setName("column").setType(type).build();

      CqlColumnMatcher.Set matcher = new CqlColumnMatcher.Set("column", TypeSpec.Basic.INT);
      boolean result = matcher.test(spec);

      assertThat(result).isFalse();
    }

    @Test
    public void wrongColumn() {
      TypeSpec.Builder type = TypeSpec.newBuilder().setBasic(TypeSpec.Basic.VARCHAR);
      ColumnSpec spec =
          ColumnSpec.newBuilder()
              .setName("column")
              .setType(TypeSpec.newBuilder().setSet(TypeSpec.Set.newBuilder().setElement(type)))
              .build();

      CqlColumnMatcher.Set matcher = new CqlColumnMatcher.Set("wrong", TypeSpec.Basic.VARCHAR);
      boolean result = matcher.test(spec);

      assertThat(result).isFalse();
    }
  }
}
