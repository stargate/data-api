package io.stargate.sgv2.jsonapi.service.schema.model;

import io.stargate.bridge.proto.QueryOuterClass;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Predicate;

/** Interface for matching a CQL column name and type. */
public interface CqlColumnMatcher extends Predicate<QueryOuterClass.ColumnSpec> {

  /** @return Column name for the matcher. */
  String name();

  /** @return If column type is matching. */
  boolean typeMatches(QueryOuterClass.ColumnSpec columnSpec);

  default boolean test(QueryOuterClass.ColumnSpec columnSpec) {
    return Objects.equals(columnSpec.getName(), name()) && typeMatches(columnSpec);
  }

  /**
   * Implementation that supports basic column types.
   *
   * @param name column name
   * @param type basic type
   */
  record BasicType(String name, QueryOuterClass.TypeSpec.Basic type) implements CqlColumnMatcher {

    @Override
    public boolean typeMatches(QueryOuterClass.ColumnSpec columnSpec) {
      return Objects.equals(columnSpec.getType().getBasic(), type);
    }
  }

  /**
   * Implementation that supports map column type. Only basic values are supported as key/value.
   *
   * @param name column name
   * @param keyType map key type
   * @param valueType map value type
   */
  record Map(
      String name, QueryOuterClass.TypeSpec.Basic keyType, QueryOuterClass.TypeSpec.Basic valueType)
      implements CqlColumnMatcher {

    @Override
    public boolean typeMatches(QueryOuterClass.ColumnSpec columnSpec) {
      QueryOuterClass.TypeSpec type = columnSpec.getType();
      if (!type.hasMap()) {
        return false;
      }

      QueryOuterClass.TypeSpec.Map map = type.getMap();
      return Objects.equals(map.getKey().getBasic(), keyType)
          && Objects.equals(map.getValue().getBasic(), valueType);
    }
  }

  /**
   * Implementation that supports tuple column type. Only basic values are supported as elements.
   *
   * @param name column name
   * @param elements types of elements in the tuple
   */
  record Tuple(String name, QueryOuterClass.TypeSpec.Basic... elements)
      implements CqlColumnMatcher {

    @Override
    public boolean typeMatches(QueryOuterClass.ColumnSpec columnSpec) {
      QueryOuterClass.TypeSpec type = columnSpec.getType();
      if (!type.hasTuple()) {
        return false;
      }

      QueryOuterClass.TypeSpec.Tuple map = type.getTuple();
      java.util.List<QueryOuterClass.TypeSpec.Basic> elementTypes =
          map.getElementsList().stream().map(QueryOuterClass.TypeSpec::getBasic).toList();
      return Objects.equals(elementTypes, Arrays.asList(elements));
    }
  }

  /**
   * Implementation that supports set column type. Only basic values are supported as elements.
   *
   * @param name column name
   * @param elementType type of elements in the set
   */
  record Set(String name, QueryOuterClass.TypeSpec.Basic elementType) implements CqlColumnMatcher {

    @Override
    public boolean typeMatches(QueryOuterClass.ColumnSpec columnSpec) {
      QueryOuterClass.TypeSpec type = columnSpec.getType();
      if (!type.hasSet()) {
        return false;
      }

      QueryOuterClass.TypeSpec.Set set = type.getSet();
      return Objects.equals(set.getElement().getBasic(), elementType);
    }
  }
}
