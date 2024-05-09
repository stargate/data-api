package io.stargate.sgv2.jsonapi.service.schema.model;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.type.*;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Predicate;

/** Interface for matching a CQL column name and type. */
public interface CqlColumnMatcher extends Predicate<ColumnMetadata> {

  /**
   * @return Column name for the matcher.
   */
  CqlIdentifier name();

  /**
   * @return If column type is matching.
   */
  boolean typeMatches(ColumnMetadata columnSpec);

  default boolean test(ColumnMetadata columnSpec) {
    return Objects.equals(columnSpec.getName(), name()) && typeMatches(columnSpec);
  }

  /**
   * Implementation that supports basic column types.
   *
   * @param name column name
   * @param type basic type
   */
  record BasicType(CqlIdentifier name, DataType type) implements CqlColumnMatcher {

    @Override
    public boolean typeMatches(ColumnMetadata columnSpec) {
      return Objects.equals(columnSpec.getType(), type);
    }
  }

  /**
   * Implementation that supports map column type. Only basic values are supported as key/value.
   *
   * @param name column name
   * @param keyType map key type
   * @param valueType map value type
   */
  record Map(CqlIdentifier name, DataType keyType, DataType valueType) implements CqlColumnMatcher {

    @Override
    public boolean typeMatches(ColumnMetadata columnSpec) {
      DataType type = columnSpec.getType();
      if (!(type instanceof MapType)) {
        return false;
      }

      MapType map = (MapType) type;
      return Objects.equals(map.getKeyType(), keyType)
          && Objects.equals(map.getValueType(), valueType);
    }
  }

  /**
   * Implementation that supports tuple column type. Only basic values are supported as elements.
   *
   * @param name column name
   * @param elements types of elements in the tuple
   */
  record Tuple(CqlIdentifier name, DataType... elements) implements CqlColumnMatcher {

    @Override
    public boolean typeMatches(ColumnMetadata columnSpec) {
      DataType type = columnSpec.getType();
      if (!(type instanceof TupleType)) {
        return false;
      }

      TupleType tuple = (TupleType) type;
      java.util.List<DataType> elementTypes = tuple.getComponentTypes();
      return Objects.equals(elementTypes, Arrays.asList(elements));
    }
  }

  /**
   * Implementation that supports set column type. Only basic values are supported as elements.
   *
   * @param name column name
   * @param elementType type of elements in the set
   */
  record Set(CqlIdentifier name, DataType elementType) implements CqlColumnMatcher {

    @Override
    public boolean typeMatches(ColumnMetadata columnSpec) {
      DataType type = columnSpec.getType();
      if (!(type instanceof SetType)) {
        return false;
      }

      SetType set = (SetType) type;
      return Objects.equals(set.getElementType(), elementType);
    }
  }

  record Vector(CqlIdentifier name, DataType subtype) implements CqlColumnMatcher {
    @Override
    public boolean typeMatches(ColumnMetadata columnSpec) {
      DataType type = columnSpec.getType();
      if (!(type instanceof VectorType)) {
        return false;
      }

      VectorType vector = (VectorType) type;
      return Objects.equals(vector.getElementType(), subtype);
    }
  }
}
