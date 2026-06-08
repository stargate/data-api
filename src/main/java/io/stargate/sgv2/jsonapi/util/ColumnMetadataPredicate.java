package io.stargate.sgv2.jsonapi.util;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.type.*;
import com.datastax.oss.driver.internal.core.type.DefaultVectorType;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiColumnDef;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmt;
import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.CQL_IDENTIFIER_COMPARATOR;

/**
 * Interface for matching a {@link ColumnMetadata} against a specified column name and type.
 *
 * <p>See implementations for concrete usage.
 */
public interface ColumnMetadataPredicate extends Predicate<ColumnMetadata> {

  Comparator<ColumnMetadataPredicate> IDENTIFIER_COMPARATOR =
          Comparator.comparing(ColumnMetadataPredicate::name, CQL_IDENTIFIER_COMPARATOR);

  /**
   * @return The name the column must have.
   */
  CqlIdentifier name();

  /**
   * @return Return <code>true</code> if and only if the column type matches the expected types,
   *     including nested types of CQL collections like a list or map.
   */
  boolean typeMatches(ColumnMetadata columnMetadata);

  /**
   * Tests if the supplied column metadata matches the name and type of this matcher.
   *
   * @param columnMetadata existing column metadata to test.
   * @throws NullPointerException if columnMetadata is null.
   * @return true if the column metadata matches the name and type of this matcher.
   */
  @Override
  default boolean test(ColumnMetadata columnMetadata) {
    Objects.requireNonNull(columnMetadata, "columnMetadata must not be null");

    return Objects.equals(columnMetadata.getName(), name()) && typeMatches(columnMetadata);
  }


  static Predicate<ColumnMetadata> anyOf(List<ColumnMetadataPredicate> predicates) {
    return predicates.stream()
            .map(p -> (Predicate<ColumnMetadata>) p)
            .reduce(Predicate::or)
            .orElse(t -> false);
  }

  /**
   * Implementation that supports basic column types.
   *
   * @param name expected column name
   * @param type expected CQL type
   */
  class BasicType implements ColumnMetadataPredicate {

    private final CqlIdentifier name;
    private final DataType type;

    public BasicType(CqlIdentifier name, DataType type) {
      this.name = Objects.requireNonNull(name, "name must not be null");
      this.type = Objects.requireNonNull(type, "type must not be null");
    }

    @Override
    public CqlIdentifier name() {
      return name;
    }

    @Override
    public boolean typeMatches(ColumnMetadata columnMetadata) {
      return Objects.equals(type, columnMetadata.getType());
    }

    @Override
    public String toString() {
      return String.format("%s(%s)", errFmt(name), errFmt(type));
    }
  }

  /** Implementation that supports map column type. and value of the map */
  class Map extends BasicType {

    public Map(CqlIdentifier name, DataType keyType, DataType valueType) {
      this(name, keyType, valueType, false);
    }

    public Map(CqlIdentifier name, DataType keyType, DataType valueType, boolean frozen) {
      super(name, DataTypes.mapOf(keyType, valueType, frozen));
    }
  }

  /** Implementation that supports tuple column type. */
  class Tuple extends BasicType {

    public Tuple(CqlIdentifier name, DataType... elements) {
      super(name, DataTypes.tupleOf(elements));
    }
  }

  /** Implementation that supports set column type. */
  class Set extends BasicType {

    public Set(CqlIdentifier name, DataType elementType) {
      super(name, DataTypes.setOf(elementType));
    }
  }

  /**
   * NOTE: this matches the column as a vector type, and the subtype of the vector, it DOES NOT
   * match the Vector Length. The {@link DefaultVectorType#equals} will match vector length, we dont
   * want that for here. Add it later if needed.
   *
   * <p>Also, this is not only checks if the column type is an instance of {@link VectorType}
   * interface, to account for our {@link
   * io.stargate.sgv2.jsonapi.service.cqldriver.override.ExtendedVectorType}
   */
  class Vector implements ColumnMetadataPredicate {

    private final CqlIdentifier name;
    private final DataType elementType;

    public Vector(CqlIdentifier name, DataType elementType) {
      this.name = Objects.requireNonNull(name, "name must not be null");
      this.elementType = Objects.requireNonNull(elementType, "subtype must not be null");
    }

    @Override
    public CqlIdentifier name() {
      return name;
    }

    @Override
    public boolean typeMatches(ColumnMetadata columnMetadata) {
      DataType type = columnMetadata.getType();
      // NOTE: checking is instance for reasons above
      if (!(type instanceof VectorType vector)) {
        return false;
      }

      return Objects.equals(vector.getElementType(), elementType);
    }

    @Override
    public String toString() {
      return String.format("%s(vector<%s>)", errFmt(name), errFmt(elementType));
    }
  }
}
