package io.stargate.sgv2.jsonapi.util;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmt;
import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.CQL_IDENTIFIER_COMPARATOR;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.type.*;
import com.datastax.oss.driver.internal.core.type.DefaultVectorType;
import java.util.Comparator;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * A predciate for matching {@link ColumnMetadata} against a specified column name and type.
 *
 * <p>See implementations for concrete usage. Is in general "util" package because while used a lot
 * with Collections may also be useful for tables.
 *
 * <p>NOTE: This was previously called <code>CqlColumnMatcher</code>
 */
public class ColumnMetadataPredicate implements Predicate<ColumnMetadata> {

  // Compare predicates by the identifier name the column must have.
  public static final Comparator<ColumnMetadataPredicate> IDENTIFIER_COMPARATOR =
      Comparator.comparing(ColumnMetadataPredicate::name, CQL_IDENTIFIER_COMPARATOR);

  protected final CqlIdentifier name;
  protected final DataType type;

  protected ColumnMetadataPredicate(CqlIdentifier name, DataType type, boolean checkNulls) {
    // subclasses may want to pass null for some values, e.g. the Vector predciate
    if (checkNulls) {
      Objects.requireNonNull(name, "name must not be null");
      Objects.requireNonNull(type, "type must not be null");
    }
    this.name = name;
    this.type = type;
  }

  /**
   * @return The name the column must have.
   */
  public CqlIdentifier name() {
    return name;
  }

  /**
   * Implementors can override for more complex type matching.
   *
   * @return Return <code>true</code> if and only if the column type matches the expected types,
   *     including nested types of CQL collections like a list or map.
   */
  protected boolean typeMatches(ColumnMetadata columnMetadata) {
    return Objects.equals(type, columnMetadata.getType());
  }

  /**
   * Tests if the supplied column metadata matches the name and type of this matcher.
   *
   * @param columnMetadata existing column metadata to test.
   * @throws NullPointerException if columnMetadata is null.
   * @return true if the column metadata matches the name and type of this matcher.
   */
  @Override
  public boolean test(ColumnMetadata columnMetadata) {
    Objects.requireNonNull(columnMetadata, "columnMetadata must not be null");

    return Objects.equals(columnMetadata.getName(), name()) && typeMatches(columnMetadata);
  }

  /** Returns the name and type we match against, e.g. <code>tx_id(uuid)</code> */
  @Override
  public String toString() {
    // No null check, errFmt will handle Nulls and print "null" for any null value.
    return String.format("%s(%s)", errFmt(name), errFmt(type));
  }

  /** Basic type matcher, for a name and a type. */
  public static class Basic extends ColumnMetadataPredicate {

    public Basic(CqlIdentifier name, DataType type) {
      super(name, type, true);
    }
  }

  /** Matches a map type, including the key and value types. */
  public static class Map extends Basic {

    public Map(CqlIdentifier name, DataType keyType, DataType valueType) {
      this(name, keyType, valueType, false);
    }

    public Map(CqlIdentifier name, DataType keyType, DataType valueType, boolean frozen) {
      super(name, DataTypes.mapOf(keyType, valueType, frozen));
      // mapOf will do null checks
    }
  }

  /** Matches a tuple type, including the elements of the tuple */
  public static class Tuple extends Basic {

    public Tuple(CqlIdentifier name, DataType... elements) {
      super(name, DataTypes.tupleOf(elements));

      // tupleOf checks the elements array is not null, does not check the elements in it.
      for (int i = 0; i < elements.length; i++) {
        Objects.requireNonNull(elements[i], "elements[" + i + "] must not be null");
      }
    }
  }

  /** Matches a set type, including the element type. */
  public static class Set extends Basic {

    public Set(CqlIdentifier name, DataType elementType) {
      super(name, DataTypes.setOf(elementType));
      // setOf does the null check
    }
  }

  /**
   * Matches a vector type, including the element type.
   *
   * <p>NOTE: this matches the column as a vector type, and the subtype of the vector, it DOES NOT
   * match the Vector Length. The {@link DefaultVectorType#equals} will match vector length, we dont
   * want that in some situations because we do not have the specifics of how long it should be.
   * Will add another predicate when that is needed.
   *
   * <p>Also, this is not only checks if the column type is an instance of {@link VectorType}
   * interface, to account for our {@link
   * io.stargate.sgv2.jsonapi.service.cqldriver.override.ExtendedVectorType}
   */
  public static class Vector extends ColumnMetadataPredicate {

    private final DataType elementType;

    /** Create a predicate to match a vector with a float element type. */
    public Vector(CqlIdentifier name) {
      // let's be honest, they are all floats.
      this(name, DataTypes.FLOAT);
    }

    public Vector(CqlIdentifier name, DataType elementType) {
      super(name, null, false);
      Objects.requireNonNull(name, "name must not be null");
      this.elementType = Objects.requireNonNull(elementType, "elementType must not be null");
    }

    @Override
    protected boolean typeMatches(ColumnMetadata columnMetadata) {
      // NOTE: checking is instance for reasons above
      if (!(columnMetadata.getType() instanceof VectorType vector)) {
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
