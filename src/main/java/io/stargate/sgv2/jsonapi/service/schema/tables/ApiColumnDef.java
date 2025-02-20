package io.stargate.sgv2.jsonapi.service.schema.tables;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.CQL_IDENTIFIER_COMPARATOR;
import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierToJsonKey;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnDesc;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlColumn;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlType;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedUserColumn;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedUserType;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorizeDefinition;
import io.stargate.sgv2.jsonapi.service.resolver.VectorizeConfigValidator;
import io.stargate.sgv2.jsonapi.util.PrettyPrintable;
import io.stargate.sgv2.jsonapi.util.PrettyToStringBuilder;

import java.util.Comparator;
import java.util.Objects;

/**
 * A column in a API Table.
 *
 * <p>Can be used in multiple circumstances:
 *
 * <ul>
 *   <li>When processing the DDL command to create a table. TODO: DDL commands are still at POC
 *       stage.
 *   <li>When building the response for an insert or read and we need to explain the schema of the
 *       result set. In this situation use {@link #from(ColumnMetadata)}.
 * </ul>
 *
 * When you have more than one use a {@link ApiColumnDefContainer} to hold them, it also contains
 * the serialization logic.
 */
public class ApiColumnDef  implements PrettyPrintable {

  public static final ColumnFactoryFromCql FROM_CQL_FACTORY = new CqlColumnFactory();
  public static ColumnFactoryFromColumnDesc FROM_COLUMN_DESC_FACTORY = new ColumnDescFactory();

  // Sort on the name of the column, using the {@link CQL_IDENTIFIER_COMPARATOR}
  public static final Comparator<ApiColumnDef> NAME_COMPARATOR =
      Comparator.comparing(ApiColumnDef::name, CQL_IDENTIFIER_COMPARATOR);

  private final CqlIdentifier name;
  private final ApiDataType type;
  private final boolean isStatic;

  public ApiColumnDef(CqlIdentifier name, ApiDataType type) {
    this(name, false, type);
  }

  public ApiColumnDef(CqlIdentifier name, boolean isStatic, ApiDataType type) {
    this.name = Objects.requireNonNull(name, "name is must not be null");
    this.isStatic = isStatic;
    this.type = Objects.requireNonNull(type, "type is must not be null");
  }

  /**
   * Gets the {@link CqlIdentifier} for the column.
   *
   * <p>NOTE: Use {@link #jsonKey()} to get the name to use in JSON.
   */
  public CqlIdentifier name() {
    return name;
  }

  /**
   * Returns the name of this column to use in JSON, or to lookup in a JSON object.
   *
   * @return Name of this column to use in JSON
   */
  public String jsonKey() {
    return cqlIdentifierToJsonKey(name);
  }

  public ApiDataType type() {
    return type;
  }

  /**
   * Gets the user API description of the type for this column.
   *
   * <p><b>NOTE:</b> Unlike calling {@link ApiDataType#columnDesc()} directly calling on the column
   * will know if the column is static, and is the preferred way when getting the desc to return to
   * the user.
   *
   * @return the user API description of the type for this column, including if the column is
   *     static.
   */
  public ColumnDesc columnDesc() {
    var typeDesc = type.columnDesc();
    return isStatic ? new ColumnDesc.StaticColumnDesc(typeDesc) : typeDesc;
  }

  private static class ColumnDescFactory extends FactoryFromDesc
      implements ColumnFactoryFromColumnDesc {

    @Override
    public ApiColumnDef create(
        String fieldName, ColumnDesc columnDesc, VectorizeConfigValidator validateVectorize)
        throws UnsupportedUserColumn {
      Objects.requireNonNull(columnDesc, "columnDesc is must not be null");

      try {
        return new ApiColumnDef(
            userNameToIdentifier(fieldName, "fieldName"),
            TypeFactoryFromColumnDesc.DEFAULT.create(columnDesc, validateVectorize));
      } catch (UnsupportedUserType e) {
        throw new UnsupportedUserColumn(fieldName, columnDesc, e);
      }
    }

    @Override
    public ApiColumnDef createUnsupported(String fieldName, ColumnDesc columnDesc) {
      Objects.requireNonNull(columnDesc, "columnDesc is must not be null");

      return new ApiColumnDef(
          userNameToIdentifier(fieldName, "fieldName"),
          TypeFactoryFromColumnDesc.DEFAULT.createUnsupported(columnDesc));
    }
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(name);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    ApiColumnDef other = (ApiColumnDef) obj;
    return Objects.equals(name, other.name);
  }


  @Override
  public String toString() {
    return toString(false);
  }

  @Override
  public PrettyToStringBuilder toString(PrettyToStringBuilder prettyToStringBuilder) {
    prettyToStringBuilder
        .append("name", name.asCql(true))
        .append("isStatic", isStatic)
        .append("type", type);
    return prettyToStringBuilder;
  }


  private static class CqlColumnFactory implements ColumnFactoryFromCql {
    /**
     * Call when converting from {@link ColumnMetadata} to {@link ApiColumnDef} where we know we can
     * write or read from the column.
     *
     * @param columnMetadata the column metadata to convert
     * @return a new instance of {@link ApiColumnDef}
     * @throws UnsupportedCqlColumn if the column metadata uses a type that is not supported by the
     *     API.
     */
    @Override
    public ApiColumnDef create(ColumnMetadata columnMetadata, VectorConfig vectorConfig)
        throws UnsupportedCqlColumn {
      Objects.requireNonNull(columnMetadata, "columnMetadata is must not be null");

      return create(
          columnMetadata.getName(),
          columnMetadata.isStatic(),
          columnMetadata.getType(),
          vectorConfig.getVectorizeDefinition(columnMetadata.getName()).orElse(null));
    }

    @Override
    public ApiColumnDef createUnsupported(ColumnMetadata columnMetadata) {
      Objects.requireNonNull(columnMetadata, "columnMetadata is must not be null");
      return new ApiColumnDef(
          columnMetadata.getName(),
          TypeFactoryFromCql.DEFAULT.createUnsupported(columnMetadata.getType()));
    }

    private static ApiColumnDef create(
        CqlIdentifier column, boolean isStatic, DataType dataType, VectorizeDefinition vectorizeDef)
        throws UnsupportedCqlColumn {

      Objects.requireNonNull(column, "column is must not be null");
      Objects.requireNonNull(dataType, "dataType is must not be null");

      try {
        return new ApiColumnDef(
            column, isStatic, TypeFactoryFromCql.DEFAULT.create(dataType, vectorizeDef));
      } catch (UnsupportedCqlType e) {
        throw new UnsupportedCqlColumn(column, dataType, e);
      }
    }
  }
}
