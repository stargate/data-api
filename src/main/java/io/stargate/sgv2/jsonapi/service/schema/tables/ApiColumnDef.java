package io.stargate.sgv2.jsonapi.service.schema.tables;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.CQL_IDENTIFIER_COMPARATOR;
import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierToJsonKey;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import io.stargate.sgv2.jsonapi.api.model.command.table.SchemaDescSource;
import io.stargate.sgv2.jsonapi.api.model.command.table.SchemaDescribable;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnDesc;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlColumn;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlType;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedUserColumn;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedUserType;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorizeDefinition;
import io.stargate.sgv2.jsonapi.service.resolver.VectorizeConfigValidator;
import io.stargate.sgv2.jsonapi.service.schema.tables.factories.*;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
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
public class ApiColumnDef implements SchemaDescribable<ColumnDesc>, Recordable {

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
   * <p><b>NOTE:</b> Unlike calling {@link ApiDataType#getSchemaDescription(SchemaDescSource)}
   * directly calling on the column will know if the column is static, and is the preferred way when
   * getting the desc to return to the user.
   *
   * @return the user API description of the type for this column, including if the column is
   *     static.
   */
  @Override
  public ColumnDesc getSchemaDescription(SchemaDescSource schemaDescSource) {
    var typeDesc = type.getSchemaDescription(schemaDescSource);
    return isStatic ? new ColumnDesc.StaticColumnDesc(typeDesc) : typeDesc;
  }

  /**
   * Creates a new {@link ApiColumnDef} from user provided {@link ColumnDesc}
   *
   * <p>...
   */
  private static class ColumnDescFactory extends TypeFactory
      implements ColumnFactoryFromColumnDesc {

    @Override
    public ApiColumnDef create(
        TypeBindingPoint bindingPoint,
        String fieldName,
        ColumnDesc columnDesc,
        VectorizeConfigValidator validateVectorize)
        throws UnsupportedUserColumn {
      Objects.requireNonNull(columnDesc, "columnDesc is must not be null");
      checkBindingPoint(bindingPoint, "create()");

      // It's up to the type factory to check if it can be created at the binding point,
      // and throw UnsupportedUserType if it cannot.
      try {
        return new ApiColumnDef(
            userNameToIdentifier(fieldName, "fieldName"),
            DefaultTypeFactoryFromColumnDesc.INSTANCE.create(
                bindingPoint, columnDesc, validateVectorize));
      } catch (UnsupportedUserType e) {
        throw new UnsupportedUserColumn(fieldName, columnDesc, e);
      }
    }

    @Override
    public boolean isTypeBindable(
        TypeBindingPoint bindingPoint,
        String fieldName,
        ColumnDesc columnDesc,
        VectorizeConfigValidator validateVectorize) {
      checkBindingPoint(bindingPoint, "isSupported()");

      return DefaultTypeFactoryFromColumnDesc.INSTANCE.isTypeBindable(
          bindingPoint, columnDesc, validateVectorize);
    }

    @Override
    public ApiColumnDef createUnsupported(String fieldName, ColumnDesc columnDesc) {
      Objects.requireNonNull(columnDesc, "columnDesc is must not be null");

      return new ApiColumnDef(
          userNameToIdentifier(fieldName, "fieldName"),
          DefaultTypeFactoryFromColumnDesc.INSTANCE.createUnsupported(columnDesc));
    }

    private static void checkBindingPoint(TypeBindingPoint bindingPoint, String methodName) {
      if (bindingPoint != TypeBindingPoint.TABLE_COLUMN
          && bindingPoint != TypeBindingPoint.UDT_FIELD) {
        throw bindingPoint.unsupportedException("ApiColumnDef.ColumnDescFactory." + methodName);
      }
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
  public DataRecorder recordTo(DataRecorder dataRecorder) {
    return dataRecorder
        .append("name", name.asCql(true))
        .append("isStatic", isStatic)
        .append("type", type);
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
    public ApiColumnDef create(
        TypeBindingPoint bindingPoint, ColumnMetadata columnMetadata, VectorConfig vectorConfig)
        throws UnsupportedCqlColumn {
      Objects.requireNonNull(columnMetadata, "columnMetadata is must not be null");

      VectorizeDefinition vectorizeDefinition;
      if (bindingPoint == TypeBindingPoint.TABLE_COLUMN) {
        Objects.requireNonNull(vectorConfig, "vectorConfig is must not be null for table columns");
        vectorizeDefinition =
            vectorConfig.getVectorizeDefinition(columnMetadata.getName()).orElse(null);
      } else {
        // cannot have vectorize in a UDT field yet.
        vectorizeDefinition = null;
      }

      if (bindingPoint != TypeBindingPoint.TABLE_COLUMN
          && bindingPoint != TypeBindingPoint.UDT_FIELD) {
        throw new IllegalArgumentException(
            "CqlColumnFactory only supports binding point %s or %s, bindingPoint: %s"
                .formatted(
                    TypeBindingPoint.TABLE_COLUMN, TypeBindingPoint.UDT_FIELD, bindingPoint));
      }
      try {
        return new ApiColumnDef(
            columnMetadata.getName(),
            columnMetadata.isStatic(),
            DefaultTypeFactoryFromCql.INSTANCE.create(
                bindingPoint, columnMetadata.getType(), vectorizeDefinition));
      } catch (UnsupportedCqlType e) {
        throw new UnsupportedCqlColumn(columnMetadata.getName(), columnMetadata.getType(), e);
      }
    }

    @Override
    public ApiColumnDef createUnsupported(ColumnMetadata columnMetadata) {
      Objects.requireNonNull(columnMetadata, "columnMetadata is must not be null");
      return new ApiColumnDef(
          columnMetadata.getName(),
          DefaultTypeFactoryFromCql.INSTANCE.createUnsupported(columnMetadata.getType()));
    }

    @Override
    public ApiColumnDef createUnsupported(DataType type) {
      Objects.requireNonNull(type, "type is must not be null");
      return new ApiColumnDef(
          CqlIdentifier.fromCql("unsupportedField"),
          DefaultTypeFactoryFromCql.INSTANCE.createUnsupported(type));
    }
  }
}
