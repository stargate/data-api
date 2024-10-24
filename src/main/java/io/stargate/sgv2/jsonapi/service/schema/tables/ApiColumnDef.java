package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlColumn;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlType;
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
public class ApiColumnDef {

  private final CqlIdentifier name;
  private final ApiDataType type;

  public ApiColumnDef(CqlIdentifier name, ApiDataType type) {
    this.name = name;
    this.type = type;
  }

  /**
   * Call when converting from {@link ColumnMetadata} to {@link ApiColumnDef} where we know we can
   * write or read from the column.
   *
   * @param columnMetadata the column metadata to convert
   * @return a new instance of {@link ApiColumnDef}
   * @throws UnsupportedCqlColumn if the column metadata uses a type that is not supported by the
   *     API.
   */
  public static ApiColumnDef from(ColumnMetadata columnMetadata) throws UnsupportedCqlColumn {
    Objects.requireNonNull(columnMetadata, "columnMetadata is must not be null");
    return from(columnMetadata.getName(), columnMetadata.getType());
  }

  public static ApiColumnDef from(CqlIdentifier column, DataType dataType)
      throws UnsupportedCqlColumn {
    Objects.requireNonNull(column, "column is must not be null");
    Objects.requireNonNull(dataType, "dataType is must not be null");

    ApiDataType apiDataType;
    try {
      apiDataType = ApiDataTypeDefs.from(dataType);
    } catch (UnsupportedCqlType e) {
      throw new UnsupportedCqlColumn(column, dataType);
    }
    return new ApiColumnDef(column, apiDataType);
  }

  /**
   * Gets the {@link CqlIdentifier} for the column.
   *
   * <p>Use {@link io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil} to get the string from the
   * identifier.
   */
  public CqlIdentifier name() {
    return name;
  }

  public ApiDataType type() {
    return type;
  }
}
