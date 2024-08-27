package io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import java.math.BigDecimal;
import java.util.Optional;

/** Tests data and utility for testing the JSON Codecs see {@link JSONCodecRegistryTest} */
public class JSONCodecRegistryTestData {

  // A CQL data type we are not going to support soon
  public final DataType UNSUPPORTED_CQL_DATA_TYPE = DataTypes.COUNTER;

  public final CqlIdentifier TABLE_NAME =
      CqlIdentifier.fromInternal("table-" + System.currentTimeMillis());
  public final CqlIdentifier COLUMN_NAME =
      CqlIdentifier.fromInternal("column-" + System.currentTimeMillis());

  // Just a random string to use when needed
  public final String RANDOM_STRING = "random-" + System.currentTimeMillis();
  public final CqlIdentifier RANDOM_CQL_IDENTIFIER =
      CqlIdentifier.fromInternal("random-" + System.currentTimeMillis());

  public final BigDecimal OUT_OF_RANGE_FOR_TINY_INT = BigDecimal.valueOf(Short.MAX_VALUE + 1);

  /**
   * Returns a mocked {@link TableMetadata} that has a column of the specified type.
   *
   * <p>Mock configured with the names on this test data object
   *
   * @param dataType
   * @return
   */
  public TableMetadata mockTableMetadata(DataType dataType) {

    var tableMetadata = mock(TableMetadata.class);
    var columnMetadata = mock(ColumnMetadata.class);

    when(columnMetadata.getName()).thenReturn(COLUMN_NAME);
    when(columnMetadata.getType()).thenReturn(dataType);

    when(tableMetadata.getName()).thenReturn(TABLE_NAME);
    when(tableMetadata.getColumn(COLUMN_NAME)).thenReturn(Optional.of(columnMetadata));

    return tableMetadata;
  }
}
