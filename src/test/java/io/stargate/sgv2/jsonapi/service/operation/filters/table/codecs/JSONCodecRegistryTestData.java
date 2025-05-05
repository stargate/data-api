package io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.internal.core.type.UserDefinedTypeBuilder;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Optional;

/** Tests data and utility for testing the JSON Codecs see {@link JSONCodecRegistryTest} */
public class JSONCodecRegistryTestData {

  // A CQL data type we are not going to support soon
  // UDT is a datatype we don't support current, use following logic to mock it
  public final DataType UNSUPPORTED_CQL_DATA_TYPE =
      new UserDefinedTypeBuilder("ks", "udt")
          .withField("a", DataTypes.INT)
          .withField("b", DataTypes.TEXT)
          .withField("c", DataTypes.FLOAT)
          .build();

  public final CqlIdentifier TABLE_NAME =
      CqlIdentifier.fromInternal("table-" + System.currentTimeMillis());
  public final CqlIdentifier COLUMN_NAME =
      CqlIdentifier.fromInternal("column-" + System.currentTimeMillis());

  // Just a random string to use when needed
  public final String RANDOM_STRING = "random-" + System.currentTimeMillis();
  public final CqlIdentifier RANDOM_CQL_IDENTIFIER = CqlIdentifier.fromInternal(RANDOM_STRING);

  public final BigDecimal OUT_OF_RANGE_FOR_BIGINT =
      BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE);

  public final BigDecimal OVERFLOW_FOR_INT = BigDecimal.valueOf(Integer.MAX_VALUE + 1L);
  public final BigDecimal UNDERFLOW_FOR_INT = BigDecimal.valueOf(Integer.MIN_VALUE - 1L);
  public final BigDecimal OVERFLOW_FOR_SMALLINT = BigDecimal.valueOf(Short.MAX_VALUE + 1L);
  public final BigDecimal UNDERFLOW_FOR_SMALLINT = BigDecimal.valueOf(Short.MIN_VALUE - 1L);
  public final BigDecimal OVERFLOW_FOR_TINYINT = BigDecimal.valueOf(Byte.MAX_VALUE + 1L);
  public final BigDecimal UNDERFLOW_FOR_TINYINT = BigDecimal.valueOf(Byte.MIN_VALUE - 1L);

  public final BigDecimal NOT_EXACT_AS_INTEGER = new BigDecimal("1.25");

  public final String UUID_VALID_STR_LC = "123e4567-e89b-12d3-a456-426614174000";
  public final String UUID_VALID_STR_UC = "A34FACED-F158-4FDB-AA32-C4128D25A20F";

  // From https://en.wikipedia.org/wiki/Base64 -- 10-to-16 character sample case, with padding
  public final String BASE64_PADDED_DECODED_STR = "light work";
  public final byte[] BASE64_PADDED_DECODED_BYTES =
      BASE64_PADDED_DECODED_STR.getBytes(StandardCharsets.UTF_8);
  public final String BASE64_PADDED_ENCODED_STR = "bGlnaHQgd29yaw==";
  public final String BASE64_UNPADDED_ENCODED_STR = "bGlnaHQgd29yaw";

  public final String INET_ADDRESS_VALID_STRING = "192.168.1.3";
  public final String INET_ADDRESS_INVALID_STRING = "not-an-ip-address";

  public final String STRING_ASCII_SAFE = "ascii-safe-string";
  public final String STRING_WITH_2BYTE_UTF8_CHAR = "text-with-2-byte-utf8-\u00a2"; // cent symbol
  public final String STRING_WITH_3BYTE_UTF8_CHAR = "text-with-3-byte-utf8-\u20ac"; // euro symbol

  // Date/time test values
  public final String DATE_VALID_STR = "2024-09-24";
  public final String DATE_INVALID_STR = "not-a-date";

  public final String DURATION_VALID_STR_CASS = "1h30m";
  public final String DURATION_VALID_STR_ISO8601 = "PT1H30M";
  public final String DURATION_INVALID_STR = "not-a-duration";

  public final String TIME_VALID_STR = "12:34:56.789";
  public final String TIME_INVALID_STR = "not-a-time";

  public final String TIMESTAMP_VALID_STR = "2024-09-12T10:15:30Z";
  public final long TIMESTAMP_VALID_NUM;

  {
    TIMESTAMP_VALID_NUM = Instant.parse(TIMESTAMP_VALID_STR).toEpochMilli();
  }

  public final String TIMESTAMP_INVALID_STR = "not-a-timestamp";

  // 4 byte / 2 UCS-2 char Unicode Surrogate Pair characters (see
  // https://codepoints.net/U+10437?lang=en)
  public final String STRING_WITH_4BYTE_SURROGATE_CHAR =
      "text-with-4-byte-surrogate-\uD801\uDC37"; // Deseret

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
