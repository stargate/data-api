package io.stargate.sgv2.jsonapi.service.shredding.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchException;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import java.math.BigDecimal;
import java.util.Date;
import org.junit.jupiter.api.Test;

public class DocumentIdTest {
  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  public void testBasicEquality() {
    assertThat(DocumentId.fromBoolean(true)).isEqualTo(DocumentId.fromBoolean(true));
    assertThat(DocumentId.fromNull()).isEqualTo(DocumentId.fromNull());
    assertThat(DocumentId.fromNumber(new BigDecimal("10.25")))
        .isEqualTo(DocumentId.fromNumber(new BigDecimal("10.25")));
    assertThat(DocumentId.fromString("abc")).isEqualTo(DocumentId.fromString("abc"));

    Date dt = new Date(123456789L);
    assertThat(DocumentId.fromTimestamp(dt)).isEqualTo(DocumentId.fromTimestamp(dt));
  }

  @Test
  public void testAsJson() {
    assertThat(DocumentId.fromBoolean(true).asJson(mapper).toString()).isEqualTo("true");
    assertThat(DocumentId.fromNull().asJson(mapper).toString()).isEqualTo("null");
    assertThat(DocumentId.fromNumber(new BigDecimal("10.25")).asJson(mapper).toString())
        .isEqualTo("10.25");
    assertThat(DocumentId.fromString("abc").asJson(mapper).toString()).isEqualTo("\"abc\"");
    assertThat(DocumentId.fromTimestamp(new Date(123456789L)).asJson(mapper).toString())
        .isEqualTo("{\"$date\":123456789}");
  }

  @Test
  public void testFromJson() throws Exception {
    assertThat(DocumentId.fromJson(mapper.readTree("true")))
        .isEqualTo(DocumentId.fromBoolean(true));
    assertThat(DocumentId.fromJson(mapper.readTree("null"))).isEqualTo(DocumentId.fromNull());
    assertThat(DocumentId.fromJson(mapper.readTree("1.25")))
        .isEqualTo(DocumentId.fromNumber(new BigDecimal("1.25")));
    assertThat(DocumentId.fromJson(mapper.readTree("\"xyz\"")))
        .isEqualTo(DocumentId.fromString("xyz"));
    assertThat(DocumentId.fromJson(mapper.readTree("{\"$date\":123456789}")))
        .isEqualTo(DocumentId.fromTimestamp(123456789L));
  }

  @Test
  public void testFromDatabaseValid() {
    assertThat(DocumentId.fromDatabase(DocumentConstants.KeyTypeId.TYPE_ID_BOOLEAN, "true"))
        .isEqualTo(DocumentId.fromBoolean(true));
    assertThat(DocumentId.fromDatabase(DocumentConstants.KeyTypeId.TYPE_ID_NULL, ""))
        .isEqualTo(DocumentId.fromNull());
    assertThat(DocumentId.fromDatabase(DocumentConstants.KeyTypeId.TYPE_ID_NUMBER, "1.25"))
        .isEqualTo(DocumentId.fromNumber(new BigDecimal("1.25")));
    assertThat(DocumentId.fromDatabase(DocumentConstants.KeyTypeId.TYPE_ID_STRING, "xyz"))
        .isEqualTo(DocumentId.fromString("xyz"));
    assertThat(DocumentId.fromDatabase(DocumentConstants.KeyTypeId.TYPE_ID_DATE, "123456789"))
        .isEqualTo(DocumentId.fromTimestamp(123456789L));
  }

  @Test
  public void testFromDatabaseInvalid() throws Exception {
    Exception e =
        catchException(
            () -> {
              DocumentId.fromDatabase(99, "abc");
            });
    assertThat(e)
        .isNotNull()
        .isInstanceOf(JsonApiException.class)
        .hasFieldOrPropertyWithValue("errorCode", ErrorCode.SHRED_BAD_DOCID_TYPE)
        .hasMessageStartingWith(
            "Bad type for '_id' property: Document Id must be a JSON String(1), Number(2), Boolean(3), NULL(4) or Date(5) instead got 99");

    e =
        catchException(
            () -> {
              DocumentId.fromDatabase(DocumentConstants.KeyTypeId.TYPE_ID_BOOLEAN, "abc");
            });
    assertThat(e)
        .isNotNull()
        .isInstanceOf(JsonApiException.class)
        .hasFieldOrPropertyWithValue("errorCode", ErrorCode.SHRED_BAD_DOCID_TYPE)
        .hasMessageStartingWith(
            "Bad type for '_id' property: Document Id type Boolean stored as invalid String 'abc' (must be 'true' or 'false')");

    e =
        catchException(
            () -> {
              DocumentId.fromDatabase(DocumentConstants.KeyTypeId.TYPE_ID_NUMBER, "abc");
            });
    assertThat(e)
        .isNotNull()
        .isInstanceOf(JsonApiException.class)
        .hasFieldOrPropertyWithValue("errorCode", ErrorCode.SHRED_BAD_DOCID_TYPE)
        .hasMessageStartingWith(
            "Bad type for '_id' property: Document Id type Number stored as invalid String 'abc' (not a valid Number)");

    e =
        catchException(
            () -> {
              DocumentId.fromDatabase(DocumentConstants.KeyTypeId.TYPE_ID_DATE, "abc");
            });
    assertThat(e)
        .isNotNull()
        .isInstanceOf(JsonApiException.class)
        .hasFieldOrPropertyWithValue("errorCode", ErrorCode.SHRED_BAD_DOCID_TYPE)
        .hasMessageStartingWith(
            "Bad type for '_id' property: Document Id type Date stored as invalid String 'abc' (needs to be Number)");
  }
}
