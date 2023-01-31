package io.stargate.sgv3.docsapi.service.shredding.model;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

public class DocumentIdTest {
  @Test
  public void testBasicEquality() {
    assertThat(DocumentId.fromBoolean(true)).isEqualTo(DocumentId.fromBoolean(true));
    assertThat(DocumentId.fromNull()).isEqualTo(DocumentId.fromNull());
    assertThat(DocumentId.fromNumber(new BigDecimal("10.25")))
        .isEqualTo(DocumentId.fromNumber(new BigDecimal("10.25")));
    assertThat(DocumentId.fromString("abc")).isEqualTo(DocumentId.fromString("abc"));
  }

  @Test
  public void testAsJson() {
    final ObjectMapper mapper = new ObjectMapper();
    assertThat(DocumentId.fromBoolean(true).asJson(mapper).toString()).isEqualTo("true");
    assertThat(DocumentId.fromNull().asJson(mapper).toString()).isEqualTo("null");
    assertThat(DocumentId.fromNumber(new BigDecimal("10.25")).asJson(mapper).toString())
        .isEqualTo("10.25");
    assertThat(DocumentId.fromString("abc").asJson(mapper).toString()).isEqualTo("\"abc\"");
  }
}
