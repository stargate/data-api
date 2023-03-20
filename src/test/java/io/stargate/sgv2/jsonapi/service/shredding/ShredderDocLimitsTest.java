package io.stargate.sgv2.jsonapi.service.shredding;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.config.DocumentLimitsConfig;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import javax.inject.Inject;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class ShredderDocLimitsTest {
  @Inject ObjectMapper objectMapper;

  @Inject Shredder shredder;

  @Inject DocumentLimitsConfig docLimits;

  // Tests for Document size/depth violations
  @Nested
  class ValidationDocSizeViolations {
    @Test
    public void catchTooBigDoc() {
      // Let's construct document above 1 meg limit (but otherwise legal), with
      // 100 x 10k String values, divided in 10 sub documents of 10 properties
      final ObjectNode bigDoc = objectMapper.createObjectNode();
      bigDoc.put("_id", 123);

      for (int ix1 = 0; ix1 < 10; ++ix1) {
        ObjectNode mainProp = bigDoc.putObject("prop" + ix1);
        for (int ix2 = 0; ix2 < 10; ++ix2) {
          mainProp.put("sub" + ix2, RandomStringUtils.randomAscii(10_000));
        }
      }

      Exception e = catchException(() -> shredder.shred(bigDoc));
      assertThat(e)
          .isNotNull()
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.SHRED_DOC_LIMIT_VIOLATION)
          .hasMessageStartingWith(ErrorCode.SHRED_DOC_LIMIT_VIOLATION.getMessage())
          .hasMessageEndingWith("exceeds maximum allowed (" + docLimits.maxDocSize() + ")");
    }

    @Test
    public void catchTooDeepDoc() {
      // Let's construct document with 20 levels of nesting (above our configs)
      final ObjectNode deepDoc = objectMapper.createObjectNode();
      deepDoc.put("_id", 123);

      ObjectNode obNode = deepDoc;
      for (int i = 0; i < 10; ++i) {
        ArrayNode array = obNode.putArray("a");
        obNode = array.addObject();
      }

      Exception e = catchException(() -> shredder.shred(deepDoc));
      assertThat(e)
          .isNotNull()
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.SHRED_DOC_LIMIT_VIOLATION)
          .hasMessageStartingWith(ErrorCode.SHRED_DOC_LIMIT_VIOLATION.getMessage())
          .hasMessageEndingWith(
              "document depth exceeds maximum allowed (" + docLimits.maxDocDepth() + ")");
    }
  }

  // Tests for count of entities (array elements, doc properties) violations
  @Nested
  class ValidationDocCountViolations {
    @Test
    public void catchTooManyObjectProps() {
      final ObjectNode doc = objectMapper.createObjectNode();
      doc.put("_id", 123);
      ObjectNode obNode = doc.putObject("subdoc");
      // Let's add 200 props in a subdoc (max allowed: 64)
      for (int i = 0; i < 200; ++i) {
        obNode.put("prop" + i, i);
      }

      Exception e = catchException(() -> shredder.shred(doc));
      assertThat(e)
          .isNotNull()
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.SHRED_DOC_LIMIT_VIOLATION)
          .hasMessageStartingWith(ErrorCode.SHRED_DOC_LIMIT_VIOLATION.getMessage())
          .hasMessageEndingWith(
              " number of properties an Object has (200) exceeds maximum allowed ("
                  + docLimits.maxObjectProperties()
                  + ")");
    }

    @Test
    public void catchTooManyArrayElements() {
      final ObjectNode doc = objectMapper.createObjectNode();
      doc.put("_id", 123);
      ArrayNode arr = doc.putArray("arr");
      // Let's add 200 elements (max allowed: 100)
      for (int i = 0; i < 200; ++i) {
        arr.add(i);
      }

      Exception e = catchException(() -> shredder.shred(doc));
      assertThat(e)
          .isNotNull()
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.SHRED_DOC_LIMIT_VIOLATION)
          .hasMessageStartingWith(ErrorCode.SHRED_DOC_LIMIT_VIOLATION.getMessage())
          .hasMessageEndingWith(
              " number of elements an Array has (200) exceeds maximum allowed ("
                  + docLimits.maxArrayLength()
                  + ")");
    }
  }

  // Tests for size of atomic value / name length violations
  @Nested
  class ValidationDocAtomicSizeViolations {
    @Test
    public void catchTooLongNames() {
      final ObjectNode doc = objectMapper.createObjectNode();
      doc.put("_id", 123);
      ObjectNode ob = doc.putObject("subdoc");
      final String propName =
          "property-with-way-too-long-name-123456789-123456789-123456789-123456789";
      ob.put(propName, true);

      Exception e = catchException(() -> shredder.shred(doc));
      assertThat(e)
          .isNotNull()
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.SHRED_DOC_LIMIT_VIOLATION)
          .hasMessageStartingWith(ErrorCode.SHRED_DOC_LIMIT_VIOLATION.getMessage())
          .hasMessageEndingWith(
              " Property name length ("
                  + propName.length()
                  + ") exceeds maximum allowed ("
                  + docLimits.maxNameLength()
                  + ")");
    }

    @Test
    public void catchTooLongStringValues() {
      final ObjectNode doc = objectMapper.createObjectNode();
      doc.put("_id", 123);
      ArrayNode arr = doc.putArray("arr");
      // Let's add 50_000 char one (exceeds max of 16_000)
      String str = RandomStringUtils.randomAscii(50_000);
      arr.add(str);

      Exception e = catchException(() -> shredder.shred(doc));
      assertThat(e)
          .isNotNull()
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.SHRED_DOC_LIMIT_VIOLATION)
          .hasMessageStartingWith(ErrorCode.SHRED_DOC_LIMIT_VIOLATION.getMessage())
          .hasMessageEndingWith(
              " String value length (50000) exceeds maximum allowed ("
                  + docLimits.maxStringLength()
                  + ")");
    }
  }
}
