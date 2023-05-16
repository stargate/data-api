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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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
    public void allowBigButNotTooBigDoc() {
      // Given we fail at 1 meg, let's try 800k (8 x 10 x 10k)
      final ObjectNode bigDoc = createBigDoc(8, 10);
      assertThat(shredder.shred(bigDoc)).isNotNull();
    }

    @Test
    public void catchTooBigDoc() {
      // Let's construct document above 1 meg limit (but otherwise legal), with
      // 100 x 10k String values, divided in 10 sub documents of 10 properties
      final ObjectNode bigDoc = createBigDoc(10, 10);

      Exception e = catchException(() -> shredder.shred(bigDoc));
      assertThat(e)
          .isNotNull()
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.SHRED_DOC_LIMIT_VIOLATION)
          .hasMessageStartingWith(ErrorCode.SHRED_DOC_LIMIT_VIOLATION.getMessage())
          .hasMessageEndingWith("exceeds maximum allowed (" + docLimits.maxSize() + ")");
    }

    private ObjectNode createBigDoc(int mainProps, int subProps) {
      final ObjectNode bigDoc = objectMapper.createObjectNode();
      bigDoc.put("_id", 123);

      for (int ix1 = 0; ix1 < mainProps; ++ix1) {
        ObjectNode mainProp = bigDoc.putObject("prop" + ix1);
        for (int ix2 = 0; ix2 < subProps; ++ix2) {
          mainProp.put("sub" + ix2, RandomStringUtils.randomAscii(10_000));
        }
      }
      return bigDoc;
    }

    @Test
    public void allowDeepButNotTooDeepDoc() {
      // We allow 7 levels of nesting so...
      final ObjectNode deepDoc = objectMapper.createObjectNode();
      deepDoc.put("_id", 123);
      ObjectNode ob = deepDoc;
      for (int i = 0; i < 7; ++i) {
        ob = ob.putObject("sub");
      }

      assertThat(shredder.shred(deepDoc)).isNotNull();
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
              "document depth exceeds maximum allowed (" + docLimits.maxDepth() + ")");
    }
  }

  // Tests for count of entities (array elements, doc properties) violations
  @Nested
  class ValidationDocCountViolations {
    @Test
    public void allowDocWithManyObjectProps() {
      // Max allowed is 64, so add 50
      final ObjectNode doc = docWithNProps(50);
      assertThat(shredder.shred(doc)).isNotNull();
    }

    @Test
    public void catchTooManyObjectProps() {
      // Max allowed 64, so fail with 100
      final ObjectNode doc = docWithNProps(100);

      Exception e = catchException(() -> shredder.shred(doc));
      assertThat(e)
          .isNotNull()
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.SHRED_DOC_LIMIT_VIOLATION)
          .hasMessageStartingWith(ErrorCode.SHRED_DOC_LIMIT_VIOLATION.getMessage())
          .hasMessageEndingWith(
              " number of properties an Object has (100) exceeds maximum allowed ("
                  + docLimits.maxObjectProperties()
                  + ")");
    }

    private ObjectNode docWithNProps(int count) {
      final ObjectNode doc = objectMapper.createObjectNode();
      doc.put("_id", 123);
      ObjectNode obNode = doc.putObject("subdoc");
      for (int i = 0; i < count; ++i) {
        obNode.put("prop" + i, i);
      }
      return doc;
    }

    @Test
    public void allowDocWithManyArrayElements() {
      // Max allowed 100, add 90
      final ObjectNode doc = docWithNArrayElems(90);
      assertThat(shredder.shred(doc)).isNotNull();
    }

    @Test
    public void catchTooManyArrayElements() {
      // Let's add 120 elements (max allowed: 100)
      final ObjectNode doc = docWithNArrayElems(120);
      Exception e = catchException(() -> shredder.shred(doc));
      assertThat(e)
          .isNotNull()
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.SHRED_DOC_LIMIT_VIOLATION)
          .hasMessageStartingWith(ErrorCode.SHRED_DOC_LIMIT_VIOLATION.getMessage())
          .hasMessageEndingWith(
              " number of elements an Array has (120) exceeds maximum allowed ("
                  + docLimits.maxArrayLength()
                  + ")");
    }

    private ObjectNode docWithNArrayElems(int count) {
      final ObjectNode doc = objectMapper.createObjectNode();
      doc.put("_id", 123);
      ArrayNode arr = doc.putArray("arr");
      for (int i = 0; i < count; ++i) {
        arr.add(i);
      }
      return doc;
    }
  }

  // Tests for size of atomic value / name length violations
  @Nested
  class ValidationDocAtomicSizeViolations {
    @Test
    public void allowNotTooLongNames() {
      final ObjectNode doc = objectMapper.createObjectNode();
      doc.put("_id", 123);
      doc.put("prop_123456789_123456789_123456789_123456789", true);
      assertThat(shredder.shred(doc)).isNotNull();
    }

    @Test
    public void catchTooLongNames() {
      final ObjectNode doc = objectMapper.createObjectNode();
      doc.put("_id", 123);
      ObjectNode ob = doc.putObject("subdoc");
      final String propName =
          "property_with_way_too_long_name_123456789_123456789_123456789_123456789";
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
                  + docLimits.maxPropertyNameLength()
                  + ")");
    }

    @ParameterizedTest
    @ValueSource(strings = {"$function", "dot.ted", "index[1]"})
    public void catchInvalidFieldName(String invalidName) {
      final ObjectNode doc = objectMapper.createObjectNode();
      doc.put("_id", 123);
      doc.put(invalidName, 123456);

      Exception e = catchException(() -> shredder.shred(doc));
      assertThat(e)
          .isNotNull()
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.SHRED_DOC_KEY_NAME_VIOLATION)
          .hasMessageStartingWith(ErrorCode.SHRED_DOC_KEY_NAME_VIOLATION.getMessage())
          .hasMessageEndingWith(
              "Document key name constraints violated: Property name ('"
                  + invalidName
                  + "') contains character(s) not allowed");
    }

    @Test
    public void allowNotTooLongStringValues() {
      final ObjectNode doc = objectMapper.createObjectNode();
      doc.put("_id", 123);
      // Max is 16_000 so do a bit less
      doc.put("text", RandomStringUtils.randomAscii(12_000));
      assertThat(shredder.shred(doc)).isNotNull();
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
