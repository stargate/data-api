package io.stargate.sgv2.jsonapi.service.shredding;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchException;

import com.fasterxml.jackson.core.exc.StreamConstraintsException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.config.DocumentLimitsConfig;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import jakarta.inject.Inject;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
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
      // Given we fail at 4 meg
      // let's try 600k (8 x 5 x 7.5k)
      final ObjectNode bigDoc = createBigDoc(8, 5);
      assertThat(shredder.shred(bigDoc)).isNotNull();

      // let's also try 1m (12 x 12 x 7.5k)
      final ObjectNode bigDoc1m = createBigDoc(12, 12);
      assertThat(shredder.shred(bigDoc1m)).isNotNull();
    }

    @Test
    public void catchTooBigDoc() {
      // Let's construct document above 4 meg limit (but otherwise legal), with
      // (12x45) x 7.5k String values, divided in 12 sub documents of 45 properties
      final ObjectNode bigDoc = createBigDoc(12, 45);

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
          mainProp.put("sub" + ix2, RandomStringUtils.randomAscii(7_500));
        }
      }
      return bigDoc;
    }

    @Test
    public void allowDeepButNotTooDeepDoc() {
      // We allow root + 15 levels of nesting so...
      final ObjectNode deepDoc = objectMapper.createObjectNode();
      deepDoc.put("_id", 123);
      ObjectNode ob = deepDoc;
      for (int i = 0; i < 15; ++i) {
        ob = ob.putObject("sub");
      }

      assertThat(shredder.shred(deepDoc)).isNotNull();
    }

    @Test
    public void catchTooDeepDoc() {
      // Let's construct document with 20 levels of nesting: above our default
      // max of 16 (currently)
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
      // Max allowed is 1,000
      final ObjectNode doc = docWithNProps("subdoc", docLimits.maxObjectProperties());
      assertThat(shredder.shred(doc)).isNotNull();
    }

    @Test
    public void allowDocWithHugeObjectNoIndex() {
      // Max allowed 1000 normally, but if Object not-indexed, not limited
      final ObjectNode doc = docWithNProps("no_index", docLimits.maxObjectProperties() + 100);
      DocumentProjector indexProjector =
          DocumentProjector.createForIndexing(null, Collections.singleton("no_index"));
      assertThat(shredder.shred(doc, null, indexProjector)).isNotNull();
    }

    @Test
    public void catchTooManyObjectProps() {
      // Max allowed 100, so fail with just one above
      final int maxObProps = docLimits.maxObjectProperties();
      final int tooManyProps = maxObProps + 1;
      final ObjectNode doc = docWithNProps("subdoc", tooManyProps);

      Exception e = catchException(() -> shredder.shred(doc));
      assertThat(e)
          .isNotNull()
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.SHRED_DOC_LIMIT_VIOLATION)
          .hasMessageStartingWith(ErrorCode.SHRED_DOC_LIMIT_VIOLATION.getMessage())
          .hasMessageEndingWith(
              " number of properties an indexable Object ('subdoc') has ("
                  + tooManyProps
                  + ") exceeds maximum allowed ("
                  + maxObProps
                  + ")");
    }

    private ObjectNode docWithNProps(String propName, int count) {
      final ObjectNode doc = objectMapper.createObjectNode();
      doc.put("_id", 123);
      ObjectNode obNode = doc.putObject(propName);
      for (int i = 0; i < count; ++i) {
        obNode.put("prop" + i, i);
      }
      return doc;
    }

    @Test
    public void catchTooManyDocProps() {
      // Max allowed 2000, create one with bit more
      final ObjectNode doc = docWithNestedProps(50, 41);

      Exception e = catchException(() -> shredder.shred(doc));
      assertThat(e)
          .isNotNull()
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.SHRED_DOC_LIMIT_VIOLATION)
          .hasMessageStartingWith(ErrorCode.SHRED_DOC_LIMIT_VIOLATION.getMessage())
          .hasMessageEndingWith(
              " total number of indexed properties (2101) in document exceeds maximum allowed ("
                  + docLimits.maxDocumentProperties()
                  + ")");
    }

    private ObjectNode docWithNestedProps(int rootCount, int leafCount) {
      final ObjectNode doc = objectMapper.createObjectNode();
      doc.put("_id", 123);

      for (int i = 0; i < rootCount; ++i) {
        ObjectNode branch = doc.putObject("root" + i);
        for (int j = 0; j < leafCount; ++j) {
          branch.put("prop" + j, j);
        }
      }
      return doc;
    }

    @Test
    public void allowDocWithManyArrayElements() {
      // Max allowed 1000, test:
      final ObjectNode doc = docWithNArrayElems("arr", docLimits.maxArrayLength());
      assertThat(shredder.shred(doc)).isNotNull();
    }

    // Test to verify that max-array-size limit only imposed on indexable fields
    @Test
    public void allowDocWithHugeArrayNoIndex() {
      // Max allowed 1000 normally, but if array not-indexed, not limited
      final ObjectNode doc = docWithNArrayElems("no_index", docLimits.maxArrayLength() + 100);
      DocumentProjector indexProjector =
          DocumentProjector.createForIndexing(null, Collections.singleton("no_index"));
      assertThat(shredder.shred(doc, null, indexProjector)).isNotNull();
    }

    @Test
    public void catchTooManyArrayElements() {
      final int arraySizeAboveMax = docLimits.maxArrayLength() + 1;
      final ObjectNode doc = docWithNArrayElems("arr", arraySizeAboveMax);
      Exception e = catchException(() -> shredder.shred(doc));
      assertThat(e)
          .isNotNull()
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.SHRED_DOC_LIMIT_VIOLATION)
          .hasMessageStartingWith(ErrorCode.SHRED_DOC_LIMIT_VIOLATION.getMessage())
          .hasMessageEndingWith(
              " number of elements an indexable Array ('arr') has ("
                  + arraySizeAboveMax
                  + ") exceeds maximum allowed ("
                  + docLimits.maxArrayLength()
                  + ")");
    }

    @Test
    public void allowMoreArrayElementsForVectors() {
      // Let's add 120 elements (max allowed normally: 100)
      final ObjectNode doc =
          docWithNArrayElems(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD, 120);
      assertThat(shredder.shred(doc)).isNotNull();
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
    public void allowNotTooLongPath() {
      final ObjectNode doc = objectMapper.createObjectNode();
      // Create 3-levels, 300 chars each, so 902 chars (3 names, 2 dots); below 1000 max
      ObjectNode ob1 = doc.putObject("abcde".repeat(60));
      ObjectNode ob2 = ob1.putObject("defgh".repeat(60));
      ObjectNode ob3 = ob2.putObject("hijkl".repeat(60));
      // and then one short one, for 992 char total path
      ob3.put("xyz".repeat(30), 123);
      assertThat(shredder.shred(doc)).isNotNull();
    }

    @Test
    public void catchTooLongPaths() {
      final ObjectNode doc = objectMapper.createObjectNode();
      // Create 3-levels, 300 chars each, for 900 + 2 and then one last segment of 100 char
      ObjectNode ob1 = doc.putObject("abcde".repeat(60));
      ObjectNode ob2 = ob1.putObject("defgh".repeat(60));
      ObjectNode ob3 = ob2.putObject("hijkl".repeat(60));
      final String lastSegment = "a1234".repeat(20);
      ob3.put(lastSegment, 123);

      Exception e = catchException(() -> shredder.shred(doc));
      assertThat(e)
          .isNotNull()
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.SHRED_DOC_LIMIT_VIOLATION)
          .hasMessageStartingWith(ErrorCode.SHRED_DOC_LIMIT_VIOLATION.getMessage())
          .hasMessageEndingWith(
              "property path length (1003) exceeds maximum allowed ("
                  + docLimits.maxPropertyPathLength()
                  + ") (path ends with '"
                  + lastSegment
                  + "')");
      ;
    }

    @Test
    public void allowNotTooLongStringValues() {
      final ObjectNode doc = objectMapper.createObjectNode();
      doc.put("_id", 123);
      // Use ASCII to keep chars == bytes, use length of just slight below max allowed
      doc.put("text", RandomStringUtils.randomAscii(docLimits.maxStringLengthInBytes() - 100));
      assertThat(shredder.shred(doc)).isNotNull();
    }

    @Test
    public void catchTooLongStringValueAscii() {
      final ObjectNode doc = objectMapper.createObjectNode();
      doc.put("_id", 123);
      ArrayNode arr = doc.putArray("arr");
      // Use ASCII to keep chars == bytes, use length of just above max allowed
      final int tooLongLength = docLimits.maxStringLengthInBytes() + 100;
      String str = RandomStringUtils.randomAscii(tooLongLength);
      arr.add(str);

      Exception e = catchException(() -> shredder.shred(doc));
      assertThat(e)
          .isNotNull()
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.SHRED_DOC_LIMIT_VIOLATION)
          .hasMessageStartingWith(ErrorCode.SHRED_DOC_LIMIT_VIOLATION.getMessage())
          .hasMessageEndingWith(
              " String value length ("
                  + tooLongLength
                  + " bytes) exceeds maximum allowed ("
                  + docLimits.maxStringLengthInBytes()
                  + " bytes)");
    }

    // Test to ensure that maximum String length validation catches case where
    // character length is below maximum byte length but byte length is above it.
    @Test
    public void catchTooLongStringValueUTF8() {
      final ObjectNode doc = objectMapper.createObjectNode();
      doc.put("_id", 123);
      // Repeat a 3-byte sequence enough times to exceed maximum
      final int tooLongCharLength = (docLimits.maxStringLengthInBytes() / 3) + 20;
      // Unicode char "न" (Devanagari script, U+0928), requires 3 bytes to encode
      String tooLongString = "\u0928".repeat(tooLongCharLength);
      doc.put("text", tooLongString);

      // First just validate constraints: String we have has character length BELOW
      // max length, and byte length ABOVE max length:
      assertThat(tooLongString).hasSizeLessThan(docLimits.maxStringLengthInBytes());
      assertThat(tooLongString.getBytes(StandardCharsets.UTF_8))
          .hasSizeGreaterThan(docLimits.maxStringLengthInBytes());

      Exception e = catchException(() -> shredder.shred(doc));
      assertThat(e)
          .isNotNull()
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.SHRED_DOC_LIMIT_VIOLATION)
          .hasMessageStartingWith(ErrorCode.SHRED_DOC_LIMIT_VIOLATION.getMessage())
          .hasMessageEndingWith(
              " String value length ("
                  + (tooLongCharLength * 3)
                  + " bytes) exceeds maximum allowed ("
                  + docLimits.maxStringLengthInBytes()
                  + " bytes)");
    }

    // Since max-number-len is handled at low-level, it's not strictly speaking
    // Shredder test -- but since it is logically related, test it here too
    // (there is separate testing for integration through doc insert)
    @Test
    public void catchTooLongNumberValues() throws Exception {
      final ObjectNode doc = objectMapper.createObjectNode();
      doc.put("_id", 123);
      ArrayNode arr = doc.putArray("arr");
      // Max 100, so use slightly above
      String numStr = "1234567890".repeat(10) + ".0";
      doc.put("number", new BigDecimal(numStr));
      arr.add(numStr);

      final String json = objectMapper.writeValueAsString(doc);

      Exception e = catchException(() -> objectMapper.readTree(json));
      assertThat(e)
          .isNotNull()
          .isInstanceOf(StreamConstraintsException.class)
          .hasMessageStartingWith("Number value length (101) exceeds the maximum allowed (100");
    }

    // Different test in that it should NOT fail but work as expected (in
    // addition to being lower level test wrt ObjectMapper
    @Test
    public void handleBigEngineeringNotation() throws Exception {
      final ObjectNode doc = objectMapper.createObjectNode();
      doc.put("_id", 123);
      final BigDecimal bigValue = new BigDecimal("2.0635595263889274e-35");
      doc.put("num", bigValue);

      final String json = objectMapper.writeValueAsString(doc);
      ObjectNode serializedDoc = (ObjectNode) objectMapper.readTree(json);
      assertThat(serializedDoc).isNotNull();
      assertThat(serializedDoc.path("num").decimalValue()).isEqualTo(bigValue);
    }
  }

  // Tests for size of atomic value / name length violations
  @Nested
  class ValidationDocNameViolations {
    @ParameterizedTest
    @ValueSource(strings = {"name", "a123", "snake_case", "camelCase", "ab-cd-ef"})
    public void allowRegularFieldNames(String validName) {
      final ObjectNode doc = objectMapper.createObjectNode();
      doc.put("_id", 123);
      doc.put(validName, 123456);

      // Enough to verify that shredder does not throw exception
      assertThat(shredder.shred(doc)).isNotNull();
    }

    @Test
    public void catchEmptyFieldName() {
      final ObjectNode doc = objectMapper.createObjectNode();
      doc.put("", 123456);

      Exception e = catchException(() -> shredder.shred(doc));
      assertThat(e)
          .isNotNull()
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.SHRED_DOC_KEY_NAME_VIOLATION)
          .hasMessageStartingWith(ErrorCode.SHRED_DOC_KEY_NAME_VIOLATION.getMessage())
          .hasMessageEndingWith("Document key name constraints violated: empty names not allowed");
    }

    @ParameterizedTest
    @ValueSource(strings = {"$function", "dot.ted", "index[1]", "a/b", "a\\b"})
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
              "Document key name constraints violated: property name ('"
                  + invalidName
                  + "') contains character(s) not allowed");
    }
  }

  private ObjectNode docWithNArrayElems(String propName, int count) {
    final ObjectNode doc = objectMapper.createObjectNode();
    doc.put("_id", 123);
    ArrayNode arr = doc.putArray(propName);
    for (int i = 0; i < count; ++i) {
      arr.add(i);
    }
    return doc;
  }
}
