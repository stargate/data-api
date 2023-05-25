package io.stargate.sgv2.jsonapi.service.shredding.model;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class DocValueHasherTest {
  @Inject ObjectMapper objectMapper;

  @Nested
  class ObjectHashing {
    // First: short enough not to use MD5
    @Test
    public void shortDocument() throws Exception {
      JsonNode doc = objectMapper.readTree("{\"key\" : \"value\"}");
      DocValueHash hash = new DocValueHasher().hash(doc);
      assertThat(hash).isNotNull();
      assertThat(hash.type()).isEqualTo(DocValueType.OBJECT);
      assertThat(hash.usesMD5()).isFalse();
      // Three lines: header with type-prefix and entry count; one line for key
      // and one line for type-prefixed value. No trailing linefeed
      assertThat(hash.hash()).isEqualTo("O1\nkey\nSvalue");
    }

    @Test
    public void shortDocumentWithDate() throws Exception {
      JsonNode doc = objectMapper.readTree("{\"key\":{\"$date\":123456}}");
      DocValueHash hash = new DocValueHasher().hash(doc);
      assertThat(hash).isNotNull();
      assertThat(hash.type()).isEqualTo(DocValueType.OBJECT);
      assertThat(hash.usesMD5()).isFalse();
      // Three lines: header with type-prefix and entry count; one line for key
      // and one line for type-prefixed value. No trailing linefeed
      assertThat(hash.hash()).isEqualTo("O1\nkey\nT123456");
    }

    @Test
    public void basicDocument() throws Exception {
      JsonNode doc =
          objectMapper.readTree(
              """
                              { "name" : "Bob",
                                "values" : [1, 2, 1, 2, true, null],
                                "address": {
                                 "street" : "Elm Street",
                                 "zipcode" : 21040
                                }
                              }
                              """);
      DocValueHasher hasher = new DocValueHasher();
      DocValueHash hash = hasher.hash(doc);

      assertThat(hash).isNotNull();
      assertThat(hash.type()).isEqualTo(DocValueType.OBJECT);
      // long enough to require MD5 hashing
      assertMD5Base64(hash);

      // Verify that we have 2 structured hashes cached (array, sub-doc and document itself)
      assertThat(hasher.structuredHashes).hasSize(3);

      // Verify that of 9 atomic values we only get 5 hashes (1 and 2 are repeated;
      // booleans and nulls are not cached)
      assertThat(hasher.atomics.seenValues).hasSize(5);
    }

    @Test
    public void testNestedObject() throws Exception {
      JsonNode doc =
          objectMapper.readTree(
              """
      { "user": {
        "name" : "Bill",
        "address": {
           "street" : "Elm Street",
           "zipcode" : 21040
         }
      } }
      """);
      DocValueHasher hasher = new DocValueHasher();
      DocValueHash hash = hasher.hash(doc);
      assertThat(hash).isNotNull();
      assertThat(hash.type()).isEqualTo(DocValueType.OBJECT);
      // long enough to require MD5 hashing
      assertMD5Base64(hash);

      // 3 documents (main doc, 2 sub-docs)
      // Note: unlike with atomic values,
      // structured values are not de-duplicated (JsonNode identity only used to avoid
      // re-processing)
      assertThat(hasher.structuredHashes).hasSize(3);

      // 3 distinct cacheable atomic values
      assertThat(hasher.atomics.seenValues).hasSize(3);
    }
  }

  @Nested
  class ArrayHashing {
    @Test
    public void shortArray() throws Exception {
      // note: while we do not support root-level arrays, hash calculation is
      // used at every level so we can test it fine:
      JsonNode doc = objectMapper.readTree("[1, true, null]");
      DocValueHash hash = new DocValueHasher().hash(doc);
      assertThat(hash).isNotNull();
      assertThat(hash.type()).isEqualTo(DocValueType.ARRAY);
      assertThat(hash.usesMD5()).isFalse();
      // Four lines: header with type-prefix and entry count; one line for each
      // element (type-prefixed value). No trailing linefeed
      assertThat(hash.hash()).isEqualTo("A3\nN1\nB1\nZ");
    }

    @Test
    public void shortArrayWithDate() throws Exception {
      JsonNode doc = objectMapper.readTree("[{\"$date\":123}]");
      DocValueHash hash = new DocValueHasher().hash(doc);
      assertThat(hash).isNotNull();
      assertThat(hash.type()).isEqualTo(DocValueType.ARRAY);
      assertThat(hash.usesMD5()).isFalse();
      // Two lines: header with type-prefix and entry count; one line for date.
      assertThat(hash.hash()).isEqualTo("A1\nT123");
    }

    @Test
    public void shortArrayComparator() throws Exception {
      // note: while we do not support root-level arrays, hash calculation is
      // used at every level so we can test it fine:
      JsonNode doc = objectMapper.readTree("[1, true, null]");
      DocValueHash hash = new DocValueHasher().hash(doc);
      assertThat(hash).isNotNull();
      List<Object> arrayData = new ArrayList<>();
      arrayData.add(new BigDecimal(1));
      arrayData.add(true);
      arrayData.add(null);
      DocValueHash hashFromList = new DocValueHasher().getHash(arrayData);
      assertThat(hash).isNotNull();
      assertThat(hashFromList).isNotNull();
      // Four lines: header with type-prefix and entry count; one line for each
      // element (type-prefixed value). No trailing linefeed
      assertThat(hash.hash()).isEqualTo(hashFromList.hash());
    }

    @Test
    public void nestedArray() throws Exception {
      JsonNode doc =
          objectMapper.readTree(
              """
                              [ [ 1, 2, 3, 2, true ], [ "abc" ], ["abc" ] ]
                              """);
      DocValueHasher hasher = new DocValueHasher();
      DocValueHash hash = hasher.hash(doc);
      assertThat(hash).isNotNull();
      assertThat(hash.type()).isEqualTo(DocValueType.ARRAY);
      // long enough to require MD5 hashing
      assertMD5Base64(hash);

      // 4 arrays (main array, 3 sub-arrays)
      // Note: unlike with atomic values,
      // structured values are not de-duplicated (JsonNode identity only used to avoid
      // re-processing)
      assertThat(hasher.structuredHashes).hasSize(4);

      // 4 distinct cacheable atomic values
      assertThat(hasher.atomics.seenValues).hasSize(4);
    }

    @Test
    public void nestedArrayComparator() throws Exception {
      JsonNode doc =
          objectMapper.readTree(
              """
                                      [ [ 1, 2, true ], [ "abc" ], ["abc" ] ]
                                      """);
      DocValueHasher hasher = new DocValueHasher();
      DocValueHash hash = hasher.hash(doc);
      assertThat(hash).isNotNull();

      List<Object> arrayData =
          List.of(
              List.of(new BigDecimal(1), new BigDecimal(2), true), List.of("abc"), List.of("abc"));
      DocValueHash hashFromList = new DocValueHasher().getHash(arrayData);
      assertThat(hashFromList).isNotNull();
      assertThat(hash.hash()).isEqualTo(hashFromList.hash());
    }
  }

  @Nested
  class SubDocHashing {
    @Test
    public void subDocComparator() throws Exception {
      // note: while we do not support root-level arrays, hash calculation is
      // used at every level so we can test it fine:
      String document =
          """
                  {"a1" : 5, "b1" : { "a2" : "abc", "b2" : true}}
                  """;
      JsonNode doc = objectMapper.readTree(document);
      DocValueHash hash = new DocValueHasher().hash(doc);
      assertThat(hash).isNotNull();
      Map<String, Object> values = new LinkedHashMap<>();
      values.put("a1", new BigDecimal(5));
      Map<String, Object> innerValues = new LinkedHashMap<>();
      innerValues.put("a2", "abc");
      innerValues.put("b2", true);
      values.put("b1", innerValues);
      DocValueHash hashFromMap = new DocValueHasher().getHash(values);
      assertThat(hash).isNotNull();
      assertThat(hashFromMap).isNotNull();
      // Four lines: header with type-prefix and entry count; one line for each
      // element (type-prefixed value). No trailing linefeed
      assertThat(hash.hash()).isEqualTo(hashFromMap.hash());
    }
  }

  @Nested
  class BooleanHashing {
    @Test
    public void testBooleanHashing() throws Exception {
      JsonNode doc = objectMapper.readTree("true");
      DocValueHasher hasher = new DocValueHasher();
      DocValueHash hash = hasher.hash(doc);
      assertThat(hash).isNotNull();
      assertThat(hash.type()).isEqualTo(DocValueType.BOOLEAN);
      assertThat(hash.usesMD5()).isFalse();
      assertThat(hash.hash()).isEqualTo("B1");
    }
  }

  @Nested
  class NullHashing {
    @Test
    public void testNullHashing() throws Exception {
      JsonNode doc = objectMapper.readTree("null");
      DocValueHasher hasher = new DocValueHasher();
      DocValueHash hash = hasher.hash(doc);
      assertThat(hash).isNotNull();
      assertThat(hash.type()).isEqualTo(DocValueType.NULL);
      assertThat(hash.usesMD5()).isFalse();
      assertThat(hash.hash()).isEqualTo("Z");
    }
  }

  @Nested
  class NumberHashing {
    @Test
    public void testSmallNumber() throws Exception {
      JsonNode doc = objectMapper.readTree("25.3");
      DocValueHasher hasher = new DocValueHasher();
      DocValueHash hash = hasher.hash(doc);
      assertThat(hash).isNotNull();
      assertThat(hash.type()).isEqualTo(DocValueType.NUMBER);
      assertThat(hash.usesMD5()).isFalse();
      assertThat(hash.hash()).isEqualTo("N25.3");
    }

    @Test
    public void testLongNumber() throws Exception {
      final String numStr = "12345678901234567890.1234567890";
      JsonNode doc = objectMapper.readTree(numStr);
      DocValueHasher hasher = new DocValueHasher();
      DocValueHash hash = hasher.hash(doc);
      assertThat(hash).isNotNull();
      assertThat(hash.type()).isEqualTo(DocValueType.NUMBER);
      assertMD5Base64(hash);
    }
  }

  @Nested
  class StringHashing {
    @Test
    public void testSimpleString() throws Exception {
      JsonNode doc = objectMapper.readTree("\"Some text\"");
      DocValueHasher hasher = new DocValueHasher();
      DocValueHash hash = hasher.hash(doc);
      assertThat(hash).isNotNull();
      assertThat(hash.type()).isEqualTo(DocValueType.STRING);
      assertThat(hash.usesMD5()).isFalse();
      assertThat(hash.hash()).isEqualTo("SSome text");
    }

    @Test
    public void testShortAndLongStrings() throws Exception {
      final DocValueHasher hasher = new DocValueHasher();
      for (int i = 1; i < 80; ++i) {
        String value = RandomStringUtils.randomAlphanumeric(i);
        JsonNode doc = objectMapper.readTree("\"" + value + "\"");
        DocValueHash hash = hasher.hash(doc);
        assertThat(hash).isNotNull();
        assertThat(hash.type()).isEqualTo(DocValueType.STRING);

        // and here we need to check cutoff point: value + prefix length LESS
        // than hashed length is encoded without MD5; at or above with MD4
        if (value.length() < (MD5Hasher.BASE64_ENCODED_MD5_LEN - 1)) {
          assertThat(hash.usesMD5()).isFalse();
          assertThat(hash.hash()).isEqualTo("S" + value);
        } else {
          assertMD5Base64(hash);
        }
      }
    }
  }

  @Nested
  class DateHashing {
    @Test
    public void validDate() throws Exception {
      JsonNode doc = objectMapper.readTree("{\"$date\":123456789}");
      DocValueHasher hasher = new DocValueHasher();
      DocValueHash hash = hasher.hash(doc);
      assertThat(hash).isNotNull();
      assertThat(hash.type()).isEqualTo(DocValueType.TIMESTAMP);
      assertThat(hash.usesMD5()).isFalse();
      assertThat(hash.hash()).isEqualTo("T123456789");
    }
  }

  /**
   * Helper method for checking that given String is valid Base64 encoded representation of a
   * 16-byte value -- presumably MD5 hash (but that can not be validated without knowing input etc)
   */
  void assertMD5Base64(DocValueHash hash) {
    assertThat(hash.usesMD5()).isTrue();
    String hashedValue = hash.hash();
    assertThat(hashedValue).hasSize(MD5Hasher.BASE64_ENCODED_MD5_LEN);
    byte[] md5 = Base64.getDecoder().decode(hashedValue);
    assertThat(md5).hasSize(16);
  }
}
