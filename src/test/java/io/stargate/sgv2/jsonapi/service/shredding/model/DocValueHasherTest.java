package io.stargate.sgv2.jsonapi.service.shredding.model;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import java.util.Base64;
import javax.inject.Inject;
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
