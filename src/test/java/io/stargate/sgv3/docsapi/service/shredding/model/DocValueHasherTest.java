package io.stargate.sgv3.docsapi.service.shredding.model;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import javax.inject.Inject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class DocValueHasherTest {
  @Inject ObjectMapper objectMapper;

  @Nested
  class ObjectHashing {
    @Test
    public void testSimpleDocument() throws Exception {
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
      assertThat(hash.usesMD5()).isTrue();
      // and total length is 22 characters for hash (no prefix)
      assertThat(hash.hash()).hasSize(MD5Hasher.BASE64_ENCODED_MD5_LEN);

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
      assertThat(hash.usesMD5()).isTrue();
      // and total length is 22 characters for hash (no prefix)
      assertThat(hash.hash()).hasSize(MD5Hasher.BASE64_ENCODED_MD5_LEN);

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
    public void testNestedArray() throws Exception {
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
      assertThat(hash.usesMD5()).isTrue();
      // and total length is 22 characters for hash (no prefix)
      assertThat(hash.hash()).hasSize(MD5Hasher.BASE64_ENCODED_MD5_LEN);

      // 4 arrays (main array, 3 sub-arrays)
      // Note: unlike with atomic values,
      // structured values are not de-duplicated (JsonNode identity only used to avoid
      // re-processing)
      assertThat(hasher.structuredHashes).hasSize(4);

      // 4 distinct cacheable atomic values
      assertThat(hasher.atomics.seenValues).hasSize(4);
    }
  }
}
