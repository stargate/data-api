package io.stargate.sgv3.docsapi.service.shredding.model;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import javax.inject.Inject;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class DocValueHasherTest {
  @Inject ObjectMapper objectMapper;

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

    // Verify that we have 2 structured hashes cached (array, sub-doc and document itself)
    assertThat(hasher.structuredHashes).hasSize(3);

    // Verify that of 9 atomic values we only get 5 hashes (1 and 2 are repeated;
    // booleans and nulls are not cached)
    assertThat(hasher.atomics.seenValues).hasSize(5);

    System.err.println("Structured: " + hasher.structuredHashes);
    System.err.println("Atomic: " + hasher.atomics.seenValues);
  }

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

    // 4 arrays (main array, 3 sub-arrays)
    // Note: unlike with atomic values,
    // structured values are not de-duplicated (JsonNode identity only used to avoid
    // re-processing)
    assertThat(hasher.structuredHashes).hasSize(4);

    // 4 distinct cacheable atomic values
    assertThat(hasher.atomics.seenValues).hasSize(4);
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

    // 3 documents (main doc, 2 sub-docs)
    // Note: unlike with atomic values,
    // structured values are not de-duplicated (JsonNode identity only used to avoid
    // re-processing)
    assertThat(hasher.structuredHashes).hasSize(3);

    // 3 distinct cacheable atomic values
    assertThat(hasher.atomics.seenValues).hasSize(3);
  }
}
