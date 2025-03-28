package io.stargate.sgv2.jsonapi.service.processor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class HybridFieldExpanderTest {
  @Inject ObjectMapper objectMapper;

  static List<Arguments> okCases() {
    return Arrays.asList(
        Arguments.of(
            """
               {
                   "_id": 1,
                   "$hybrid": "monkeys"
               }
               """,
            """
               {
                   "_id": 1,
                   "$lexical": "monkeys",
                   "$vectorize": "monkeys"
               }
               """),
        Arguments.of(
            """
               {
                   "_id": 2,
                   "$hybrid": {
                     "$lexical": "banana monkey",
                     "$vectorize": "monkeys eating bananas"
                   }
               }
               """,
            """
                {
                   "_id": 2,
                   "$lexical": "banana monkey",
                   "$vectorize": "monkeys eating bananas"
               }
               """),
        Arguments.of(
            """
                   {
                       "_id": 3,
                       "$hybrid": {
                         "$vectorize": "monkeys eating bananas"
                       }
                   }
                   """,
            """
                    {
                       "_id": 3,
                       "$lexical": null,
                       "$vectorize": "monkeys eating bananas"
                   }
                   """),
        Arguments.of(
            """
                 {
                     "_id": 4,
                     "$hybrid": {
                       "$lexical": "banana monkey"
                     }
                 }
                 """,
            """
                  {
                     "_id": 4,
                     "$lexical": "banana monkey",
                     "$vectorize": null
                 }
                 """));
  }

  @ParameterizedTest
  @MethodSource("okCases")
  void hybridOkTest(String inputJson, String outputJson) throws Exception {
    JsonNode doc = objectMapper.readTree(inputJson);

    HybridFieldExpander.expandHybridField(0, 1, doc);
    assertThat(doc).isEqualTo(objectMapper.readTree(outputJson));
  }

  static List<Arguments> failCases() {
    return Arrays.asList(
        Arguments.of(
            """
               {
                   "_id": 1,
                   "$hybrid": 123
               }
               """,
            ErrorCodeV1.HYBRID_FIELD_UNSUPPORTED_VALUE_TYPE,
            "Unsupported JSON value type for '$hybrid' field: expected String, Object or `null` but received Number (Document 1 of 1)"),
        Arguments.of(
            """
                       {
                           "_id": 1,
                           "$hybrid": [ "abc" ]
                       }
                       """,
            ErrorCodeV1.HYBRID_FIELD_UNSUPPORTED_VALUE_TYPE,
            "Unsupported JSON value type for '$hybrid' field: expected String, Object or `null` but received Array (Document 1 of 1)"),
        Arguments.of(
            """
               {
                   "_id": 1,
                   "$hybrid": {
                     "unknown": "value"
                   }
               }
               """,
            ErrorCodeV1.HYBRID_FIELD_UNKNOWN_SUBFIELDS,
            "Unrecognized sub-field(s) for '$hybrid' Object: expected '$lexical' and/or '$vectorize' but encountered: 'unknown' (Document 1 of 1)"),
        Arguments.of(
            """
                {
                  "_id": "hybrid-fail-bad-subfield-types",
                  "$hybrid": {
                    "$lexical": 145
                   }
                }
                   """,
            ErrorCodeV1.HYBRID_FIELD_UNSUPPORTED_SUBFIELD_VALUE_TYPE,
            "Unsupported JSON value type for '$hybrid' sub-field: expected String or `null` for '$lexical' but received Number (Document 1 of 1)"));
  }

  @ParameterizedTest
  @MethodSource("failCases")
  void hybridFailTest(String inputJson, ErrorCodeV1 errorCode, String errorMessage)
      throws Exception {
    final JsonNode doc = objectMapper.readTree(inputJson);
    Throwable t = catchThrowable(() -> HybridFieldExpander.expandHybridField(0, 1, doc));
    assertThat(t)
        .isNotNull()
        .hasFieldOrPropertyWithValue("errorCode", errorCode)
        .hasMessageContaining(errorMessage);
  }
}
