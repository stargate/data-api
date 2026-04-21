package io.stargate.sgv2.jsonapi.service.processor;

import static io.stargate.sgv2.jsonapi.metrics.CommandFeature.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.exception.RequestException;
import io.stargate.sgv2.jsonapi.metrics.CommandFeatures;
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
               """,
            CommandFeatures.of(HYBRID)),
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
               """,
            CommandFeatures.of(HYBRID, LEXICAL, VECTORIZE)),
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
                   """,
            CommandFeatures.of(HYBRID, VECTORIZE)),
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
                 """,
            CommandFeatures.of(HYBRID, LEXICAL)),
        Arguments.of(
            """
                 {
                     "_id": 5,
                     "$lexical": "I like cheese",
                     "$vectorize": "I like cheese"
                 }
                 """,
            """
                  {
                     "_id": 5,
                     "$lexical": "I like cheese",
                     "$vectorize": "I like cheese"
                 }
                 """,
            CommandFeatures.of(LEXICAL, VECTORIZE)),
        Arguments.of(
            """
                         {
                             "_id": 5,
                             "$lexical": "I like cheese",
                             "$vector": "[1,2,3]"
                         }
                         """,
            """
                          {
                             "_id": 5,
                             "$lexical": "I like cheese",
                             "$vector": "[1,2,3]"
                         }
                         """,
            CommandFeatures.of(LEXICAL, VECTOR)),
        Arguments.of(
            """
                 {
                     "_id": 6,
                     "$lexical": null,
                     "$vectorize": "I like cheese"
                 }
                 """,
            """
                 {
                     "_id": 6,
                     "$lexical": null,
                     "$vectorize": "I like cheese"
                 }
                 """,
            CommandFeatures.of(VECTORIZE)),
        Arguments.of(
            """
                 {
                     "_id": 7,
                     "$lexical": "I like cheese",
                     "$vectorize": null
                 }
                 """,
            """
                 {
                     "_id": 7,
                     "$lexical": "I like cheese",
                     "$vectorize": null
                 }
                 """,
            CommandFeatures.of(LEXICAL)));
  }

  @ParameterizedTest
  @MethodSource("okCases")
  void hybridOkTest(String inputJson, String outputJson, CommandFeatures features)
      throws Exception {
    JsonNode doc = objectMapper.readTree(inputJson);

    // Setup mock CommandContext
    CommandContext mockContext = mock(CommandContext.class);
    CommandFeatures commandFeatures = CommandFeatures.create();
    when(mockContext.commandFeatures()).thenReturn(commandFeatures);

    HybridFieldExpander.expandHybridField(mockContext, 0, 1, doc);
    assertThat(doc).isEqualTo(objectMapper.readTree(outputJson));
    assertThat(commandFeatures).isEqualTo(features);
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
            RequestException.Code.HYBRID_FIELD_UNSUPPORTED_VALUE_TYPE,
            "Unsupported JSON value type for '$hybrid' field: expected String, Object or `null` but received Number (Document 1 of 1)"),
        Arguments.of(
            """
                       {
                           "_id": 1,
                           "$hybrid": [ "abc" ]
                       }
                       """,
            RequestException.Code.HYBRID_FIELD_UNSUPPORTED_VALUE_TYPE,
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
            RequestException.Code.HYBRID_FIELD_UNKNOWN_SUBFIELDS,
            "Unknown sub-field(s) for '$hybrid' field: expected '$lexical' and/or '$vectorize' but encountered: 'unknown' (Document 1 of 1)"),
        Arguments.of(
            """
                {
                  "_id": "hybrid-fail-bad-subfield-types",
                  "$hybrid": {
                    "$lexical": 145
                   }
                }
                """,
            RequestException.Code.HYBRID_FIELD_UNSUPPORTED_SUBFIELD_VALUE_TYPE,
            "Unsupported JSON value type for '$hybrid' sub-field: expected String or `null` for '$lexical' but received Number (Document 1 of 1)"),
        Arguments.of(
            """
                        {
                          "_id": 1,
                          "$hybrid": "monkeys bananas",
                          "$lexical": "bananas",
                          "$vectorize": "monkeys like bananas"
                        }
                        """,
            RequestException.Code.HYBRID_FIELD_CONFLICT,
            null),
        // Conflict whenever $hybrid is present, even if null, in addition to $lexical and/or
        // $vectorize
        Arguments.of(
            """
                            {
                              "_id": 1,
                              "$hybrid": null,
                              "$vectorize": "monkeys like bananas"
                            }
                            """,
            RequestException.Code.HYBRID_FIELD_CONFLICT,
            null),
        // Conflict reported for actual collisions, even if values identical
        Arguments.of(
            """
                    {
                      "_id": 1,
                      "$hybrid": {
                        "$lexical": "bananas",
                        "$vectorize": "monkeys like bananas"
                       },
                       "$lexical": "bananas"
                    }
                    """,
            RequestException.Code.HYBRID_FIELD_CONFLICT,
            null),
        // Conflict reported even if individual (sub-)fields do not overlap
        Arguments.of(
            """
                    {
                      "_id": 1,
                      "$vectorize": "monkeys like bananas",
                      "$hybrid": {
                        "$lexical": "bananas"
                       }
                    }
                    """,
            RequestException.Code.HYBRID_FIELD_CONFLICT,
            null),
        // Conflict whenever $hybrid and $vector are used together
        Arguments.of(
            """
                            {
                              "_id": 1,
                              "$vector": [1,2,3],
                              "$hybrid": "I like cheese"
                            }
                            """,
            RequestException.Code.HYBRID_FIELD_CONFLICT,
            null));
  }

  @ParameterizedTest
  @MethodSource("failCases")
  void hybridFailTest(String inputJson, RequestException.Code errorCode, String errorMessage)
      throws Exception {
    final JsonNode doc = objectMapper.readTree(inputJson);

    // Setup mock CommandContext
    CommandContext mockContext = mock(CommandContext.class);
    CommandFeatures commandFeatures = CommandFeatures.create();
    when(mockContext.commandFeatures()).thenReturn(commandFeatures);

    Throwable t =
        catchThrowable(() -> HybridFieldExpander.expandHybridField(mockContext, 0, 1, doc));
    assertThat(t).isNotNull().hasFieldOrPropertyWithValue("code", errorCode.name());
    if (errorMessage != null) {
      assertThat(t).hasMessageContaining(errorMessage);
    }
  }
}
