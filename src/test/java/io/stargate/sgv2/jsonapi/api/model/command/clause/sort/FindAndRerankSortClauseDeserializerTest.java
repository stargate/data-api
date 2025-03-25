package io.stargate.sgv2.jsonapi.api.model.command.clause.sort;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.util.recordable.PrettyPrintable;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tests for the {@link FindAndRerankSortClauseDeserializer}. */
public class FindAndRerankSortClauseDeserializerTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(FindAndRerankSortClauseDeserializerTest.class);

  private FindAndRerankSortClauseDeserializer deserializer;

  @Mock private JsonParser jsonParser;

  @Mock private DeserializationContext deserializationContext;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    deserializer = new FindAndRerankSortClauseDeserializer();
  }

  @Test
  public void testEqualsAndHash() {
    var value1 =
        new FindAndRerankSort("vectorize sort", "lexical sort", new float[] {1.1f, 2.2f, 3.3f});

    var diffVectorize =
        new FindAndRerankSort("vectorize sort 2", "lexical sort", new float[] {1.1f, 2.2f, 3.3f});
    var diffLexical =
        new FindAndRerankSort("vectorize sort", "lexical sort 2", new float[] {1.1f, 2.2f, 3.3f});
    var diffVector =
        new FindAndRerankSort(
            "vectorize sort", "lexical sort", new float[] {1.1f, 2.2f, 3.3f, 4.4f});

    assertThat(value1).as("Object equals self").isEqualTo(value1);
    assertThat(value1).as("different vectorize sort").isNotEqualTo(diffVectorize);
    assertThat(value1).as("different lexical sort").isNotEqualTo(diffLexical);
    assertThat(value1).as("different vector").isNotEqualTo(diffVector);

    assertThat(value1.hashCode()).as("hash code equals self").isEqualTo(value1.hashCode());
    assertThat(value1.hashCode())
        .as("hash code different vectorize sort")
        .isNotEqualTo(diffVectorize.hashCode());
    assertThat(value1.hashCode())
        .as("hash code different lexical sort")
        .isNotEqualTo(diffLexical.hashCode());
    assertThat(value1.hashCode())
        .as("hash code different vector")
        .isNotEqualTo(diffVector.hashCode());
  }

  @ParameterizedTest
  @MethodSource("validSortsTestCases")
  public void validSorts(String jsonString, FindAndRerankSort expected) throws Exception {

    LOGGER.debug("validSorts() - jsonString: {}", jsonString);
    ObjectMapper mapper = new ObjectMapper();
    when(deserializationContext.readTree(jsonParser)).thenReturn(mapper.readTree(jsonString));

    FindAndRerankSort actual = deserializer.deserialize(jsonParser, deserializationContext);
    LOGGER.debug("validSorts() - actual: {}", PrettyPrintable.pprint(actual));

    assertEquals(expected, actual);
  }

  private static Stream<Arguments> validSortsTestCases() {
    return Stream.of(
        Arguments.of(
            """
            {}
            """,
            FindAndRerankSort.NO_ARG_SORT),
        Arguments.of(
            """
            null
            """,
            FindAndRerankSort.NO_ARG_SORT),
        // ----
        // $hybrid only
        Arguments.of(
            """
            { "$hybrid" : "same for hybrid and lexical" }
            """,
            new FindAndRerankSort(
                "same for hybrid and lexical", "same for hybrid and lexical", null)),
        Arguments.of(
            """
            { "$hybrid" : "" }
            """,
            new FindAndRerankSort(null, null, null)),
        // ----
        // maximum fields, resolver works out the valid combinations
        Arguments.of(
            """
            { "$hybrid" : { "$vectorize" : "vectorize sort", "$lexical" : "lexical sort", "$vector" : [1.1, 2.2, 3.3]} }
            """,
            new FindAndRerankSort(
                "vectorize sort", "lexical sort", new float[] {1.1f, 2.2f, 3.3f})),
        Arguments.of(
            """
            { "$hybrid" : { "$vectorize" : "vectorize sort", "$lexical" : "lexical sort"} }
            """,
            new FindAndRerankSort("vectorize sort", "lexical sort", null)),
        // ----
        // $lexical variations
        Arguments.of(
            """
            { "$hybrid" : { "$vectorize" : "vectorize sort", "$lexical" : null} }
            """,
            new FindAndRerankSort("vectorize sort", null, null)),
        Arguments.of(
            """
            { "$hybrid" : { "$vectorize" : "vectorize sort", "$lexical" : ""} }
            """,
            new FindAndRerankSort("vectorize sort", null, null)),
        Arguments.of(
            """
            { "$hybrid" : { "$vectorize" : "vectorize sort"} }
            """,
            new FindAndRerankSort("vectorize sort", null, null)),
        // ----
        // $vectorize variations
        Arguments.of(
            """
            { "$hybrid" : { "$vectorize" : "vectorize sort", "$lexical" : "lexical sort"} }
            """,
            new FindAndRerankSort("vectorize sort", "lexical sort", null)),
        Arguments.of(
            """
            { "$hybrid" : { "$vectorize" : null, "$lexical" : "lexical sort"} }
            """,
            new FindAndRerankSort(null, "lexical sort", null)),
        Arguments.of(
            """
            { "$hybrid" : { "$vectorize" : "", "$lexical" : "lexical sort"} }
            """,
            new FindAndRerankSort(null, "lexical sort", null)),
        Arguments.of(
            """
            { "$hybrid" : {"$lexical" : "lexical sort"} }
            """,
            new FindAndRerankSort(null, "lexical sort", null)),
        // ----
        // $vector variations
        Arguments.of(
            """
            { "$hybrid" : { "$vectorize" : "vectorize", "$lexical" : "lexical", "$vector" : null} }
            """,
            new FindAndRerankSort("vectorize", "lexical", null)));
  }

  @ParameterizedTest
  @MethodSource("invalidSortsTestCases")
  public void invalidSorts(String jsonString, String message) throws Exception {

    LOGGER.debug("invalidSorts() - jsonString: {}", jsonString);
    ObjectMapper mapper = new ObjectMapper();
    when(deserializationContext.readTree(jsonParser)).thenReturn(mapper.readTree(jsonString));

    var error =
        assertThrows(
            JsonMappingException.class,
            () -> deserializer.deserialize(jsonParser, deserializationContext));
    LOGGER.debug("validSorts() - error.getMessage(): {}", error.getMessage());

    assertThat(error).isInstanceOf(JsonMappingException.class).hasMessageContaining(message);
  }

  private static Stream<Arguments> invalidSortsTestCases() {
    return Stream.of(
        // ----
        // unexpected fields
        Arguments.of(
            """
            1
            """,
            "sort clause must be an object"),
        Arguments.of(
            """
            { "fake" : 1}
            """,
            "Expected fields: $hybrid. Unexpected fields: fake"),
        Arguments.of(
            """
            { "$hybrid" : {"fake" : 1} }
            """,
            "Expected fields: $lexical, $vectorize, $vector. Unexpected fields: fake"),
        // ----
        // $hybrid only
        Arguments.of(
            """
            { "$hybrid" : 1}
            """,
            "Field $hybrid may only be of types TextNode, ObjectNode, but got: IntNode"),
        // ----
        // Combinations
        Arguments.of(
            """
            { "$hybrid" : { "$vectorize" : 1, "$lexical" : "", "$vector" : [1.1, 2.2, 3.3]} }
            """,
            "Field $vectorize may only be of types NullNode, TextNode, but got: IntNode"),
        Arguments.of(
            """
            { "$hybrid" : { "$vectorize" : "", "$lexical" : 1, "$vector" : [1.1, 2.2, 3.3]} }
            """,
            "Field $lexical may only be of types NullNode, TextNode, but got: IntNode"),
        Arguments.of(
            """
            { "$hybrid" : { "$vector" : "", "$lexical" : "", "$vector" : 1} }
            """,
            "Field $vector may only be of types NullNode, ArrayNode, but got: IntNode"));
  }
}
