package io.stargate.sgv2.jsonapi.util;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.POJONode;
import java.io.IOException;
import java.util.Comparator;
import java.util.Date;
import org.junit.jupiter.api.Test;

// No need for injection
public class JsonNodeComparatorTest {
  private final ObjectMapper mapper = new ObjectMapper();

  private final Comparator<JsonNode> COMP = JsonNodeComparator.ascending();

  private final JsonNode MISSING = mapper.missingNode();

  @Test
  public void testOrderingBoolean() {
    _verifyIdentityEquals("true");
    _verifyIdentityEquals("false");

    _verifyAscending("false", "true");
  }

  @Test
  public void testOrderingDate() {
    assertThat(COMP.compare(new POJONode(new Date(10L)), new POJONode(new Date(10L)))).isEqualTo(0);
    assertThat(COMP.compare(new POJONode(new Date(10L)), new POJONode(new Date(11L))))
        .isLessThan(0);
    assertThat(COMP.compare(new POJONode(new Date(11L)), new POJONode(new Date(10L))))
        .isGreaterThan(0);
  }

  @Test
  public void testOrderingNumbers() {
    _verifyIdentityEquals("0");
    _verifyIdentityEquals("-9");
    _verifyIdentityEquals("0.25");

    _verifyAscending("0", "1");
    _verifyAscending("1", "2");
    _verifyAscending("-1", "0");
    _verifyAscending("0.0", "0.05");
    _verifyAscending("0.101", "0.11");
    _verifyAscending("1", "1.25");
    _verifyAscending("-10", "1");
  }

  @Test
  public void testOrderingStrings() {
    _verifyIdentityEquals("\"\"");
    _verifyIdentityEquals("\"abc\"");
    _verifyIdentityEquals("\"xyz 125\"");

    _verifyAscending("\"a\"", "\"b\"");
    _verifyAscending("\"a100\"", "\"a99\"");
    _verifyAscending("\"abc\"", "\"abca\"");
  }

  @Test
  public void testOrderingMissing() {
    // All missing instances are equal
    _verifyEquals(mapper.missingNode(), mapper.missingNode());
  }

  @Test
  public void testOrderingNull() {
    // All null instances are equal
    _verifyIdentityEquals("null");
  }

  @Test
  public void testOrderingArrays() {
    _verifyIdentityEquals("[]");
    _verifyIdentityEquals("[1, false, 3]");
    _verifyIdentityEquals("[1, \"foo\", { \"z\": true }]");

    // Ordered comparison by element, recursively. If one subset of the other,
    // longer one sorted last
    _verifyAscending("[0]", "[1]");
    _verifyAscending("[1,0]", "[1,1]");
    _verifyAscending("[1,2]", "[1,2,0]");
    _verifyAscending("[1,[0]]", "[1,[1]]");
  }

  @Test
  public void testOrderingObjects() {
    _verifyIdentityEquals("{}");
    _verifyIdentityEquals("{\"x\":1, \"y\":2, \"arr\": [ 1, 2, 3]}");

    // Ordered comparison by field: first field name, then value; if same fields
    // starting, longer one sorted last
    _verifyAscending("{\"a\":1}", "{\"b\":0}");
    _verifyAscending("{\"a\":1}", "{\"a\":2}");
    _verifyAscending("{\"a\":1}", "{\"a\":1,\"b\":0}");
  }

  @Test
  public void testOrderingMixedScalars() {
    // Ordering by type: (missing), NULL, NUMBER, STRING, OBJECT, ARRAY, BOOLEAN
    _verifyAscending(MISSING, jsonNode("null"));
    _verifyAscending(MISSING, jsonNode("1"));
    _verifyAscending(MISSING, jsonNode("\"0\""));
    _verifyAscending(MISSING, jsonNode("{}"));
    _verifyAscending(MISSING, jsonNode("[]"));
    _verifyAscending(MISSING, jsonNode("false"));

    _verifyAscending("null", "1");
    _verifyAscending("null", "\"0\"");
    _verifyAscending("null", "{}");
    _verifyAscending("null", "[]");
    _verifyAscending("null", "false");

    _verifyAscending("1", "\"0\"");
    _verifyAscending("1", "{}");
    _verifyAscending("1", "[]");
    _verifyAscending("1", "true");

    _verifyAscending("\"abc\"", "{}");
    _verifyAscending("\"abc\"", "[]");
    _verifyAscending("\"abc\"", "false");

    _verifyAscending("{}", "[]");
    _verifyAscending("{}", "false");

    _verifyAscending("[]", "false");
  }

  @Test
  public void testOrderingMixedArrays() {
    // Ordering by type: NULL, NUMBER, STRING, OBJECT, ARRAY, BOOLEAN
    _verifyAscending("[null]", "[1]");
    _verifyAscending("[null]", "[\"0\"]");
    _verifyAscending("[null]", "[{}]");
    _verifyAscending("[null]", "[[]]");
    _verifyAscending("[null]", "[false]");

    _verifyAscending("[1,5]", "[1,\"abc\"]");
    _verifyAscending("[1,5]", "[1,{}]");
    _verifyAscending("[1,5]", "[1,[14]]");
    _verifyAscending("[1,5]", "[1,true]");
  }

  @Test
  public void testOrderingMixedObject() {
    // Ordering first by field name, then by type
    _verifyAscending("{}", "{\"a\":1}");
    _verifyAscending("{\"x\":2}", "{\"y\":1}");
    _verifyAscending("{\"x\":1}", "{\"x\":\"value\"}");
    _verifyAscending("{\"x\":1}", "{\"x\":1,\"a\":null}");
  }

  private void _verifyAscending(String json1, String json2) {
    // verify symmetry by comparing both as given and the reverse
    assertThat(COMP.compare(jsonNode(json1), jsonNode(json2))).isLessThan(0);
    assertThat(COMP.compare(jsonNode(json2), jsonNode(json1))).isGreaterThan(0);
  }

  private void _verifyAscending(JsonNode node1, JsonNode node2) {
    assertThat(COMP.compare(node1, node2)).isLessThan(0);
    assertThat(COMP.compare(node2, node1)).isGreaterThan(0);
  }

  private void _verifyIdentityEquals(String jsonValue) {
    _verifyEquals(jsonValue, jsonValue);
  }

  private void _verifyEquals(String json1, String json2) {
    _verifyEquals(jsonNode(json1), jsonNode(json2));
  }

  private void _verifyEquals(JsonNode node1, JsonNode node2) {
    // verify both ways to ensure comparison is symmetric
    assertThat(COMP.compare(node1, node2)).isEqualTo(0);
    assertThat(COMP.compare(node2, node1)).isEqualTo(0);
  }

  private JsonNode jsonNode(String json) {
    try {
      return mapper.readTree(json);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
