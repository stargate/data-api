package io.stargate.sgv2.jsonapi.service.operation.filters.collection;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

public class OpenSearchDslIdRewriterTest {

  private final ObjectMapper objectMapper = new ObjectMapper();

  @Test
  public void testEncodeSimpleId() {
    assertThat(OpenSearchIdEncoder.encode("product-3")).isEqualTo("{_0=1, _1=product-3}");
  }

  @Test
  public void testRewriteTermId() throws Exception {
    String input =
        """
        {"term": {"_id": "product-3"}}
        """;
    JsonNode node = objectMapper.readTree(input);
    JsonNode rewritten = OpenSearchDslIdRewriter.rewrite(node);

    assertThat(rewritten.at("/term/_id").asText()).isEqualTo("{_0=1, _1=product-3}");
  }

  @Test
  public void testRewriteTermsId() throws Exception {
    String input =
        """
        {"terms": {"_id": ["product-2", "product-3"]}}
        """;
    JsonNode node = objectMapper.readTree(input);
    JsonNode rewritten = OpenSearchDslIdRewriter.rewrite(node);

    assertThat(rewritten.at("/terms/_id/0").asText()).isEqualTo("{_0=1, _1=product-2}");
    assertThat(rewritten.at("/terms/_id/1").asText()).isEqualTo("{_0=1, _1=product-3}");
  }

  @Test
  public void testRewriteIdsQuery() throws Exception {
    String input =
        """
        {"ids": {"values": ["product-2", "product-3", "product-5"]}}
        """;
    JsonNode node = objectMapper.readTree(input);
    JsonNode rewritten = OpenSearchDslIdRewriter.rewrite(node);

    assertThat(rewritten.at("/ids/values/0").asText()).isEqualTo("{_0=1, _1=product-2}");
    assertThat(rewritten.at("/ids/values/1").asText()).isEqualTo("{_0=1, _1=product-3}");
    assertThat(rewritten.at("/ids/values/2").asText()).isEqualTo("{_0=1, _1=product-5}");
  }

  @Test
  public void testRewriteNestedBoolQuery() throws Exception {
    String input =
        """
        {
          "bool": {
            "must": [
              { "match": { "status": "pending" } }
            ],
            "filter": [
              { "terms": { "_id": ["product-2", "product-3"] } }
            ]
          }
        }
        """;
    JsonNode node = objectMapper.readTree(input);
    JsonNode rewritten = OpenSearchDslIdRewriter.rewrite(node);

    assertThat(rewritten.at("/bool/must/0/match/status").asText()).isEqualTo("pending");
    assertThat(rewritten.at("/bool/filter/0/terms/_id/0").asText())
        .isEqualTo("{_0=1, _1=product-2}");
    assertThat(rewritten.at("/bool/filter/0/terms/_id/1").asText())
        .isEqualTo("{_0=1, _1=product-3}");
  }

  @Test
  public void testSearchAfterNotRewritten() throws Exception {
    String input =
        """
        {
          "size": 20,
          "query": { "match": { "description": "cassandra" } },
          "sort": [
            { "_score": { "order": "desc" } },
            { "_id": { "order": "asc" } }
          ],
          "search_after": [0.87, "{_0=1, _1=doc-abc123}"]
        }
        """;
    JsonNode node = objectMapper.readTree(input);
    JsonNode rewritten = OpenSearchDslIdRewriter.rewrite(node);

    assertThat(rewritten.at("/search_after/1").asText()).isEqualTo("{_0=1, _1=doc-abc123}");
  }

  @Test
  public void testNonIdQueryUnchanged() throws Exception {
    String input =
        """
        {"match": {"title": "Widget"}}
        """;
    JsonNode node = objectMapper.readTree(input);
    JsonNode rewritten = OpenSearchDslIdRewriter.rewrite(node);

    assertThat(rewritten).isEqualTo(node);
  }
}
