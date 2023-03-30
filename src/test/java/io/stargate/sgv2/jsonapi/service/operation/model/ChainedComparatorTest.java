package io.stargate.sgv2.jsonapi.service.operation.model;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.FindOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.ReadDocument;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import java.math.BigDecimal;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import javax.inject.Inject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class ChainedComparatorTest {
  @Inject ObjectMapper objectMapper;

  @Nested
  class Compare {

    @Test
    public void compareBool() {
      Comparator<ReadDocument> comparator =
          new ChainedComparator(List.of(new FindOperation.OrderBy("col", true)), objectMapper);
      // Already ordered
      assertThat(
              comparator.compare(
                  ReadDocument.from(
                      DocumentId.fromString("key1"),
                      UUID.randomUUID(),
                      null,
                      List.of(objectMapper.getNodeFactory().booleanNode(false))),
                  ReadDocument.from(
                      DocumentId.fromString("key2"),
                      UUID.randomUUID(),
                      null,
                      List.of(objectMapper.getNodeFactory().booleanNode(true)))))
          .isLessThan(0);

      // Has to reverse
      assertThat(
              comparator.compare(
                  ReadDocument.from(
                      DocumentId.fromString("key2"),
                      UUID.randomUUID(),
                      null,
                      List.of(objectMapper.getNodeFactory().booleanNode(true))),
                  ReadDocument.from(
                      DocumentId.fromString("key1"),
                      UUID.randomUUID(),
                      null,
                      List.of(objectMapper.getNodeFactory().booleanNode(false)))))
          .isGreaterThan(0);
      // Same value ordered by document id - already ordered
      assertThat(
              comparator.compare(
                  ReadDocument.from(
                      DocumentId.fromString("key1"),
                      UUID.randomUUID(),
                      null,
                      List.of(objectMapper.getNodeFactory().booleanNode(true))),
                  ReadDocument.from(
                      DocumentId.fromString("key2"),
                      UUID.randomUUID(),
                      null,
                      List.of(objectMapper.getNodeFactory().booleanNode(true)))))
          .isLessThan(0);

      // Same value ordered by document id - order reversed
      assertThat(
              comparator.compare(
                  ReadDocument.from(
                      DocumentId.fromString("key2"),
                      UUID.randomUUID(),
                      null,
                      List.of(objectMapper.getNodeFactory().booleanNode(true))),
                  ReadDocument.from(
                      DocumentId.fromString("key1"),
                      UUID.randomUUID(),
                      null,
                      List.of(objectMapper.getNodeFactory().booleanNode(true)))))
          .isGreaterThan(0);
    }

    @Test
    public void compareText() {
      Comparator<ReadDocument> comparator =
          new ChainedComparator(List.of(new FindOperation.OrderBy("col", true)), objectMapper);
      // Already ordered
      assertThat(
              comparator.compare(
                  ReadDocument.from(
                      DocumentId.fromString("key1"),
                      UUID.randomUUID(),
                      null,
                      List.of(objectMapper.getNodeFactory().textNode("abc"))),
                  ReadDocument.from(
                      DocumentId.fromString("key2"),
                      UUID.randomUUID(),
                      null,
                      List.of(objectMapper.getNodeFactory().textNode("xyz")))))
          .isLessThan(0);

      // Has to reverse
      assertThat(
              comparator.compare(
                  ReadDocument.from(
                      DocumentId.fromString("key2"),
                      UUID.randomUUID(),
                      null,
                      List.of(objectMapper.getNodeFactory().textNode("xyz"))),
                  ReadDocument.from(
                      DocumentId.fromString("key1"),
                      UUID.randomUUID(),
                      null,
                      List.of(objectMapper.getNodeFactory().textNode("abc")))))
          .isGreaterThan(0);
      // Same value ordered by document id - already ordered
      assertThat(
              comparator.compare(
                  ReadDocument.from(
                      DocumentId.fromString("key1"),
                      UUID.randomUUID(),
                      null,
                      List.of(objectMapper.getNodeFactory().textNode("abc"))),
                  ReadDocument.from(
                      DocumentId.fromString("key2"),
                      UUID.randomUUID(),
                      null,
                      List.of(objectMapper.getNodeFactory().textNode("abc")))))
          .isLessThan(0);

      // Same value ordered by document id - order reversed
      assertThat(
              comparator.compare(
                  ReadDocument.from(
                      DocumentId.fromString("key2"),
                      UUID.randomUUID(),
                      null,
                      List.of(objectMapper.getNodeFactory().textNode("abc"))),
                  ReadDocument.from(
                      DocumentId.fromString("key1"),
                      UUID.randomUUID(),
                      null,
                      List.of(objectMapper.getNodeFactory().textNode("abc")))))
          .isGreaterThan(0);
    }

    @Test
    public void compareNumber() {
      Comparator<ReadDocument> comparator =
          new ChainedComparator(List.of(new FindOperation.OrderBy("col", true)), objectMapper);
      // Already ordered
      assertThat(
              comparator.compare(
                  ReadDocument.from(
                      DocumentId.fromString("key1"),
                      UUID.randomUUID(),
                      null,
                      List.of(objectMapper.getNodeFactory().numberNode(new BigDecimal(1)))),
                  ReadDocument.from(
                      DocumentId.fromString("key2"),
                      UUID.randomUUID(),
                      null,
                      List.of(objectMapper.getNodeFactory().numberNode(new BigDecimal(2))))))
          .isLessThan(0);

      // Has to reverse
      assertThat(
              comparator.compare(
                  ReadDocument.from(
                      DocumentId.fromString("key2"),
                      UUID.randomUUID(),
                      null,
                      List.of(objectMapper.getNodeFactory().numberNode(new BigDecimal(2)))),
                  ReadDocument.from(
                      DocumentId.fromString("key1"),
                      UUID.randomUUID(),
                      null,
                      List.of(objectMapper.getNodeFactory().numberNode(new BigDecimal(1))))))
          .isGreaterThan(0);
      // Same value ordered by document id - already ordered
      assertThat(
              comparator.compare(
                  ReadDocument.from(
                      DocumentId.fromString("key1"),
                      UUID.randomUUID(),
                      null,
                      List.of(objectMapper.getNodeFactory().numberNode(new BigDecimal(1)))),
                  ReadDocument.from(
                      DocumentId.fromString("key2"),
                      UUID.randomUUID(),
                      null,
                      List.of(objectMapper.getNodeFactory().numberNode(new BigDecimal(1))))))
          .isLessThan(0);

      // Same value ordered by document id - order reversed
      assertThat(
              comparator.compare(
                  ReadDocument.from(
                      DocumentId.fromString("key2"),
                      UUID.randomUUID(),
                      null,
                      List.of(objectMapper.getNodeFactory().numberNode(new BigDecimal(1)))),
                  ReadDocument.from(
                      DocumentId.fromString("key1"),
                      UUID.randomUUID(),
                      null,
                      List.of(objectMapper.getNodeFactory().numberNode(new BigDecimal(1))))))
          .isGreaterThan(0);
    }

    @Test
    public void compareDifferentTypes() {
      Comparator<ReadDocument> comparator =
          new ChainedComparator(List.of(new FindOperation.OrderBy("col", true)), objectMapper);
      // Compare different data type boolean and text
      assertThat(
              comparator.compare(
                  ReadDocument.from(
                      DocumentId.fromString("key2"),
                      UUID.randomUUID(),
                      null,
                      List.of(objectMapper.getNodeFactory().booleanNode(true))),
                  ReadDocument.from(
                      DocumentId.fromString("key1"),
                      UUID.randomUUID(),
                      null,
                      List.of(objectMapper.getNodeFactory().textNode("abc")))))
          .isGreaterThan(0);

      // Compare different data type null and number
      assertThat(
              comparator.compare(
                  ReadDocument.from(
                      DocumentId.fromString("key2"),
                      UUID.randomUUID(),
                      null,
                      List.of(objectMapper.getNodeFactory().nullNode())),
                  ReadDocument.from(
                      DocumentId.fromString("key1"),
                      UUID.randomUUID(),
                      null,
                      List.of(objectMapper.getNodeFactory().numberNode(new BigDecimal(1))))))
          .isLessThan(0);

      // Compare different data type missing data and number
      assertThat(
              comparator.compare(
                  ReadDocument.from(
                      DocumentId.fromString("key1"),
                      UUID.randomUUID(),
                      null,
                      List.of(objectMapper.getNodeFactory().numberNode(new BigDecimal(1)))),
                  ReadDocument.from(
                      DocumentId.fromString("key2"),
                      UUID.randomUUID(),
                      null,
                      List.of(objectMapper.getNodeFactory().missingNode()))))
          .isGreaterThan(0);

      // Compare different data type missing data and null - ordered by document id - order reversed
      assertThat(
              comparator.compare(
                  ReadDocument.from(
                      DocumentId.fromString("key2"),
                      UUID.randomUUID(),
                      null,
                      List.of(objectMapper.getNodeFactory().nullNode())),
                  ReadDocument.from(
                      DocumentId.fromString("key1"),
                      UUID.randomUUID(),
                      null,
                      List.of(objectMapper.getNodeFactory().missingNode()))))
          .isGreaterThan(0);
    }
  }
}
