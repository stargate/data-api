package io.stargate.sgv2.jsonapi.service.resolver.model.impl.matcher;

import static org.assertj.core.api.Assertions.assertThat;

import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterOperation;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonLiteral;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonType;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ValueComparisonOperation;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ValueComparisonOperator;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

public class CaptureGroupTest {
  @Nested
  class CaptureGroupCreator {

    @Test
    public void withCaptureToAddCaptures() throws Exception {
      List<FilterOperation> filters =
          List.of(
              new ValueComparisonOperation(
                  ValueComparisonOperator.EQ, new JsonLiteral("abc", JsonType.STRING)));
      Map.Entry<String, List<FilterOperation>> expected =
          new AbstractMap.SimpleEntry<String, List<FilterOperation>>("test", filters);
      CaptureGroup captureGroup = new CaptureGroup(new HashMap<>());
      captureGroup.withCapture("test", filters);

      assertThat(captureGroup.captures().entrySet()).contains(expected);
    }
  }

  @Nested
  class CaptureGroupDBClause {

    @Test
    public void addAllCaptures() throws Exception {
      List<FilterOperation<String>> filters =
          List.of(
              new ValueComparisonOperation(
                  ValueComparisonOperator.EQ, new JsonLiteral("val1", JsonType.STRING)),
              new ValueComparisonOperation(
                  ValueComparisonOperator.EQ, new JsonLiteral("val2", JsonType.STRING)));
      CaptureGroup<String> captureGroup =
          new CaptureGroup(new HashMap<String, List<FilterOperation<String>>>());
      captureGroup.withCapture("test", filters);
      final List<String> response = new ArrayList<>();
      String expected1 = "test:EQ:val1";
      String expected2 = "test:EQ:val2";
      captureGroup.consumeAllCaptures(
          consumer ->
              response.add(
                  consumer.path() + ":" + consumer.operator().toString() + ":" + consumer.value()));
      assertThat(response).contains(expected1, expected2);
    }
  }
}
