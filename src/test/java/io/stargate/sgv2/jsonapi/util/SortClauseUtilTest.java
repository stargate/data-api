package io.stargate.sgv2.jsonapi.util;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.FindOperation;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import java.util.List;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class SortClauseUtilTest {
  @Nested
  class HappyPathSortClauseUtil {
    @Test
    public void emptySort() {
      final List<FindOperation.OrderBy> orderBy = SortClauseUtil.resolveOrderBy(null);
      assertThat(orderBy).isNull();
    }

    @Test
    public void happyPath() {
      SortClause sortClause =
          new SortClause(
              List.of(SortExpression.sort("col1", true), SortExpression.sort("col2", false)));
      final List<FindOperation.OrderBy> orderBy = SortClauseUtil.resolveOrderBy(sortClause);
      assertThat(orderBy)
          .isNotNull()
          .satisfies(
              order -> {
                assertThat(order).hasSize(2);
                assertThat(order.get(0).column()).isEqualTo("col1");
                assertThat(order.get(0).ascending()).isTrue();
                assertThat(order.get(1).column()).isEqualTo("col2");
                assertThat(order.get(1).ascending()).isFalse();
              });
    }

    @Test
    public void happyPathVectorSearch() {
      SortClause sortClause =
          new SortClause(List.of(SortExpression.vsearch(new float[] {0.11f, 0.22f, 0.33f})));
      final float[] vsearch = SortClauseUtil.resolveVsearch(sortClause);
      assertThat(vsearch)
          .isNotNull()
          .satisfies(
              order -> {
                assertThat(order).hasSize(3);
                assertThat(order[0]).isEqualTo(0.11f);
                assertThat(order[1]).isEqualTo(0.22f);
                assertThat(order[2]).isEqualTo(0.33f);
              });
    }
  }
}
