package io.stargate.sgv2.jsonapi.util;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.FindOperation;
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
              List.of(new SortExpression("col1", true), new SortExpression("col2", false)));
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
  }
}
