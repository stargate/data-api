package io.stargate.sgv2.jsonapi.service.operation.tasks;

import static org.assertj.core.api.Assertions.assertThat;

import io.stargate.sgv2.jsonapi.service.schema.tables.TableSchemaObject;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

public class TaskGroupTest {

  private static TableSchemaObject mockTable() {
    return BaseTaskAssertions.mockTable("ks", "tbl");
  }

  private static BaseTaskTestTask okTask(int position) {
    return new BaseTaskTestTask(position, mockTable(), TaskRetryPolicy.NO_RETRY);
  }

  private static BaseTaskTestTask errorTask(int position, String name) {
    var t = new BaseTaskTestTask(position, mockTable(), TaskRetryPolicy.NO_RETRY);
    t.maybeAddFailure(new RuntimeException("Exception for task '" + name + "'"));
    return t;
  }

  @Test
  public void failFastWithErrorInTheMiddle() {
    TaskGroup<BaseTaskTestTask, TableSchemaObject> group = new TaskGroup<>(true);

    List<BaseTaskTestTask> tasks = new ArrayList<>();
    tasks.add(okTask(0)); // a
    tasks.add(okTask(1)); // b
    tasks.add(errorTask(2, "c")); // c error
    tasks.add(okTask(3)); // d
    tasks.add(okTask(4)); // e
    group.addAll(tasks);

    assertThat(group.shouldFailFast(tasks.get(0))).isFalse(); // a
    assertThat(group.shouldFailFast(tasks.get(1))).isFalse(); // b
    assertThat(group.shouldFailFast(tasks.get(2))).isTrue(); // c
    assertThat(group.shouldFailFast(tasks.get(3))).isTrue(); // d
    assertThat(group.shouldFailFast(tasks.get(4))).isTrue(); // e
  }

  @Test
  public void failFastWithMultipleError() {
    TaskGroup<BaseTaskTestTask, TableSchemaObject> group = new TaskGroup<>(true);

    List<BaseTaskTestTask> tasks = new ArrayList<>();
    tasks.add(okTask(0)); // a
    tasks.add(errorTask(1, "b")); // b error
    tasks.add(okTask(2)); // c
    tasks.add(errorTask(3, "d")); // d error
    tasks.add(okTask(4)); // e
    group.addAll(tasks);

    assertThat(group.shouldFailFast(tasks.get(0))).isFalse(); // a
    assertThat(group.shouldFailFast(tasks.get(1))).isTrue(); // b
    assertThat(group.shouldFailFast(tasks.get(2))).isTrue(); // c
    assertThat(group.shouldFailFast(tasks.get(3))).isTrue(); // d
    assertThat(group.shouldFailFast(tasks.get(4))).isTrue(); // e
  }

  @Test
  public void failFastWithErrorInTheBeginning() {
    TaskGroup<BaseTaskTestTask, TableSchemaObject> group = new TaskGroup<>(true);

    List<BaseTaskTestTask> tasks = new ArrayList<>();
    tasks.add(errorTask(0, "a")); // a error
    tasks.add(okTask(1)); // b
    tasks.add(okTask(2)); // c
    tasks.add(okTask(3)); // d
    tasks.add(okTask(4)); // e
    group.addAll(tasks);

    assertThat(group.shouldFailFast(tasks.get(0))).isTrue(); // a
    assertThat(group.shouldFailFast(tasks.get(1))).isTrue(); // b
    assertThat(group.shouldFailFast(tasks.get(2))).isTrue(); // c
    assertThat(group.shouldFailFast(tasks.get(3))).isTrue(); // d
    assertThat(group.shouldFailFast(tasks.get(4))).isTrue(); // e
  }

  @Test
  public void failFastWithErrorInTheEnd() {
    TaskGroup<BaseTaskTestTask, TableSchemaObject> group = new TaskGroup<>(true);

    List<BaseTaskTestTask> tasks = new ArrayList<>();
    tasks.add(okTask(0)); // a
    tasks.add(okTask(1)); // b
    tasks.add(okTask(2)); // c
    tasks.add(okTask(3)); // d
    tasks.add(errorTask(4, "e")); // e error
    group.addAll(tasks);

    assertThat(group.shouldFailFast(tasks.get(0))).isFalse(); // a
    assertThat(group.shouldFailFast(tasks.get(1))).isFalse(); // b
    assertThat(group.shouldFailFast(tasks.get(2))).isFalse(); // c
    assertThat(group.shouldFailFast(tasks.get(3))).isFalse(); // d
    assertThat(group.shouldFailFast(tasks.get(4))).isTrue(); // e
  }

  @Test
  public void parallelProcessing_noFailFastWithErrorInTheMiddle() {
    TaskGroup<BaseTaskTestTask, TableSchemaObject> group = new TaskGroup<>(false);

    List<BaseTaskTestTask> tasks = new ArrayList<>();
    tasks.add(okTask(0)); // a
    tasks.add(okTask(1)); // b
    tasks.add(errorTask(2, "c")); // c error
    tasks.add(okTask(3)); // d
    tasks.add(okTask(4)); // e
    group.addAll(tasks);

    assertThat(group.shouldFailFast(tasks.get(0))).isFalse(); // a
    assertThat(group.shouldFailFast(tasks.get(1))).isFalse(); // b
    assertThat(group.shouldFailFast(tasks.get(2))).isFalse(); // c (still false in parallel)
    assertThat(group.shouldFailFast(tasks.get(3))).isFalse(); // d
    assertThat(group.shouldFailFast(tasks.get(4))).isFalse(); // e
  }

  @Test
  public void parallelProcessing_noFailFastWithMultipleErrors() {
    TaskGroup<BaseTaskTestTask, TableSchemaObject> group =
        new TaskGroup<>(); // defaults to parallel

    List<BaseTaskTestTask> tasks = new ArrayList<>();
    tasks.add(okTask(0)); // a
    tasks.add(errorTask(1, "b")); // b error
    tasks.add(okTask(2)); // c
    tasks.add(errorTask(3, "d")); // d error
    tasks.add(okTask(4)); // e
    group.addAll(tasks);

    assertThat(group.shouldFailFast(tasks.get(0))).isFalse(); // a
    assertThat(group.shouldFailFast(tasks.get(1))).isFalse(); // b
    assertThat(group.shouldFailFast(tasks.get(2))).isFalse(); // c
    assertThat(group.shouldFailFast(tasks.get(3))).isFalse(); // d
    assertThat(group.shouldFailFast(tasks.get(4))).isFalse(); // e
  }
}
