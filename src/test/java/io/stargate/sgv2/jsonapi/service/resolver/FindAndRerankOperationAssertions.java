package io.stargate.sgv2.jsonapi.service.resolver;

import static org.assertj.core.api.Assertions.assertThat;

import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCommand;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.reranking.IntermediateCollectionReadTask;
import io.stargate.sgv2.jsonapi.service.operation.reranking.Rank;
import io.stargate.sgv2.jsonapi.service.operation.reranking.RerankingTask;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads the tasks of an operation built by {@link FindAndRerankOperationBuilder}, for the builder
 * tests. The task tree is private, so this reads it with reflection.
 */
final class FindAndRerankOperationAssertions {

  private FindAndRerankOperationAssertions() {}

  /**
   * What the builder put on a read task, at build time.
   *
   * @param position The index of the leg in the plan. It is also the index of the deferred read of
   *     the reranking task that this read task fills, {@link #assertReadTasks(Operation)} checks
   *     that.
   * @param sortPaths The sort paths of the inner find, empty for a vectorize read until the vector
   *     is ready.
   * @param limit The limit of the inner find.
   * @param includeSimilarity The <code>includeSimilarity</code> option of the inner find.
   * @param includeSortVector The <code>includeSortVector</code> option of the inner find.
   * @param hasDeferredVectorize True if the read waits for a vectorize.
   * @param rankSource The rank source of the deferred read the task fills.
   */
  record ReadTaskSnapshot(
      int position,
      List<String> sortPaths,
      int limit,
      boolean includeSimilarity,
      boolean includeSortVector,
      boolean hasDeferredVectorize,
      Rank.RankSource rankSource) {}

  /**
   * The inner task groups of the operation, in the order they run: the operation runs one composite
   * task per group.
   */
  static List<List<?>> innerTaskGroups(Operation<CollectionSchemaObject> operation)
      throws ReflectiveOperationException {
    var groups = new ArrayList<List<?>>();
    for (var compositeTask : (List<?>) privateField(operation, "taskGroup")) {
      groups.add((List<?>) privateField(compositeTask, "innerTaskGroup"));
    }
    return groups;
  }

  /**
   * Asserts there is one read task per deferred read of the reranking task, and that the read task
   * at position i fills the deferred read at index i.
   *
   * @return The read tasks of the operation, in the order they were added to their group.
   */
  static List<ReadTaskSnapshot> assertReadTasks(Operation<CollectionSchemaObject> operation)
      throws ReflectiveOperationException {
    var deferredReads = assertRerankDeferredReads(operation);
    var readTasks = new ArrayList<ReadTaskSnapshot>();
    for (var group : innerTaskGroups(operation)) {
      for (var task : group) {
        if (task instanceof IntermediateCollectionReadTask readTask) {
          var deferredAndSource = deferredReads.get(readTask.position());
          assertThat(privateField(readTask, "commandResultAction"))
              .as("read task %s fills the deferred read at the same index", readTask.position())
              .isSameAs(deferredAndSource.deferredRead().deferredAction());

          var findCommand = (FindCommand) privateField(readTask, "findCommand");
          readTasks.add(
              new ReadTaskSnapshot(
                  readTask.position(),
                  findCommand.sortDefinition().getSortExpressionPaths(),
                  findCommand.options().limit(),
                  findCommand.options().includeSimilarity(),
                  findCommand.options().includeSortVector(),
                  privateField(readTask, "deferredVectorize") != null,
                  deferredAndSource.rankSource()));
        }
      }
    }
    assertThat(readTasks).as("one read task per deferred read").hasSameSizeAs(deferredReads);
    return readTasks;
  }

  /**
   * Asserts the last group has only the reranking task.
   *
   * @return The deferred reads of the reranking task.
   */
  @SuppressWarnings("unchecked")
  static List<RerankingTask.DeferredCommandWithSource> assertRerankDeferredReads(
      Operation<CollectionSchemaObject> operation) throws ReflectiveOperationException {
    var lastGroup = innerTaskGroups(operation).getLast();
    assertThat(lastGroup).as("rerank group").hasSize(1);
    var rerankTask = lastGroup.getFirst();
    assertThat(rerankTask).isInstanceOf(RerankingTask.class);
    return (List<RerankingTask.DeferredCommandWithSource>)
        privateField(rerankTask, "deferredReads");
  }

  static Object privateField(Object target, String name) throws ReflectiveOperationException {
    var field = target.getClass().getDeclaredField(name);
    field.setAccessible(true);
    return field.get(target);
  }
}
