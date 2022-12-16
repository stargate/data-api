package io.stargate.sgv3.docsapi.operations;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv3.docsapi.bridge.query.QueryExecutor;
import io.stargate.sgv3.docsapi.commands.CommandContext;
import io.stargate.sgv3.docsapi.shredding.WritableShreddedDocument;
import java.util.ArrayList;

/** Find and update a document. */
public class UpdateOperation extends ModifyOperation {

  // The operation that returns the documents to be updated
  private final ReadOperation readOp;
  private DocumentUpdaterFunction documentUpdater;

  /** What the update command should project into the {@link OperationResult} */
  public enum UpdateProjection {
    NONE, // default, no projection
    ORIGINAL, // the original documents
    MODIFIED // modified and updserted (if specified) documents
  }

  private final UpdateProjection updateProjection;

  public UpdateOperation(
      CommandContext commandContext,
      ReadOperation readOp,
      DocumentUpdaterFunction documentUpdater,
      UpdateProjection updateProjection) {
    super(commandContext);
    this.readOp = readOp;
    this.documentUpdater = documentUpdater;
    this.updateProjection = updateProjection;
  }

  /**
   * Need to override because this command is composed of others, we dont have an implementation for
   * executeInternal
   */
  @Override
  public Uni<OperationResult> execute(QueryExecutor queryExecutor) {

    // First, we get all of the documents we want to update
    Uni<OperationResult> readOpResult = readOp.execute(queryExecutor);
    final Uni<ModifyOperationPage> updatedResponse =
        readOpResult
            .onItem()
            .transform(readRes -> documentUpdater.updateDocuments(readRes.docs))
            .onItem()
            .transformToUni(
                documentUpdater -> {
                  if (documentUpdater.upserted().isEmpty()) {
                    final ConditionalInsertOperation conditionalInsertOperation =
                        new ConditionalInsertOperation(
                            getCommandContext(), documentUpdater.modified());
                    return conditionalInsertOperation.executeInternal(queryExecutor);
                  } else {
                    throw new RuntimeException("Upserted documents not supported");
                  }
                });
    return updatedResponse
        .onItem()
        .transform(
            response -> {
              OperationResult.Builder resultBuilder =
                  OperationResult.builder()
                      .withMatchedCount(response.insertedIds.size() + response.updatedIds.size())
                      .withInsertedIds(response.insertedIds)
                      .withUpdatedIds(response.updatedIds);
              switch (updateProjection) {
                case NONE:
                  // nothing to do
                  break;
                case ORIGINAL:
                  // TODO: not thought about updating multiple docs, am guessing page state is
                  // always null
                  // resultBuilder.withDocs(response.docs, null);
                  break;
                case MODIFIED:
                  // TODO, assuming upserting is caught above, this will be ok and work when we have
                  // it
                  var allDocs =
                      new ArrayList<WritableShreddedDocument>(
                          response.insertedIds.size() + response.updatedIds.size());
                  allDocs.addAll(response.insertedDocs);
                  allDocs.addAll(response.updatedDocs);
                  resultBuilder.withDocs(allDocs, null);
                  break;
              }
              return resultBuilder.build();
            });
  }

  @Override
  protected Uni<ModifyOperationPage> executeInternal(QueryExecutor queryExecutor) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public OperationPlan getPlan() {
    // this operation has in memory work, is not fully push down
    return new OperationPlan(false);
  }
}
