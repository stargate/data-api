package io.stargate.sgv2.jsonapi.service.operation;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.*;
import java.util.Objects;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link Operation} for running any type of {@link OperationAttempt} grouped into a {@link
 * OperationAttemptContainer}.
 *
 * <p>
 *
 * @param <SchemaT> The type of the schema object that the operation is working with.
 * @param <AttemptT> The type of the operation attempt that the operation is working with.
 */
public class GenericOperation<
        SchemaT extends SchemaObject, AttemptT extends OperationAttempt<AttemptT, SchemaT>>
    implements Operation {

  private static final Logger LOGGER = LoggerFactory.getLogger(GenericOperation.class);

  private final DefaultDriverExceptionHandler.Factory<SchemaT> exceptionHandlerFactory;
  private final OperationAttemptContainer<SchemaT, AttemptT> attempts;
  private final OperationAttemptPageBuilder<SchemaT, AttemptT> pageBuilder;

  /**
   * Create a new {@link GenericOperation} with the provided {@link OperationAttemptContainer},
   *
   * @param attempts The attempts to run, grouped into a container that has config about how to run
   *     them as a group.
   * @param pageBuilder The builder to use for creating the {@link CommandResult} from the attempts.
   * @param exceptionHandlerFactory Factory to create the handler to use for exceptions thrown by
   *     the driver, exceptions thrown by the driver are passed through here before being added to
   *     the {@link OperationAttempt}.
   */
  public GenericOperation(
      OperationAttemptContainer<SchemaT, AttemptT> attempts,
      OperationAttemptPageBuilder<SchemaT, AttemptT> pageBuilder,
      DefaultDriverExceptionHandler.Factory<SchemaT> exceptionHandlerFactory) {

    this.attempts = Objects.requireNonNull(attempts, "attempts cannot be null");
    this.pageBuilder = Objects.requireNonNull(pageBuilder, "pageBuilder cannot be null");
    this.exceptionHandlerFactory =
        Objects.requireNonNull(exceptionHandlerFactory, "exceptionHandlerFactory cannot be null");
  }

  /**
   * Execute the attempts passed to the operation using the provided {@link QueryExecutor}.
   *
   * <p>This is a generic operation that can be used to execute any type of attempts, the attempts
   * are executed using the configuration of the supplied {@link OperationAttemptContainer} and the
   * {@link OperationAttempt} itself. The results are grouped using supplied {@link
   * OperationAttemptPageBuilder}, which created the {@link CommandResult}. Errors when executing
   * the attempts are caught and attached to the {@link OperationAttempt} so the Page Builder can
   * include them in the response.
   *
   * @param dataApiRequestInfo Request information used to get the tenantId and token
   * @param queryExecutor The {@link QueryExecutor} to use for executing the attempts
   * @return A supplier of {@link CommandResult} that represents the result of running all the
   *     attempts.
   */
  @Override
  public Uni<Supplier<CommandResult>> execute(
      DataApiRequestInfo dataApiRequestInfo, QueryExecutor queryExecutor) {

    LOGGER.debug("execute() - starting to process attempts={}", attempts);

    return startMulti(dataApiRequestInfo, queryExecutor)
        .collect()
        .in(() -> pageBuilder, OperationAttemptAccumulator::accumulate)
        .onItem()
        .invoke(() -> LOGGER.debug("execute() - finished processing attempts={}", attempts))
        .onItem()
        .invoke(attempts::throwIfNotAllTerminal)
        .onItem()
        .transform(OperationAttemptPageBuilder::getOperationPage);
  }

  /**
   * Start a {@link Multi} for processing the {@link #attempts}, the style of multi depends on the
   * attempts configuration.
   *
   * @return a {@link Multi} that emits {@link AttemptT} according to the attempts configuration.
   */
  protected Multi<AttemptT> startMulti(
      DataApiRequestInfo dataApiRequestInfo, QueryExecutor queryExecutor) {

    // TODO - for now we create the CommandQueryExecutor here , later change the Operation interface
    var commandQueryExecutor =
        new CommandQueryExecutor(
            queryExecutor.getCqlSessionCache(),
            new CommandQueryExecutor.RequestContext(
                dataApiRequestInfo.getTenantId(), dataApiRequestInfo.getCassandraToken()),
            CommandQueryExecutor.QueryTarget.TABLE);

    // Common start pattern for all operations
    var attemptMulti = Multi.createFrom().iterable(attempts).onItem();

    if (attempts.getSequentialProcessing()) {
      // We want to process the attempts sequentially, and stop processing the attempts if one fails
      // This should not cause the multi to emit a failure, we track the failures in the attempts
      // (transformToUniAndConcatenate is for sequential processing)

      return attemptMulti.transformToUniAndConcatenate(
          attempt -> {
            var failFast = attempts.shouldFailFast();
            LOGGER.debug(
                "startMulti() - dequeuing attempt for sequential processing, failFast={}, attempt={}",
                failFast,
                attempt);

            if (failFast) {
              // Stop processing attemps, but we do not want to return a UniFailure, so we set the
              // attempt to skipped
              // and do not call exectute() on it.
              return Uni.createFrom().item(attempt.setSkippedIfReady());
            }
            return attempt.execute(commandQueryExecutor, exceptionHandlerFactory);
          });
    }

    // Parallel processing using transformToUniAndMerge() - no extra testing.
    return attemptMulti.transformToUniAndMerge(
        readAttempt -> readAttempt.execute(commandQueryExecutor, exceptionHandlerFactory));
  }
}
