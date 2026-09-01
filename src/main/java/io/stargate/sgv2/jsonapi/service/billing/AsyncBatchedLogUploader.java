package io.stargate.sgv2.jsonapi.service.billing;

import io.smallrye.mutiny.Uni;

/** A function that uploads a batch of log records, normally to S3. */
@FunctionalInterface
public interface AsyncBatchedLogUploader extends AutoCloseable {

  /**
   * Called to upload the batch of records.
   *
   * @param batch The batch of log records to upload
   * @return A Uni of the result of the operation
   */
  Uni<UploadResult> upload(BatchedLogBuffer.Batch batch);

  @Override
  default void close() {}

  /**
   * Result of the upload call.
   *
   * @param success <code>true</code> if the operation succeeded, false otherwise.
   * @param throwable The throwable associated with an error state.
   * @param batch The batch that was uploaded, or attempted to be uploaded.
   */
  record UploadResult(boolean success, Throwable throwable, BatchedLogBuffer.Batch batch) {}
}
