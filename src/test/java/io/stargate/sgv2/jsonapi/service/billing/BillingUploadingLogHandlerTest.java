package io.stargate.sgv2.jsonapi.service.billing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

import io.smallrye.mutiny.Uni;
import java.util.List;
import java.util.logging.LogRecord;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.verification.VerificationMode;

/** */
public class BillingUploadingLogHandlerTest extends BillingTestBase {

  // *********************************************************
  // Handler interface - Producer side of the handler
  // *********************************************************

  /** Null record silently dropped by handler */
  @Test
  public void publishSilentDropNulls() {

    var fixture = defaultLogHandlerFixture();

    fixture.logHandler().publish(null);
    fixture.logHandler().publish(null);
    fixture.logHandler().publish(null);

    verify(
            fixture.buffer(),
            times(0).description("publishSilentDropNulls() - no calls to buffer.offer()"))
        .offer(any());
  }

  /** Non null record silently dropped by handler when closed */
  @Test
  public void publishSilentDropWhenClosed() {

    var fixture = defaultLogHandlerFixture();
    fixture.logHandler().close();

    fixture.logHandler().publish(fixture.logRecords().getFirst());
    fixture.logHandler().publish(fixture.logRecords().getFirst());
    fixture.logHandler().publish(fixture.logRecords().getFirst());

    verify(
            fixture.buffer(),
            times(0).description("publishSilentDropWhenClosed() - no calls to buffer.offer()"))
        .offer(any());
  }

  /** Records published when buffer is full are dropped, no error */
  @Test
  public void publishSilentWhenBufferFull() {

    var fixture = defaultLogHandlerFixture();

    // return false, buffer full , go away
    when(fixture.buffer().offer(any())).thenReturn(false);

    fixture.logHandler().publish(fixture.logRecords().getFirst());
    fixture.logHandler().publish(fixture.logRecords().getFirst());
    fixture.logHandler().publish(fixture.logRecords().getFirst());

    verify(
            fixture.buffer(),
            times(3).description("publishSilentWhenBufferFull() - called offer for each record"))
        .offer(fixture.logRecords().getFirst());
  }

  /** Passing record to handler, is then passed to the buffer. */
  @Test
  public void publishOfferSucceed() {

    var fixture = defaultLogHandlerFixture();
    var slice = Slice.to(MAX_BATCH_BYTES_NUM_MESSAGES);
    var expectedLogRecords = slice.stream(fixture.logRecords()).toList();

    var argCaptor = ArgumentCaptor.forClass(LogRecord.class);

    for (var record : expectedLogRecords) {
      fixture.logHandler().publish(record);
    }

    verify(
            fixture.buffer(),
            times(expectedLogRecords.size())
                .description("publishOfferSucceed() - buffer called for each log record"))
        .offer(argCaptor.capture());
    var actualLogRecords = argCaptor.getAllValues();

    assertThat(actualLogRecords)
        .as("publishOfferSucceed() - all and only expected records passed to the buffer")
        .containsExactlyElementsOf(expectedLogRecords);
  }

  /** Calling flush on handler that is NOT uploading does nothing */
  @Test
  public void flushNullOpIfNotStarted() {

    var fixture = defaultLogHandlerFixture();

    fixture.logHandler().flush();
    // there is no uploading, so should not ask for next batch
    verify(fixture.buffer(), never()).nextBatch(anyBoolean());
  }

  /**
   * Verify the number of times a function was called, but with a timeout to wait. e.g. when waiting
   * for the uploading thread to wakeup
   */
  private VerificationMode timeoutTimes(String desc, int times) {
    return timeout(2000).times(times).description(desc);
  }

  /** Calling flush on handler that is uploading causes handler to check buffer. */
  @Test
  public void flushChecksForBatch() {

    var fixture1 = defaultLogHandlerFixture();
    try (var handlerThread =
        fixture1.startHandlerUploading("flushChecksForBatch() - close not called")) {
      fixture1.logHandler().flush();
      // close has not been called, so it should not drain
      verify(fixture1.buffer(), timeoutTimes("nextBatch() called once with drain false", 1))
          .nextBatch(false);

      // this is a bit stupid, calling close to close the upload thread
      fixture1.logHandler().close();
    }

    var fixture2 = defaultLogHandlerFixture();
    try (var handlerThread =
        fixture2.startHandlerUploading("flushChecksForBatch() - close is called")) {
      fixture2.logHandler().unsafeClose();
      fixture2.logHandler().flush();
      // close has been called, so it should drain buffer
      verify(fixture2.buffer(), timeoutTimes("nextBatch() called once with drain true", 1))
          .nextBatch(true);
    }
  }

  @Test
  public void closeWithoutUploadThreadReturns() {

    var fixture1 = defaultLogHandlerFixture();
    // NOT STARTING upload
    fixture1.logHandler().close();
    // upload not running, should not try to get a batch
    verify(fixture1.buffer(), timeoutTimes("nextBatch() never called", 0)).nextBatch(anyBoolean());
    // should have closed the uploader
    verify(fixture1.uploader(), timeoutTimes("uploader.close() called", 1)).close();
  }

  /**
   * Call close, but the upload thread has not released the upload permit, so close cannot detect
   * upload has finished.
   */
  @Test
  public void closeReturnsWhenUploadUnstopped() {

    var fixture1 = defaultLogHandlerFixture(true, false, true);
    // NOT STARTING upload, but acquire the permit it would take
    fixture1.logHandler().unsafeAcquireUploadPermit();
    // close() will not return until it times out waiting for the upload thread to finish
    fixture1.logHandler().close();
  }

  /**
   * Calling close() when the handler is running should cause the buffer to be called to drain it.
   */
  @Test
  public void closeCausesBufferDrain() {

    var fixture1 = defaultLogHandlerFixture();
    try (var handlerThread = fixture1.startHandlerUploading("closeCausesBufferDrain()")) {
      threadSleep(100); // give the uploader time to get into the wait on wakeup

      fixture1.logHandler().close();
      // close has  been called, so it should drain
      verify(fixture1.buffer(), timeoutTimes("nextBatch() called with drain=true", 1))
          .nextBatch(true);
      verify(fixture1.uploader(), timeoutTimes("uploader.close() called", 1)).close();
    }
    // the auto closable will wait for the upload thread to naturally exit
  }

  // *********************************************************
  // startUploading - Consumer side of the handler
  // *********************************************************

  /**
   * Calling startUpLoad twice on different threads, fails because there can be only one active
   * thread running the function
   *
   * <p>Cannot call on same thread as it will be parked running the upload
   */
  @Test
  public void startUploadingCalledTwiceFails() {

    var fixture1 = defaultLogHandlerFixture();
    try (var handlerThread1 =
        fixture1.startHandlerUploading("startUploadingCalledTwiceFails() - 1st")) {
      // make sure the worker thread has time to start
      threadSleep(10);

      // the exception will happen when startUploading is entered, but we
      // wont get the error until calling close() which calls Future.get()
      var closable = fixture1.startHandlerUploading("startUploadingCalledTwiceFails() - 2nd");
      // make sure the worker thread has time to start
      threadSleep(10);

      assertThatThrownBy(closable::close, "startUploadingCalledTwiceFails() - second call")
          .isInstanceOf(IllegalStateException.class);

      // stop the first thread that is running startUpload()
      fixture1.logHandler().close();
    }
  }

  /** In normal operation startUpload detects three batches and sends to uploader */
  @Test
  public void startUploadingSendsToUploader() {

    var NUM_BATCHES = 3;
    var fixture1 = defaultLogHandlerFixture();
    try (var handlerThread1 =
        fixture1.startHandlerUploading("startUploadingSendsToUploader() - upload thread")) {
      // make sure the worker thread has time to start and get to the sleep.
      threadSleep(100);

      // ** TESTING NORMAL OPERATION

      // setup buffer to return three batches we will collect in normal operations
      var expectedNormalBatches = mockUploading(fixture1, NUM_BATCHES, false);
      // handler should be sleeping because of long sleep, flush will wake it up.
      fixture1.logHandler().flush();
      // wait for it the handler to call the uploader
      var normalCaptor = ArgumentCaptor.forClass(BatchedLogBuffer.Batch.class);
      verify(
              fixture1.uploader(),
              timeoutTimes(
                  "startUploadingSendsToUploader() - normal mode", expectedNormalBatches.size()))
          .upload(normalCaptor.capture());
      var actualNormalBatches = normalCaptor.getAllValues();

      // ** TESTING CLOSE / SHUTDOWN OPERATION

      // reset counter , will already be NUM_BATCHES from the normal operation check above
      clearInvocations(fixture1.uploader());
      // we are using the long upload sleep, uploader should be asleep again, send batches
      // and close to see we get correct behavior
      var expectedShutdownBatches = mockUploading(fixture1, NUM_BATCHES, true);
      // close to get shutdown operations
      fixture1.logHandler().close();
      // wait for it the handler to call the uploader
      var shutdownCaptor = ArgumentCaptor.forClass(BatchedLogBuffer.Batch.class);
      verify(
              fixture1.uploader(),
              timeoutTimes(
                  "startUploadingSendsToUploader() - shutdown mode",
                  expectedShutdownBatches.size()))
          .upload(shutdownCaptor.capture());
      var actualShutdownBatches = shutdownCaptor.getAllValues();

      assertThat(actualNormalBatches)
          .as("startUploadingSendsToUploader() - batches from normal operation match")
          .containsExactlyElementsOf(expectedNormalBatches);

      assertThat(actualShutdownBatches)
          .as("startUploadingSendsToUploader() - batches from shutdown operation match")
          .containsExactlyElementsOf(expectedShutdownBatches);

      // we have closed handler, should exit block now
    }
  }

  private List<BatchedLogBuffer.Batch> mockUploading(
      Fixture fixture, int numBatches, boolean expectDrainFully) {
    // by default the mock buffer will be returning null for nextBatch(), the
    // log handler should be in a wait because we are using long upload sleep

    var expectedBatches =
        IntStream.range(0, numBatches).mapToObj(i -> mock(BatchedLogBuffer.Batch.class)).toList();

    // we expect the uploader to be called with these batches, and it needs to
    // return a Uni with an UploadResult - but we dont need to keep the UploadResult
    // MUST set up the uploader to return a value before putting batches into the buffer
    for (var batch : expectedBatches) {
      var result = new AsyncBatchedLogUploader.UploadResult(batch, null);
      when(fixture.uploader().upload(batch)).thenReturn(Uni.createFrom().item(result));
    }

    // now connect the Batch's to the buffer so it will return them. Once this is done
    // the upload thread will pick them up once woken
    // expectDrainFully depends on if the test is closed the handler.
    var nextBatchStub = when(fixture.buffer().nextBatch(expectDrainFully));
    for (var batch : expectedBatches) {
      nextBatchStub = nextBatchStub.thenReturn(batch);
    }
    // now return null as sentinel for no more data
    nextBatchStub.thenReturn(null);

    return expectedBatches;
  }
}
