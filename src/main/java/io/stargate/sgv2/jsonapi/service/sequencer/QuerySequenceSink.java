package io.stargate.sgv2.jsonapi.service.sequencer;

/** Represents a sink of the query sequence. */
public interface QuerySequenceSink<RESULT> {

  /**
   * @return Returns {@link ReactiveQuerySequenceExecutor} that executes sequence in a reactive
   *     manner.
   */
  ReactiveQuerySequenceExecutor<RESULT> reactive();
}
