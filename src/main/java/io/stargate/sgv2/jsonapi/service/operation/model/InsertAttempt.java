package io.stargate.sgv2.jsonapi.service.operation.model;

import java.util.Optional;

// TODO AARON COMMENTS, comparable so we can re-order the inserts when building the results
public interface InsertAttempt extends Comparable<InsertAttempt> {

  int position();

  WritableDocRow docRow();

  Optional<Throwable> failure();

  @Override
  default int compareTo(InsertAttempt o) {
    return Integer.compare(position(), o.position());
  }
}
