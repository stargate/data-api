package io.stargate.sgv2.jsonapi.service.operation.model;

import io.stargate.sgv2.jsonapi.service.shredding.DocRowIdentifer;
import io.stargate.sgv2.jsonapi.service.shredding.WritableDocRow;
import java.util.Optional;

// TODO AARON COMMENTS, comparable so we can re-order the inserts when building the results
public interface InsertAttempt extends Comparable<InsertAttempt> {

  int position();

  // AARON comments this is here because we may not have the row if we fail to shred.
  // But what if we dont have enough to get the row id
  Optional<DocRowIdentifer> docRowID();

  Optional<WritableDocRow> docRow();

  Optional<Throwable> failure();

  InsertAttempt maybeAddFailure(Throwable failure);

  @Override
  default int compareTo(InsertAttempt o) {
    return Integer.compare(position(), o.position());
  }
}
