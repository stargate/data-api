package io.stargate.sgv2.jsonapi.api.model.command;

import java.util.Optional;

/** Interface for commands that can be windowed using `skip` and `limit` options. */
public interface Windowable {

  /*
   * @return The maximum number of documents that can be returned for the request.
   * Default limit will be page size used for fetching data
   */
  default Optional<Integer> limit() {
    return Optional.empty();
  }

  /*
   * @return The number of documents to skip before returning the sorted documents.
   * Default skip will be `0`
   */
  default Optional<Integer> skip() {
    return Optional.empty();
  }
}
