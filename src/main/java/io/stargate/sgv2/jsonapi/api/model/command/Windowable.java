package io.stargate.sgv2.jsonapi.api.model.command;

import java.util.Optional;

public interface Windowable {

  default Optional<Integer> limit() {
    return Optional.empty();
  }

  default Optional<Integer> skip() {
    return Optional.empty();
  }
}
