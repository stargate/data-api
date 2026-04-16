package io.stargate.sgv2.jsonapi.api.v1.vectorize.testspec;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import org.jspecify.annotations.NonNull;

public record SpecFile(File file, TestSpec spec, JsonNode root) {

  @Override
  public @NonNull String toString() {
    return new StringBuilder("SpecFile{")
        .append("file=")
        .append(file)
        .append("spec.meta=")
        .append(spec.meta())
        .toString();
  }
}
