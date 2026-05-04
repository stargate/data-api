package io.stargate.sgv2.jsonapi.testbench.testspec;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import org.jspecify.annotations.NonNull;

/**
 * A Spec file is any JSON file we have that is used to drive the test suite, workflows etc.
 * <p>
 * This is a container of the file, it's raw JSON, and the {@link TestSpec} which is the common
 * parsed object from the JSON.
 * </p>
 */
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
