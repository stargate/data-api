package io.stargate.sgv2.jsonapi.service.operation.query;

/** Represents options for in-memory sorting. */
public record InMemorySortOption(int returnLimit, int skip, int errorLimit) {

  public static InMemorySortOption from(int returnLimit, int skip, int errorLimit) {
    return new InMemorySortOption(returnLimit, skip, errorLimit);
  }
}
