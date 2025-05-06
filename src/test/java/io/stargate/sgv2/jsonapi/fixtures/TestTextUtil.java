package io.stargate.sgv2.jsonapi.fixtures;

import com.github.javafaker.Faker;

/** Helper methods for generating textual data for use in the tests. */
public class TestTextUtil {
  /**
   * Helper method for generating a Document with exactly specified length (in characters), composed
   * of Latin words in "Lorem Ipsum" style.
   *
   * @param targetLength Exact length of the generated string
   * @param separator Separator between sentences.
   */
  public static String generateTextDoc(int targetLength, String separator) {
    Faker faker = new Faker();
    StringBuilder sb = new StringBuilder(targetLength + 100);

    while (sb.length() < targetLength) {
      sb.append(faker.lorem().sentence()).append(separator);
    }
    return sb.substring(0, targetLength);
  }
}
