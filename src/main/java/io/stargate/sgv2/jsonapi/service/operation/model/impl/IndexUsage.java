package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;

/**
 * This class is used to track the usage of indexes in a query. It is used to generate metrics for
 * different commands by index column usage
 */
public class IndexUsage {
  public boolean primaryKeyTag,
      existKeysIndexTag,
      arraySizeIndexTag,
      arrayContainsTag,
      booleanIndexTag,
      numberIndexTag,
      textIndexTag,
      timestampIndexTag,
      nullIndexTag,
      vectorIndexTag;

  /** This method is used to generate the tags for the index usage */
  public Tags getTags() {
    Tags tags =
        Tags.of(
            Tag.of("key", String.valueOf(primaryKeyTag)),
            Tag.of("exist_keys", String.valueOf(existKeysIndexTag)),
            Tag.of("array_size", String.valueOf(arraySizeIndexTag)),
            Tag.of("array_contains", String.valueOf(arrayContainsTag)),
            Tag.of("query_bool_values", String.valueOf(booleanIndexTag)),
            Tag.of("query_dbl_values", String.valueOf(numberIndexTag)),
            Tag.of("query_text_values", String.valueOf(textIndexTag)),
            Tag.of("query_timestamp_values", String.valueOf(timestampIndexTag)),
            Tag.of("query_null_values", String.valueOf(nullIndexTag)),
            Tag.of("query_vector_value", String.valueOf(vectorIndexTag)));
    return tags;
  }

  /**
   * This method is used to merge the index usage of two different types for filters used in a query
   *
   * @param indexUsage
   */
  public void merge(IndexUsage indexUsage) {
    this.arrayContainsTag |= indexUsage.arrayContainsTag;
    this.primaryKeyTag |= indexUsage.primaryKeyTag;
    this.existKeysIndexTag |= indexUsage.existKeysIndexTag;
    this.arraySizeIndexTag |= indexUsage.arraySizeIndexTag;
    this.booleanIndexTag |= indexUsage.booleanIndexTag;
    this.numberIndexTag |= indexUsage.numberIndexTag;
    this.textIndexTag |= indexUsage.textIndexTag;
    this.timestampIndexTag |= indexUsage.timestampIndexTag;
    this.nullIndexTag |= indexUsage.nullIndexTag;
    this.vectorIndexTag |= indexUsage.vectorIndexTag;
  }
}
