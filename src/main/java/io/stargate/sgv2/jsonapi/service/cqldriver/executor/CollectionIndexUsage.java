package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import com.google.common.base.Preconditions;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.stargate.sgv2.jsonapi.service.schema.collections.spec.SuperShreddingMetadata;

/**
 * This class is used to track the usage of indexes in a query. It is used to generate metrics for
 * different commands by index column usage
 */
public class CollectionIndexUsage implements IndexUsage {

  public boolean primaryKeyTag,
      existKeysIndexTag,
      arraySizeIndexTag,
      arrayContainsTag,
      booleanIndexTag,
      numberIndexTag,
      textIndexTag,
      timestampIndexTag,
      nullIndexTag,
      vectorIndexTag,
      lexicalIndexTag;

  /**
   * This method is used to generate the tags for the index usage
   *
   * @return
   */
  @Override
  public Tags getTags() {
    return Tags.of(
        Tag.of("key", String.valueOf(primaryKeyTag)),
        Tag.of("exist_keys", String.valueOf(existKeysIndexTag)),
        Tag.of("array_size", String.valueOf(arraySizeIndexTag)),
        Tag.of(SuperShreddingMetadata.Names.ARRAY_CONTAINS, String.valueOf(arrayContainsTag)),
        Tag.of(SuperShreddingMetadata.Names.QUERY_BOOLEAN_VALUES, String.valueOf(booleanIndexTag)),
        Tag.of(SuperShreddingMetadata.Names.QUERY_DOUBLE_VALUES, String.valueOf(numberIndexTag)),
        Tag.of(SuperShreddingMetadata.Names.QUERY_NULL_VALUES, String.valueOf(nullIndexTag)),
        Tag.of(SuperShreddingMetadata.Names.QUERY_TEXT_VALUES, String.valueOf(textIndexTag)),
        Tag.of(
            SuperShreddingMetadata.Names.QUERY_TIMESTAMP_VALUES, String.valueOf(timestampIndexTag)),
        Tag.of(SuperShreddingMetadata.Names.QUERY_VECTOR_VALUE, String.valueOf(vectorIndexTag)),
        Tag.of(SuperShreddingMetadata.Names.QUERY_LEXICAL_VALUE, String.valueOf(lexicalIndexTag)));
  }

  /**
   * This method is used to merge the index usage of two different types for filters used in a query
   *
   * @param indexUsage
   */
  public void merge(IndexUsage indexUsage) {
    Preconditions.checkArgument(
        indexUsage instanceof CollectionIndexUsage, "Cannot merge different types of index usage");
    var other = (CollectionIndexUsage) indexUsage;

    this.arrayContainsTag |= other.arrayContainsTag;
    this.primaryKeyTag |= other.primaryKeyTag;
    this.existKeysIndexTag |= other.existKeysIndexTag;
    this.arraySizeIndexTag |= other.arraySizeIndexTag;
    this.booleanIndexTag |= other.booleanIndexTag;
    this.numberIndexTag |= other.numberIndexTag;
    this.textIndexTag |= other.textIndexTag;
    this.timestampIndexTag |= other.timestampIndexTag;
    this.nullIndexTag |= other.nullIndexTag;
    this.vectorIndexTag |= other.vectorIndexTag;
  }
}
