package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import static io.stargate.sgv2.jsonapi.service.schema.collections.spec.SuperShreddingMetadata.Names;

import com.google.common.base.Preconditions;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;

/**
 * This class is used to track the usage of indexes in a query. It is used to generate metrics for
 * different commands by index column usage
 */
public class CollectionIndexUsage implements IndexUsage {

  public boolean primaryKeyTag;
  public boolean existKeysIndexTag;
  public boolean arraySizeIndexTag;
  public boolean arrayContainsTag;
  public boolean booleanIndexTag;
  public boolean numberIndexTag;
  public boolean textIndexTag;
  public boolean timestampIndexTag;
  public boolean nullIndexTag;
  public boolean vectorIndexTag;
  public boolean lexicalIndexTag;

  /**
   * This method is used to generate the tags for the index usage
   *
   * @return
   */
  @Override
  public Tags getTags() {
    return Tags.of(
        Tag.of(Names.KEY, String.valueOf(primaryKeyTag)),
        Tag.of(Names.EXIST_KEYS, String.valueOf(existKeysIndexTag)),
        Tag.of(Names.ARRAY_SIZE, String.valueOf(arraySizeIndexTag)),
        Tag.of(Names.ARRAY_CONTAINS, String.valueOf(arrayContainsTag)),
        Tag.of(Names.QUERY_BOOLEAN_VALUES, String.valueOf(booleanIndexTag)),
        Tag.of(Names.QUERY_DOUBLE_VALUES, String.valueOf(numberIndexTag)),
        Tag.of(Names.QUERY_NULL_VALUES, String.valueOf(nullIndexTag)),
        Tag.of(Names.QUERY_TEXT_VALUES, String.valueOf(textIndexTag)),
        Tag.of(Names.QUERY_TIMESTAMP_VALUES, String.valueOf(timestampIndexTag)),
        Tag.of(Names.QUERY_VECTOR_VALUE, String.valueOf(vectorIndexTag)),
        Tag.of(Names.QUERY_LEXICAL_VALUE, String.valueOf(lexicalIndexTag)));
  }

  /**
   * This method is used to merge the index usage of two different types for filters used in a query
   *
   * @param indexUsage
   */
  @Override
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
    this.lexicalIndexTag |= other.lexicalIndexTag;
  }
}
