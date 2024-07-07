package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import io.micrometer.core.instrument.Tags;

public interface IndexUsage {

  /** Default implementation when we are not tracking index usage for the schema object */
  IndexUsage NO_OP =
      new IndexUsage() {
        @Override
        public Tags getTags() {
          return Tags.empty();
        }

        @Override
        public void merge(IndexUsage indexUsage) {
          // NO-OP
        }
      };

  /**
   * This method is used to generate the tags for the index usage
   *
   * @return
   */
  Tags getTags();

  /**
   * This method is used to merge the index usage of two different types for filters used in a query
   *
   * @param indexUsage
   */
  void merge(IndexUsage indexUsage);
}
