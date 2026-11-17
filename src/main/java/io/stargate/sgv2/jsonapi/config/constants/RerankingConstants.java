package io.stargate.sgv2.jsonapi.config.constants;

public interface RerankingConstants {

  interface HybridSearchLimits {
    int MIN = 1;
    int DEFAULT = 50;
    int MAX = 100;
  }

  // TODO: this and HybridSearchLimits make the config complicated so we can override the injected
  // config at test time. Either do not use injected config, or use the proper override.
  /** The min, default and max for the rerankCandidateLimit option of findAndRerank. */
  interface RerankCandidateLimits {
    // TODO: the design doc says "an integer greater than 1", that's different from
    // HybridSearchLimits
    int MIN = 2;
    // TODO: the design doc says "The maximum value and the default value are TBC"
    int DEFAULT = 50;
    int MAX = 100;
  }
}
