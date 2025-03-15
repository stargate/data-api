package io.stargate.sgv2.jsonapi.config.constants;

public interface RerankingConstants {

  interface RerankingColumn {
    String ENABLED = "enabled";
    String SERVICE = "service";
  }

  interface RerankingService {
    String PROVIDER = "provider";
    String MODEL_NAME = "modelName";
    String AUTHENTICATION = "authentication";
    String PARAMETERS = "parameters";
  }
}
