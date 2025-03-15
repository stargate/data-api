package io.stargate.sgv2.jsonapi.config.constants;

public interface RerankConstants {

  interface RerankColumn {
    String ENABLED = "enabled";
    String SERVICE = "service";
  }

  interface RerankService {
    String PROVIDER = "provider";
    String MODEL_NAME = "modelName";
    String AUTHENTICATION = "authentication";
    String PARAMETERS = "parameters";
  }
}
