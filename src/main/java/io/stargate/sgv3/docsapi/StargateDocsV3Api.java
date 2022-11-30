package io.stargate.sgv3.docsapi;

import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.api.common.grpc.SourceApiQualifier;
import io.stargate.sgv3.docsapi.config.constants.OpenApiConstants;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Application;
import org.eclipse.microprofile.openapi.annotations.Components;
import org.eclipse.microprofile.openapi.annotations.OpenAPIDefinition;
import org.eclipse.microprofile.openapi.annotations.enums.ParameterIn;
import org.eclipse.microprofile.openapi.annotations.enums.SecuritySchemeIn;
import org.eclipse.microprofile.openapi.annotations.enums.SecuritySchemeType;
import org.eclipse.microprofile.openapi.annotations.info.Info;
import org.eclipse.microprofile.openapi.annotations.media.ExampleObject;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.security.SecurityScheme;

@OpenAPIDefinition(
    // note that info is defined via the properties
    info = @Info(title = "", version = ""),
    components =
        @Components(

            // security schemes
            securitySchemes = {
              @SecurityScheme(
                  securitySchemeName = OpenApiConstants.SecuritySchemes.TOKEN,
                  type = SecuritySchemeType.APIKEY,
                  in = SecuritySchemeIn.HEADER,
                  apiKeyName = HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME)
            },

            // reusable parameters
            parameters = {
              @Parameter(
                  in = ParameterIn.PATH,
                  name = "database",
                  required = true,
                  schema = @Schema(implementation = String.class, pattern = "\\w+"),
                  description = "The database where the collection is located.",
                  example = "cycling"),
              @Parameter(
                  in = ParameterIn.PATH,
                  name = "collection",
                  required = true,
                  schema = @Schema(implementation = String.class, pattern = "\\w+"),
                  description = "The name of the collection.",
                  example = "events")
            },

            // reusable examples
            examples = {
              @ExampleObject(
                  name = "findOne",
                  summary = "`findOne` command",
                  value =
                      """
                      {
                        "findOne": {
                            "sort": ["-race.competitors"]
                        }
                      }
                      """),
              @ExampleObject(
                  name = "insertOne",
                  summary = "`insertOne` command",
                  value =
                      """
                      {
                        "insertOne": {
                          "document": {
                            "location": "London",
                            "race": {
                              "competitors": 100,
                              "start_date": "2022-08-15"
                            }
                          }
                        }
                      }
                      """),
            }))
public class StargateDocsV3Api extends Application {

  @Produces
  @SourceApiQualifier
  public String sourceApi() {
    return "rest";
  }
}
