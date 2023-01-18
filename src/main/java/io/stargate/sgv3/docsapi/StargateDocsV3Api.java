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
                            "sort": ["-race.competitors"],
                            "filter": {"user", "name"}
                        }
                      }
                      """),
              @ExampleObject(
                  name = "deleteOne",
                  summary = "`deleteOne` command",
                  value =
                      """
                            {
                              "deleteOne": {
                                  "filter": {"username", "user1"}
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
                            "_id": "1",
                            "location": "London",
                            "race": {
                              "competitors": 100,
                              "start_date": "2022-08-15"
                            }
                          }
                        }
                      }
                      """),
              @ExampleObject(
                  name = "createCollection",
                  summary = "`CreateCollection` command",
                  value =
                      """
                            {
                                "createCollection": {
                                  "name": "test_collection"
                                }
                            }
                            """),
              @ExampleObject(
                  name = "resultRead",
                  summary = "Read command result",
                  value =
                      """
                      {
                        "data": {
                          "docs": [
                            {
                               "_id": "1",
                               "location": "London",
                               "race": {
                                 "competitors": 100,
                                 "start_date": "2022-08-15"
                               }
                            },
                            {
                               "_id": "2",
                               "location": "Barcelona",
                               "race": {
                                 "competitors": 125,
                                 "start_date": "2022-09-26"
                               }
                            }
                          ],
                          "nextPageState": "jA8qg0AitZ8q28568GybNQ==",
                          "count": 2
                        }
                      }
                      """),
              @ExampleObject(
                  name = "resultInsert",
                  summary = "Insert command result",
                  value =
                      """
                      {
                        "status": {
                            "insertedIds": ["1", "2"]
                        }
                      }
                      """),
              @ExampleObject(
                  name = "resultDelete",
                  summary = "Delete command result",
                  value =
                      """
                                {
                                  "status": {
                                      "deletedIds": ["1", "2"]
                                  }
                                }
                                """),
              @ExampleObject(
                  name = "resultError",
                  summary = "Error result",
                  value =
                      """
                      {
                        "errors": [
                          {
                            "message": "The command failed because of some specific reason."
                          }
                        ]
                      }
                      """),
              @ExampleObject(
                  name = "resultCreateCollection",
                  summary = "Create result",
                  value =
                      """
                      {
                        "status": {
                            "ok": 1
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
