package io.stargate.sgv2.jsonapi.service.projection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class DocumentProjectorTest {
  @Inject ObjectMapper objectMapper;

  // Tests for validating issues with Projection definitions
  @Nested
  class ProjectorDefValidation {
    @Test
    public void verifyProjectionJsonObject() throws Exception {
      JsonNode def = objectMapper.readTree(" [ 1, 2, 3 ]");
      Throwable t = catchThrowable(() -> DocumentProjector.createFromDefinition(def));
      assertThat(t)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_PROJECTION_PARAM)
          .hasMessage("Unsupported projection parameter: definition must be OBJECT, was ARRAY");
    }

    @Test
    public void verifyNoEmptyPath() throws Exception {
      JsonNode def =
          objectMapper.readTree(
              """
              { "root" :
                { "branch" :
                  { "": 1 }
                }
              }
              """);
      Throwable t = catchThrowable(() -> DocumentProjector.createFromDefinition(def));
      assertThat(t)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_PROJECTION_PARAM)
          .hasMessage(
              "Unsupported projection parameter: empty paths (and path segments) not allowed");
    }

    @Test
    public void verifyNoIncludeAfterExclude() throws Exception {
      JsonNode def =
          objectMapper.readTree(
              """
              { "excludeMe" : 0,
                "excludeMeToo" : 0,
                "include.me" : 1
              }
              """);
      Throwable t = catchThrowable(() -> DocumentProjector.createFromDefinition(def));
      assertThat(t)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_PROJECTION_PARAM)
          .hasMessage(
              "Unsupported projection parameter: cannot include 'include.me' on exclusion projection");
    }

    @Test
    public void verifyNoPathOverlap() throws Exception {
      JsonNode def =
          objectMapper.readTree(
              """
                      { "branch" : 1,
                        "branch.x.leaf" : 1,
                        "include.me" : 1
                      }
                      """);
      Throwable t = catchThrowable(() -> DocumentProjector.createFromDefinition(def));
      assertThat(t)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_PROJECTION_PARAM)
          .hasMessage(
              "Unsupported projection parameter: projection path conflict between 'branch' and 'branch.x.leaf'");

      // Should be caught regardless of ordering (longer vs shorter path first)
      JsonNode def2 =
          objectMapper.readTree(
              """
                      { "a.y.leaf" : 1,
                        "a" : 1,
                        "value" : 1
                      }
                      """);
      Throwable t2 = catchThrowable(() -> DocumentProjector.createFromDefinition(def2));
      assertThat(t2)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_PROJECTION_PARAM)
          .hasMessage(
              "Unsupported projection parameter: projection path conflict between 'a' and 'a.y.leaf'");
    }

    @Test
    public void verifyNoExcludeAfterInclude() throws Exception {
      JsonNode def =
          objectMapper.readTree(
              """
              { "includeMe" : 1,
                "misc" : {
                   "nested": {
                     "do" : true,
                     "dont" : false
                    }
                },
                "includeMe2" : 1
              }
              """);
      Throwable t = catchThrowable(() -> DocumentProjector.createFromDefinition(def));
      assertThat(t)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_PROJECTION_PARAM)
          .hasMessage(
              "Unsupported projection parameter: cannot exclude 'misc.nested.dont' on inclusion projection");
    }

    @Test
    public void verifyProjectionEquality() throws Exception {
      String defStr1 = "{ \"field1\" : 1, \"field2\": 1 }";
      String defStr2 = "{ \"field1\" : 0, \"field2\": 0 }";

      DocumentProjector proj1 =
          DocumentProjector.createFromDefinition(objectMapper.readTree(defStr1));
      assertThat(proj1.isInclusion()).isTrue();
      DocumentProjector proj2 =
          DocumentProjector.createFromDefinition(objectMapper.readTree(defStr2));
      assertThat(proj2.isInclusion()).isFalse();

      // First, verify equality of identical definitions
      assertThat(proj1)
          .isEqualTo(DocumentProjector.createFromDefinition(objectMapper.readTree(defStr1)));
      assertThat(proj2)
          .isEqualTo(DocumentProjector.createFromDefinition(objectMapper.readTree(defStr2)));

      // Then inequality
      assertThat(proj1)
          .isNotEqualTo(DocumentProjector.createFromDefinition(objectMapper.readTree(defStr2)));
      assertThat(proj2)
          .isNotEqualTo(DocumentProjector.createFromDefinition(objectMapper.readTree(defStr1)));
    }
  }

  @Nested
  class ProjectorSliceValidation {
    @Test
    public void verifyNoUnknownOperators() throws Exception {
      JsonNode def =
          objectMapper.readTree(
              """
                      { "include" : {
                           "$set" : 1
                         }
                      }
                      """);
      Throwable t = catchThrowable(() -> DocumentProjector.createFromDefinition(def));
      assertThat(t)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_PROJECTION_PARAM)
          .hasMessage(
              "Unsupported projection parameter: unrecognized/unsupported projection operator '$set' (only '$slice' supported)");
    }

    @Test
    public void verifyNoOverlapWithPath() throws Exception {
      JsonNode def =
          objectMapper.readTree(
              """
                              {
                                "branch.x" : {
                                   "$slice" : 3
                                },
                                "branch.x.leaf" : 1
                              }
                              """);
      Throwable t = catchThrowable(() -> DocumentProjector.createFromDefinition(def));
      assertThat(t)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_PROJECTION_PARAM)
          .hasMessage(
              "Unsupported projection parameter: projection path conflict between 'branch.x' and 'branch.x.leaf'");
    }

    @Test
    public void verifyNoRootOperators() throws Exception {
      JsonNode def =
          objectMapper.readTree(
              """
                              { "$slice" : 1 }
                              """);
      Throwable t = catchThrowable(() -> DocumentProjector.createFromDefinition(def));
      assertThat(t)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_PROJECTION_PARAM)
          .hasMessage(
              "Unsupported projection parameter: path cannot start with '$' (no root-level operators)");
    }

    @Test
    public void verifySliceDefinitionNumberOrArray() throws Exception {
      JsonNode def =
          objectMapper.readTree(
              """
                      {
                        "include" : {
                           "$slice" : "text-not-accepted"
                        }
                      }
                      """);
      Throwable t = catchThrowable(() -> DocumentProjector.createFromDefinition(def));
      assertThat(t)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_PROJECTION_PARAM)
          .hasMessageStartingWith(
              "Unsupported projection parameter: path ('include') has unsupported parameter for '$slice' (STRING)");
    }

    @Test
    public void verifySliceDefinitionToReturnPositive() throws Exception {
      JsonNode def =
          objectMapper.readTree(
              """
                              {
                                "include" : {
                                   "$slice" : [1, -2]
                                }
                              }
                              """);
      Throwable t = catchThrowable(() -> DocumentProjector.createFromDefinition(def));
      assertThat(t)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_PROJECTION_PARAM)
          .hasMessageStartingWith(
              "Unsupported projection parameter: path ('include') has unsupported parameter for '$slice' (ARRAY): second NUMBER");
    }
  }

  @Nested
  class ProjectorApplyInclusions {
    @Test
    public void testSimpleIncludeWithId() throws Exception {
      final JsonNode doc =
          objectMapper.readTree(
              """
                      { "_id" : 1,
                         "value1" : true,
                         "value2" : false,
                         "nested" : {
                            "x": 3,
                            "y": 4,
                            "z": -1
                         },
                         "nested2" : {
                            "z": 5
                         },
                         "$vector" : [0.11, 0.22, 0.33, 0.44]
                      }
                      """);
      DocumentProjector projection =
          DocumentProjector.createFromDefinition(
              objectMapper.readTree(
                  """
                      { "value2" : 1,
                        "nested" : {
                           "x": 1
                        },
                        "nested.z": 1,
                        "nosuchprop": 1,
                        "$vector": 1
                      }
                      """));
      assertThat(projection.isInclusion()).isTrue();
      projection.applyProjection(doc);
      assertThat(doc)
          .isEqualTo(
              objectMapper.readTree(
                  """
              { "_id" : 1,
                 "value2" : false,
                 "nested" : {
                    "x": 3,
                    "z": -1
                 },
                 "$vector" : [0.11, 0.22, 0.33, 0.44]
              }
                      """));
    }

    @Test
    public void testSimpleIncludeWithSimilarity() throws Exception {
      final JsonNode doc =
          objectMapper.readTree(
              """
            { "_id" : 1,
               "value1" : true,
               "value2" : false,
               "nested" : {
                  "x": 3,
                  "y": 4,
                  "z": -1
               },
               "nested2" : {
                  "z": 5
               },
               "$vector" : [0.11, 0.22, 0.33, 0.44]
            }
            """);
      DocumentProjector projection =
          DocumentProjector.createFromDefinition(
              objectMapper.readTree(
                  """
                { "value2" : 1,
                  "$vector": 1,
                  "$similarity": 1
                }
                """));
      assertThat(projection.isInclusion()).isTrue();
      projection.applyProjection(doc, 0.25f);
      assertThat(doc.get("value2")).isNotNull();
      assertThat(doc.get("$vector")).isNotNull();
      assertThat(doc.get("$similarity").floatValue()).isEqualTo(0.25f);
    }

    @Test
    public void testSimpleIncludeWithoutId() throws Exception {
      final JsonNode doc =
          objectMapper.readTree(
              """
                      { "_id" : 1,
                         "value1" : true,
                         "nested" : {
                            "x": 3,
                            "z": -1
                         },
                         "nested2" : {
                            "z": 5
                         }
                      }
                      """);
      DocumentProjector projection =
          DocumentProjector.createFromDefinition(
              objectMapper.readTree(
                  """
                      { "value1" : 1,
                        "nested" : {
                           "x": 1
                        },
                        "_id": 0,
                        "nested2.unknown": 1
                      }
                      """));
      assertThat(projection.isInclusion()).isTrue();
      projection.applyProjection(doc);
      assertThat(doc)
          .isEqualTo(
              objectMapper.readTree(
                  """
              {
                 "value1": true,
                 "nested" : {
                    "x": 3
                 },
                 "nested2" : { }
              }
              """));
    }

    @Test
    public void testSimpleIncludeInArray() throws Exception {
      final JsonNode doc =
          objectMapper.readTree(
              """
                      { "values" : [ {
                           "x": 1,
                           "y": 2
                        }, {
                           "y": false,
                           "z": true
                        } ],
                        "array2": [1, 2],
                        "array3": [2, 3]
                      }
              """);
      DocumentProjector projection =
          DocumentProjector.createFromDefinition(
              objectMapper.readTree("{ \"values.y\": 1, \"values.z\":1, \"array3\":1}"));
      assertThat(projection.isInclusion()).isTrue();
      projection.applyProjection(doc);
      assertThat(doc)
          .isEqualTo(
              objectMapper.readTree(
                  """
                      { "values" : [ {
                           "y": 2
                        }, {
                           "y": false,
                           "z": true
                        } ],
                        "array3": [2, 3]
                      }
                      """));
    }
  }

  @Nested
  class ProjectorApplyExclusions {
    @Test
    public void excludeWithIdIncluded() throws Exception {
      final JsonNode doc =
          objectMapper.readTree(
              """
                      {  "_id" : 123,
                         "value1" : true,
                         "value2" : false,
                         "nested" : {
                            "x": 3,
                            "y": 4,
                            "z": -1
                         },
                         "nested2" : {
                            "z": 5
                         },
                         "$vector" : [0.11, 0.22, 0.33, 0.44]
                      }
                      """);
      DocumentProjector projection =
          DocumentProjector.createFromDefinition(
              objectMapper.readTree(
                  """
                                  {
                                    "value1" : 0,
                                    "nested" : {
                                       "x": 0
                                    },
                                    "nested.z": 0,
                                    "nosuchprop": 0,
                                    "$vector": 0
                                  }
                                  """));
      assertThat(projection.isInclusion()).isFalse();
      projection.applyProjection(doc);
      assertThat(doc)
          .isEqualTo(
              objectMapper.readTree(
                  """
                          {
                             "_id" : 123,
                             "value2" : false,
                             "nested" : {
                                "y": 4
                             },
                             "nested2" : {
                               "z": 5
                             }
                          }
                                  """));
    }

    @Test
    public void excludeWithIdExcluded() throws Exception {
      final JsonNode doc =
          objectMapper.readTree(
              """
                      { "_id" : 123,
                         "value1" : true,
                         "nested" : {
                            "x": 3,
                            "z": -1
                         },
                         "nested2" : {
                            "z": 5
                         }
                      }
                      """);
      DocumentProjector projection =
          DocumentProjector.createFromDefinition(
              objectMapper.readTree(
                  """
                                  {
                                    "_id": 0,
                                    "value1" : 0,
                                    "nested" : {
                                       "x": 0
                                    },
                                    "nested2.unknown": 0
                                  }
                                  """));
      assertThat(projection.isInclusion()).isFalse();
      projection.applyProjection(doc);
      assertThat(doc)
          .isEqualTo(
              objectMapper.readTree(
                  """
                          {
                             "nested" : {
                                "z": -1
                             },
                             "nested2" : {
                               "z" : 5
                             }
                          }
                          """));
    }

    @Test
    public void excludeInArray() throws Exception {
      JsonNode doc =
          objectMapper.readTree(
              """
                              { "values" : [ {
                                   "x": 1,
                                   "y": 2
                                }, {
                                   "y": false,
                                   "z": true
                                } ],
                                "array2": [2, 3],
                                "array3": [2, 3]
                              }
                      """);
      DocumentProjector projection =
          DocumentProjector.createFromDefinition(
              objectMapper.readTree("{ \"values.y\": 0, \"values.z\":0,\"array3\":0}"));
      assertThat(projection.isInclusion()).isFalse();
      projection.applyProjection(doc);
      assertThat(doc)
          .isEqualTo(
              objectMapper.readTree(
                  """
                                  { "values" : [ {
                                       "x": 1
                                    }, {
                                    } ],
                                    "array2": [2, 3]
                                  }
                                  """));
    }

    @Test
    public void excludeInSubDoc() throws Exception {
      JsonNode doc =
          objectMapper.readTree(
              """
                      {
                        "_id": "doc5",
                        "username": "user5",
                        "sub_doc" : {
                          "a": 5,
                          "b": {
                            "c": "v1",
                            "d": false
                          }
                        }
                      }
                      """);
      DocumentProjector projection =
          DocumentProjector.createFromDefinition(objectMapper.readTree("{ \"sub_doc.b\": 0 }"));
      assertThat(projection.isInclusion()).isFalse();
      projection.applyProjection(doc);
      assertThat(doc)
          .isEqualTo(
              objectMapper.readTree(
                  """
                              {
                                "_id": "doc5",
                                "username": "user5",
                                "sub_doc" : {
                                  "a": 5
                                }
                              }
                              """));
    }

    // "Empty" Projection is not really inclusion or exclusion, but technically
    // let's consider it exclusion for sake of consistency (empty list to exclude
    // is same as no Projection applied; empty inclusion would produce no output)
    @Test
    public void emptyProjectionAsExclude() throws Exception {
      final String docJson =
          """
                      {
                        "_id": "doc5",
                        "value": 4
                      }
                      """;
      JsonNode doc = objectMapper.readTree(docJson);
      DocumentProjector projection =
          DocumentProjector.createFromDefinition(objectMapper.readTree("{}"));
      assertThat(projection.isInclusion()).isFalse();
      projection.applyProjection(doc);
      assertThat(doc).isEqualTo(objectMapper.readTree(docJson));
    }
  }

  @Nested
  class ProjectorApplySimpleSlice {
    @Test
    public void simpleSliceWithExclusionsHead() throws Exception {
      JsonNode doc =
          objectMapper.readTree(
              """
                      { "values" : [ 1, 2, 3, 4, 5 ],
                        "x": 1,
                        "y": 2
                      }
                      """);
      DocumentProjector projection =
          DocumentProjector.createFromDefinition(
              objectMapper.readTree(
                  """
                          {
                            "x": 0,
                            "values": {
                              "$slice": 3
                             }
                           }
                        """));
      assertThat(projection.isInclusion()).isFalse();
      projection.applyProjection(doc);
      assertThat(doc)
          .isEqualTo(
              objectMapper.readTree(
                  """
                          { "values" : [ 1, 2, 3 ],
                            "y": 2
                          }
                          """));
    }

    @Test
    public void simpleSliceWithExclusionsTail() throws Exception {
      JsonNode doc =
          objectMapper.readTree(
              """
                      { "values" : [ 1, 2, 3, 4, 5 ],
                        "x": 1,
                        "y": 2
                      }
                      """);
      DocumentProjector projection =
          DocumentProjector.createFromDefinition(
              objectMapper.readTree(
                  """
                    {
                      "y": 0,
                      "values": {
                        "$slice": -2
                       }
                     }
      """));
      assertThat(projection.isInclusion()).isFalse();
      projection.applyProjection(doc);
      assertThat(doc)
          .isEqualTo(
              objectMapper.readTree(
                  """
                          { "values" : [ 4, 5 ],
                            "x": 1
                          }
                            """));
    }

    @Test
    public void simpleSliceWithExclusionsNested() throws Exception {
      JsonNode doc =
          objectMapper.readTree(
              """
                              {
                                "data": {
                                  "values" : [ 1, 2, 3, 4, 5 ],
                                  "x": 9
                                },
                                "x": 17,
                                "y": 13
                              }
                              """);
      DocumentProjector projection =
          DocumentProjector.createFromDefinition(
              objectMapper.readTree(
                  """
                                      {
                                        "x": 0,
                                        "data.values": {
                                          "$slice": 2
                                         }
                                       }
                                    """));
      assertThat(projection.isInclusion()).isFalse();
      projection.applyProjection(doc);
      assertThat(doc)
          .isEqualTo(
              objectMapper.readTree(
                  """
                                  {
                                    "data": {
                                      "values" : [ 1, 2 ],
                                      "x": 9
                                    },
                                    "y": 13
                                  }
                                    """));
    }

    @Test
    public void simpleSliceWithInclusionsHead() throws Exception {
      JsonNode doc =
          objectMapper.readTree(
              """
                      { "values" : [ 1, 2, 3, 4, 5 ],
                        "x": 9,
                        "y": -7
                      }
                      """);
      DocumentProjector projection =
          DocumentProjector.createFromDefinition(
              objectMapper.readTree(
                  """
                    {
                      "x": 1,
                      "values": {
                        "$slice": 2
                       }
                     }
      """));
      assertThat(projection.isInclusion()).isTrue();
      projection.applyProjection(doc);
      assertThat(doc)
          .isEqualTo(
              objectMapper.readTree(
                  """
                                      { "values" : [ 1, 2 ],
                                        "x": 9
                                      }
                                      """));
    }

    @Test
    public void simpleSliceWithInclusionsHeadOverrun() throws Exception {
      JsonNode doc =
          objectMapper.readTree(
              """
                              { "values" : [ 1, 2, 3, 4, 5 ],
                                "x": 9,
                                "y": -7
                              }
                              """);
      DocumentProjector projection =
          DocumentProjector.createFromDefinition(
              objectMapper.readTree(
                  """
                                {
                                  "x": 1,
                                  "values": {
                                    "$slice": 17
                                   }
                                 }
                  """));
      assertThat(projection.isInclusion()).isTrue();
      projection.applyProjection(doc);
      assertThat(doc)
          .isEqualTo(
              objectMapper.readTree(
                  """
                                                  { "values" : [ 1, 2, 3, 4, 5 ],
                                                    "x": 9
                                                  }
                                                  """));
    }

    @Test
    public void simpleSliceWithInclusionsTail() throws Exception {
      JsonNode doc =
          objectMapper.readTree(
              """
                      { "values" : [ 1, 2, 3, 4, 5 ],
                        "x": 9,
                        "y": -7
                      }
                      """);
      DocumentProjector projection =
          DocumentProjector.createFromDefinition(
              objectMapper.readTree(
                  """
                    {
                      "values": {
                        "$slice": -4
                       },
                       "y": 1
                     }
      """));
      assertThat(projection.isInclusion()).isTrue();
      projection.applyProjection(doc);
      assertThat(doc)
          .isEqualTo(
              objectMapper.readTree(
                  """
                          { "values" : [ 2, 3, 4, 5 ],
                            "y": -7
                          }
                          """));
    }

    @Test
    public void simpleSliceWithInclusionsTailOverrun() throws Exception {
      JsonNode doc =
          objectMapper.readTree(
              """
                              { "values" : [ 1, 2, 3, 4, 5 ],
                                "x": 9,
                                "y": -7
                              }
                              """);
      DocumentProjector projection =
          DocumentProjector.createFromDefinition(
              objectMapper.readTree(
                  """
                                {
                                  "values": {
                                    "$slice": -99
                                   },
                                   "y": 1
                                 }
                  """));
      assertThat(projection.isInclusion()).isTrue();
      projection.applyProjection(doc);
      assertThat(doc)
          .isEqualTo(
              objectMapper.readTree(
                  """
                                      { "values" : [ 1, 2, 3, 4, 5 ],
                                        "y": -7
                                      }
                                      """));
    }

    @Test
    public void simpleSliceWithInclusionsTailSubDocs() throws Exception {
      JsonNode doc =
          objectMapper.readTree(
              """
                      {
                        "data": {
                           "values" : [ {"a":1}, {"b":2}, {"c":3}],
                           "x": 16
                         },
                        "x": 9,
                        "y": -7
                      }
                      """);
      DocumentProjector projection =
          DocumentProjector.createFromDefinition(
              objectMapper.readTree(
                  """
                          {
                            "data.values": {
                              "$slice": -2
                             },
                             "y": 1
                           }
                        """));
      assertThat(projection.isInclusion()).isTrue();
      projection.applyProjection(doc);
      assertThat(doc)
          .isEqualTo(
              objectMapper.readTree(
                  """
                      {
                        "data": {
                           "values" : [{"b":2}, {"c":3}]
                         },
                        "y": -7
                      }
                    """));
    }

    @Test
    public void simpleSliceWithInclusionsTailNestedOverrun() throws Exception {
      JsonNode doc =
          objectMapper.readTree(
              """
                                      {
                                        "data": {
                                           "values" : [ 1, 2, 3, 4, 5 ],
                                           "x": 16
                                         },
                                        "x": 9,
                                        "y": -7
                                      }
                                      """);
      DocumentProjector projection =
          DocumentProjector.createFromDefinition(
              objectMapper.readTree(
                  """
                                                  {
                                                    "data.values": {
                                                      "$slice": -99
                                                     },
                                                     "y": 1
                                                   }
                                          """));
      assertThat(projection.isInclusion()).isTrue();
      projection.applyProjection(doc);
      assertThat(doc)
          .isEqualTo(
              objectMapper.readTree(
                  """
                                          {
                                            "data": {
                                               "values" : [ 1, 2, 3, 4, 5 ]
                                             },
                                            "y": -7
                                          }
                                          """));
    }
  }

  @Nested
  class ProjectorApplyFullSlice {
    @Test
    public void fullSliceWithExclusionsHead() throws Exception {
      JsonNode doc =
          objectMapper.readTree(
              """
                      { "values" : [ 1, 2, 3, 4, 5 ],
                        "x": 7,
                        "y": 9
                      }
                      """);
      DocumentProjector projection =
          DocumentProjector.createFromDefinition(
              objectMapper.readTree(
                  """
                          {
                            "x": 0,
                            "values": {
                              "$slice": [1, 3]
                             }
                           }
                          """));
      assertThat(projection.isInclusion()).isFalse();
      projection.applyProjection(doc);
      assertThat(doc)
          .isEqualTo(
              objectMapper.readTree(
                  """
                          { "values" : [ 2, 3, 4 ],
                            "y": 9
                          }
                          """));
    }

    @Test
    public void fullSliceWithExclusionsHeadNested() throws Exception {
      JsonNode doc =
          objectMapper.readTree(
              """
                      {
                         "data": {
                            "values" : [ 1, 2, 3, 4, 5 ],
                            "x": 7
                          },
                          "y": 9
                      }
                      """);
      DocumentProjector projection =
          DocumentProjector.createFromDefinition(
              objectMapper.readTree(
                  """
                                      {
                                        "x": 0,
                                        "data.values": {
                                          "$slice": [1, 3]
                                         }
                                       }
                                      """));
      assertThat(projection.isInclusion()).isFalse();
      projection.applyProjection(doc);
      assertThat(doc)
          .isEqualTo(
              objectMapper.readTree(
                  """
                          {
                             "data": {
                                "values" : [ 2, 3, 4 ],
                                "x": 7
                              },
                              "y": 9
                          }
                          """));
    }

    @Test
    public void fullSliceWithExclusionsHeadOverrun() throws Exception {
      JsonNode doc =
          objectMapper.readTree(
              """
                      { "values" : [ 1, 2, 3, 4, 5 ],
                        "x": 7,
                        "y": 9
                      }
                      """);
      DocumentProjector projection =
          DocumentProjector.createFromDefinition(
              objectMapper.readTree(
                  """
                          {
                            "x": 0,
                            "values": {
                              "$slice": [1, 111]
                             }
                           }
                          """));
      assertThat(projection.isInclusion()).isFalse();
      projection.applyProjection(doc);
      assertThat(doc)
          .isEqualTo(
              objectMapper.readTree(
                  """
                          { "values" : [ 2, 3, 4, 5 ],
                            "y": 9
                          }
                          """));
    }

    @Test
    public void fullSliceWithExclusionsTail() throws Exception {
      JsonNode doc =
          objectMapper.readTree(
              """
                      { "values" : [ 1, 2, 3, 4, 5 ],
                        "x": 7,
                        "y": 9
                      }
                      """);
      DocumentProjector projection =
          DocumentProjector.createFromDefinition(
              objectMapper.readTree(
                  """
                          {
                            "y": 0,
                            "values": {
                              "$slice": [-3, 2]
                             }
                           }
                  """));
      assertThat(projection.isInclusion()).isFalse();
      projection.applyProjection(doc);
      assertThat(doc)
          .isEqualTo(
              objectMapper.readTree(
                  """
                          { "values" : [ 3, 4 ],
                            "x": 7
                          }
                            """));
    }

    @Test
    public void fullSliceWithExclusionsTailOverrun() throws Exception {
      JsonNode doc =
          objectMapper.readTree(
              """
                      { "values" : [ 1, 2, 3, 4, 5 ],
                        "x": 7,
                        "y": 9
                      }
                      """);
      DocumentProjector projection =
          DocumentProjector.createFromDefinition(
              objectMapper.readTree(
                  """
                          {
                            "y": 0,
                            "values": {
                              "$slice": [-3, 199]
                             }
                           }
                          """));
      assertThat(projection.isInclusion()).isFalse();
      projection.applyProjection(doc);
      assertThat(doc)
          .isEqualTo(
              objectMapper.readTree(
                  """
                          { "values" : [ 3, 4, 5 ],
                            "x": 7
                          }
                          """));
    }

    @Test
    public void fullSliceWithInclusionsHead() throws Exception {
      JsonNode doc =
          objectMapper.readTree(
              """
                      { "values" : [ 1, 2, 3, 4, 5 ],
                        "x": 7,
                        "y": 9
                      }
                      """);
      DocumentProjector projection =
          DocumentProjector.createFromDefinition(
              objectMapper.readTree(
                  """
                          {
                            "x": 1,
                            "values": {
                              "$slice": [1, 3]
                             }
                           }
                          """));
      assertThat(projection.isInclusion()).isTrue();
      projection.applyProjection(doc);
      assertThat(doc)
          .isEqualTo(
              objectMapper.readTree(
                  """
                          { "values" : [ 2, 3, 4 ],
                            "x": 7
                          }
                          """));
    }

    @Test
    public void fullSliceWithInclusionsHeadOverrun() throws Exception {
      JsonNode doc =
          objectMapper.readTree(
              """
                      { "values" : [ 1, 2, 3, 4, 5 ],
                        "x": 7,
                        "y": 9
                      }
                      """);
      DocumentProjector projection =
          DocumentProjector.createFromDefinition(
              objectMapper.readTree(
                  """
                          {
                            "x": 1,
                            "values": {
                              "$slice": [1, 111]
                             }
                           }
                          """));
      assertThat(projection.isInclusion()).isTrue();
      projection.applyProjection(doc);
      assertThat(doc)
          .isEqualTo(
              objectMapper.readTree(
                  """
                          { "values" : [ 2, 3, 4, 5 ],
                            "x": 7
                          }
                          """));
    }

    @Test
    public void fullSliceWithInclusionsTail() throws Exception {
      JsonNode doc =
          objectMapper.readTree(
              """
                      { "values" : [ 1, 2, 3, 4, 5 ],
                        "x": 7,
                        "y": 9
                      }
                      """);
      DocumentProjector projection =
          DocumentProjector.createFromDefinition(
              objectMapper.readTree(
                  """
                          {
                            "y": 1,
                            "values": {
                              "$slice": [-3, 2]
                             }
                           }
                          """));
      assertThat(projection.isInclusion()).isTrue();
      projection.applyProjection(doc);
      assertThat(doc)
          .isEqualTo(
              objectMapper.readTree(
                  """
                          { "values" : [ 3, 4 ],
                            "y": 9
                          }
                            """));
    }

    @Test
    public void fullSliceWithInclusionsTailOverrun() throws Exception {
      JsonNode doc =
          objectMapper.readTree(
              """
                      { "values" : [ 1, 2, 3, 4, 5 ],
                        "x": 7,
                        "y": 9
                      }
                      """);
      DocumentProjector projection =
          DocumentProjector.createFromDefinition(
              objectMapper.readTree(
                  """
                          {
                            "y": 1,
                            "values": {
                              "$slice": [-3, 199]
                             }
                           }
                              """));
      assertThat(projection.isInclusion()).isTrue();
      projection.applyProjection(doc);
      assertThat(doc)
          .isEqualTo(
              objectMapper.readTree(
                  """
                          { "values" : [ 3, 4, 5 ],
                            "y": 9
                          }
                            """));
    }
  }
}
