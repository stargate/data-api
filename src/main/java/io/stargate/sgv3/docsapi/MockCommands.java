package io.stargate.sgv3.docsapi;

// Mock commands to use while the service is just running in a jar
public class MockCommands {

  public static String createCollection =
      """
        {
            "createCollection": {
                "name" : "test"
            }
        }
    """;

  // ==================================================
  // Postman - Basic Insert and find by Id
  // ==================================================

  public static String findOne_doc1_by_id =
      """
        {
            "findOne": {
                "filter" : {"_id": "doc1"}
            }
        }
    """;

  public static String insertOne_doc1 =
      """
        {
            "insertOne": {
                "document" : {"_id": "doc1", "username" : "aaron"}
            }
        }
    """;

  public static String insertOne_doc2 =
      """
        {
            "insertOne": {
                "document" : {"_id": "doc2", "username" : "mahesh"}
            }
        }
    """;

  public static String insertOne_doc3 =
      """
        {
            "insertOne": {
                "document" : {"_id": "doc3", "username" : "mahesh"}
            }
        }
    """;

  // ==================================================
  // Postman - Clause - Filter - Equals for Top Level String, Numbers, Boolean, null
  // ==================================================

  public static String insertOne_doc2_text_number_bool_null =
      """
        {
            "insertOne": {
                "document" : {"_id": "doc2", "username" : "aaron", "human": true, "age": 47, "password": null}
            }
        }
    """;

  public static String find = """
        {
            "find": {

            }
        }
    """;

  public static String find_with_Filter =
      """
        {
            "find": {
                "filter" : {"username": "mahesh"}
            }
        }
    """;

  /*
  *  To run find_with_Filter_nextPage
  *  1) Insert insertOne_doc1, insertOne_doc2, and insertOne_doc3
  *  2) Change the page size to 1 - docapi.operations.CQLStatement.RESULTS_PER_PAGE
  *  3) Run find_with_Filter
  *  4) Copy the nextPageState value to pagingState in the below query find_with_Filter_nextPage

   */
  public static String find_with_Filter_nextPage =
      """
        {
            "find": {
                "filter" : {"username": "mahesh"},
                "options" : {"pagingState" : "000c001004646f633200f07ffffffe0094feaef8fd9baca90873e6e8a2a594950004" }
            }
        }
    """;

  public static String find_with_Filter_limit =
      """
        {
            "find": {
                "filter" : {"username": "mahesh"},
                "options" : {"limit": 1 }
            }
        }
    """;
  public static String findOne_doc2_by_id =
      """
        {
            "findOne": {
                "filter" : {"_id": "doc2"}
            }
        }
    """;

  public static String findOne_doc2_by_username =
      """
        {
            "findOne": {
                "filter" : {"username": "aaron"}
            }
        }
    """;

  public static String findOne_doc2_by_age =
      """
        {
            "findOne": {
                "filter" : {"age": 47}
            }
        }
    """;

  public static String findOne_doc2_by_human =
      """
        {
            "findOne": {
                "filter" : {"human": true}
            }
        }
    """;

  public static String findOne_doc2_by_password =
      """
        {
            "findOne": {
                "filter" : {"password": null}
            }
        }
    """;

  // ==================================================
  // Postman - Clause - Filter - Multiple equals conditions
  // ==================================================

  public static String findOne_doc2_by_age_human_password =
      """
        {
            "findOne": {
                "filter" : {"age": 47, "human": true, "password": null}
            }
        }
    """;

  // ==================================================
  // Postman - DML Command - find
  // ==================================================

  public static String find_no_filter =
      String.format(
          """
        {
            "find": {
                "filter" : {}
            }
        }
    """,
          System.currentTimeMillis());

  // ==================================================
  // Postman - DML Command - updateOne
  // ==================================================

  public static String updateOne_doc1_by_id =
      String.format(
          """
        {
            "updateOne": {
                "filter" : {"_id": "doc1"},
                "update" : {"$set" : {"updated_field" : "System.currentTimeMillis = %s"}}
            }
        }
    """,
          System.currentTimeMillis());

  // ==================================================
  // Postman - DML Command - findOneAndUpdate
  // ==================================================

  public static String findOneAndUpdate_doc1_by_id =
      String.format(
          """
        {
            "findOneAndUpdate": {
                "filter" : {"_id": "doc1"},
                "update" : {"$set" : {"find and updated updated_field" : "System.currentTimeMillis = %s"}},
                "options" : {"returnDocument": "before"}
            }
        }
    """,
          System.currentTimeMillis());
}
