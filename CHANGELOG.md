# Changelog

## [Unreleased](https://github.com/stargate/data-api/tree/HEAD)

[Full Changelog](https://github.com/stargate/data-api/compare/v1.0.18...HEAD)

## [v1.0.18](https://github.com/stargate/data-api/tree/v1.0.18) (2024-10-15)

[Full Changelog](https://github.com/stargate/data-api/compare/v1.0.17...v1.0.18)

**Closed issues:**

- Can't insert vector field with tables [\#1533](https://github.com/stargate/data-api/issues/1533)
- Issue with `COLLECTION\_NOT\_EXIST` error w/ Errors v2 [\#1531](https://github.com/stargate/data-api/issues/1531)
- Table Filter, $binary EJSON for Blob column datatype. [\#1513](https://github.com/stargate/data-api/issues/1513)
- Enable HTTP request log \(access log\) in Production [\#1498](https://github.com/stargate/data-api/issues/1498)
- Data API JsonCodec: Vector, add support for `vector\<float\>` [\#1492](https://github.com/stargate/data-api/issues/1492)
- Refactor WhereCQLClauseAnalyzer [\#1485](https://github.com/stargate/data-api/issues/1485)
- Table Primary Key filtering, multiple corner cases. [\#1481](https://github.com/stargate/data-api/issues/1481)
- Further detailed consideration of how we enable ALLOW FILTERING  [\#1476](https://github.com/stargate/data-api/issues/1476)
- code tidy: use  the CommandResultBuilder  everywhere  [\#1463](https://github.com/stargate/data-api/issues/1463)
- code tidy: tweak CommandResultBuilder to make it more buildery  [\#1461](https://github.com/stargate/data-api/issues/1461)
- accommodate ALLOW FILTERING to OnGoingWhereClause for table [\#1445](https://github.com/stargate/data-api/issues/1445)
- Data API JsonCodec impl: misc other type\(s\) \(inet\) [\#1386](https://github.com/stargate/data-api/issues/1386)

**Merged pull requests:**

- Implement \#1492: basic Vector\<Float\> support with Number arrays [\#1542](https://github.com/stargate/data-api/pull/1542) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- `createVectorIndex` command implementation [\#1541](https://github.com/stargate/data-api/pull/1541) ([maheshrajamani](https://github.com/maheshrajamani))
- Table filter scalar, uuid/timeuuid/inet [\#1538](https://github.com/stargate/data-api/pull/1538) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Fix \#1531: revert change to error messages [\#1536](https://github.com/stargate/data-api/pull/1536) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- List tables refactor [\#1535](https://github.com/stargate/data-api/pull/1535) ([maheshrajamani](https://github.com/maheshrajamani))
- Add unit tests, filter operators against scalar column datatypes [\#1528](https://github.com/stargate/data-api/pull/1528) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Add one IT for binary vector support [\#1527](https://github.com/stargate/data-api/pull/1527) ([Hazel-Datastax](https://github.com/Hazel-Datastax))
- Create index command implementation [\#1526](https://github.com/stargate/data-api/pull/1526) ([maheshrajamani](https://github.com/maheshrajamani))
- Do not publish json-api image to dockerhub [\#1522](https://github.com/stargate/data-api/pull/1522) ([Yuqi-Du](https://github.com/Yuqi-Du))

## [v1.0.17](https://github.com/stargate/data-api/tree/v1.0.17) (2024-10-09)

[Full Changelog](https://github.com/stargate/data-api/compare/v1.0.16...v1.0.17)

**Implemented enhancements:**

- adding tracing ID [\#593](https://github.com/stargate/data-api/issues/593)

**Closed issues:**

- Implement `listTables` command [\#1500](https://github.com/stargate/data-api/issues/1500)
- Support binary vectors for Collections [\#1494](https://github.com/stargate/data-api/issues/1494)
- Unexpected Field "embeddingsByType" in Response from `amazon.titan-embed-text-v2:0` Model Causing Deserialization Error [\#1486](https://github.com/stargate/data-api/issues/1486)
- Handle `null` from CQL driver when converting column values to JSON \(`findOne` etc\) [\#1460](https://github.com/stargate/data-api/issues/1460)
- Data API JsonCodec impl: unfrozen container types \(Set, List\) [\#1450](https://github.com/stargate/data-api/issues/1450)
- Incorporate warning msg with Table read path [\#1447](https://github.com/stargate/data-api/issues/1447)
- Data API JsonCodec impl: UUID \(uuid, timeuuid\) [\#1442](https://github.com/stargate/data-api/issues/1442)
- bridge related documentation cleanups  [\#1441](https://github.com/stargate/data-api/issues/1441)
- Add tests to ensure that "fractional" values like `10.0` and scientific notation like `1.23E+02` accepted as integer values [\#1429](https://github.com/stargate/data-api/issues/1429)
- Tables number codec: support "NaN", "Infinity", "-Infinity" for `float`, `double` CQL types [\#1428](https://github.com/stargate/data-api/issues/1428)
- refactor table insert operation, move calling the driver to TableInsertAttempt [\#1424](https://github.com/stargate/data-api/issues/1424)
- Optimize `JSONCodecRegistry` to-CQL codec lookup: linear search won't scale [\#1417](https://github.com/stargate/data-api/issues/1417)
- addIndex seems to fail on names with uppercase \(missing double quotes\)? [\#1404](https://github.com/stargate/data-api/issues/1404)
- Data API JsonCodec impl: "blob" \(Binary\) type [\#1403](https://github.com/stargate/data-api/issues/1403)
- Data API JsonCodec impl: misc other type\(s\) \(inet\) [\#1386](https://github.com/stargate/data-api/issues/1386)
- Data API JsonCodec impl: date/time values \(date, duration, time, timestamp\) [\#1385](https://github.com/stargate/data-api/issues/1385)
- Add new keyspace commands, deprecate namespace commands [\#1376](https://github.com/stargate/data-api/issues/1376)
- Data API, JsonCodec testing: textual values \(ascii, text, varchar\) [\#1364](https://github.com/stargate/data-api/issues/1364)
- Create collection fails if there is an existing CQL table of the same name [\#1198](https://github.com/stargate/data-api/issues/1198)
- Azure OpenAI: use "deployment name" instead of "deployment ID" [\#1126](https://github.com/stargate/data-api/issues/1126)

**Merged pull requests:**

- Add "inet" codec [\#1517](https://github.com/stargate/data-api/pull/1517) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- use stargate-v2.1.0-BETA-18 [\#1512](https://github.com/stargate/data-api/pull/1512) ([Yuqi-Du](https://github.com/Yuqi-Du))
- List tables command [\#1510](https://github.com/stargate/data-api/pull/1510) ([maheshrajamani](https://github.com/maheshrajamani))
- Fix bump\_stargate script [\#1507](https://github.com/stargate/data-api/pull/1507) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Support binary vectors for Collections [\#1506](https://github.com/stargate/data-api/pull/1506) ([Hazel-Datastax](https://github.com/Hazel-Datastax))
- use stargate-v2.1.0-BETA-17 [\#1505](https://github.com/stargate/data-api/pull/1505) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Fix \#1498: enable access/request log by default [\#1503](https://github.com/stargate/data-api/pull/1503) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Add instruction for Data API on-prem support [\#1502](https://github.com/stargate/data-api/pull/1502) ([Yuqi-Du](https://github.com/Yuqi-Du))
- SchemaObject changes to support multiple vector configs [\#1499](https://github.com/stargate/data-api/pull/1499) ([maheshrajamani](https://github.com/maheshrajamani))
- Fix \#1442: add codecs for UUID type\(s\) [\#1496](https://github.com/stargate/data-api/pull/1496) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Table filter refactor [\#1490](https://github.com/stargate/data-api/pull/1490) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Add vectorize config to tables [\#1489](https://github.com/stargate/data-api/pull/1489) ([maheshrajamani](https://github.com/maheshrajamani))
- Fix `Unable to parse embedding provider response message` errors from Bedrock `amazon.titan-embed-text-v2:0` [\#1487](https://github.com/stargate/data-api/pull/1487) ([Hazel-Datastax](https://github.com/Hazel-Datastax))
- Update display name and help text for Azure OpenAI parameters [\#1483](https://github.com/stargate/data-api/pull/1483) ([Hazel-Datastax](https://github.com/Hazel-Datastax))
- Offline Mode Fix [\#1475](https://github.com/stargate/data-api/pull/1475) ([kathirsvn](https://github.com/kathirsvn))
- Create table command implementation [\#1473](https://github.com/stargate/data-api/pull/1473) ([maheshrajamani](https://github.com/maheshrajamani))
- Unit tests for operation attempt [\#1472](https://github.com/stargate/data-api/pull/1472) ([amorton](https://github.com/amorton))
- Fix \#1450: add support for List, Set codecs [\#1471](https://github.com/stargate/data-api/pull/1471) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix \#1460, handle CQL nulls to  JSON gracefully [\#1464](https://github.com/stargate/data-api/pull/1464) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Offline mode fix [\#1456](https://github.com/stargate/data-api/pull/1456) ([kathirsvn](https://github.com/kathirsvn))
- Refactor CQLSessionCache and CqlCredentials [\#1455](https://github.com/stargate/data-api/pull/1455) ([amorton](https://github.com/amorton))
- Include error ID in the error response for new error format [\#1454](https://github.com/stargate/data-api/pull/1454) ([amorton](https://github.com/amorton))
- Add CommandResultBuilder [\#1453](https://github.com/stargate/data-api/pull/1453) ([amorton](https://github.com/amorton))
- Fix \#1404: failing addIndex with case-sensitive name [\#1452](https://github.com/stargate/data-api/pull/1452) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fixes \#1385: add date/time codec support [\#1449](https://github.com/stargate/data-api/pull/1449) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Table Filters for scalar column, ALLOW FILTERING, WARNING [\#1448](https://github.com/stargate/data-api/pull/1448) ([Yuqi-Du](https://github.com/Yuqi-Du))
- clean up stargate bridge related documentation [\#1444](https://github.com/stargate/data-api/pull/1444) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Fix \#1417: optimize/improve codec lookup handling [\#1443](https://github.com/stargate/data-api/pull/1443) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fixes \#1403: Add support for Blob types for insert [\#1435](https://github.com/stargate/data-api/pull/1435) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Refactor to add  OperationAttempt [\#1434](https://github.com/stargate/data-api/pull/1434) ([amorton](https://github.com/amorton))
- Fixes \#1428: support "not a number" values for 'float', 'double' columns [\#1433](https://github.com/stargate/data-api/pull/1433) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fixes \#1429: add ITs to verify handling of "fractional ints" \(allowed/non-allowed\) [\#1432](https://github.com/stargate/data-api/pull/1432) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- add xml-format plugin [\#1430](https://github.com/stargate/data-api/pull/1430) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Start refactoring CollectionSchemaObject [\#1427](https://github.com/stargate/data-api/pull/1427) ([amorton](https://github.com/amorton))

## [v1.0.16](https://github.com/stargate/data-api/tree/v1.0.16) (2024-09-17)

[Full Changelog](https://github.com/stargate/data-api/compare/v1.0.15...v1.0.16)

**Closed issues:**

- Error object V2 templates failing to load in IT's  [\#1421](https://github.com/stargate/data-api/issues/1421)
- Simplify the NamedValueContainers  [\#1411](https://github.com/stargate/data-api/issues/1411)
- remove the WritableDocRow interface [\#1410](https://github.com/stargate/data-api/issues/1410)
- Move or Delete AwsBedrockVectorSearchIntegrationTest [\#1399](https://github.com/stargate/data-api/issues/1399)
- createTable command with `\_id` errors out [\#1396](https://github.com/stargate/data-api/issues/1396)
- Data API filter refactor, logicalExpression [\#1389](https://github.com/stargate/data-api/issues/1389)
- Exceptions from Database \(via CQL Driver\) incorrectly mapped to HTTP 500, should be 504 \(or 502\) [\#1383](https://github.com/stargate/data-api/issues/1383)
- Data API/JsonCodec: floating-point \(FP\) numeric values \(`decimal`, `double`, `float`\), testing, impl [\#1366](https://github.com/stargate/data-api/issues/1366)
- Data API, JsonCodec testing: integral numeric values \(`bigint`, `smallint`, `int`, `tinyint`, `varint`\) [\#1365](https://github.com/stargate/data-api/issues/1365)
- Change `ObjectMapperConfiguration` to allow static access, not just injection [\#1361](https://github.com/stargate/data-api/issues/1361)
- Not able to filter by \_id with tables [\#1357](https://github.com/stargate/data-api/issues/1357)
- Do not expose table commands in swaggerUI before the feature goes public [\#1353](https://github.com/stargate/data-api/issues/1353)
- Not able to use column names with upper-case letters, special characters [\#1349](https://github.com/stargate/data-api/issues/1349)
- Upgrade Jackson to 2.17\(.2\) \(from 2.16\(.2\)\) to align with Quarkus [\#1343](https://github.com/stargate/data-api/issues/1343)
- Remove use of `io.stargate:sgv2-api-parent` parent pom by Data API [\#1341](https://github.com/stargate/data-api/issues/1341)
- Update to DSE 6.9.1 [\#1336](https://github.com/stargate/data-api/issues/1336)
- Refactor feature flags to use combination of Quarkus config and per-request overrides [\#1335](https://github.com/stargate/data-api/issues/1335)
- Should be able to use column names like `\_id` with API Tables too [\#1334](https://github.com/stargate/data-api/issues/1334)
- Refactor traverseForNot and flip in the API tier  [\#1319](https://github.com/stargate/data-api/issues/1319)

**Merged pull requests:**

- clean up namespace terminology for spec and test files, convert to keyspace [\#1426](https://github.com/stargate/data-api/pull/1426) ([Yuqi-Du](https://github.com/Yuqi-Du))
- fixes \#1421 - improved error template loading [\#1422](https://github.com/stargate/data-api/pull/1422) ([amorton](https://github.com/amorton))
- Fix \#1364: add tests, fixes to text value codecs \(to CQL\) [\#1419](https://github.com/stargate/data-api/pull/1419) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- simplify the NamedValueContainers structure [\#1414](https://github.com/stargate/data-api/pull/1414) ([amorton](https://github.com/amorton))
- Remove no longer used WritableDocRow Fixes \#1410 [\#1413](https://github.com/stargate/data-api/pull/1413) ([amorton](https://github.com/amorton))
- Add publicCommandName for command interface, add PublicCommandName enum [\#1408](https://github.com/stargate/data-api/pull/1408) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Minor refactoring: hide type, name fields of SchemaObject [\#1405](https://github.com/stargate/data-api/pull/1405) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Remove AWS bedrock IT test [\#1402](https://github.com/stargate/data-api/pull/1402) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Add test for \#1396: allow use of columns with "quotable" names \(like "\_id"\) for tables [\#1401](https://github.com/stargate/data-api/pull/1401) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Bump peter-evans/create-pull-request from 6 to 7 in the github-actions group [\#1398](https://github.com/stargate/data-api/pull/1398) ([dependabot[bot]](https://github.com/apps/dependabot))
- deprecate namespace, convert to keyspace [\#1397](https://github.com/stargate/data-api/pull/1397) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Fix \#1366: add missing CQL FP number type conversions, unit tests [\#1395](https://github.com/stargate/data-api/pull/1395) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Move the Error Object V2 code from "playing" package to exceptions [\#1392](https://github.com/stargate/data-api/pull/1392) ([amorton](https://github.com/amorton))
- DriverExceptionHandler and APIException Improvements [\#1391](https://github.com/stargate/data-api/pull/1391) ([amorton](https://github.com/amorton))
- Fix \#1335: add general-purpose Feature flag system [\#1390](https://github.com/stargate/data-api/pull/1390) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Refactor filter path, remove LogicalExpression from DB operation level. [\#1388](https://github.com/stargate/data-api/pull/1388) ([Yuqi-Du](https://github.com/Yuqi-Du))
- insertOne with validation  [\#1387](https://github.com/stargate/data-api/pull/1387) ([amorton](https://github.com/amorton))
- Fix \#1383: map most recognized SERVER\_ errors to 504 or 502, leave 500 for unrecognized [\#1384](https://github.com/stargate/data-api/pull/1384) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix a minor flaw in logging non-JsonApiException logging by CommandProcessor \(missing stack trace\) [\#1382](https://github.com/stargate/data-api/pull/1382) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix problem with ErrorCode initialization wrt config access [\#1380](https://github.com/stargate/data-api/pull/1380) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Error object v2 [\#1371](https://github.com/stargate/data-api/pull/1371) ([amorton](https://github.com/amorton))
- Fixes \#1365: add integral number codecs, tests [\#1367](https://github.com/stargate/data-api/pull/1367) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
-  Starting tests for JSONCodecRegistry [\#1363](https://github.com/stargate/data-api/pull/1363) ([amorton](https://github.com/amorton))
- hide table feature in swaggerUI before it goes public [\#1360](https://github.com/stargate/data-api/pull/1360) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Fix `findOne`/`find` wrt `\_id` column \(actually all filterables\) [\#1359](https://github.com/stargate/data-api/pull/1359) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Add invertForTableCommand override for table path [\#1356](https://github.com/stargate/data-api/pull/1356) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Fix \#1349: support "non-standard" column names [\#1355](https://github.com/stargate/data-api/pull/1355) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Rename Java package for API Tables ITs \(to conform to Java package naming convention\) [\#1354](https://github.com/stargate/data-api/pull/1354) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Update `help` text for OpenAI and huggingfaceDedicated [\#1352](https://github.com/stargate/data-api/pull/1352) ([Hazel-Datastax](https://github.com/Hazel-Datastax))
- remove stargate parent pom [\#1351](https://github.com/stargate/data-api/pull/1351) ([Yuqi-Du](https://github.com/Yuqi-Du))
- update Data API to DSE 6.9.1 [\#1350](https://github.com/stargate/data-api/pull/1350) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Fix CreateTableOperation identifier issue, add several table ITs. [\#1348](https://github.com/stargate/data-api/pull/1348) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Reintroduce not operator refactor [\#1347](https://github.com/stargate/data-api/pull/1347) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Revert "init" [\#1345](https://github.com/stargate/data-api/pull/1345) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Fixes \#1343: Upgrade Jackson to 2.17\(.2\) from 2.16\(.2\) [\#1344](https://github.com/stargate/data-api/pull/1344) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Second part of Quarkus 3.13 upgrade: convert ITs to use @WithTestResource [\#1342](https://github.com/stargate/data-api/pull/1342) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix find command limit option for tables [\#1340](https://github.com/stargate/data-api/pull/1340) ([maheshrajamani](https://github.com/maheshrajamani))
- Added support for addIndex and dropIndex commands [\#1322](https://github.com/stargate/data-api/pull/1322) ([maheshrajamani](https://github.com/maheshrajamani))
- Update Data API to the latest Quarkus \(3.13.2\) [\#1253](https://github.com/stargate/data-api/pull/1253) ([tatu-at-datastax](https://github.com/tatu-at-datastax))

## [v1.0.15](https://github.com/stargate/data-api/tree/v1.0.15) (2024-08-14)

[Full Changelog](https://github.com/stargate/data-api/compare/v1.0.14...v1.0.15)

**Implemented enhancements:**

- Bridge Removal - separate Data API Logging from Quarkus Common Module [\#978](https://github.com/stargate/data-api/issues/978)
- Bridge Removal - separate Data API Metrics from Quarkus Common Module [\#974](https://github.com/stargate/data-api/issues/974)

**Closed issues:**

- Create and use new `ErrorCode` for Vector-size-mismatch case of `INVALID\_QUERY` [\#1332](https://github.com/stargate/data-api/issues/1332)
- Misspelt class name AwsBedrockEnbeddingProvider [\#1308](https://github.com/stargate/data-api/issues/1308)
- Remove unnecessary \(if so\) `equals\(\)`, `hashCode\(\)` implementations from `IDFilterBase` sub-classes [\#1288](https://github.com/stargate/data-api/issues/1288)
- Convert internal "assert"-style `RuntimeException` into `JsonApiException` with `ErrorCode.SERVER\_INTERNAL\_ERROR` [\#1287](https://github.com/stargate/data-api/issues/1287)
- Update Data API to HCD 1.0.0 [\#1285](https://github.com/stargate/data-api/issues/1285)
- Update Data API to DSE 6.9.0 [\#1284](https://github.com/stargate/data-api/issues/1284)
- Re-factor: use `ErrorCode.toApiException\(\)` everywhere instead of `new JsonApiException` [\#1283](https://github.com/stargate/data-api/issues/1283)
- Remove dse-next persistence from ITs [\#1282](https://github.com/stargate/data-api/issues/1282)
- Remove `url` requirement in `EmbeddingProvidersConfig` yaml config matching  [\#1231](https://github.com/stargate/data-api/issues/1231)
- Log error codes and messages  [\#1197](https://github.com/stargate/data-api/issues/1197)
- Create a nightly run CI to test all vectorize models for all providers [\#1196](https://github.com/stargate/data-api/issues/1196)
- Hugging Face Serverless arbitrary model choice [\#1145](https://github.com/stargate/data-api/issues/1145)
- Provider side embedding model choice  [\#1144](https://github.com/stargate/data-api/issues/1144)
- Bridge Removal - fix tests with whatever profile and resources that tests relied on [\#1140](https://github.com/stargate/data-api/issues/1140)
- Remove dependency for Stargate bridge - quarkus commons [\#920](https://github.com/stargate/data-api/issues/920)
- Postman Collection workflow run failing [\#840](https://github.com/stargate/data-api/issues/840)

**Merged pull requests:**

- Allow use of `\_id` for "insertOne" [\#1338](https://github.com/stargate/data-api/pull/1338) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix jar publishing workflows [\#1337](https://github.com/stargate/data-api/pull/1337) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Fixes \#1332: separate out ErrorCode.VECTOR\_SIZE\_MISMATCH from INVALID\_QUERY [\#1333](https://github.com/stargate/data-api/pull/1333) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Removed offline mode code [\#1330](https://github.com/stargate/data-api/pull/1330) ([kathirsvn](https://github.com/kathirsvn))
- Offline mode fix [\#1329](https://github.com/stargate/data-api/pull/1329) ([kathirsvn](https://github.com/kathirsvn))
- workaround to fix docker-compose in postman GH workflow [\#1328](https://github.com/stargate/data-api/pull/1328) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- rename bedrock embedding provider to fix spelling error [\#1327](https://github.com/stargate/data-api/pull/1327) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Extend error response with additional information [\#1325](https://github.com/stargate/data-api/pull/1325) ([Hazel-Datastax](https://github.com/Hazel-Datastax))
- POC for deleteOne and deleteMany for tables [\#1324](https://github.com/stargate/data-api/pull/1324) ([amorton](https://github.com/amorton))
- FindMany bug fix and extra logging for Tables POC [\#1323](https://github.com/stargate/data-api/pull/1323) ([amorton](https://github.com/amorton))
- Drop table command implementation [\#1321](https://github.com/stargate/data-api/pull/1321) ([maheshrajamani](https://github.com/maheshrajamani))
-  POC for updateOne on a table [\#1318](https://github.com/stargate/data-api/pull/1318) ([amorton](https://github.com/amorton))
- PoC for Projection implementation for API Tables [\#1315](https://github.com/stargate/data-api/pull/1315) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- POC for table projections and using JSONCodec [\#1314](https://github.com/stargate/data-api/pull/1314) ([amorton](https://github.com/amorton))
- POC for table filters with codecs for findOne [\#1313](https://github.com/stargate/data-api/pull/1313) ([amorton](https://github.com/amorton))
- Added ValidatableCommandClause POC [\#1309](https://github.com/stargate/data-api/pull/1309) ([amorton](https://github.com/amorton))
- Remove native image support [\#1307](https://github.com/stargate/data-api/pull/1307) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Use Stargate v2.1.0-BETA-14 [\#1306](https://github.com/stargate/data-api/pull/1306) ([github-actions[bot]](https://github.com/apps/github-actions))
- Create table command implementation [\#1303](https://github.com/stargate/data-api/pull/1303) ([maheshrajamani](https://github.com/maheshrajamani))
- Fix \#1287: convert internal assertion exceptions to use JsonApiException.SERVER\_INTERNAL\_ERROR [\#1301](https://github.com/stargate/data-api/pull/1301) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- update project to use JDK 21 [\#1300](https://github.com/stargate/data-api/pull/1300) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- update openjdk base image [\#1299](https://github.com/stargate/data-api/pull/1299) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- 415 for invalid Content-Type header, remove redundant Token Filter [\#1298](https://github.com/stargate/data-api/pull/1298) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Unify JsonApiException construction for easier management [\#1297](https://github.com/stargate/data-api/pull/1297) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- disabling IT with dse-next from CI workflow [\#1296](https://github.com/stargate/data-api/pull/1296) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Convert Delete\*IntegrationTests to compact code [\#1295](https://github.com/stargate/data-api/pull/1295) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Initial refactoring for API Tables feature \(complete\) [\#1292](https://github.com/stargate/data-api/pull/1292) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- update to DSE 6.9.0, HCD 1.0.0 [\#1286](https://github.com/stargate/data-api/pull/1286) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Fix Nvidia header problem [\#1280](https://github.com/stargate/data-api/pull/1280) ([Hazel-Datastax](https://github.com/Hazel-Datastax))
- fix stargate log level to info [\#1279](https://github.com/stargate/data-api/pull/1279) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- run unit and integration tests in parallel in CI [\#1278](https://github.com/stargate/data-api/pull/1278) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- vectorization on demand [\#1258](https://github.com/stargate/data-api/pull/1258) ([Yuqi-Du](https://github.com/Yuqi-Du))
- CreateCollection: Check if a table follows Data API collection pattern [\#1249](https://github.com/stargate/data-api/pull/1249) ([Yuqi-Du](https://github.com/Yuqi-Du))

## [v1.0.14](https://github.com/stargate/data-api/tree/v1.0.14) (2024-07-15)

[Full Changelog](https://github.com/stargate/data-api/compare/v1.0.13...v1.0.14)

**Highlights:**

- Support AWS Bedrock Embedding Provider
- Set lower default for Azure OpenAI model "text-embedding-3-large" to observed legal value
- Add `displayName` and `hint` in embedding providers parameters configuration
- Error handling improvements

**Closed issues:**

- Improve error message when EGW timeout [\#1254](https://github.com/stargate/data-api/issues/1254)
- Improve the error message when users call vectorize but don't provide the provider key through `x-embedding-api-key` [\#1250](https://github.com/stargate/data-api/issues/1250)
- Azure Open AI provider \(`azureOpenAI`\) default settings for large model \(`text-embedding-3-large`\) wrong [\#1241](https://github.com/stargate/data-api/issues/1241)
- Replace internal \(non-user-triggerable\) `ErrorCode` with `ErrorCode.SERVER\_INTERNAL\_ERROR`  [\#1238](https://github.com/stargate/data-api/issues/1238)
- Improve failure message for `ErrorCode.COMMAND\_UNKNOWN` to include legal options [\#1236](https://github.com/stargate/data-api/issues/1236)
- Unify `ErrorCode`s `COMMAND\_NOT\_IMPLEMENTED` and `NO\_COMMAND\_MATCHED` into `COMMAND\_UNKNOWN` [\#1232](https://github.com/stargate/data-api/issues/1232)
- PoC for "API Tables": `findRows` command on existing CQL table\(s\) [\#1224](https://github.com/stargate/data-api/issues/1224)
- Add namespace and collection name to log lines [\#1168](https://github.com/stargate/data-api/issues/1168)

**Merged pull requests:**

- refine log levels to eliminate unwanted messages [\#1270](https://github.com/stargate/data-api/pull/1270) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Fix a performance issue wrt. not reusing ObjectMappers for command logging [\#1269](https://github.com/stargate/data-api/pull/1269) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Null check for errors in CommandResult [\#1268](https://github.com/stargate/data-api/pull/1268) ([kathirsvn](https://github.com/kathirsvn))
- Add `hint` and `displayName` in parameters [\#1267](https://github.com/stargate/data-api/pull/1267) ([Hazel-Datastax](https://github.com/Hazel-Datastax))
- Increase mem limit of single-HCD set up from 2 to 3 gigs; DSEx3 to 2.5 gigs per node [\#1264](https://github.com/stargate/data-api/pull/1264) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix Quarkus REST package refs/names as per 3.9 upgrade instructions [\#1263](https://github.com/stargate/data-api/pull/1263) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Revert "Follow up for PR \#1251: Remove `Optional` and centralize validation" [\#1262](https://github.com/stargate/data-api/pull/1262) ([Hazel-Datastax](https://github.com/Hazel-Datastax))
- Reduce boiler-plate code for "CountIntegrationTest" by 40% [\#1261](https://github.com/stargate/data-api/pull/1261) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix \#1241: lower default for Azure OpenAI model "text-embedding-3-large" to observed legal value [\#1260](https://github.com/stargate/data-api/pull/1260) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Follow up for PR \#1251: Remove `Optional` and centralize validation [\#1259](https://github.com/stargate/data-api/pull/1259) ([Hazel-Datastax](https://github.com/Hazel-Datastax))
- Improve error message when EGW timeout [\#1255](https://github.com/stargate/data-api/pull/1255) ([Hazel-Datastax](https://github.com/Hazel-Datastax))
- Use Stargate v2.1.0-BETA-13 [\#1252](https://github.com/stargate/data-api/pull/1252) ([github-actions[bot]](https://github.com/apps/github-actions))
- Small fix: Improve error message when not providing provider key through `x-embedding-api-key` [\#1251](https://github.com/stargate/data-api/pull/1251) ([Hazel-Datastax](https://github.com/Hazel-Datastax))
- Fix a problem that would prevent Quarkus 3.12 upgrade \(due to change in Smallrye lib\) [\#1248](https://github.com/stargate/data-api/pull/1248) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- fix http port to 8181, refactor application.yaml as alphabetic order [\#1247](https://github.com/stargate/data-api/pull/1247) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Update Quarkus Data API uses to 3.9.5 \(from parent 3.6.x\) [\#1246](https://github.com/stargate/data-api/pull/1246) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Refactor unit test profiles, try to speed things up [\#1244](https://github.com/stargate/data-api/pull/1244) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix accidental dependencies to shaded packages [\#1243](https://github.com/stargate/data-api/pull/1243) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fixes \#1238: convert internal-only `ErrorCode`s into `ErrorCode.SERVER\_INTERNAL\_ERROR` [\#1239](https://github.com/stargate/data-api/pull/1239) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fixes \#1236: add valid Command names in fail message [\#1237](https://github.com/stargate/data-api/pull/1237) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix base64 encoding for 'cassandra' in docstring [\#1235](https://github.com/stargate/data-api/pull/1235) ([hemidactylus](https://github.com/hemidactylus))
- Added highlights for v1.0.13 release [\#1234](https://github.com/stargate/data-api/pull/1234) ([kathirsvn](https://github.com/kathirsvn))
- Fix \#1232: unify/rename 2 different ErrorCodes for similar thing [\#1233](https://github.com/stargate/data-api/pull/1233) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fixed the index usage metrics for IDFilter and InFilter [\#1227](https://github.com/stargate/data-api/pull/1227) ([maheshrajamani](https://github.com/maheshrajamani))
- Update READMEs wrt running things locally [\#1225](https://github.com/stargate/data-api/pull/1225) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Support AWS Bedrock Embedding Provider [\#1219](https://github.com/stargate/data-api/pull/1219) ([Hazel-Datastax](https://github.com/Hazel-Datastax))
- Refactoring to allow fixing \#1216 by making `JsonApiException` accept HTTP status code, propagate [\#1217](https://github.com/stargate/data-api/pull/1217) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Bridge-Removal - Detach the quarkus-common-module dependency \(OFF WE GO!\) [\#1191](https://github.com/stargate/data-api/pull/1191) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Bridge-Removal - miscellaneous cleanups [\#1149](https://github.com/stargate/data-api/pull/1149) ([Yuqi-Du](https://github.com/Yuqi-Du))

## [v1.0.13](https://github.com/stargate/data-api/tree/v1.0.13) (2024-07-01)

[Full Changelog](https://github.com/stargate/data-api/compare/v1.0.12...v1.0.13)

**Highlights:**

- Error handling improvements
- Max-collection/max-indexes-available limits aligned with HCD defaults
- Fixed InsertMany batch failing completely in some scenarios
- Added two more VoyageAI models voyage-finance-2 and voyage-multilingual-2

**Closed issues:**

- `SERVER\_UNHANDLED\_ERROR ` from `IllegalArgumentException` from CQL Driver if passing empty token as Astra credential [\#1210](https://github.com/stargate/data-api/issues/1210)
- `SERVER\_UNHANDLED\_ERROR` from `UnrecognizedPropertyException` [\#1207](https://github.com/stargate/data-api/issues/1207)
- Unmapped `AllNodesFailedException` \(`SERVER\_UNHANDLED\_ERROR`\): `com.datastax.oss.driver.api.core.DriverTimeoutException` not handled [\#1205](https://github.com/stargate/data-api/issues/1205)
- Include type of unmapped exception in message by `ThrowableToErrorMapper` [\#1203](https://github.com/stargate/data-api/issues/1203)
- Enable `StreamReadFeature.INCLUDE\_SOURCE\_IN\_LOCATION` to get JSON source \(snippet\) logged [\#1201](https://github.com/stargate/data-api/issues/1201)
- Invalid non-JSON request \("Unexpected character"\) results in `SERVER\_UNHANDLED\_ERROR` 500 failure [\#1200](https://github.com/stargate/data-api/issues/1200)
- Invalid JSON structure throwing `UnrecognizedPropertyException ` results in `SERVER\_UNHANDLED\_ERROR` 500 failure [\#1199](https://github.com/stargate/data-api/issues/1199)
- Update max-collection/max-indexes-available limits to align with HCD defaults [\#1194](https://github.com/stargate/data-api/issues/1194)
- Rename misleading `ErrorCode.INVALID\_COLLECTION\_NAME` as `EXISTING\_COLLECTION\_DIFFERENT\_SETTINGS` [\#1185](https://github.com/stargate/data-api/issues/1185)
- Change `EmbeddingProvider` from interface to abstract class [\#1173](https://github.com/stargate/data-api/issues/1173)
- NullPointerException from the multi credential supports [\#1172](https://github.com/stargate/data-api/issues/1172)
- insertMany batch fails completely \(instead of individual doc\) for some constraints violations [\#1167](https://github.com/stargate/data-api/issues/1167)
- Remove shared secret embedding provider options  [\#1153](https://github.com/stargate/data-api/issues/1153)
- Refactor Embedding Providers [\#1152](https://github.com/stargate/data-api/issues/1152)

**Merged pull requests:**

- Fixes \#1205: add explicit handling of `DriverTimeoutException` in ThrowableToErrorMapper [\#1214](https://github.com/stargate/data-api/pull/1214) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fixes \#1199: map `UnrecognizedPropertyException` to INVALID\_REQUEST\_UNKNOWN\_FIELD [\#1213](https://github.com/stargate/data-api/pull/1213) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fixes \#1200: add handling for invalid JSON requests [\#1212](https://github.com/stargate/data-api/pull/1212) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fixes \#1210: pre-validate token before passing to Java CQL driver [\#1211](https://github.com/stargate/data-api/pull/1211) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Small fix: Add status code in the error message when provider returning non-JSON content [\#1209](https://github.com/stargate/data-api/pull/1209) ([Hazel-Datastax](https://github.com/Hazel-Datastax))
- Fixes \#1207: handle case of unknown "createCollection.options.indexing" fields [\#1208](https://github.com/stargate/data-api/pull/1208) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fixes \#1203: add Exception class direcly in logged message [\#1204](https://github.com/stargate/data-api/pull/1204) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fixes \#1201: enable inclusion of source JSON snippet on parsing failures [\#1202](https://github.com/stargate/data-api/pull/1202) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix \#1194: change max-collection/max-indexes-available setting \(double up\) [\#1195](https://github.com/stargate/data-api/pull/1195) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Add new VoyageAI models [\#1192](https://github.com/stargate/data-api/pull/1192) ([Hazel-Datastax](https://github.com/Hazel-Datastax))
- Index usage counter metrics by commands [\#1190](https://github.com/stargate/data-api/pull/1190) ([maheshrajamani](https://github.com/maheshrajamani))
- Refactor embedding providers classes [\#1189](https://github.com/stargate/data-api/pull/1189) ([Hazel-Datastax](https://github.com/Hazel-Datastax))
- Disable shared secret default [\#1188](https://github.com/stargate/data-api/pull/1188) ([maheshrajamani](https://github.com/maheshrajamani))
- Fixed \#1185: change `ErrorCode` constant to better reflect semantics [\#1187](https://github.com/stargate/data-api/pull/1187) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Update UpstageAI discontinued model [\#1183](https://github.com/stargate/data-api/pull/1183) ([Hazel-Datastax](https://github.com/Hazel-Datastax))
- Fix modelName constraint for huggingFace Dedicated provider [\#1181](https://github.com/stargate/data-api/pull/1181) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Fix \#1167: handle shredding failures properly for `insertMany` [\#1180](https://github.com/stargate/data-api/pull/1180) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- adding highlights to changelog [\#1174](https://github.com/stargate/data-api/pull/1174) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Add 'returnDocumentResponses' for "insertMany" [\#1161](https://github.com/stargate/data-api/pull/1161) ([tatu-at-datastax](https://github.com/tatu-at-datastax))

## [v1.0.12](https://github.com/stargate/data-api/tree/v1.0.12) (2024-06-17)

[Full Changelog](https://github.com/stargate/data-api/compare/v1.0.11...v1.0.12)

**Highlights:**

- Upstage Embedding provider display name, change from 'Upstage AI' to 'Upstage'
- logging improvement(add namespace and collection)
- error messages improvement
   - improve SHRED_DOC_KEY_NAME_VIOLATION error code message "Document field name invalid"
   - improve Data API vectorize error messages
- fix vectorize integration credentials bug regarding the table comment


**Closed issues:**

- Better handling for multi part credentials in createCollection [\#1142](https://github.com/stargate/data-api/issues/1142)

**Merged pull requests:**

- Add updated auth to table comment [\#1177](https://github.com/stargate/data-api/pull/1177) ([Hazel-Datastax](https://github.com/Hazel-Datastax))
- Add namespace and collection mdc log as needed [\#1176](https://github.com/stargate/data-api/pull/1176) ([Yuqi-Du](https://github.com/Yuqi-Du))
- upstage displayName and huggingFaceDedicated modelName small fix [\#1175](https://github.com/stargate/data-api/pull/1175) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Improve message for ErrorCode.SHRED\_DOC\_KEY\_NAME\_VIOLATION [\#1171](https://github.com/stargate/data-api/pull/1171) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Improve Error Messages from Embedding Providers [\#1159](https://github.com/stargate/data-api/pull/1159) ([Hazel-Datastax](https://github.com/Hazel-Datastax))

## [v1.0.11](https://github.com/stargate/data-api/tree/v1.0.11) (2024-06-13)

[Full Changelog](https://github.com/stargate/data-api/compare/v1.0.10...v1.0.11)

**Highlights:**

- Support includeSortVector option for find and findOne command (API addition)
- Improvements to vectorize feature
  - Adds HuggingFace dedicated provider support
  - Exclude $vector and $vectorize from default projection 
  - OpenAI provider - support for organization id and project id in request header

**Closed issues:**

- docker-compose changes to consider [\#1158](https://github.com/stargate/data-api/issues/1158)
- Log selected options on create collection [\#1155](https://github.com/stargate/data-api/issues/1155)
- Jina AI timeouts when model is cold  [\#1131](https://github.com/stargate/data-api/issues/1131)
- Support OpenAI Organization and  Project optional headers [\#1128](https://github.com/stargate/data-api/issues/1128)
- projection {"$vector": 1} does not project out the other fields anymore [\#1106](https://github.com/stargate/data-api/issues/1106)
- Misleading error message about projection having $fields [\#1038](https://github.com/stargate/data-api/issues/1038)
- Re-apply default Projection change to exclude `$vector` \(\#1005\) [\#1032](https://github.com/stargate/data-api/issues/1032)
- Exclude $vector and $vectorize from the default Projection [\#1005](https://github.com/stargate/data-api/issues/1005)

**Merged pull requests:**

- Better handling for multi part credentials in createCollection [\#1164](https://github.com/stargate/data-api/pull/1164) ([Hazel-Datastax](https://github.com/Hazel-Datastax))
- Log collection setting stored in comment [\#1163](https://github.com/stargate/data-api/pull/1163) ([maheshrajamani](https://github.com/maheshrajamani))
- Align Open AI text-embedding-3 dimension defaults to LangChain expectations [\#1162](https://github.com/stargate/data-api/pull/1162) ([Hazel-Datastax](https://github.com/Hazel-Datastax))
- Openai support for organization id and project id in request header [\#1160](https://github.com/stargate/data-api/pull/1160) ([maheshrajamani](https://github.com/maheshrajamani))
- Add huggingface dedicated provider support [\#1157](https://github.com/stargate/data-api/pull/1157) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Fix \#1038: improve error message for unknown dollar-starting field [\#1156](https://github.com/stargate/data-api/pull/1156) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Align gateway request properties with Mutiny HTTP client and Defaults [\#1154](https://github.com/stargate/data-api/pull/1154) ([Hazel-Datastax](https://github.com/Hazel-Datastax))
- Support `includeSortVector` option for `find` and `findOne` command [\#1151](https://github.com/stargate/data-api/pull/1151) ([maheshrajamani](https://github.com/maheshrajamani))
- Docker compose scripts to use  dse 6.9 as backend [\#1147](https://github.com/stargate/data-api/pull/1147) ([maheshrajamani](https://github.com/maheshrajamani))
- Fix null header exception from embedding provider's response [\#1139](https://github.com/stargate/data-api/pull/1139) ([Hazel-Datastax](https://github.com/Hazel-Datastax))
- \(for 1.0.11\) Fixes \#1032, re-does \#1005: exclude $vector and $vectorize from default projection [\#1037](https://github.com/stargate/data-api/pull/1037) ([tatu-at-datastax](https://github.com/tatu-at-datastax))

## [v1.0.10](https://github.com/stargate/data-api/tree/v1.0.10) (2024-06-04)

[Full Changelog](https://github.com/stargate/data-api/compare/v1.0.9...v1.0.10)

**Highlights:**

- Support for running Data API with DSE 6.9 and HCD

**Closed issues:**

- $vectorize: Misleading error when `authentication.providerKey` not set [\#1124](https://github.com/stargate/data-api/issues/1124)
- Params not passing to EGW [\#1111](https://github.com/stargate/data-api/issues/1111)
- Data API logs are not collected in github integration-tests [\#1034](https://github.com/stargate/data-api/issues/1034)

**Merged pull requests:**

- Bridge-Removal: Tests file cleanup with necessary profile and resources [\#1141](https://github.com/stargate/data-api/pull/1141) ([Yuqi-Du](https://github.com/Yuqi-Du))
- rollback cql-session check [\#1138](https://github.com/stargate/data-api/pull/1138) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Check for container start up using cqlsh [\#1136](https://github.com/stargate/data-api/pull/1136) ([maheshrajamani](https://github.com/maheshrajamani))
- Re-create PR \#1132 to get CI to succeed [\#1135](https://github.com/stargate/data-api/pull/1135) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix DSE 6.9 version tag [\#1134](https://github.com/stargate/data-api/pull/1134) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix c2-3338, add more info on exception message [\#1133](https://github.com/stargate/data-api/pull/1133) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Run CI using HCD image  [\#1130](https://github.com/stargate/data-api/pull/1130) ([maheshrajamani](https://github.com/maheshrajamani))
- Changes for using dse 6.9  for CI [\#1129](https://github.com/stargate/data-api/pull/1129) ([maheshrajamani](https://github.com/maheshrajamani))

## [v1.0.9](https://github.com/stargate/data-api/tree/v1.0.9) (2024-05-29)

[Full Changelog](https://github.com/stargate/data-api/compare/v1.0.8...v1.0.9)

**Highlights:**

- Multiple bug fixes and minor improvements to vectorize feature

**Closed issues:**

- Error out when `SHARED\_SECRET` and `HEADER` are disabled but provide the apiKey in createCollection [\#1114](https://github.com/stargate/data-api/issues/1114)
- Index creation needs to be wrapped with quote [\#1110](https://github.com/stargate/data-api/issues/1110)
- Data API exception mapper \(`ThrowableToErrorMapper`\) does not log error for unmapped server errors \(`ErrorCode.SERVER\_UNHANDLED\_ERROR`\) [\#1107](https://github.com/stargate/data-api/issues/1107)
- UpstageAI model name should not end with hyphen: add that in UpstageAI embedding client [\#1102](https://github.com/stargate/data-api/issues/1102)
- Possible NPE in `EmbeddingProviderFactory` for mismatched/missing embedding provider id [\#1098](https://github.com/stargate/data-api/issues/1098)
- JinaAI models don't work for the first few minutes  [\#1093](https://github.com/stargate/data-api/issues/1093)
- Add \(re-\)batching for calls to external embedding providers [\#1078](https://github.com/stargate/data-api/issues/1078)
- Optimize CI workflow [\#919](https://github.com/stargate/data-api/issues/919)
- findCollections and createCollection do not show the correct error message when the Astra token is invalid [\#712](https://github.com/stargate/data-api/issues/712)

**Merged pull requests:**

- Update red hat base images for docker, due to Snyk notification [\#1125](https://github.com/stargate/data-api/pull/1125) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Micro batching for embedding clients [\#1122](https://github.com/stargate/data-api/pull/1122) ([maheshrajamani](https://github.com/maheshrajamani))
- Remove `vectorDimension:0` for range dimension in `findEmbeddingProviders` [\#1121](https://github.com/stargate/data-api/pull/1121) ([Hazel-Datastax](https://github.com/Hazel-Datastax))
- Fix to check if session is valid after creation [\#1119](https://github.com/stargate/data-api/pull/1119) ([kathirsvn](https://github.com/kathirsvn))
- Error messages that to be sent from embedding gateway [\#1118](https://github.com/stargate/data-api/pull/1118) ([maheshrajamani](https://github.com/maheshrajamani))
- Create collection mixed casing fix [\#1117](https://github.com/stargate/data-api/pull/1117) ([maheshrajamani](https://github.com/maheshrajamani))
- Fix schema collectionSettings cache issue [\#1116](https://github.com/stargate/data-api/pull/1116) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Offline Mode - Index cql & Vector fixes [\#1115](https://github.com/stargate/data-api/pull/1115) ([kathirsvn](https://github.com/kathirsvn))
- Minor fix to error message given for invalid providerKey [\#1113](https://github.com/stargate/data-api/pull/1113) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix \#1107: log unmapped exceptions in addition to mapping [\#1112](https://github.com/stargate/data-api/pull/1112) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Changes to improve CI timing - Fix 2 [\#1109](https://github.com/stargate/data-api/pull/1109) ([maheshrajamani](https://github.com/maheshrajamani))
- Vectorize validation using String.isBlank\(\)  [\#1108](https://github.com/stargate/data-api/pull/1108) ([maheshrajamani](https://github.com/maheshrajamani))
- Handle empty string `$vectorize` field [\#1105](https://github.com/stargate/data-api/pull/1105) ([maheshrajamani](https://github.com/maheshrajamani))
- Error out in findEmbeddingProviders when vectorize is disabled [\#1104](https://github.com/stargate/data-api/pull/1104) ([Hazel-Datastax](https://github.com/Hazel-Datastax))
- Fix \#1102: move hyphen from UpstageAI model base name to suffix \(for better UI exp\) [\#1103](https://github.com/stargate/data-api/pull/1103) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Update config to accept environment variable [\#1101](https://github.com/stargate/data-api/pull/1101) ([Hazel-Datastax](https://github.com/Hazel-Datastax))
- Fix \#1098: prevent NPE for unknown embedding provider id [\#1099](https://github.com/stargate/data-api/pull/1099) ([tatu-at-datastax](https://github.com/tatu-at-datastax))

## [v1.0.8](https://github.com/stargate/data-api/tree/v1.0.8) (2024-05-16)

[Full Changelog](https://github.com/stargate/data-api/compare/v1.0.7...v1.0.8)

**Highlights:**

- Add multiple embedding providers for vectorize feature:
  - Vertex AI
  - Jina AI
  - Mistral
  - Upstage AI
  - VoyageAPI
  - Huggingface

**Closed issues:**

- Cannot create collection for Mistral  [\#1094](https://github.com/stargate/data-api/issues/1094)
- Remove curly brackets in vertexai's url [\#1092](https://github.com/stargate/data-api/issues/1092)
- NPE for VoyageAI embedding provider [\#1088](https://github.com/stargate/data-api/issues/1088)
- Remove apiVersion in azureOpenAI [\#1087](https://github.com/stargate/data-api/issues/1087)
- Add UpstageAI embedding provider [\#1073](https://github.com/stargate/data-api/issues/1073)
- Add VoyageAI embedding provider [\#1066](https://github.com/stargate/data-api/issues/1066)

**Merged pull requests:**

- add Jina exponential retry backoff [\#1097](https://github.com/stargate/data-api/pull/1097) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Improve CI workflow [\#1096](https://github.com/stargate/data-api/pull/1096) ([maheshrajamani](https://github.com/maheshrajamani))
- Fix \#1092: problem with VertexAI embedding client [\#1095](https://github.com/stargate/data-api/pull/1095) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix \#1088: handle `null` valued service params [\#1090](https://github.com/stargate/data-api/pull/1090) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Add parameters verification in createCollection [\#1089](https://github.com/stargate/data-api/pull/1089) ([Hazel-Datastax](https://github.com/Hazel-Datastax))
- Remove apiVersion in azureOpenAI [\#1086](https://github.com/stargate/data-api/pull/1086) ([Hazel-Datastax](https://github.com/Hazel-Datastax))
- GRPC propert need to fetched using `"` for service name [\#1085](https://github.com/stargate/data-api/pull/1085) ([maheshrajamani](https://github.com/maheshrajamani))
- Made the grpc to use Managed channel directly to use it in blocking mode [\#1084](https://github.com/stargate/data-api/pull/1084) ([maheshrajamani](https://github.com/maheshrajamani))
- Remove the `authentication` in `createCollection` if the auth supports `NONE` or `HEADER` [\#1083](https://github.com/stargate/data-api/pull/1083) ([Hazel-Datastax](https://github.com/Hazel-Datastax))
- Changes to wire credential validation [\#1082](https://github.com/stargate/data-api/pull/1082) ([maheshrajamani](https://github.com/maheshrajamani))
- Add condition when embedding gateway return 0 for vector dimension [\#1081](https://github.com/stargate/data-api/pull/1081) ([Hazel-Datastax](https://github.com/Hazel-Datastax))
- Add 5 more HuggingFace model definitions [\#1080](https://github.com/stargate/data-api/pull/1080) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- fix vector-dimension and configProducer retry [\#1079](https://github.com/stargate/data-api/pull/1079) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Add provider display name and disable cohere and vertexai [\#1077](https://github.com/stargate/data-api/pull/1077) ([Hazel-Datastax](https://github.com/Hazel-Datastax))
- Make getSupportedProviders Grpc call during startup and add retry [\#1076](https://github.com/stargate/data-api/pull/1076) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Fix \#1073: add embedding client for UpstageAI [\#1075](https://github.com/stargate/data-api/pull/1075) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Use Stargate v2.1.0-BETA-12 [\#1074](https://github.com/stargate/data-api/pull/1074) ([github-actions[bot]](https://github.com/apps/github-actions))
- Add skeletal metadata for Upstage AI: no implementation due to spec ambiguity [\#1072](https://github.com/stargate/data-api/pull/1072) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Add JinaAI embedding provider [\#1071](https://github.com/stargate/data-api/pull/1071) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Add support for dimension range in createCollection [\#1070](https://github.com/stargate/data-api/pull/1070) ([Hazel-Datastax](https://github.com/Hazel-Datastax))
- Add support for Mistral [\#1069](https://github.com/stargate/data-api/pull/1069) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Fix \#1066: Implement VoyageAI embedding client [\#1068](https://github.com/stargate/data-api/pull/1068) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Update authentication verification in createCollection [\#1065](https://github.com/stargate/data-api/pull/1065) ([Hazel-Datastax](https://github.com/Hazel-Datastax))
- Added validate credential service in proto [\#1064](https://github.com/stargate/data-api/pull/1064) ([maheshrajamani](https://github.com/maheshrajamani))
- Remove histograms from command processor metrics [\#1029](https://github.com/stargate/data-api/pull/1029) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))

## [v1.0.7](https://github.com/stargate/data-api/tree/v1.0.7) (2024-05-07)

[Full Changelog](https://github.com/stargate/data-api/compare/v1.0.6...v1.0.7)

**Highlights:**

- Add Azure OpenAI provider for vectorize feature
- Various changes to allow Data API code to be reused by other applications (No impact to Astra)

**Closed issues:**

- Querying by string \_id matches ObjectId \_id [\#1045](https://github.com/stargate/data-api/issues/1045)
- Increase DEFAULT\_MAX\_DOCUMENT\_INSERT\_COUNT to 100 \(from 20\) [\#1042](https://github.com/stargate/data-api/issues/1042)
- Java driver TruncateException returned on DeleteMany command [\#948](https://github.com/stargate/data-api/issues/948)
- Java driver ProtocolError on FindCommand [\#947](https://github.com/stargate/data-api/issues/947)
- Java driver ServerError on multiple operations [\#946](https://github.com/stargate/data-api/issues/946)
- Java driver AllNodesFailedException on multiple operations [\#945](https://github.com/stargate/data-api/issues/945)
- \[Enhancement\] - Add option `explain` to `findNamespaces\(\)`  [\#932](https://github.com/stargate/data-api/issues/932)
- Tenant ID does not appear on all log lines for tenant [\#847](https://github.com/stargate/data-api/issues/847)
- Run JSON API with DSE 7 [\#600](https://github.com/stargate/data-api/issues/600)

**Merged pull requests:**

- Sstable converter fixes [\#1063](https://github.com/stargate/data-api/pull/1063) ([kathirsvn](https://github.com/kathirsvn))
- Fix bugs found in demo [\#1062](https://github.com/stargate/data-api/pull/1062) ([Hazel-Datastax](https://github.com/Hazel-Datastax))
- update workflows to publish jar [\#1061](https://github.com/stargate/data-api/pull/1061) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Wire command name to MeteredEmbeddingProvider [\#1060](https://github.com/stargate/data-api/pull/1060) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Update `createCollection` Command [\#1059](https://github.com/stargate/data-api/pull/1059) ([Hazel-Datastax](https://github.com/Hazel-Datastax))
- EGW, get supported providers [\#1058](https://github.com/stargate/data-api/pull/1058) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Additional minor changes caused by PR\#1048 [\#1057](https://github.com/stargate/data-api/pull/1057) ([Hazel-Datastax](https://github.com/Hazel-Datastax))
- Add support for Azure OpenAI embedding service [\#1056](https://github.com/stargate/data-api/pull/1056) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Renaming the service so can override it easily in the helm chart [\#1055](https://github.com/stargate/data-api/pull/1055) ([maheshrajamani](https://github.com/maheshrajamani))
- Move embedding-service class under embedding package [\#1054](https://github.com/stargate/data-api/pull/1054) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Offline mode CI and formatting fixes [\#1053](https://github.com/stargate/data-api/pull/1053) ([kathirsvn](https://github.com/kathirsvn))
- Bump the github-actions group with 5 updates [\#1052](https://github.com/stargate/data-api/pull/1052) ([dependabot[bot]](https://github.com/apps/dependabot))
- Enable Dependabot update for Github action updates [\#1050](https://github.com/stargate/data-api/pull/1050) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- update GH action versions [\#1049](https://github.com/stargate/data-api/pull/1049) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Update Embedding Providers Config  [\#1048](https://github.com/stargate/data-api/pull/1048) ([Hazel-Datastax](https://github.com/Hazel-Datastax))
- Added support for embedding gateway [\#1047](https://github.com/stargate/data-api/pull/1047) ([maheshrajamani](https://github.com/maheshrajamani))
- Change CI to always runs for pull request, not just for specified changes [\#1046](https://github.com/stargate/data-api/pull/1046) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix \#1042: increase `DEFAULT\_MAX\_DOCUMENT\_INSERT\_COUNT` to 100 \(from 20\) [\#1043](https://github.com/stargate/data-api/pull/1043) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fixed change log for v1.0.6 manually. [\#1040](https://github.com/stargate/data-api/pull/1040) ([kathirsvn](https://github.com/kathirsvn))
- Publish Data API jar to internal repo in offline mode [\#1025](https://github.com/stargate/data-api/pull/1025) ([kathirsvn](https://github.com/kathirsvn))
- Data API as library - Offline Mode [\#905](https://github.com/stargate/data-api/pull/905) ([kathirsvn](https://github.com/kathirsvn))

## [v1.0.6](https://github.com/stargate/data-api/tree/v1.0.6) (2024-04-17)

[Full Changelog](https://github.com/stargate/data-api/compare/v1.0.5...v1.0.6)

**Highlights:**

- Internal refactoring and minor bug fixes 
- Various changes to allow Data API code to run without a Stargate coordinator (No impact to Astra)

**Implemented enhancements:**

- Bridge Removal - Separate Data API QueryBuilder from Quarkus Common Module [\#977](https://github.com/stargate/data-api/issues/977)
- Bridge Removal - Data API headerBased Auth, header resolver [\#976](https://github.com/stargate/data-api/issues/976)
- Bridge Removal - Data API Tenant resolver [\#975](https://github.com/stargate/data-api/issues/975)

**Closed issues:**

- Revert \(parts of\) \#1005: default project to include everything \(similar to `{"\*":1}`\) [\#1031](https://github.com/stargate/data-api/issues/1031)
- Exclude $vector and $vectorize from the default Projection [\#1005](https://github.com/stargate/data-api/issues/1005)
- Upsertion with `projection: { \_id: 0 }` appears broken [\#1000](https://github.com/stargate/data-api/issues/1000)
- Extend test framework to support running without coordinator [\#987](https://github.com/stargate/data-api/issues/987)
- internal server error with Data Api during astrapy ci test [\#970](https://github.com/stargate/data-api/issues/970)
- Add username/password option to RequestInfo [\#644](https://github.com/stargate/data-api/issues/644)
- Token is useless in open source version of JSON API [\#632](https://github.com/stargate/data-api/issues/632)
- Define acceptable consistency levels set globally [\#94](https://github.com/stargate/data-api/issues/94)

**Merged pull requests:**
- Pass DataAPIRequestInfo instead of injecting [\#1036](https://github.com/stargate/data-api/pull/1036) ([kathirsvn](https://github.com/kathirsvn))
- Use Stargate v2.1.0-BETA-11 [\#1035](https://github.com/stargate/data-api/pull/1035) ([github-actions[bot]](https://github.com/apps/github-actions))
- Fixes \#1031: partially revert change \#1005, making default Projection go back to "include all" [\#1033](https://github.com/stargate/data-api/pull/1033) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix DSE Integration, Username/Password credential based token [\#1030](https://github.com/stargate/data-api/pull/1030) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Add tenantId to driver logs [\#1027](https://github.com/stargate/data-api/pull/1027) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Add test to show \#1000 no longer applies [\#1026](https://github.com/stargate/data-api/pull/1026) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Quarkus Common Removal: Separate tenant resolver, token resolver, hea… [\#1024](https://github.com/stargate/data-api/pull/1024) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Username/Password credential based token format [\#1023](https://github.com/stargate/data-api/pull/1023) ([maheshrajamani](https://github.com/maheshrajamani))
- Quarkus Common Removal: Data API queryBuilder [\#1022](https://github.com/stargate/data-api/pull/1022) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Retry on truncate error `Failed to interrupt compactions` [\#1021](https://github.com/stargate/data-api/pull/1021) ([Hazel-Datastax](https://github.com/Hazel-Datastax))
- Add support for default exclusion of $vector and $vectorize [\#1016](https://github.com/stargate/data-api/pull/1016) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Refactor `ThrowableToErrorMapper` class [\#1013](https://github.com/stargate/data-api/pull/1013) ([Hazel-Datastax](https://github.com/Hazel-Datastax))
- Changes for dse-7 integration [\#1012](https://github.com/stargate/data-api/pull/1012) ([maheshrajamani](https://github.com/maheshrajamani))

## [v1.0.5](https://github.com/stargate/data-api/tree/v1.0.5) (2024-04-03)

[Full Changelog](https://github.com/stargate/data-api/compare/v1.0.4...v1.0.5)

**Closed issues:**

- Refactor `DocumentProjector` by adding `IndexingProjector`, unshare \(re-duplicate\) code [\#1006](https://github.com/stargate/data-api/issues/1006)
- \[Indexing Option\] Do not create index in case of deny: \["\*"\] [\#1002](https://github.com/stargate/data-api/issues/1002)
- Support wildcard "include EVERYTHING" and "include NOTHING" for Projection clause [\#1001](https://github.com/stargate/data-api/issues/1001)
- Java driver ReadFailureException on Find command [\#949](https://github.com/stargate/data-api/issues/949)
- NullPointerException on CreateCollection [\#944](https://github.com/stargate/data-api/issues/944)
- Was able to use $vectorize used with non vectorize collection  [\#901](https://github.com/stargate/data-api/issues/901)
- Add Metrics for in-flight commands \(Gauge on `MeteredCommandProcessor`\) [\#854](https://github.com/stargate/data-api/issues/854)
- CQL execution profile for transactional queries [\#791](https://github.com/stargate/data-api/issues/791)
- Wording of timeout error message [\#743](https://github.com/stargate/data-api/issues/743)
- Microbench for $in and $nin [\#723](https://github.com/stargate/data-api/issues/723)

**Merged pull requests:**

- Adding back InsertOperationTest [\#1018](https://github.com/stargate/data-api/pull/1018) ([maheshrajamani](https://github.com/maheshrajamani))
- Fix vectorize time metrics [\#1015](https://github.com/stargate/data-api/pull/1015) ([Hazel-Datastax](https://github.com/Hazel-Datastax))
- Use Stargate v2.1.0-BETA-10 [\#1014](https://github.com/stargate/data-api/pull/1014) ([github-actions[bot]](https://github.com/apps/github-actions))
- Move `EmbeddingProviderResponseValidation` class to the correct package [\#1011](https://github.com/stargate/data-api/pull/1011) ([Hazel-Datastax](https://github.com/Hazel-Datastax))
- Reduce CQL driver metrics and shredder metrics [\#1010](https://github.com/stargate/data-api/pull/1010) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Map Java driver ReadFailureException on Find command to JsonApiException [\#1009](https://github.com/stargate/data-api/pull/1009) ([Hazel-Datastax](https://github.com/Hazel-Datastax))
- Add support for "star" inclusion/exclusion \(include all/exclude all\) [\#1008](https://github.com/stargate/data-api/pull/1008) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Refactor DocumentProjector by splitting out IndexingProjector [\#1007](https://github.com/stargate/data-api/pull/1007) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Deny all improvement to not create super shredder indexes [\#1004](https://github.com/stargate/data-api/pull/1004) ([maheshrajamani](https://github.com/maheshrajamani))

## [v1.0.4](https://github.com/stargate/data-api/tree/v1.0.4) (2024-03-27)

[Full Changelog](https://github.com/stargate/data-api/compare/v1.0.3...v1.0.4)

**Closed issues:**

- Returned identifiers when working with different `defaultId` \(ObjectId\) are String [\#995](https://github.com/stargate/data-api/issues/995)
- ChangeLog not generating during release workflow [\#990](https://github.com/stargate/data-api/issues/990)

**Merged pull requests:**

- Adjust CQL execution profile timeouts [\#999](https://github.com/stargate/data-api/pull/999) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Stop tagging CQL metrics by session id [\#998](https://github.com/stargate/data-api/pull/998) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Fix \#995: return full JSON Extension value for \_id, not Stringified [\#997](https://github.com/stargate/data-api/pull/997) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Changelog script fix after renaming the repo [\#991](https://github.com/stargate/data-api/pull/991) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Fix 1.0.3 link in CHANGELOG.md [\#989](https://github.com/stargate/data-api/pull/989) ([tatu-at-datastax](https://github.com/tatu-at-datastax))

## [v1.0.3](https://github.com/stargate/jsonapi/tree/v1.0.3) (2024-03-20)

[Full Changelog](https://github.com/stargate/jsonapi/compare/v1.0.2...v1.0.3)

**Closed issues:**

- Add `EstimatedDocumentCount` command [\#793](https://github.com/stargate/data-api/issues/793)
- Renaming to Data API [\#860](https://github.com/stargate/data-api/issues/860)
- Support auto-generation/explicit use of ObjectId, UUID (v4, v6, v7) as document ids (_id) [\#922](https://github.com/stargate/data-api/issues/922)
- Filter $not with $size as 0 not working correctly [\#981](https://github.com/stargate/data-api/issues/981)

**Merged pull requests:**

- Implement EstimatedDocumentCount operation [\#866](https://github.com/stargate/data-api/pull/866)
- ObjectId, UUID typed support (auto-generation, explicit usage) [\#928](https://github.com/stargate/data-api/pull/928)
- Update Jackson to 2.16.2 (from 2.16.1) [\#980](https://github.com/stargate/data-api/pull/980)
- Fixes \#981: fix $not with $size 0 [\#982](https://github.com/stargate/data-api/pull/982)

## [v1.0.2](https://github.com/stargate/jsonapi/tree/v1.0.2) (2024-03-11)

[Full Changelog](https://github.com/stargate/jsonapi/compare/v1.0.1...v1.0.2)

**Closed issues:**

- Support pagination to `UpdateMany` command [\#937](https://github.com/stargate/jsonapi/issues/937)
- Rollback on CreateCollection failure [\#912](https://github.com/stargate/jsonapi/issues/912)
- Change `EmbeddingService` and `VectorProvider` to `EmbeddingProvider` [\#911](https://github.com/stargate/jsonapi/issues/911)
- Missing `errorCode` when `find\(\)` fails with invalid Collection name [\#904](https://github.com/stargate/jsonapi/issues/904)
- Confusing error message when trying to find by multiple `\_id` values [\#898](https://github.com/stargate/jsonapi/issues/898)
- Truncate value length included by `ConstraintViolationExceptionMapper` into configurable max length [\#895](https://github.com/stargate/jsonapi/issues/895)
- OpenAPI spec, investigate sdk generator tool stainless [\#892](https://github.com/stargate/jsonapi/issues/892)
- Invalid request / allow filtering error on find command \(due to failed index creation for `createCollection`\) [\#812](https://github.com/stargate/jsonapi/issues/812)
- Add content size metrics for serialization \(shredding output\) to `doc\_json` [\#759](https://github.com/stargate/jsonapi/issues/759)
- Add timing metrics for serialization \(shredding output\) to `doc\_json` [\#669](https://github.com/stargate/jsonapi/issues/669)
- Add timing metrics for deserialization of `doc\_json` [\#668](https://github.com/stargate/jsonapi/issues/668)

**Merged pull requests:**

- Enable CI for PRs against feature branches \("feature/\*"\) [\#942](https://github.com/stargate/jsonapi/pull/942) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- some cleanup with bridge removal [\#941](https://github.com/stargate/jsonapi/pull/941) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Revert "remove some bridge related dependencies" [\#940](https://github.com/stargate/jsonapi/pull/940) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Paginated updateMany [\#939](https://github.com/stargate/jsonapi/pull/939) ([maheshrajamani](https://github.com/maheshrajamani))
- Fix deprecated references to InjectMock: use non-deprecated import [\#931](https://github.com/stargate/jsonapi/pull/931) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- collection limit rollback change [\#929](https://github.com/stargate/jsonapi/pull/929) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Remove gauge metrics from driver [\#914](https://github.com/stargate/jsonapi/pull/914) ([maheshrajamani](https://github.com/maheshrajamani))
- Fixes \#898: improve error message for "multiple \_id filters" case [\#913](https://github.com/stargate/jsonapi/pull/913) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- bug fix: valid vector metric [\#908](https://github.com/stargate/jsonapi/pull/908) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Fixes \#904: convert ConstraintsViolationException to JsonApiException to include `errorCode` [\#906](https://github.com/stargate/jsonapi/pull/906) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- delete the performance testing github workflow [\#902](https://github.com/stargate/jsonapi/pull/902) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Fixes \#895: truncate reported value that fails constraint violation if too long [\#899](https://github.com/stargate/jsonapi/pull/899) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fixes \#893: add IT for InsertMany failure due to doc count exceeding limit [\#894](https://github.com/stargate/jsonapi/pull/894) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- remove some bridge related dependencies [\#890](https://github.com/stargate/jsonapi/pull/890) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Rename jsonapi -\> dataapi [\#864](https://github.com/stargate/jsonapi/pull/864) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Build doc json size metrics and doc counter metrics [\#829](https://github.com/stargate/jsonapi/pull/829) ([Hazel-Datastax](https://github.com/Hazel-Datastax))

## [v1.0.1](https://github.com/stargate/jsonapi/tree/v1.0.1) (2024-02-20)

[Full Changelog](https://github.com/stargate/jsonapi/compare/v1.0.0...v1.0.1)

**Closed issues:**

- Unify terms "field" and "property" in `ErrorCode` as "property" [\#884](https://github.com/stargate/jsonapi/issues/884)
- Do not return AllNodesFailedException error to the client [\#882](https://github.com/stargate/jsonapi/issues/882)
- Missing `property` name for "too long String" validation failure [\#875](https://github.com/stargate/jsonapi/issues/875)
- Unclear error message for Bean Validation failures [\#873](https://github.com/stargate/jsonapi/issues/873)
- Filtering array element by $size: 0 doesn't seem to work [\#862](https://github.com/stargate/jsonapi/issues/862)
- Increase current `quarkus.http.limits.max-body-size` from `5M` to `20M` [\#856](https://github.com/stargate/jsonapi/issues/856)
- Postman Collection workflow run failing [\#840](https://github.com/stargate/jsonapi/issues/840)
- Document Limits: increase max property path length to 1000 \(from 250\) [\#820](https://github.com/stargate/jsonapi/issues/820)
- Add tenant tags to driver metrics [\#795](https://github.com/stargate/jsonapi/issues/795)
- Enable HTTP query logging [\#752](https://github.com/stargate/jsonapi/issues/752)
- Catch and re-throw Java CQL Driver fail exceptions as `JsonApiException` with appropriate `errorCode`s [\#751](https://github.com/stargate/jsonapi/issues/751)
- all-in-one soak test workload [\#739](https://github.com/stargate/jsonapi/issues/739)
- Add option to include profiling tools in Docker image [\#671](https://github.com/stargate/jsonapi/issues/671)
- Driver token map computation failure [\#661](https://github.com/stargate/jsonapi/issues/661)

**Merged pull requests:**

- Fixes \#884: unify JSON Object member reference to "property" \(from "field" etc\) [\#885](https://github.com/stargate/jsonapi/pull/885) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- add tenantId to driver metrics [\#883](https://github.com/stargate/jsonapi/pull/883) ([Yuqi-Du](https://github.com/Yuqi-Du))
- index creation failure msg, usage with collection not fully indexed [\#879](https://github.com/stargate/jsonapi/pull/879) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Fix \#875: improve error message by including field name [\#878](https://github.com/stargate/jsonapi/pull/878) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Create new  DataApiRequestInfo to remove stargate bridge connection [\#876](https://github.com/stargate/jsonapi/pull/876) ([maheshrajamani](https://github.com/maheshrajamani))
- Fixes \#873: improve Bean Validation failure messages [\#874](https://github.com/stargate/jsonapi/pull/874) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Increase default maximum HTTP body size at server level from 5MB to 20MB [\#869](https://github.com/stargate/jsonapi/pull/869) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Use Stargate v2.1.0-BETA-9 [\#868](https://github.com/stargate/jsonapi/pull/868) ([github-actions[bot]](https://github.com/apps/github-actions))
- Fix broken postman collection workflow [\#867](https://github.com/stargate/jsonapi/pull/867) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Bug fix for array size filter for `0` length [\#863](https://github.com/stargate/jsonapi/pull/863) ([maheshrajamani](https://github.com/maheshrajamani))
- 05-Feb-2024 backup of Postman Data API collection [\#861](https://github.com/stargate/jsonapi/pull/861) ([johnsmartco](https://github.com/johnsmartco))
- removed merge conflict in CONFIGURATION.md [\#859](https://github.com/stargate/jsonapi/pull/859) ([shubanker](https://github.com/shubanker))
- Update pom.xml version for 1.0.0 \(final\) release [\#857](https://github.com/stargate/jsonapi/pull/857) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Build Docker image with profiling tools [\#855](https://github.com/stargate/jsonapi/pull/855) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- \(incremental\) Catch and re-throw Java CQL Driver fail exceptions as JsonApiException [\#851](https://github.com/stargate/jsonapi/pull/851) ([Yuqi-Du](https://github.com/Yuqi-Du))
- soak test [\#740](https://github.com/stargate/jsonapi/pull/740) ([Yuqi-Du](https://github.com/Yuqi-Du))

## [v1.0.0](https://github.com/stargate/jsonapi/tree/v1.0.0) (2024-02-02)

[Full Changelog](https://github.com/stargate/jsonapi/compare/v1.0.0-RC-3...v1.0.0)

**Closed issues:**

- INVALID\_REQUST error code typo [\#796](https://github.com/stargate/jsonapi/issues/796)
- Document limits: remove individual max property name limit \(leave just max path limit\) [\#819](https://github.com/stargate/jsonapi/issues/819)
- Document Limits: increase max property path length to 1000 \(from 250\) [\#820](https://github.com/stargate/jsonapi/issues/820)
- Document limits: only apply Max Object Properties limits on indexed fields [\#821](https://github.com/stargate/jsonapi/issues/821)
- Document limits: only apply Max Document Properties limits on indexed fields [\#850](https://github.com/stargate/jsonapi/issues/850)

**Merged pull requests:**

- Fixes \#820: increase max path length limit to 1000 \(from 250\) [\#853](https://github.com/stargate/jsonapi/pull/853) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fixes \#850: apply max-doc-properties on indexed properties only, not all [\#852](https://github.com/stargate/jsonapi/pull/852) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fixes \#819: remove max limit for Object property name \(leaving full path max\) [\#849](https://github.com/stargate/jsonapi/pull/849) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Backup of Data API Postman collection [\#846](https://github.com/stargate/jsonapi/pull/846) ([johnsmartco](https://github.com/johnsmartco))

## [v1.0.0-RC-3](https://github.com/stargate/jsonapi/tree/v1.0.0-RC-3) (2024-01-31)

[Full Changelog](https://github.com/stargate/jsonapi/compare/v1.0.0-RC-2...v1.0.0-RC-3)

**Closed issues:**

- Document limits: increase "max-number-length" from 50 to 100 digits [\#823](https://github.com/stargate/jsonapi/issues/823)
- Document limits: only apply Max Array Element limit on indexed fields [\#822](https://github.com/stargate/jsonapi/issues/822)
- Document limits: increase max doc length limit: 1M -\> 4M [\#817](https://github.com/stargate/jsonapi/issues/817)

**Merged pull requests:**

- Changes for adding retry delay [\#845](https://github.com/stargate/jsonapi/pull/845) ([maheshrajamani](https://github.com/maheshrajamani))
- Fixes \#821: apply max-object-properties/max-doc-properties only on indexed Objects [\#844](https://github.com/stargate/jsonapi/pull/844) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Refactoring: clean up / unify Shredder-thrown JsonApiExceptions [\#843](https://github.com/stargate/jsonapi/pull/843) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Refactor exception mappers into single package \(instead of split\) [\#842](https://github.com/stargate/jsonapi/pull/842) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- increase max doc length limit: 1M -\> 4M [\#841](https://github.com/stargate/jsonapi/pull/841) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Fixes \#822: only limit array size for indexed fields \(ignore non-indexed\) [\#838](https://github.com/stargate/jsonapi/pull/838) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix \#823: increase max number length limit to 100 \(from 50\) [\#826](https://github.com/stargate/jsonapi/pull/826) ([tatu-at-datastax](https://github.com/tatu-at-datastax))

## [v1.0.0-RC-2](https://github.com/stargate/jsonapi/tree/v1.0.0-RC-2) (2024-01-25)

[Full Changelog](https://github.com/stargate/jsonapi/compare/v1.0.0-RC-1...v1.0.0-RC-2)

**Closed issues:**

- \[regression\] - Adding `indexing` options in the `createCollection` remove idempotence [\#835](https://github.com/stargate/jsonapi/issues/835)
- Create collection error message is confusing [\#830](https://github.com/stargate/jsonapi/issues/830)
- Add validation of `"options"` property of `createCollection` command to avoid silent failures [\#828](https://github.com/stargate/jsonapi/issues/828)
- Document limits: increase allowed max nesting from 8 to 16 levels [\#818](https://github.com/stargate/jsonapi/issues/818)
- Improve error message when collection name omitted [\#800](https://github.com/stargate/jsonapi/issues/800)
- Optimize case of "no projection" \(include all\) so that `doc\_json` embedded directly [\#667](https://github.com/stargate/jsonapi/issues/667)

**Merged pull requests:**

- Fixes \#835: ensure "createCollection\(\)" still idempotent with "no-index" options [\#836](https://github.com/stargate/jsonapi/pull/836) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Update dse-next to latest as of 2024-01-24: 4.0.11-0248d170a615 [\#834](https://github.com/stargate/jsonapi/pull/834) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Create collection index creation ordering [\#833](https://github.com/stargate/jsonapi/pull/833) ([maheshrajamani](https://github.com/maheshrajamani))
- error out with invalid option when createCollection [\#832](https://github.com/stargate/jsonapi/pull/832) ([Yuqi-Du](https://github.com/Yuqi-Du))
- improve error msg, too many indexes and can not create a collection [\#831](https://github.com/stargate/jsonapi/pull/831) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Fix \#818: increase max doc depth allowed from 8 to 16 [\#825](https://github.com/stargate/jsonapi/pull/825) ([tatu-at-datastax](https://github.com/tatu-at-datastax))

## [v1.0.0-RC-1](https://github.com/stargate/jsonapi/tree/v1.0.0-RC-1) (2024-01-22)

[Full Changelog](https://github.com/stargate/jsonapi/compare/v1.0.0-BETA-7...v1.0.0-RC-1)

**Closed issues:**

- Misnamed config property: `stargate.jsonapi.document.limits.max-filter-object-properties` -- should be Operations, not Doc Config [\#815](https://github.com/stargate/jsonapi/issues/815)
- Count command optimization [\#809](https://github.com/stargate/jsonapi/issues/809)
- Handle DOCUMENT\_ALREADY\_EXISTS error in insert [\#801](https://github.com/stargate/jsonapi/issues/801)
- Invalid filter, misuse of `$` operators  [\#798](https://github.com/stargate/jsonapi/issues/798)
- Sync Document Limit settings to `jsonapi /CONFIGURATION.md` [\#797](https://github.com/stargate/jsonapi/issues/797)
- Validate paths used for `indexing` options for `CreateCollection` [\#790](https://github.com/stargate/jsonapi/issues/790)
- \[Indexing options\] Error using non indexed fields for filtering, sorting [\#768](https://github.com/stargate/jsonapi/issues/768)
- \[Indexing options\] Shredder changes to index fields [\#767](https://github.com/stargate/jsonapi/issues/767)
- \[Indexing options\] Add indexing option to cache [\#766](https://github.com/stargate/jsonapi/issues/766)
- \[Indexing options\] Create collection changes to add indexing options [\#765](https://github.com/stargate/jsonapi/issues/765)
- Multiple filter for same field not resolved [\#763](https://github.com/stargate/jsonapi/issues/763)
- Command-level logging [\#741](https://github.com/stargate/jsonapi/issues/741)
- Add option to limit index creation [\#560](https://github.com/stargate/jsonapi/issues/560)

**Merged pull requests:**

- Fixes \#815: move "max filter properties" to `OperationsConfig` where it belongs [\#816](https://github.com/stargate/jsonapi/pull/816) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Simplify construction of JsonApiException, reduce boilerplate code [\#814](https://github.com/stargate/jsonapi/pull/814) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Change JSON API to Data API and update doc link seen in swagger [\#813](https://github.com/stargate/jsonapi/pull/813) ([johnsmartco](https://github.com/johnsmartco))
- Count optimization changes [\#811](https://github.com/stargate/jsonapi/pull/811) ([maheshrajamani](https://github.com/maheshrajamani))
- add tenantId to each log lines [\#810](https://github.com/stargate/jsonapi/pull/810) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Improve error message when collection name omitted [\#808](https://github.com/stargate/jsonapi/pull/808) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Changed the timeout and added retry logic. [\#807](https://github.com/stargate/jsonapi/pull/807) ([maheshrajamani](https://github.com/maheshrajamani))
- Do not return HTTP 504 Gateway Timeout on DB timeout  [\#806](https://github.com/stargate/jsonapi/pull/806) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Fix to \#790 implementation to ensure \["\*"\] is accepted as "allow" or "deny" list [\#805](https://github.com/stargate/jsonapi/pull/805) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Add validation of paths used for indexing "allow"/"deny" lists. [\#804](https://github.com/stargate/jsonapi/pull/804) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- invalid $ operator use case, INVALID\_REQUEST typo fix  [\#803](https://github.com/stargate/jsonapi/pull/803) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Handle DOCUMENT\_ALREADY\_EXISTS error on insert  [\#802](https://github.com/stargate/jsonapi/pull/802) ([maheshrajamani](https://github.com/maheshrajamani))
- Fixes \#797: update document limits in CONFIGURATION.md [\#799](https://github.com/stargate/jsonapi/pull/799) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Command level logging changes [\#794](https://github.com/stargate/jsonapi/pull/794) ([kathirsvn](https://github.com/kathirsvn))
- Fixes \#767: Second part of "no-index" changes: allow large non-indexed String values, implicit allow for "$vector" [\#792](https://github.com/stargate/jsonapi/pull/792) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Error out when using non indexed fields [\#781](https://github.com/stargate/jsonapi/pull/781) ([Hazel-Datastax](https://github.com/Hazel-Datastax))
- `$not` operator [\#760](https://github.com/stargate/jsonapi/pull/760) ([maheshrajamani](https://github.com/maheshrajamani))

## [v1.0.0-BETA-7](https://github.com/stargate/jsonapi/tree/v1.0.0-BETA-7) (2024-01-11)

[Full Changelog](https://github.com/stargate/jsonapi/compare/v1.0.0-BETA-6...v1.0.0-BETA-7)

**Closed issues:**

- Configurable CQL query timeouts by query type [\#783](https://github.com/stargate/jsonapi/issues/783)
- Increase limits for maximum properties per Object \(64 to 1,000\), per Document \(1,000 to 2,000\) [\#776](https://github.com/stargate/jsonapi/issues/776)
- Reduce max Vector dimension limit from 10,000 to 4,096 not to fail due to SAI/ANN limit [\#772](https://github.com/stargate/jsonapi/issues/772)
- \[Indexing options\] Add indexing options to findCollection response [\#769](https://github.com/stargate/jsonapi/issues/769)
- insertMany default ordered flag to false [\#761](https://github.com/stargate/jsonapi/issues/761)
- Bug: ALL filter not working inside OR [\#749](https://github.com/stargate/jsonapi/issues/749)
- Increase `DocumentLimitsConfig.maxArrayLength` from 100 to 1000 [\#745](https://github.com/stargate/jsonapi/issues/745)
- Tune maximum property name length \(from 48 to 100\), add separate setting for max-total-length \(250\) [\#744](https://github.com/stargate/jsonapi/issues/744)
- Set Error codes in CommandResult.Error [\#720](https://github.com/stargate/jsonapi/issues/720)
- fix maximum number of values for $in [\#711](https://github.com/stargate/jsonapi/issues/711)
- JSON API should identify itself to bridge as jsonapi [\#387](https://github.com/stargate/jsonapi/issues/387)

**Merged pull requests:**

- Set retry policy [\#788](https://github.com/stargate/jsonapi/pull/788) ([maheshrajamani](https://github.com/maheshrajamani))
- set the application name when creating CQL session [\#787](https://github.com/stargate/jsonapi/pull/787) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Add no-index support for Shredder \(part 1\) [\#786](https://github.com/stargate/jsonapi/pull/786) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Changes to have ddl and count profiles [\#785](https://github.com/stargate/jsonapi/pull/785) ([maheshrajamani](https://github.com/maheshrajamani))
- insertMany nosqlbench workload [\#784](https://github.com/stargate/jsonapi/pull/784) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Change the InsertManyCommand.Options.ordered to false as default [\#782](https://github.com/stargate/jsonapi/pull/782) ([maheshrajamani](https://github.com/maheshrajamani))
- Shredder refactoring to isolate parts for easier no-caching implement… [\#780](https://github.com/stargate/jsonapi/pull/780) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- \[Indexing options\] Changes for find collections to return indexing option [\#779](https://github.com/stargate/jsonapi/pull/779) ([maheshrajamani](https://github.com/maheshrajamani))
- updating spec to match guidance in \#407 [\#778](https://github.com/stargate/jsonapi/pull/778) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Fixes \#776: Update max object, document properties limits [\#777](https://github.com/stargate/jsonapi/pull/777) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Lower limit for max-$vector dimension to highest backend supports \(4,096\) from earlier \(16,000\) [\#775](https://github.com/stargate/jsonapi/pull/775) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- fix: multi filters for one field [\#774](https://github.com/stargate/jsonapi/pull/774) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Use Stargate v2.1.0-BETA-8 [\#773](https://github.com/stargate/jsonapi/pull/773) ([github-actions[bot]](https://github.com/apps/github-actions))
- Changes for indexing option - create collection and cache [\#771](https://github.com/stargate/jsonapi/pull/771) ([maheshrajamani](https://github.com/maheshrajamani))
- Fixes \#745: increase max array length limit to 1,000 \(from 100\) [\#770](https://github.com/stargate/jsonapi/pull/770) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix \#744: increase maximum property name length \(48-\>100\), add new path limit \(250\) [\#762](https://github.com/stargate/jsonapi/pull/762) ([tatu-at-datastax](https://github.com/tatu-at-datastax))

## [v1.0.0-BETA-6](https://github.com/stargate/jsonapi/tree/v1.0.0-BETA-6) (2024-01-02)

[Full Changelog](https://github.com/stargate/jsonapi/compare/v1.0.0-BETA-5...v1.0.0-BETA-6)

**Implemented enhancements:**

- Improve max-String-value limit checking to be based on UTF-8 \(byte\) length, not char count [\#710](https://github.com/stargate/jsonapi/issues/710)

**Closed issues:**

- Add JSON Logging as the default option for non-dev [\#747](https://github.com/stargate/jsonapi/issues/747)
- $in and $nin, support for array and subdoc [\#732](https://github.com/stargate/jsonapi/issues/732)
- Problem with inserting documents with "too long numbers" [\#726](https://github.com/stargate/jsonapi/issues/726)
- Add ITs for "max-number-length" violation checking for `insertMany`, `findOneAndUpdate`, `findOneAndReplace` [\#724](https://github.com/stargate/jsonapi/issues/724)
- Extend nosqlbench tests for new filter operations [\#718](https://github.com/stargate/jsonapi/issues/718)
- Set serial consistency to LOCAL\_SERIAL [\#716](https://github.com/stargate/jsonapi/issues/716)
- Lower maximum String value length from 16,000 to 8.000 [\#713](https://github.com/stargate/jsonapi/issues/713)
- $nin support [\#709](https://github.com/stargate/jsonapi/issues/709)
- token is invalid, error out as keyspace not found instead of 401 unauthenticate [\#708](https://github.com/stargate/jsonapi/issues/708)
- 401 instead of 500 for bad credentials [\#707](https://github.com/stargate/jsonapi/issues/707)
- Use lower consistency in CQL vector search queries [\#691](https://github.com/stargate/jsonapi/issues/691)
- Filter and sort clause field name validation [\#690](https://github.com/stargate/jsonapi/issues/690)
- Add IT for maximum String value length [\#686](https://github.com/stargate/jsonapi/issues/686)
- Update stargate-mongoose and create-astradb-mongoose-app with latest jsonapi changes [\#664](https://github.com/stargate/jsonapi/issues/664)
- Investigate if Jackson 2.15 provided "fast floating-point" improves BigDecimal reads, enable [\#653](https://github.com/stargate/jsonapi/issues/653)
- Investigate if Jackson 2.15 provided "fast floating-point" improves `BigDecimal` writes, enable [\#652](https://github.com/stargate/jsonapi/issues/652)
- Metrics for the SessionCache [\#646](https://github.com/stargate/jsonapi/issues/646)
- Closing session with possible in-flight queries [\#645](https://github.com/stargate/jsonapi/issues/645)
- Improve max Collection limit verification to be based on available SAIs, not existing Tables [\#608](https://github.com/stargate/jsonapi/issues/608)
- DataStoreConfig is not produced, use DataStoreProperties instead [\#508](https://github.com/stargate/jsonapi/issues/508)

**Merged pull requests:**

- AllFilter with OR, bug fix [\#757](https://github.com/stargate/jsonapi/pull/757) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Fixes \#653: enable fast\(er\) parsing/writing of Doubles, BigDecimals [\#756](https://github.com/stargate/jsonapi/pull/756) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- CQL Query logging changes. [\#754](https://github.com/stargate/jsonapi/pull/754) ([kathirsvn](https://github.com/kathirsvn))
- Retry policy changes [\#750](https://github.com/stargate/jsonapi/pull/750) ([kathirsvn](https://github.com/kathirsvn))
- Set json format logging as the default [\#748](https://github.com/stargate/jsonapi/pull/748) ([kathirsvn](https://github.com/kathirsvn))
- Fix C2-3155: include actual property name that is too long in exception message [\#746](https://github.com/stargate/jsonapi/pull/746) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Setting ignore bridge flag to true by default [\#738](https://github.com/stargate/jsonapi/pull/738) ([kathirsvn](https://github.com/kathirsvn))
- In nin array subdoc [\#737](https://github.com/stargate/jsonapi/pull/737) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Limit number of Collections created by both max-collections and SAIs available [\#736](https://github.com/stargate/jsonapi/pull/736) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- extend nosqlbench tests with sai changes [\#735](https://github.com/stargate/jsonapi/pull/735) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Added error code in case of invalid query [\#731](https://github.com/stargate/jsonapi/pull/731) ([maheshrajamani](https://github.com/maheshrajamani))
- CQLSessionCache - evictionListener changed to removalListener [\#730](https://github.com/stargate/jsonapi/pull/730) ([kathirsvn](https://github.com/kathirsvn))
- Add Unit Test to reproduce, fix \#726 [\#727](https://github.com/stargate/jsonapi/pull/727) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Add ITs to verify enforcement of "too long number" \(InsertMany, FindOneAndUpdate, -Replace\) [\#725](https://github.com/stargate/jsonapi/pull/725) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Use Stargate v2.1.0-BETA-7 [\#722](https://github.com/stargate/jsonapi/pull/722) ([github-actions[bot]](https://github.com/apps/github-actions))
- Changes with cassandra not operator [\#721](https://github.com/stargate/jsonapi/pull/721) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Fix \#710: use byte-length for max String value, not characters [\#719](https://github.com/stargate/jsonapi/pull/719) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix \#713: change max-String-value length from 16,000 to 8,000 [\#717](https://github.com/stargate/jsonapi/pull/717) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix to return 401 instead of 500 [\#715](https://github.com/stargate/jsonapi/pull/715) ([kathirsvn](https://github.com/kathirsvn))
- Change Stargate Coordinator Docker tag to use to make IT runs on IDE\(A\) work [\#714](https://github.com/stargate/jsonapi/pull/714) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Change the consistency level for vector search [\#706](https://github.com/stargate/jsonapi/pull/706) ([Hazel-Datastax](https://github.com/Hazel-Datastax))
- Range query support  [\#705](https://github.com/stargate/jsonapi/pull/705) ([maheshrajamani](https://github.com/maheshrajamani))
- Filter and sort clause field name validation [\#697](https://github.com/stargate/jsonapi/pull/697) ([Hazel-Datastax](https://github.com/Hazel-Datastax))
- Fix \#686: add ITs to verify Max-String-Value-Length constaints [\#687](https://github.com/stargate/jsonapi/pull/687) ([tatu-at-datastax](https://github.com/tatu-at-datastax))

## [v1.0.0-BETA-5](https://github.com/stargate/jsonapi/tree/v1.0.0-BETA-5) (2023-12-06)

[Full Changelog](https://github.com/stargate/jsonapi/compare/v1.0.0-BETA-4...v1.0.0-BETA-5)

**Closed issues:**

- Provide better error message when accessing non-JSON API table [\#659](https://github.com/stargate/jsonapi/issues/659)
- Update to Jackson 2.16.0 [\#650](https://github.com/stargate/jsonapi/issues/650)
- Remove all gRPC bridge communications [\#649](https://github.com/stargate/jsonapi/issues/649)
- Write unit test for JsonapiTableMatcher [\#643](https://github.com/stargate/jsonapi/issues/643)
- Verify handling of Collection with capital letter\(s\) [\#639](https://github.com/stargate/jsonapi/issues/639)
- Feature request: Add truncate/empty collection support [\#631](https://github.com/stargate/jsonapi/issues/631)
- $in, with empty array, should find nothing [\#625](https://github.com/stargate/jsonapi/issues/625)
- CQL branch, filter, cql mulfunction with same values [\#623](https://github.com/stargate/jsonapi/issues/623)
- Operations implement predicate instead of override equals and hashcode [\#36](https://github.com/stargate/jsonapi/issues/36)
- Revisit FilterMatcher and FilterMatchRules logic [\#34](https://github.com/stargate/jsonapi/issues/34)

**Merged pull requests:**

- Use Stargate v2.1.0-BETA-6 [\#700](https://github.com/stargate/jsonapi/pull/700) ([github-actions[bot]](https://github.com/apps/github-actions))
- Use Stargate v2.1.0-BETA-5 [\#699](https://github.com/stargate/jsonapi/pull/699) ([github-actions[bot]](https://github.com/apps/github-actions))
- Use Stargate v2.1.0-BETA-4 [\#696](https://github.com/stargate/jsonapi/pull/696) ([github-actions[bot]](https://github.com/apps/github-actions))
- Update dse-next to latest as of 2023-12-01: 4.0.11-a09f8c1431ab [\#695](https://github.com/stargate/jsonapi/pull/695) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- convert ReadAndUpdateOperationRetryTest unit test [\#694](https://github.com/stargate/jsonapi/pull/694) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Throw error on invalid json api table schema [\#693](https://github.com/stargate/jsonapi/pull/693) ([maheshrajamani](https://github.com/maheshrajamani))
- Convert "SerialConsistencyOverrideOperationTest" to not use ValidatingBridge [\#692](https://github.com/stargate/jsonapi/pull/692) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- convert ReadAndUpdateOperationTest [\#688](https://github.com/stargate/jsonapi/pull/688) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Convert `InsertOperationTest` to use native CQL not validating Bridge [\#684](https://github.com/stargate/jsonapi/pull/684) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix delete unit test [\#683](https://github.com/stargate/jsonapi/pull/683) ([maheshrajamani](https://github.com/maheshrajamani))
- Un-Disable 3 tests that pass with recent `JsonTerm` fix [\#682](https://github.com/stargate/jsonapi/pull/682) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- JsonTerm Refactoring for unit test [\#681](https://github.com/stargate/jsonapi/pull/681) ([maheshrajamani](https://github.com/maheshrajamani))
- Docker compose fix [\#678](https://github.com/stargate/jsonapi/pull/678) ([kathirsvn](https://github.com/kathirsvn))
- Convert "FindOperationTest" to not use ValidatingBridge  [\#677](https://github.com/stargate/jsonapi/pull/677) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Remove those "XxxOperationTest"s that do not seem to add any value [\#675](https://github.com/stargate/jsonapi/pull/675) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Convert "CountOperationTest" unit test to not use Validating Bridge but direct QueryExecutor mocking [\#672](https://github.com/stargate/jsonapi/pull/672) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Disable bridge usage [\#670](https://github.com/stargate/jsonapi/pull/670) ([maheshrajamani](https://github.com/maheshrajamani))
- Moved JAVA\_OPTS\_APPEND out of Dockerfile [\#665](https://github.com/stargate/jsonapi/pull/665) ([kathirsvn](https://github.com/kathirsvn))
- Unit test for JsonapiTableMatcher and CqlColumnMatcher [\#663](https://github.com/stargate/jsonapi/pull/663) ([Hazel-Datastax](https://github.com/Hazel-Datastax))
- Add manually exported JSON API Postman collections from Stargate-Cass… [\#660](https://github.com/stargate/jsonapi/pull/660) ([johnsmartco](https://github.com/johnsmartco))
- Add truncate/empty collection support [\#658](https://github.com/stargate/jsonapi/pull/658) ([Hazel-Datastax](https://github.com/Hazel-Datastax))
- Fix \#639: add testing of CreateCollection with mixed-case name, fix quoting for FindCollections [\#654](https://github.com/stargate/jsonapi/pull/654) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Update Jackson to 2.16.0 \(from 2.15.3\) [\#651](https://github.com/stargate/jsonapi/pull/651) ([tatu-at-datastax](https://github.com/tatu-at-datastax))

## [v1.0.0-BETA-4](https://github.com/stargate/jsonapi/tree/v1.0.0-BETA-4) (2023-11-17)

[Full Changelog](https://github.com/stargate/jsonapi/compare/v1.0.0-BETA-3...v1.0.0-BETA-4)

**Closed issues:**

- Empty projection \(`{ }`\) for `find\(\)` includes nothing; should instead include everything \(similar to missing \(`null`\) projection\) [\#634](https://github.com/stargate/jsonapi/issues/634)
- $similarity accepted within projection [\#633](https://github.com/stargate/jsonapi/issues/633)
- Add `DocumentLimitsConfig` limit for total Properties per Document \(1000?\) [\#630](https://github.com/stargate/jsonapi/issues/630)
- CQL branch, pagination issue [\#627](https://github.com/stargate/jsonapi/issues/627)
- Add Integration Tests for max String property length; max Document size to ensure ability to store max valid values, docs [\#622](https://github.com/stargate/jsonapi/issues/622)
- Increase Java Driver local pooling to 8 \(from default of just 1\) [\#617](https://github.com/stargate/jsonapi/issues/617)
- Pagination name discrepancy "nextPageState" vs "pagingState" [\#596](https://github.com/stargate/jsonapi/issues/596)
- Replace `X-Cassandra-Token` with `Token` [\#569](https://github.com/stargate/jsonapi/issues/569)
- Remove namespace from the API [\#562](https://github.com/stargate/jsonapi/issues/562)
- Map `StreamConstraintsException` to `JsonApiException` \(wrt max-number-len\) [\#448](https://github.com/stargate/jsonapi/issues/448)
- SPEC - document the createDatabase and createCollection commands [\#137](https://github.com/stargate/jsonapi/issues/137)

**Merged pull requests:**

- fix changelog [\#656](https://github.com/stargate/jsonapi/pull/656) ([Yuqi-Du](https://github.com/Yuqi-Du))
- fixed token [\#647](https://github.com/stargate/jsonapi/pull/647) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Fix \#634: add tests to verify that empty JSON Object as projection includes whole doc [\#642](https://github.com/stargate/jsonapi/pull/642) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Update to latest dse-next backend [\#641](https://github.com/stargate/jsonapi/pull/641) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Add to \#630: update `jsonapi-spec.md` too [\#640](https://github.com/stargate/jsonapi/pull/640) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix \#630: limit max properties per doc to 1000 [\#636](https://github.com/stargate/jsonapi/pull/636) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix \#633: prevent use of "$similarity" in projection [\#635](https://github.com/stargate/jsonapi/pull/635) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- add pagination IT in main branch [\#628](https://github.com/stargate/jsonapi/pull/628) ([Yuqi-Du](https://github.com/Yuqi-Du))
- $in with empty array should find nothing [\#626](https://github.com/stargate/jsonapi/pull/626) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Fix \#622: add Integration tests verifying both biggest allowed, and bigger-than-allowed JSON docs, values [\#624](https://github.com/stargate/jsonapi/pull/624) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Enable node level metrics [\#621](https://github.com/stargate/jsonapi/pull/621) ([amorton](https://github.com/amorton))
- Fix \#446: expose StreamConstaintsException as ApiException [\#619](https://github.com/stargate/jsonapi/pull/619) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Use Stargate v2.1.0-BETA-3 [\#618](https://github.com/stargate/jsonapi/pull/618) ([github-actions[bot]](https://github.com/apps/github-actions))
- Fix \#596: renaming "pagingState"/"nextPagingState" as "pageState"/"nextPageState" [\#614](https://github.com/stargate/jsonapi/pull/614) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Native cql changes [\#606](https://github.com/stargate/jsonapi/pull/606) ([maheshrajamani](https://github.com/maheshrajamani))
- add more in createCollection [\#605](https://github.com/stargate/jsonapi/pull/605) ([johnsmartco](https://github.com/johnsmartco))

## [v1.0.0-BETA-3](https://github.com/stargate/jsonapi/tree/v1.0.0-BETA-3) (2023-11-02)

[Full Changelog](https://github.com/stargate/jsonapi/compare/v1.0.0-BETA-2...v1.0.0-BETA-3)

**Implemented enhancements:**

- Improve JSON API error messages with more informative help and \(where possible\) suggested user action [\#552](https://github.com/stargate/jsonapi/issues/552)

**Fixed bugs:**

- UNAUTHENTICATED: Invalid token msg is override because of Metrics tag exception [\#603](https://github.com/stargate/jsonapi/issues/603)

**Closed issues:**

- Misleading error message on nonexisting table when implying vector-enabled [\#609](https://github.com/stargate/jsonapi/issues/609)
- Remove extra trailing white space from text aggregated for text search [\#602](https://github.com/stargate/jsonapi/issues/602)
- Native Image build failure [\#597](https://github.com/stargate/jsonapi/issues/597)
- ExceptionClass exposure in error message [\#578](https://github.com/stargate/jsonapi/issues/578)
- Enforce limit of maximum 5 Collections per ---database--- namespace [\#577](https://github.com/stargate/jsonapi/issues/577)
- Fully rely on List\<Expression\<BuildCondition\>\> for build where clause [\#543](https://github.com/stargate/jsonapi/issues/543)
- SPEC - Document using $slice in projections  [\#130](https://github.com/stargate/jsonapi/issues/130)

**Merged pull requests:**

- fix collection not exist [\#612](https://github.com/stargate/jsonapi/pull/612) ([Yuqi-Du](https://github.com/Yuqi-Du))
- \Revert "fix collection not exist" \(accidental push to main\) [\#611](https://github.com/stargate/jsonapi/pull/611) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- token header name "X-Casssandra-Token" -\> "Token" [\#607](https://github.com/stargate/jsonapi/pull/607) ([Yuqi-Du](https://github.com/Yuqi-Du))
- fix error mapping  [\#604](https://github.com/stargate/jsonapi/pull/604) ([Yuqi-Du](https://github.com/Yuqi-Du))
- ensure JVM heap memory settings applied in Java-based image [\#599](https://github.com/stargate/jsonapi/pull/599) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Fix native image building with datastax driver dependency [\#598](https://github.com/stargate/jsonapi/pull/598) ([kathirsvn](https://github.com/kathirsvn))
- Verify max 5 collections creation \(per namespace\) [\#595](https://github.com/stargate/jsonapi/pull/595) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Update dse-next reference for ITs to latest one as well [\#592](https://github.com/stargate/jsonapi/pull/592) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- provide additional detail on swagger docs for insertMany [\#591](https://github.com/stargate/jsonapi/pull/591) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- expose ExceptionClass only in debug mode [\#584](https://github.com/stargate/jsonapi/pull/584) ([Yuqi-Du](https://github.com/Yuqi-Du))

## [v1.0.0-BETA-2](https://github.com/stargate/jsonapi/tree/v1.0.0-BETA-2) (2023-10-24)

[Full Changelog](https://github.com/stargate/jsonapi/compare/v1.0.0-BETA-1...v1.0.0-BETA-2)

**Fixed bugs:**

- Performance tests returning bad status code errors [\#422](https://github.com/stargate/jsonapi/issues/422)

**Closed issues:**

- CountDocumentsCommands should be CountDocumentsCommand [\#583](https://github.com/stargate/jsonapi/issues/583)
- Add `isEmpty\(\)` method in `JsonFieldExtractor` [\#579](https://github.com/stargate/jsonapi/issues/579)
- Add validation of path expression configuration for Field Extractor \(in `json-api-analyzer-filter`\) [\#574](https://github.com/stargate/jsonapi/issues/574)
- Allow passing of empty JSON Object \(`{ }`\) for `sort` for `find` and `findOne` Commands [\#572](https://github.com/stargate/jsonapi/issues/572)
- Vector options name change [\#567](https://github.com/stargate/jsonapi/issues/567)
- Add simple micro-benchmarking of Field Extractor for `json-api-analyzer-filter` [\#564](https://github.com/stargate/jsonapi/issues/564)
- Add ability to "JSON detect" input given to Field Extractor [\#563](https://github.com/stargate/jsonapi/issues/563)
- includeSimilarity option doesn't seem to work with `findOne\(\)` [\#558](https://github.com/stargate/jsonapi/issues/558)
- Implement efficient JSON document filtering to be used by "JSON analyzer" for $text indexing [\#554](https://github.com/stargate/jsonapi/issues/554)
- Provide a way to display collection options [\#550](https://github.com/stargate/jsonapi/issues/550)
- Limit number of filtering fields in find commands [\#548](https://github.com/stargate/jsonapi/issues/548)
- support $and, $or [\#547](https://github.com/stargate/jsonapi/issues/547)
- Handling of createCollection command [\#546](https://github.com/stargate/jsonapi/issues/546)
- Json api messaging if namespace is not found. [\#545](https://github.com/stargate/jsonapi/issues/545)
- Add NoSQLBench test for insertMany [\#541](https://github.com/stargate/jsonapi/issues/541)
- revisit jsonapi native image build and publish process [\#494](https://github.com/stargate/jsonapi/issues/494)
- Limit number of documents to count [\#431](https://github.com/stargate/jsonapi/issues/431)
- Decide on system and non-jsonapi namespace handling [\#341](https://github.com/stargate/jsonapi/issues/341)
- Implement `$in` support [\#291](https://github.com/stargate/jsonapi/issues/291)

**Merged pull requests:**

- Update to latest dse-db-all [\#587](https://github.com/stargate/jsonapi/pull/587) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- typo fix: CountDocumentsCommands -\> CountDocumentsCommand [\#585](https://github.com/stargate/jsonapi/pull/585) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Update Jackson to latest release, 2.15.3 [\#580](https://github.com/stargate/jsonapi/pull/580) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix \#572: add ITs to verify empty JSON Object is valid for `sort` of `find`/`findOne`, fix as necessary [\#576](https://github.com/stargate/jsonapi/pull/576) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Changes for create collection option names [\#575](https://github.com/stargate/jsonapi/pull/575) ([maheshrajamani](https://github.com/maheshrajamani))
- Use Stargate v2.1.0-BETA-2 [\#573](https://github.com/stargate/jsonapi/pull/573) ([github-actions[bot]](https://github.com/apps/github-actions))
- Limit number of filtering fields in find commands [\#570](https://github.com/stargate/jsonapi/pull/570) ([Hazel-Datastax](https://github.com/Hazel-Datastax))
- fix changelog [\#568](https://github.com/stargate/jsonapi/pull/568) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Add error message in createCollection\(\) [\#566](https://github.com/stargate/jsonapi/pull/566) ([Hazel-Datastax](https://github.com/Hazel-Datastax))
- Support and or [\#565](https://github.com/stargate/jsonapi/pull/565) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Vertex AI embedding client fix [\#561](https://github.com/stargate/jsonapi/pull/561) ([maheshrajamani](https://github.com/maheshrajamani))
- Return create collection options as part of findCollections response [\#559](https://github.com/stargate/jsonapi/pull/559) ([maheshrajamani](https://github.com/maheshrajamani))
- Handle error when the namespace doesn't exist [\#557](https://github.com/stargate/jsonapi/pull/557) ([Hazel-Datastax](https://github.com/Hazel-Datastax))
- Update `dse-next` dependency to latest \(4.0.11-669ae5e3994d\) [\#556](https://github.com/stargate/jsonapi/pull/556) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Add NosqlBench insertmany test [\#555](https://github.com/stargate/jsonapi/pull/555) ([Hazel-Datastax](https://github.com/Hazel-Datastax))
- update base image to smaller runtime version [\#553](https://github.com/stargate/jsonapi/pull/553) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Use Stargate v2.1.0-BETA-1 [\#551](https://github.com/stargate/jsonapi/pull/551) ([github-actions[bot]](https://github.com/apps/github-actions))
- Update Docker base images to address Python vuln/CVE [\#549](https://github.com/stargate/jsonapi/pull/549) ([tatu-at-datastax](https://github.com/tatu-at-datastax))

## [v1.0.0-BETA-1](https://github.com/stargate/jsonapi/tree/v1.0.0-BETA-1) (2023-09-27)

[Full Changelog](https://github.com/stargate/jsonapi/compare/v1.0.0-ALPHA-14...v1.0.0-BETA-1)

**Closed issues:**

- \[Vectorize\] Make vectorize\(\) method call run on worker thread [\#537](https://github.com/stargate/jsonapi/issues/537)
- \[Vectorize\] Add validation for update clause [\#533](https://github.com/stargate/jsonapi/issues/533)
- \[Vectorize\] Remove $vectorize field in the document  [\#532](https://github.com/stargate/jsonapi/issues/532)
- Add metrics tagging for vector functionality [\#526](https://github.com/stargate/jsonapi/issues/526)
- \[Vectorize\] Implement embedding service call [\#524](https://github.com/stargate/jsonapi/issues/524)
- pricing investigation for JSON API [\#523](https://github.com/stargate/jsonapi/issues/523)
- JSON API should allow hyphens \(`-`\) in document property names \(but should not allow empty String\) [\#521](https://github.com/stargate/jsonapi/issues/521)
- findOneAndDelete by vector has NPE issue in concurrent situation [\#517](https://github.com/stargate/jsonapi/issues/517)
- findOne filtering by `$vector` returns no results [\#516](https://github.com/stargate/jsonapi/issues/516)
- User-friendly notification of unsupported features [\#389](https://github.com/stargate/jsonapi/issues/389)

**Merged pull requests:**

- Vectorize update validation [\#542](https://github.com/stargate/jsonapi/pull/542) ([maheshrajamani](https://github.com/maheshrajamani))
- Run operation resolver on worker thread [\#540](https://github.com/stargate/jsonapi/pull/540) ([maheshrajamani](https://github.com/maheshrajamani))
- Add metrics for sort type and vector enabled [\#539](https://github.com/stargate/jsonapi/pull/539) ([maheshrajamani](https://github.com/maheshrajamani))
- ensure password is masked on ecr login [\#538](https://github.com/stargate/jsonapi/pull/538) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Use Stargate v2.1.0-ALPHA-11 [\#536](https://github.com/stargate/jsonapi/pull/536) ([github-actions[bot]](https://github.com/apps/github-actions))
- In support [\#535](https://github.com/stargate/jsonapi/pull/535) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Update dse-db-all dependency to latest \(same as Stargate\) [\#534](https://github.com/stargate/jsonapi/pull/534) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix update vector [\#531](https://github.com/stargate/jsonapi/pull/531) ([maheshrajamani](https://github.com/maheshrajamani))
- update to beta [\#530](https://github.com/stargate/jsonapi/pull/530) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Fixed JSON API Documentation url [\#529](https://github.com/stargate/jsonapi/pull/529) ([kathirsvn](https://github.com/kathirsvn))
- Add embedding support api [\#528](https://github.com/stargate/jsonapi/pull/528) ([maheshrajamani](https://github.com/maheshrajamani))
- Use Stargate v2.1.0-ALPHA-10 [\#527](https://github.com/stargate/jsonapi/pull/527) ([github-actions[bot]](https://github.com/apps/github-actions))
- Fix \#521: allow hyphen in names, prevent empty String [\#522](https://github.com/stargate/jsonapi/pull/522) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Use Stargate v2.1.0-ALPHA-9 [\#520](https://github.com/stargate/jsonapi/pull/520) ([github-actions[bot]](https://github.com/apps/github-actions))
- Fixes \#516: add validation to fail on attempts to filter on $vector \(except with $exists\) [\#519](https://github.com/stargate/jsonapi/pull/519) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- fix NPE when concurrent delete [\#518](https://github.com/stargate/jsonapi/pull/518) ([Yuqi-Du](https://github.com/Yuqi-Du))
- add fallout configuration and fallout nosqlbench workload [\#515](https://github.com/stargate/jsonapi/pull/515) ([Yuqi-Du](https://github.com/Yuqi-Du))
- add nb test cases for vector search jsonapi [\#512](https://github.com/stargate/jsonapi/pull/512) ([Yuqi-Du](https://github.com/Yuqi-Du))

## [v1.0.0-ALPHA-14](https://github.com/stargate/jsonapi/tree/v1.0.0-ALPHA-14) (2023-08-21)

[Full Changelog](https://github.com/stargate/jsonapi/compare/v1.0.0-ALPHA-13...v1.0.0-ALPHA-14)

**Closed issues:**

- Reduce index columns [\#505](https://github.com/stargate/jsonapi/issues/505)
- IT cases for vector size validations [\#497](https://github.com/stargate/jsonapi/issues/497)
- Create APP using JSON API [\#465](https://github.com/stargate/jsonapi/issues/465)
- \[Design Revisit\] Projection handling of $vector [\#461](https://github.com/stargate/jsonapi/issues/461)

**Merged pull requests:**

- Use Stargate v2.1.0-ALPHA-7 [\#513](https://github.com/stargate/jsonapi/pull/513) ([github-actions[bot]](https://github.com/apps/github-actions))
- Swagger vector search [\#511](https://github.com/stargate/jsonapi/pull/511) ([maheshrajamani](https://github.com/maheshrajamani))
- User friendly unsupported feature notification [\#510](https://github.com/stargate/jsonapi/pull/510) ([Yuqi-Du](https://github.com/Yuqi-Du))
- Create integration test for vector unmatched size\(insert/find\) [\#503](https://github.com/stargate/jsonapi/pull/503) ([Yuqi-Du](https://github.com/Yuqi-Du))

## [v1.0.0-ALPHA-13](https://github.com/stargate/jsonapi/tree/v1.0.0-ALPHA-13) (2023-08-16)

[Full Changelog](https://github.com/stargate/jsonapi/compare/v1.0.0-ALPHA-12...v1.0.0-ALPHA-13)

**Closed issues:**

- Create Collection with vector options fail [\#504](https://github.com/stargate/jsonapi/issues/504)

**Merged pull requests:**

- Merge array\_equals and sub\_doc\_equals field into query\_text\_value [\#506](https://github.com/stargate/jsonapi/pull/506) ([maheshrajamani](https://github.com/maheshrajamani))

## [v1.0.0-ALPHA-12](https://github.com/stargate/jsonapi/tree/v1.0.0-ALPHA-12) (2023-08-10)

[Full Changelog](https://github.com/stargate/jsonapi/compare/v1.0.0-ALPHA-11...v1.0.0-ALPHA-12)

**Closed issues:**

- Sort with `$vector: {}` throws an unreadable error [\#493](https://github.com/stargate/jsonapi/issues/493)
- Default docker-compose config shouldn't require build [\#489](https://github.com/stargate/jsonapi/issues/489)
- Include vector similarity in vector search result [\#463](https://github.com/stargate/jsonapi/issues/463)

**Merged pull requests:**

- Projection clause support for similarity function score [\#500](https://github.com/stargate/jsonapi/pull/500) ([maheshrajamani](https://github.com/maheshrajamani))
- Use Stargate v2.1.0-ALPHA-6 [\#499](https://github.com/stargate/jsonapi/pull/499) ([github-actions[bot]](https://github.com/apps/github-actions))
- update parent workflow for 2.1.x event [\#498](https://github.com/stargate/jsonapi/pull/498) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Fixed sort clause deserialization for non array values [\#495](https://github.com/stargate/jsonapi/pull/495) ([maheshrajamani](https://github.com/maheshrajamani))
- pull docker image for coordinator from ECR [\#492](https://github.com/stargate/jsonapi/pull/492) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Bumping version for next jsonapi release [\#491](https://github.com/stargate/jsonapi/pull/491) ([github-actions[bot]](https://github.com/apps/github-actions))
- Docker compose defaults [\#490](https://github.com/stargate/jsonapi/pull/490) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))

## [v1.0.0-ALPHA-11](https://github.com/stargate/jsonapi/tree/v1.0.0-ALPHA-11) (2023-08-02)

[Full Changelog](https://github.com/stargate/jsonapi/compare/v1.0.0-ALPHA-10...v1.0.0-ALPHA-11)

**Closed issues:**

- Vector search schema cache by tenant [\#486](https://github.com/stargate/jsonapi/issues/486)
- Cannot `createCollection\(\)` with vector option without sending similarity function [\#483](https://github.com/stargate/jsonapi/issues/483)
- Projection handling of $vector [\#480](https://github.com/stargate/jsonapi/issues/480)
- Extend JSON API vector search functionality support [\#478](https://github.com/stargate/jsonapi/issues/478)
- Relax array size validation for $vector field [\#475](https://github.com/stargate/jsonapi/issues/475)
- Validation for vector search [\#460](https://github.com/stargate/jsonapi/issues/460)
- Find\* command changes for vector search [\#459](https://github.com/stargate/jsonapi/issues/459)
- Document shredder changes for vector search [\#458](https://github.com/stargate/jsonapi/issues/458)
- Update command changes for vector search [\#457](https://github.com/stargate/jsonapi/issues/457)
- Insert command changes to support vector search [\#456](https://github.com/stargate/jsonapi/issues/456)
- Create collection enhancement to support vector search configuration [\#455](https://github.com/stargate/jsonapi/issues/455)
- JSON API Logging [\#404](https://github.com/stargate/jsonapi/issues/404)

**Merged pull requests:**

- update workflows to reference coordinator-dse-next image [\#488](https://github.com/stargate/jsonapi/pull/488) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Find command options validation and tenant based cache [\#487](https://github.com/stargate/jsonapi/pull/487) ([maheshrajamani](https://github.com/maheshrajamani))
- Default vector search function name to cosine. [\#485](https://github.com/stargate/jsonapi/pull/485) ([maheshrajamani](https://github.com/maheshrajamani))
- update workflows to reference dse-next backend [\#484](https://github.com/stargate/jsonapi/pull/484) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Extended the commands that supports sort clause to also support vector search [\#482](https://github.com/stargate/jsonapi/pull/482) ([maheshrajamani](https://github.com/maheshrajamani))
- Support vector field in projection clause [\#481](https://github.com/stargate/jsonapi/pull/481) ([maheshrajamani](https://github.com/maheshrajamani))
- Allow bigger arrays for vector embeddings [\#479](https://github.com/stargate/jsonapi/pull/479) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- $set and $unset operation support for vector data field [\#477](https://github.com/stargate/jsonapi/pull/477) ([maheshrajamani](https://github.com/maheshrajamani))
- Convert non-dev docker-compose too; rename [\#476](https://github.com/stargate/jsonapi/pull/476) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Find and findOne command changes to support vector search [\#474](https://github.com/stargate/jsonapi/pull/474) ([maheshrajamani](https://github.com/maheshrajamani))
- docker compose change [\#473](https://github.com/stargate/jsonapi/pull/473) ([maheshrajamani](https://github.com/maheshrajamani))
- Reduce logging of known+handled cases [\#472](https://github.com/stargate/jsonapi/pull/472) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Change backend for ITs from `persistence-dse-6.8` to `persistence-dse-next` \(pre-7.0\) [\#471](https://github.com/stargate/jsonapi/pull/471) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Update Stargate dep to 2.1.0-ALPHA-4 [\#470](https://github.com/stargate/jsonapi/pull/470) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Vector search changes for shredder and Insert command [\#469](https://github.com/stargate/jsonapi/pull/469) ([maheshrajamani](https://github.com/maheshrajamani))
- Update SG dependency to 2.1.0-ALPHA-3 [\#468](https://github.com/stargate/jsonapi/pull/468) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Refactored FindOperation constructor usage [\#467](https://github.com/stargate/jsonapi/pull/467) ([maheshrajamani](https://github.com/maheshrajamani))
- Bumping version for next jsonapi release [\#464](https://github.com/stargate/jsonapi/pull/464) ([github-actions[bot]](https://github.com/apps/github-actions))
- Use Stargate v2.0.16 [\#453](https://github.com/stargate/jsonapi/pull/453) ([github-actions[bot]](https://github.com/apps/github-actions))

## [v1.0.0-ALPHA-10](https://github.com/stargate/jsonapi/tree/v1.0.0-ALPHA-10) (2023-07-14)

[Full Changelog](https://github.com/stargate/jsonapi/compare/v1.0.0-ALPHA-9...v1.0.0-ALPHA-10)

**Fixed bugs:**

- Coordinator logs full of warning message about aggregation key [\#413](https://github.com/stargate/jsonapi/issues/413)

**Merged pull requests:**

- Use Stargate v2.0.15 [\#440](https://github.com/stargate/jsonapi/pull/440) ([github-actions[bot]](https://github.com/apps/github-actions))
- Add command name to count metrics [\#437](https://github.com/stargate/jsonapi/pull/437) ([maheshrajamani](https://github.com/maheshrajamani))
- Refactor id-handling part of Shredding [\#436](https://github.com/stargate/jsonapi/pull/436) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- add fallout smoke test [\#423](https://github.com/stargate/jsonapi/pull/423) ([ivansenic](https://github.com/ivansenic))

## [v1.0.0-ALPHA-9](https://github.com/stargate/jsonapi/tree/v1.0.0-ALPHA-9) (2023-05-22)

[Full Changelog](https://github.com/stargate/jsonapi/compare/v1.0.0-ALPHA-8...v1.0.0-ALPHA-9)

**Closed issues:**

- Use count\(1\) in count operation query [\#424](https://github.com/stargate/jsonapi/issues/424)
- Validate Document field names so they conform to the spec [\#408](https://github.com/stargate/jsonapi/issues/408)
- Intermittent DEADLINE\_EXCEEDED error [\#208](https://github.com/stargate/jsonapi/issues/208)
- Ensure nextPageState in requests is handled securely  [\#139](https://github.com/stargate/jsonapi/issues/139)

**Merged pull requests:**

- Fix \#433: allow \_id with $setOnInsert [\#434](https://github.com/stargate/jsonapi/pull/434) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix \#408: validate characters of document field names [\#429](https://github.com/stargate/jsonapi/pull/429) ([tatu-at-datastax](https://github.com/tatu-at-datastax))

## [v1.0.0-ALPHA-8](https://github.com/stargate/jsonapi/tree/v1.0.0-ALPHA-8) (2023-05-12)

[Full Changelog](https://github.com/stargate/jsonapi/compare/v1.0.0-ALPHA-7...v1.0.0-ALPHA-8)

**Fixed bugs:**

- Docker-comspse memory settings cause coordinator OOM [\#414](https://github.com/stargate/jsonapi/issues/414)

**Closed issues:**

- Driver response is not reflecting in the Mongoose Model API response [\#418](https://github.com/stargate/jsonapi/issues/418)
- findOneAndUpdate with upsert flag not returning \_id [\#417](https://github.com/stargate/jsonapi/issues/417)
- findOneAndReplace upsert not generating \_id [\#416](https://github.com/stargate/jsonapi/issues/416)
- JSON API HTTP error codes [\#407](https://github.com/stargate/jsonapi/issues/407)
- \(Date/Time support\) Add `$currentDate` update operation  [\#397](https://github.com/stargate/jsonapi/issues/397)
- findOne with options should throw an error back [\#395](https://github.com/stargate/jsonapi/issues/395)
- \(Date/Time support\) Allow update operators on Date/Time valued fields [\#372](https://github.com/stargate/jsonapi/issues/372)
- Add first-class support for Date-Time values for JSON-API [\#362](https://github.com/stargate/jsonapi/issues/362)

**Merged pull requests:**

- Count operation - fix to use count\(1\) instead of count\("key"\)  [\#427](https://github.com/stargate/jsonapi/pull/427) ([maheshrajamani](https://github.com/maheshrajamani))
- Use Stargate v2.0.13 [\#426](https://github.com/stargate/jsonapi/pull/426) ([github-actions[bot]](https://github.com/apps/github-actions))
- Fix \_id field returned for findOneAndReplace and findOneAndUpdate with upsert option [\#421](https://github.com/stargate/jsonapi/pull/421) ([maheshrajamani](https://github.com/maheshrajamani))
- closes \#414: adapts java args for docker-compose [\#420](https://github.com/stargate/jsonapi/pull/420) ([ivansenic](https://github.com/ivansenic))
- Http status code change [\#415](https://github.com/stargate/jsonapi/pull/415) ([maheshrajamani](https://github.com/maheshrajamani))
- Fix \#395: verify that "no options" Commands are not given any options [\#412](https://github.com/stargate/jsonapi/pull/412) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix \#397: add $currentDate implementation [\#410](https://github.com/stargate/jsonapi/pull/410) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Bumping version for next jsonapi release [\#409](https://github.com/stargate/jsonapi/pull/409) ([github-actions[bot]](https://github.com/apps/github-actions))
- performance test workflow improvements [\#406](https://github.com/stargate/jsonapi/pull/406) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))

## [v1.0.0-ALPHA-7](https://github.com/stargate/jsonapi/tree/v1.0.0-ALPHA-7) (2023-05-01)

[Full Changelog](https://github.com/stargate/jsonapi/compare/v1.0.0-ALPHA-6...v1.0.0-ALPHA-7)

**Closed issues:**

- Spec - Support deleteOne\(\) and updateOne\(\) with sort [\#400](https://github.com/stargate/jsonapi/issues/400)
- Support `deleteOne\(\)` and `updateOne\(\)` with `sort` [\#396](https://github.com/stargate/jsonapi/issues/396)
- Change JSON API default port number [\#381](https://github.com/stargate/jsonapi/issues/381)
- Add no-sql bench test for find command with sort [\#303](https://github.com/stargate/jsonapi/issues/303)
- Design + Spec service side implementation of sort + offset [\#156](https://github.com/stargate/jsonapi/issues/156)
- consistent "document" / "documents" fields for insert commands  [\#141](https://github.com/stargate/jsonapi/issues/141)

**Merged pull requests:**

- Fix \#372: add tests to verify $date-value updates work as-is [\#405](https://github.com/stargate/jsonapi/pull/405) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Default port change from  8080 -\> 8181 [\#403](https://github.com/stargate/jsonapi/pull/403) ([maheshrajamani](https://github.com/maheshrajamani))
- closes \#141: introduce single and multi document responses [\#402](https://github.com/stargate/jsonapi/pull/402) ([ivansenic](https://github.com/ivansenic))
- Spec update for sort clause, updateOne and deleteOne [\#401](https://github.com/stargate/jsonapi/pull/401) ([maheshrajamani](https://github.com/maheshrajamani))
- Added sort option for `deleteOne` and `updateOne` command [\#399](https://github.com/stargate/jsonapi/pull/399) ([maheshrajamani](https://github.com/maheshrajamani))
- Bumping version for next jsonapi release [\#398](https://github.com/stargate/jsonapi/pull/398) ([github-actions[bot]](https://github.com/apps/github-actions))
- measure size of data directory after test [\#328](https://github.com/stargate/jsonapi/pull/328) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))

## [v1.0.0-ALPHA-6](https://github.com/stargate/jsonapi/tree/v1.0.0-ALPHA-6) (2023-04-25)

[Full Changelog](https://github.com/stargate/jsonapi/compare/v1.0.0-ALPHA-5...v1.0.0-ALPHA-6)

**Closed issues:**

- `findOneAndUpdate\(\)` seems to not return result if no updates to apply [\#390](https://github.com/stargate/jsonapi/issues/390)
- \(Date/Time support\) Change Insert and Update operation [\#378](https://github.com/stargate/jsonapi/issues/378)
- \(Date/Time support\) Support full filtering on Date/Time valued fields [\#375](https://github.com/stargate/jsonapi/issues/375)
- \(Date/Time support\) Support sorting on Date/Time valued fields [\#374](https://github.com/stargate/jsonapi/issues/374)
- \(Date/Time support\) Ensure Projection of Date/Time valued fields works [\#373](https://github.com/stargate/jsonapi/issues/373)
- \(Date/Time support\) allow use of Date/Time as Document Id \(`\_id`\) [\#371](https://github.com/stargate/jsonapi/issues/371)
- \(Date/Time support\) Add DB column `query\_timestamp\_values`, Shredding support [\#370](https://github.com/stargate/jsonapi/issues/370)
- Improve failure message on "inserting too many documents" to include limit, number sent [\#365](https://github.com/stargate/jsonapi/issues/365)
- Errors in response [\#350](https://github.com/stargate/jsonapi/issues/350)
- find command options validation [\#349](https://github.com/stargate/jsonapi/issues/349)
- Add serial consistency validation with next Stargate upgrade. [\#344](https://github.com/stargate/jsonapi/issues/344)
- Implement findCollections command [\#320](https://github.com/stargate/jsonapi/issues/320)
- Spec: should `findOne\(\)` have `count` entry in response? [\#313](https://github.com/stargate/jsonapi/issues/313)
- Document all json document config options [\#305](https://github.com/stargate/jsonapi/issues/305)
- Setting Serial consistency for write operation [\#238](https://github.com/stargate/jsonapi/issues/238)
- Indicate minimum required versions of docker and docker-compose in README [\#227](https://github.com/stargate/jsonapi/issues/227)
- Ensure namespace and collection names are validated [\#149](https://github.com/stargate/jsonapi/issues/149)
- SPEC document countDocuments\(\) [\#147](https://github.com/stargate/jsonapi/issues/147)
- SPEC - documentation for $exists  [\#122](https://github.com/stargate/jsonapi/issues/122)

**Merged pull requests:**

- nosqlbench test for find command [\#394](https://github.com/stargate/jsonapi/pull/394) ([maheshrajamani](https://github.com/maheshrajamani))
- Fix \#373: add basic tests to verify "$date" wrt Projection [\#393](https://github.com/stargate/jsonapi/pull/393) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- `findOneAndUpdate\(\)` and `findOneAndReplace\(\)` to return document when no data change [\#392](https://github.com/stargate/jsonapi/pull/392) ([maheshrajamani](https://github.com/maheshrajamani))
- Error message text fix grater -\> greater as per comment in PR \#361 [\#391](https://github.com/stargate/jsonapi/pull/391) ([maheshrajamani](https://github.com/maheshrajamani))
- Add info to countDocuments spec - needs review [\#385](https://github.com/stargate/jsonapi/pull/385) ([johnsmartco](https://github.com/johnsmartco))
- Date field sorting [\#384](https://github.com/stargate/jsonapi/pull/384) ([maheshrajamani](https://github.com/maheshrajamani))
- Add initial handling of Date type as Document id [\#383](https://github.com/stargate/jsonapi/pull/383) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Add DocValueHasher tests wrt \#370 [\#382](https://github.com/stargate/jsonapi/pull/382) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Changes for date filter [\#380](https://github.com/stargate/jsonapi/pull/380) ([maheshrajamani](https://github.com/maheshrajamani))
- Changes for Insert and Update operation to handle date field [\#379](https://github.com/stargate/jsonapi/pull/379) ([maheshrajamani](https://github.com/maheshrajamani))
- Add "query\_timestamp\_values" column in Collections, update shredder [\#376](https://github.com/stargate/jsonapi/pull/376) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- closes \#350: errorCode and exceptionClass to be reported always with … [\#369](https://github.com/stargate/jsonapi/pull/369) ([ivansenic](https://github.com/ivansenic))
- `$in` operator support for empty array [\#368](https://github.com/stargate/jsonapi/pull/368) ([maheshrajamani](https://github.com/maheshrajamani))
- closes \#313: remove data.count from command responses [\#367](https://github.com/stargate/jsonapi/pull/367) ([ivansenic](https://github.com/ivansenic))
- Fixes \#365: indicate max docs, actual docs inserted in error \(fail\) message [\#366](https://github.com/stargate/jsonapi/pull/366) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Additional test wrt \#329 as per code review suggestion [\#364](https://github.com/stargate/jsonapi/pull/364) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Added serial consistency for writer operations [\#363](https://github.com/stargate/jsonapi/pull/363) ([maheshrajamani](https://github.com/maheshrajamani))
- Added validation for find command options. [\#361](https://github.com/stargate/jsonapi/pull/361) ([maheshrajamani](https://github.com/maheshrajamani))
- closes \#149: finalized namespace and collection names validations [\#360](https://github.com/stargate/jsonapi/pull/360) ([ivansenic](https://github.com/ivansenic))
- use upper-case collection and namespace names in the int tests [\#359](https://github.com/stargate/jsonapi/pull/359) ([ivansenic](https://github.com/ivansenic))
- NoSQLBench workloads - auto token generation [\#358](https://github.com/stargate/jsonapi/pull/358) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Bumping version for next jsonapi release [\#357](https://github.com/stargate/jsonapi/pull/357) ([github-actions[bot]](https://github.com/apps/github-actions))
- update to dse 6.8.34 [\#351](https://github.com/stargate/jsonapi/pull/351) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Fix broken postman workflow [\#347](https://github.com/stargate/jsonapi/pull/347) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- closes \#305: reorganize and document config options [\#331](https://github.com/stargate/jsonapi/pull/331) ([ivansenic](https://github.com/ivansenic))

## [v1.0.0-ALPHA-5](https://github.com/stargate/jsonapi/tree/v1.0.0-ALPHA-5) (2023-04-14)

[Full Changelog](https://github.com/stargate/jsonapi/compare/v1.0.0-ALPHA-4...v1.0.0-ALPHA-5)

**Fixed bugs:**

- Keyspace and table names not handling case-sensitivity \(missing escaping in CQL statements\) [\#337](https://github.com/stargate/jsonapi/issues/337)
- All retries of `HTTPLimitsIntegrationTest` fail on native [\#266](https://github.com/stargate/jsonapi/issues/266)

**Closed issues:**

- Implement`$in` operator for `\_id` field [\#346](https://github.com/stargate/jsonapi/issues/346)
- Add Projection tests for `findOneAndReplace` [\#330](https://github.com/stargate/jsonapi/issues/330)
- Add support for `$slice` Array Projection modifier [\#329](https://github.com/stargate/jsonapi/issues/329)
- Improve testing of `findOneAndUpdate\(\)` wrt Projection [\#325](https://github.com/stargate/jsonapi/issues/325)
- Change sort clause format [\#323](https://github.com/stargate/jsonapi/issues/323)
- Implement findNamespaces command [\#322](https://github.com/stargate/jsonapi/issues/322)
- Implement dropNamespace command [\#321](https://github.com/stargate/jsonapi/issues/321)
- Implement findOneAndDelete command [\#319](https://github.com/stargate/jsonapi/issues/319)
- Support sort for findOne and findOneAndUpdate command [\#312](https://github.com/stargate/jsonapi/issues/312)
- Implement findOneAndReplace command [\#311](https://github.com/stargate/jsonapi/issues/311)
- Sort testing improvements [\#306](https://github.com/stargate/jsonapi/issues/306)
- Implement full Projection for `find\(\)`/`findOne\(\)`/`findOneAndUpdate\(\)` [\#300](https://github.com/stargate/jsonapi/issues/300)
- Decrease count-down latch on failing concurrency tests [\#298](https://github.com/stargate/jsonapi/issues/298)
- Support empty `options` object for all commands [\#293](https://github.com/stargate/jsonapi/issues/293)
- Update to DSE 6.8.33 [\#287](https://github.com/stargate/jsonapi/issues/287)
- updateOne 'upsert' flag response fields missing [\#275](https://github.com/stargate/jsonapi/issues/275)
- updateMany 'upsert' flag response fields missing [\#273](https://github.com/stargate/jsonapi/issues/273)
- Implement in-memory sort [\#272](https://github.com/stargate/jsonapi/issues/272)
- Implement retries for availability faults [\#261](https://github.com/stargate/jsonapi/issues/261)
- Refactor `UpdateTargetLocator` as `ActionTargetLocator` for use with in-memory sorting, projection [\#257](https://github.com/stargate/jsonapi/issues/257)
- Pull Stargate images from ECR for IT [\#246](https://github.com/stargate/jsonapi/issues/246)
- Check that different update operators do not target same field \(conflict\) [\#232](https://github.com/stargate/jsonapi/issues/232)
- Implement $mul update operator [\#224](https://github.com/stargate/jsonapi/issues/224)
- Implement `$setOnInsert` update operator [\#219](https://github.com/stargate/jsonapi/issues/219)
- Define `status.moreData` in the spec [\#207](https://github.com/stargate/jsonapi/issues/207)
- Separate concerns in the `ReadOperation` [\#201](https://github.com/stargate/jsonapi/issues/201)
- Confirm all response messages against spec [\#178](https://github.com/stargate/jsonapi/issues/178)
- Implement document limits  [\#173](https://github.com/stargate/jsonapi/issues/173)
- Support dotted notation path in all clauses  [\#171](https://github.com/stargate/jsonapi/issues/171)
- Error message when collection name has invalid chars [\#167](https://github.com/stargate/jsonapi/issues/167)
- Implement general update operator `$rename` [\#165](https://github.com/stargate/jsonapi/issues/165)
- Support missing or empty projection clause for commands [\#158](https://github.com/stargate/jsonapi/issues/158)
- Decide on CQL LOGGED / UNLOGGED BATCH for deleteMany and updateMany  [\#146](https://github.com/stargate/jsonapi/issues/146)
- Limit number of docs for insertMany to 10 [\#144](https://github.com/stargate/jsonapi/issues/144)
- Implement document rules [\#136](https://github.com/stargate/jsonapi/issues/136)
- Implement order of operations for updates [\#133](https://github.com/stargate/jsonapi/issues/133)
- SPEC - Document that we maintain field order  [\#129](https://github.com/stargate/jsonapi/issues/129)
- Spec - define $size for arrays  [\#111](https://github.com/stargate/jsonapi/issues/111)
- Spec - Define $all for arrays [\#110](https://github.com/stargate/jsonapi/issues/110)
- Spec - Define equality handling with arrays and subdocs [\#109](https://github.com/stargate/jsonapi/issues/109)

**Merged pull requests:**

- Kathirsvn/openapi path fix [\#356](https://github.com/stargate/jsonapi/pull/356) ([kathirsvn](https://github.com/kathirsvn))
- Use Stargate v2.0.12 [\#355](https://github.com/stargate/jsonapi/pull/355) ([github-actions[bot]](https://github.com/apps/github-actions))
- Add $size [\#354](https://github.com/stargate/jsonapi/pull/354) ([johnsmartco](https://github.com/johnsmartco))
- Add section Equality handling with arrays and subdocs, and $all [\#353](https://github.com/stargate/jsonapi/pull/353) ([johnsmartco](https://github.com/johnsmartco))
- Fix capitalization of intra-topic links [\#352](https://github.com/stargate/jsonapi/pull/352) ([johnsmartco](https://github.com/johnsmartco))
- `$in` operator support for `\_id` field [\#348](https://github.com/stargate/jsonapi/pull/348) ([maheshrajamani](https://github.com/maheshrajamani))
- Added upsert support for findOneAndReplace [\#345](https://github.com/stargate/jsonapi/pull/345) ([maheshrajamani](https://github.com/maheshrajamani))
- Handle collection name with case sensitivity. [\#343](https://github.com/stargate/jsonapi/pull/343) ([maheshrajamani](https://github.com/maheshrajamani))
- Fixes \#329: Support `$slice` modifier for Projection [\#342](https://github.com/stargate/jsonapi/pull/342) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- closes \#320: implement findCollections command  [\#340](https://github.com/stargate/jsonapi/pull/340) ([ivansenic](https://github.com/ivansenic))
- closes \#322: implement findNamespaces command [\#339](https://github.com/stargate/jsonapi/pull/339) ([ivansenic](https://github.com/ivansenic))
- Fixes \#330: add tests for FindOneAndReplace + projection [\#338](https://github.com/stargate/jsonapi/pull/338) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Sort clause format change [\#336](https://github.com/stargate/jsonapi/pull/336) ([maheshrajamani](https://github.com/maheshrajamani))
- closes \#321: dropNamespace command [\#335](https://github.com/stargate/jsonapi/pull/335) ([ivansenic](https://github.com/ivansenic))
- Add findCollections and misc edits in JSON API calls to Spec [\#334](https://github.com/stargate/jsonapi/pull/334) ([johnsmartco](https://github.com/johnsmartco))
- Use Stargate v2.0.11 [\#333](https://github.com/stargate/jsonapi/pull/333) ([github-actions[bot]](https://github.com/apps/github-actions))
- FindOneAndDelete command implementation [\#332](https://github.com/stargate/jsonapi/pull/332) ([maheshrajamani](https://github.com/maheshrajamani))
- Update jsonapi-spec.md [\#327](https://github.com/stargate/jsonapi/pull/327) ([johnsmartco](https://github.com/johnsmartco))
- Fixes \#325: Add tests for `findOneAndUpdate\(\)` with Projection [\#326](https://github.com/stargate/jsonapi/pull/326) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Implementation for findOneAndReplace command [\#324](https://github.com/stargate/jsonapi/pull/324) ([maheshrajamani](https://github.com/maheshrajamani))
- Support sort for findOne and findOneAndUpdate commands  [\#318](https://github.com/stargate/jsonapi/pull/318) ([maheshrajamani](https://github.com/maheshrajamani))
- Allow optional "options" property in `Command` without disabling unknown property check [\#317](https://github.com/stargate/jsonapi/pull/317) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- closes \#144: limit max documents in insert many [\#315](https://github.com/stargate/jsonapi/pull/315) ([ivansenic](https://github.com/ivansenic))
- Update README.md [\#314](https://github.com/stargate/jsonapi/pull/314) ([johnsmartco](https://github.com/johnsmartco))
- closes \#293: allow empty options object in all commands [\#310](https://github.com/stargate/jsonapi/pull/310) ([ivansenic](https://github.com/ivansenic))
- closes \#261: grpc retries and timeouts to fix availability failures [\#309](https://github.com/stargate/jsonapi/pull/309) ([ivansenic](https://github.com/ivansenic))
- Sort refactor and add test cases [\#308](https://github.com/stargate/jsonapi/pull/308) ([maheshrajamani](https://github.com/maheshrajamani))
- use DSE 6.8.33 in int tests [\#307](https://github.com/stargate/jsonapi/pull/307) ([ivansenic](https://github.com/ivansenic))
- Fixes \#300: implement findXxx\(\) projection [\#302](https://github.com/stargate/jsonapi/pull/302) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix remaining gap in \#158, connect findOneAndUpdate\(\) to projection [\#301](https://github.com/stargate/jsonapi/pull/301) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- closes \#298: count down latch to be decrease on assertion errors [\#299](https://github.com/stargate/jsonapi/pull/299) ([ivansenic](https://github.com/ivansenic))
- fix bumping of parent version script [\#297](https://github.com/stargate/jsonapi/pull/297) ([ivansenic](https://github.com/ivansenic))
- In memory sort - merged [\#296](https://github.com/stargate/jsonapi/pull/296) ([maheshrajamani](https://github.com/maheshrajamani))
- Fix postman workflow [\#294](https://github.com/stargate/jsonapi/pull/294) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Use latest tagged image in performance tests [\#289](https://github.com/stargate/jsonapi/pull/289) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- automated test of postman collection [\#288](https://github.com/stargate/jsonapi/pull/288) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Fix \#158: support empty Projection clause; add full decoding of projection definition \(but not yet processing\) [\#286](https://github.com/stargate/jsonapi/pull/286) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Use Stargate v2.0.10 [\#285](https://github.com/stargate/jsonapi/pull/285) ([github-actions[bot]](https://github.com/apps/github-actions))
- Refactor FilterableResolver to address \#201 [\#283](https://github.com/stargate/jsonapi/pull/283) ([maheshrajamani](https://github.com/maheshrajamani))
- separate nosqlbench execution in spaces to have more connections [\#282](https://github.com/stargate/jsonapi/pull/282) ([ivansenic](https://github.com/ivansenic))
- improve readbility and flow in the ReadAndUpdateOperation\#processUpdate [\#281](https://github.com/stargate/jsonapi/pull/281) ([ivansenic](https://github.com/ivansenic))
- Bug fix for upsert if filter is based on non id filter [\#280](https://github.com/stargate/jsonapi/pull/280) ([maheshrajamani](https://github.com/maheshrajamani))
- add workflow to run NoSQLBench performance tests [\#279](https://github.com/stargate/jsonapi/pull/279) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Add  Document limits, validation [\#278](https://github.com/stargate/jsonapi/pull/278) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Create performance-testing.yaml [\#277](https://github.com/stargate/jsonapi/pull/277) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Initial post-conversion markdown versions of specs, more edits likely [\#276](https://github.com/stargate/jsonapi/pull/276) ([johnsmartco](https://github.com/johnsmartco))
- updating incorrect references to Docs API [\#274](https://github.com/stargate/jsonapi/pull/274) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Add `$rename` update operator [\#271](https://github.com/stargate/jsonapi/pull/271) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- closes \#266: make HTTPLimitsIntegrationTest stable [\#270](https://github.com/stargate/jsonapi/pull/270) ([ivansenic](https://github.com/ivansenic))
- closes \#246: pull coord image from ecr for int tests [\#269](https://github.com/stargate/jsonapi/pull/269) ([ivansenic](https://github.com/ivansenic))
- relates to \#178: namespace and collection commands per spec [\#268](https://github.com/stargate/jsonapi/pull/268) ([ivansenic](https://github.com/ivansenic))
- Add checks to make sure Update Operators do not use conflicting/overlapping paths [\#267](https://github.com/stargate/jsonapi/pull/267) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- closes \#178: findOne and findOneUpdate per spec, improved tests [\#265](https://github.com/stargate/jsonapi/pull/265) ([ivansenic](https://github.com/ivansenic))
- cancel running ci workflows for pr branches [\#264](https://github.com/stargate/jsonapi/pull/264) ([ivansenic](https://github.com/ivansenic))
- closes \#207: add spec for [\#263](https://github.com/stargate/jsonapi/pull/263) ([ivansenic](https://github.com/ivansenic))
- UpdateTargetLocator-\>ActionTargetLocator, refactor part 2 [\#262](https://github.com/stargate/jsonapi/pull/262) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- add full path to swagger page [\#259](https://github.com/stargate/jsonapi/pull/259) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Initial refactoring of UpdateTargetLocator [\#258](https://github.com/stargate/jsonapi/pull/258) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Add more ITs for $min/$max based on code review for \#233 impl [\#256](https://github.com/stargate/jsonapi/pull/256) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- update docker compose readme [\#255](https://github.com/stargate/jsonapi/pull/255) ([ivansenic](https://github.com/ivansenic))
- relates to \#178: int test improvements for update commands [\#254](https://github.com/stargate/jsonapi/pull/254) ([ivansenic](https://github.com/ivansenic))
- Bumping version for next jsonapi release [\#253](https://github.com/stargate/jsonapi/pull/253) ([github-actions[bot]](https://github.com/apps/github-actions))
- Fix \#224: implement `$mul` update operator [\#252](https://github.com/stargate/jsonapi/pull/252) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- add quickstart instructions [\#251](https://github.com/stargate/jsonapi/pull/251) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))

## [v1.0.0-ALPHA-4](https://github.com/stargate/jsonapi/tree/v1.0.0-ALPHA-4) (2023-03-10)

[Full Changelog](https://github.com/stargate/jsonapi/compare/v1.0.0-ALPHA-3...v1.0.0-ALPHA-4)

**Closed issues:**

- Push multi-architecture Docker images for JSON API [\#245](https://github.com/stargate/jsonapi/issues/245)
- Configure Quarkus to limit maximum HTTP request size \(`quarkus.http.limits.max-body-size`\) to 1 meg \(from default 10\) [\#242](https://github.com/stargate/jsonapi/issues/242)
- Enclose `String`-backed `DocumentId` values in quotes \(for Exception messages\) [\#240](https://github.com/stargate/jsonapi/issues/240)

**Merged pull requests:**

- Multi arch release action changes [\#250](https://github.com/stargate/jsonapi/pull/250) ([maheshrajamani](https://github.com/maheshrajamani))
- Multiarch add docker/setup-buildx-action@v2 [\#249](https://github.com/stargate/jsonapi/pull/249) ([maheshrajamani](https://github.com/maheshrajamani))
- Multiarch add setup-docker-buildx action [\#248](https://github.com/stargate/jsonapi/pull/248) ([maheshrajamani](https://github.com/maheshrajamani))
- Build and push multi architecture docker images. [\#247](https://github.com/stargate/jsonapi/pull/247) ([maheshrajamani](https://github.com/maheshrajamani))
- Bumping version for next jsonapi release [\#244](https://github.com/stargate/jsonapi/pull/244) ([github-actions[bot]](https://github.com/apps/github-actions))
- Configure Quarkus to limit maximum HTTP request size \(quarkus.http.limits.max-body-size\) to 1 meg  [\#243](https://github.com/stargate/jsonapi/pull/243) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Minor typo/wording fixes to network spec [\#239](https://github.com/stargate/jsonapi/pull/239) ([cowtowncoder](https://github.com/cowtowncoder))

## [v1.0.0-ALPHA-3](https://github.com/stargate/jsonapi/tree/v1.0.0-ALPHA-3) (2023-03-09)

[Full Changelog](https://github.com/stargate/jsonapi/compare/v1.0.0-ALPHA-2...v1.0.0-ALPHA-3)

**Closed issues:**

- Update - LWT transaction failure retries [\#228](https://github.com/stargate/jsonapi/issues/228)
- Implement $min and $max update operators [\#223](https://github.com/stargate/jsonapi/issues/223)
- Delete - LWT transaction failure retries [\#217](https://github.com/stargate/jsonapi/issues/217)
- Implement failure modes for update and delete operations [\#214](https://github.com/stargate/jsonapi/issues/214)
- updateOne and updateMany Response fields [\#168](https://github.com/stargate/jsonapi/issues/168)
- Enforce execution of field update operations is in alphabetic order [\#164](https://github.com/stargate/jsonapi/issues/164)
- SPEC - Document Multi Document Failure Considerations [\#160](https://github.com/stargate/jsonapi/issues/160)
- Allow insert of an empty document using insertOne [\#140](https://github.com/stargate/jsonapi/issues/140)
- Implement Array operators $size and $all [\#85](https://github.com/stargate/jsonapi/issues/85)

**Merged pull requests:**

- Fix \#240: surround String-valued DocumentId in single quotes [\#241](https://github.com/stargate/jsonapi/pull/241) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Failure modes for update and delete operations [\#237](https://github.com/stargate/jsonapi/pull/237) ([maheshrajamani](https://github.com/maheshrajamani))
- relates to \#178: update commands per spec, improved tests [\#236](https://github.com/stargate/jsonapi/pull/236) ([ivansenic](https://github.com/ivansenic))
- Create CODE\_OF\_CONDUCT.md [\#235](https://github.com/stargate/jsonapi/pull/235) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Create LICENSE [\#234](https://github.com/stargate/jsonapi/pull/234) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Implement $min and $max update operators [\#233](https://github.com/stargate/jsonapi/pull/233) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Update - LWT transaction failure retries [\#231](https://github.com/stargate/jsonapi/pull/231) ([maheshrajamani](https://github.com/maheshrajamani))
- adding readme to docs folder [\#230](https://github.com/stargate/jsonapi/pull/230) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Bumping version for next jsonapi release [\#226](https://github.com/stargate/jsonapi/pull/226) ([github-actions[bot]](https://github.com/apps/github-actions))
- Fix \#219: add $setOnInsert update operation [\#225](https://github.com/stargate/jsonapi/pull/225) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Delete - LWT failure retries [\#218](https://github.com/stargate/jsonapi/pull/218) ([maheshrajamani](https://github.com/maheshrajamani))
- relates to \#178: insert commands per spec, added oerdered/unordered, better tests [\#216](https://github.com/stargate/jsonapi/pull/216) ([ivansenic](https://github.com/ivansenic))
- Spec update for failure modes [\#197](https://github.com/stargate/jsonapi/pull/197) ([amorton](https://github.com/amorton))

## [v1.0.0-ALPHA-2](https://github.com/stargate/jsonapi/tree/v1.0.0-ALPHA-2) (2023-03-06)

[Full Changelog](https://github.com/stargate/jsonapi/compare/v1.0.0-ALPHA-1...v1.0.0-ALPHA-2)

**Closed issues:**

- Numeric Document ID stored as scientific notation in doc json [\#210](https://github.com/stargate/jsonapi/issues/210)
- Removed unused/unneeded `docProperties` / `doc\_properties` [\#204](https://github.com/stargate/jsonapi/issues/204)
- Upsert - Add all equals filter data to document [\#199](https://github.com/stargate/jsonapi/issues/199)
- Return deletedCount for deleteOne and deleteMany [\#194](https://github.com/stargate/jsonapi/issues/194)
- Detect change in field order for $set update operator with Object value [\#190](https://github.com/stargate/jsonapi/issues/190)
- Release workflow does not run [\#189](https://github.com/stargate/jsonapi/issues/189)
- Support nested path \("dot notation"\) for $push operator [\#185](https://github.com/stargate/jsonapi/issues/185)
- Support nested path \("dot notation"\) for $pop operator [\#184](https://github.com/stargate/jsonapi/issues/184)
- Support nested path \("dot notation"\) for $inc operator [\#183](https://github.com/stargate/jsonapi/issues/183)
- SPEC - document moreData behaviour for deleteMany and updateMany [\#169](https://github.com/stargate/jsonapi/issues/169)
- Support nested path \("dot notation"\) for $set operator [\#166](https://github.com/stargate/jsonapi/issues/166)
- Ensure \_id is always the first field in the document [\#126](https://github.com/stargate/jsonapi/issues/126)
- Consider using return value of `UpdateOperation.updateDocument\(\)` to calculate number of updated documents to return [\#84](https://github.com/stargate/jsonapi/issues/84)
- Support nested path \("dot notation"\) for update operators [\#74](https://github.com/stargate/jsonapi/issues/74)
- Implement array update operator `$addToSet` [\#73](https://github.com/stargate/jsonapi/issues/73)

**Merged pull requests:**

- Fix \#210 don't use engineering notation for `BigDecimal` \(as doc id\) [\#222](https://github.com/stargate/jsonapi/pull/222) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix \#164: order individual update operation actions as per spec [\#221](https://github.com/stargate/jsonapi/pull/221) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix \#190: use order-sensitive equality comparison for Object-value $set update [\#220](https://github.com/stargate/jsonapi/pull/220) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- add failure mode test for countDocuments [\#215](https://github.com/stargate/jsonapi/pull/215) ([ivansenic](https://github.com/ivansenic))
- Use Stargate v2.0.9 [\#213](https://github.com/stargate/jsonapi/pull/213) ([github-actions[bot]](https://github.com/apps/github-actions))
- adapt the update parent workflow [\#212](https://github.com/stargate/jsonapi/pull/212) ([ivansenic](https://github.com/ivansenic))
- fixing test resource defaults [\#211](https://github.com/stargate/jsonapi/pull/211) ([ivansenic](https://github.com/ivansenic))
- realtes to \#178: adapt delete commands per spec, improve tests [\#209](https://github.com/stargate/jsonapi/pull/209) ([ivansenic](https://github.com/ivansenic))
- Add `$addToSet` update operator [\#206](https://github.com/stargate/jsonapi/pull/206) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Fix \#204: Remove "doc\_properties" column, "docProperties" shredded doc field [\#205](https://github.com/stargate/jsonapi/pull/205) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Add support for dot-notation path for $push update operator [\#203](https://github.com/stargate/jsonapi/pull/203) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- relates to \#178: adapt countDocuments per spec, improve tests [\#202](https://github.com/stargate/jsonapi/pull/202) ([ivansenic](https://github.com/ivansenic))
- Add Equals DBFilters to newly created document [\#200](https://github.com/stargate/jsonapi/pull/200) ([maheshrajamani](https://github.com/maheshrajamani))
- fixes for docker compose [\#198](https://github.com/stargate/jsonapi/pull/198) ([ivansenic](https://github.com/ivansenic))
- Support dot-notation with $inc update operator [\#196](https://github.com/stargate/jsonapi/pull/196) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Return deletedCount for deleteOne and deleteMany [\#195](https://github.com/stargate/jsonapi/pull/195) ([maheshrajamani](https://github.com/maheshrajamani))
- Support dot notation with $pop update operation [\#193](https://github.com/stargate/jsonapi/pull/193) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Update Many command implementation [\#192](https://github.com/stargate/jsonapi/pull/192) ([maheshrajamani](https://github.com/maheshrajamani))
- Bumping version for next jsonapi release [\#191](https://github.com/stargate/jsonapi/pull/191) ([github-actions[bot]](https://github.com/apps/github-actions))

## [v1.0.0-ALPHA-1](https://github.com/stargate/jsonapi/tree/v1.0.0-ALPHA-1) (2023-02-24)

[Full Changelog](https://github.com/stargate/jsonapi/compare/5/merge...v1.0.0-ALPHA-1)

**Fixed bugs:**

- Unknown command throws `HTTP 400` instead of message [\#87](https://github.com/stargate/jsonapi/issues/87)

**Closed issues:**

- SPEC - confirm the general response format and what each command returns   [\#175](https://github.com/stargate/jsonapi/issues/175)
- Remove 'pageSize' from the options of find command [\#162](https://github.com/stargate/jsonapi/issues/162)
- Implement options for findOneAndUpdate and updateOne [\#161](https://github.com/stargate/jsonapi/issues/161)
- Implement limit for deleteMany [\#159](https://github.com/stargate/jsonapi/issues/159)
- Support nested path \("dot notation"\) for `$unset` operator [\#153](https://github.com/stargate/jsonapi/issues/153)
- Limit number of documents for deleteMany and updateMany commands [\#145](https://github.com/stargate/jsonapi/issues/145)
- Return an error if a document \_id exists on insertion  [\#143](https://github.com/stargate/jsonapi/issues/143)
- SPEC - Decide on rules for documents  [\#135](https://github.com/stargate/jsonapi/issues/135)
- SPEC - document supported path notations for filtering and projection [\#123](https://github.com/stargate/jsonapi/issues/123)
- Implement count command [\#116](https://github.com/stargate/jsonapi/issues/116)
- Define command options for all commands [\#108](https://github.com/stargate/jsonapi/issues/108)
- Change equality query to use `array\_contains` for atomics once shredder includes them [\#107](https://github.com/stargate/jsonapi/issues/107)
- Change shredding to add atomic fields into `array\_contains` as well, to support array-or-atomic `$eq` [\#106](https://github.com/stargate/jsonapi/issues/106)
- JSON API deleteCollection operation [\#105](https://github.com/stargate/jsonapi/issues/105)
- Support `$position`  modifier for `$push` Array update operation [\#104](https://github.com/stargate/jsonapi/issues/104)
- Define micrometer match patterns [\#102](https://github.com/stargate/jsonapi/issues/102)
- Implement $eq support for sub document [\#96](https://github.com/stargate/jsonapi/issues/96)
- Rename `database` to `namespace` [\#93](https://github.com/stargate/jsonapi/issues/93)
- Change path-name convention shredder uses to be same as needed by filtering \(no need to de-shred\) [\#91](https://github.com/stargate/jsonapi/issues/91)
- Implement $eq comparator for array type elements [\#89](https://github.com/stargate/jsonapi/issues/89)
- Support `$each` modifier for `$push` array update operation [\#80](https://github.com/stargate/jsonapi/issues/80)
- Implement array update operator `$pop` [\#77](https://github.com/stargate/jsonapi/issues/77)
- Implement array update operator `$push` [\#72](https://github.com/stargate/jsonapi/issues/72)
- Implement "$inc" update operator [\#71](https://github.com/stargate/jsonapi/issues/71)
- Implement $exists filter operator - Only true condition [\#70](https://github.com/stargate/jsonapi/issues/70)
- Update to JSON API [\#67](https://github.com/stargate/jsonapi/issues/67)
- Add test to ensure empty String not valid `DocumentId` \(and add validation if not yet done\) [\#61](https://github.com/stargate/jsonapi/issues/61)
- Change DB schema for typed Document Id \(`Tuple\<tinyint, Text\>`\) [\#58](https://github.com/stargate/jsonapi/issues/58)
- Add `DocumentId` abstraction to support typed `\_id`s \(not just Strings\) [\#57](https://github.com/stargate/jsonapi/issues/57)
- Handle json deserialization exception [\#56](https://github.com/stargate/jsonapi/issues/56)
- Add check for `\_id` for `$set`, `$unset` update operations [\#53](https://github.com/stargate/jsonapi/issues/53)
- Create UpdateOne command [\#51](https://github.com/stargate/jsonapi/issues/51)
- Options for different commands [\#48](https://github.com/stargate/jsonapi/issues/48)
- Create findOneAndUpdate command [\#45](https://github.com/stargate/jsonapi/issues/45)
- Base Integration Test class [\#44](https://github.com/stargate/jsonapi/issues/44)
- \[Filter clause deserializer\] Handle Bson type comparison query operators  [\#42](https://github.com/stargate/jsonapi/issues/42)
- Boolean index implementation [\#41](https://github.com/stargate/jsonapi/issues/41)
- Handle boolean expressions \(and, or, not\) for filters [\#33](https://github.com/stargate/jsonapi/issues/33)
- Remove `ValidatedStargateBridge` and use from common [\#30](https://github.com/stargate/jsonapi/issues/30)
- Implement Delete One command [\#29](https://github.com/stargate/jsonapi/issues/29)
- Add \_id field to the document [\#28](https://github.com/stargate/jsonapi/issues/28)
- Injecting StargateBridge directly not revolving the metadata [\#22](https://github.com/stargate/jsonapi/issues/22)
- Introduce the framework based exception and mapper [\#9](https://github.com/stargate/jsonapi/issues/9)
- Ignore SG v2 exception mappers [\#8](https://github.com/stargate/jsonapi/issues/8)

**Merged pull requests:**

- release workflow fixes [\#188](https://github.com/stargate/jsonapi/pull/188) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- first alpha version [\#186](https://github.com/stargate/jsonapi/pull/186) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- DeleteMany command implementation [\#182](https://github.com/stargate/jsonapi/pull/182) ([maheshrajamani](https://github.com/maheshrajamani))
- Remove 'pageSize' from the options of find command [\#181](https://github.com/stargate/jsonapi/pull/181) ([maheshrajamani](https://github.com/maheshrajamani))
- run CI on pull request only on code-related changes [\#180](https://github.com/stargate/jsonapi/pull/180) ([ivansenic](https://github.com/ivansenic))
- workflow for updating stargate version [\#179](https://github.com/stargate/jsonapi/pull/179) ([ivansenic](https://github.com/ivansenic))
- Added Request & Response to API Spec [\#177](https://github.com/stargate/jsonapi/pull/177) ([amorton](https://github.com/amorton))
- Support dot notation for $set [\#176](https://github.com/stargate/jsonapi/pull/176) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Spec changes for limits, options, field names and paths [\#170](https://github.com/stargate/jsonapi/pull/170) ([amorton](https://github.com/amorton))
- Changes for FindOneAndUpdateCommand and UpdateOneCommand options support [\#163](https://github.com/stargate/jsonapi/pull/163) ([maheshrajamani](https://github.com/maheshrajamani))
- fix schema in the collection commands [\#157](https://github.com/stargate/jsonapi/pull/157) ([ivansenic](https://github.com/ivansenic))
- re-org to match features matrix spreadsheet [\#155](https://github.com/stargate/jsonapi/pull/155) ([amorton](https://github.com/amorton))
- Support dot notation with `$unset` [\#154](https://github.com/stargate/jsonapi/pull/154) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Return an error if a document \_id exists on insertion  [\#152](https://github.com/stargate/jsonapi/pull/152) ([maheshrajamani](https://github.com/maheshrajamani))
- Add support for $position modifier of $push update operator [\#151](https://github.com/stargate/jsonapi/pull/151) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Change for equality query to use array\_contains for atomics [\#150](https://github.com/stargate/jsonapi/pull/150) ([maheshrajamani](https://github.com/maheshrajamani))
- closes \#105: delete collection command implementation [\#148](https://github.com/stargate/jsonapi/pull/148) ([ivansenic](https://github.com/ivansenic))
- adding pull request template [\#121](https://github.com/stargate/jsonapi/pull/121) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Fix \#106: shred atomic values into "array\_contains" too [\#120](https://github.com/stargate/jsonapi/pull/120) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- closes \#56: remove WebApplicationException from the error responses [\#119](https://github.com/stargate/jsonapi/pull/119) ([ivansenic](https://github.com/ivansenic))
- adding nosqlbench test scripts [\#118](https://github.com/stargate/jsonapi/pull/118) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Count command implementation [\#117](https://github.com/stargate/jsonapi/pull/117) ([maheshrajamani](https://github.com/maheshrajamani))
- Make DBFilterBase as separate class [\#115](https://github.com/stargate/jsonapi/pull/115) ([maheshrajamani](https://github.com/maheshrajamani))
- closes \#87: handle unknown command [\#114](https://github.com/stargate/jsonapi/pull/114) ([ivansenic](https://github.com/ivansenic))
- closes \#102: protect against metric tag explosion on unauthorized access [\#113](https://github.com/stargate/jsonapi/pull/113) ([ivansenic](https://github.com/ivansenic))
- $eq test case for unmatched sub doc order query [\#103](https://github.com/stargate/jsonapi/pull/103) ([maheshrajamani](https://github.com/maheshrajamani))
- Change terminology from "database" to "namespace" [\#101](https://github.com/stargate/jsonapi/pull/101) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Implement $pop update operator [\#100](https://github.com/stargate/jsonapi/pull/100) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Add support for `$each` modifier of `$push` update operator [\#99](https://github.com/stargate/jsonapi/pull/99) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Implement $eq support for sub document [\#98](https://github.com/stargate/jsonapi/pull/98) ([maheshrajamani](https://github.com/maheshrajamani))
- closes \#30: update stargate to v2.0.8 [\#95](https://github.com/stargate/jsonapi/pull/95) ([ivansenic](https://github.com/ivansenic))
- Fix \#91: remove escaping/decorating of dotted path Shredder produces [\#92](https://github.com/stargate/jsonapi/pull/92) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Changes for $eq support for array data type [\#90](https://github.com/stargate/jsonapi/pull/90) ([maheshrajamani](https://github.com/maheshrajamani))
- C2-2432: implement create namespace \(database\) command [\#88](https://github.com/stargate/jsonapi/pull/88) ([ivansenic](https://github.com/ivansenic))
- Changes for all and size operator [\#86](https://github.com/stargate/jsonapi/pull/86) ([maheshrajamani](https://github.com/maheshrajamani))
- Fix \#71: implement `$inc` update operator [\#83](https://github.com/stargate/jsonapi/pull/83) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Last part of \#72 \(validation of modifiers wrt `$push`\) [\#82](https://github.com/stargate/jsonapi/pull/82) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Exists filter operator [\#81](https://github.com/stargate/jsonapi/pull/81) ([maheshrajamani](https://github.com/maheshrajamani))
- Add "$push" update operator, tests [\#79](https://github.com/stargate/jsonapi/pull/79) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- additional renaming [\#78](https://github.com/stargate/jsonapi/pull/78) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Create release workflow with pushing docker images to ECR [\#76](https://github.com/stargate/jsonapi/pull/76) ([ivansenic](https://github.com/ivansenic))
- finalize docker image build and publish on commit [\#75](https://github.com/stargate/jsonapi/pull/75) ([ivansenic](https://github.com/ivansenic))
- rename to JSON API [\#69](https://github.com/stargate/jsonapi/pull/69) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- \_id field to support String, Number, Boolean and Null Json type [\#66](https://github.com/stargate/jsonapi/pull/66) ([maheshrajamani](https://github.com/maheshrajamani))
- move to stargate/core as code owners [\#65](https://github.com/stargate/jsonapi/pull/65) ([ivansenic](https://github.com/ivansenic))
- relates to C2-2413: create and publish docker image on the main push [\#64](https://github.com/stargate/jsonapi/pull/64) ([ivansenic](https://github.com/ivansenic))
- Fix \#61: Add validation that '\_id' can't be empty String, test to verify [\#63](https://github.com/stargate/jsonapi/pull/63) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- terminology update: database -\> namespace [\#62](https://github.com/stargate/jsonapi/pull/62) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Support empty or null options for commands [\#60](https://github.com/stargate/jsonapi/pull/60) ([maheshrajamani](https://github.com/maheshrajamani))
- Add `DocumentId` abstraction, convert to String before storing in DB [\#59](https://github.com/stargate/jsonapi/pull/59) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Changes to support $eq comparison operator. [\#55](https://github.com/stargate/jsonapi/pull/55) ([maheshrajamani](https://github.com/maheshrajamani))
- WIP: Fix \#53: Add validation for `$set`, `$unset` Update operations not to allow changing `\_id` [\#54](https://github.com/stargate/jsonapi/pull/54) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- accidentally put spec in wrong directory [\#52](https://github.com/stargate/jsonapi/pull/52) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- UpdateOneCommand implementation. [\#50](https://github.com/stargate/jsonapi/pull/50) ([maheshrajamani](https://github.com/maheshrajamani))
- Add docker compose configuration [\#49](https://github.com/stargate/jsonapi/pull/49) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- moving api spec to new repo [\#47](https://github.com/stargate/jsonapi/pull/47) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Find one and update [\#46](https://github.com/stargate/jsonapi/pull/46) ([maheshrajamani](https://github.com/maheshrajamani))
- Boolean index changes \# [\#43](https://github.com/stargate/jsonapi/pull/43) ([maheshrajamani](https://github.com/maheshrajamani))
- Removed the Optional handling where its not needed [\#40](https://github.com/stargate/jsonapi/pull/40) ([maheshrajamani](https://github.com/maheshrajamani))
- avoid saving docker images on failed runs [\#39](https://github.com/stargate/jsonapi/pull/39) ([jeffreyscarpenter](https://github.com/jeffreyscarpenter))
- Changes for insert many [\#38](https://github.com/stargate/jsonapi/pull/38) ([maheshrajamani](https://github.com/maheshrajamani))
- Find command implementation [\#37](https://github.com/stargate/jsonapi/pull/37) ([maheshrajamani](https://github.com/maheshrajamani))
- DeleteOne command \#29 [\#35](https://github.com/stargate/jsonapi/pull/35) ([maheshrajamani](https://github.com/maheshrajamani))
- Generate and add \_id field in Doc if not passed [\#32](https://github.com/stargate/jsonapi/pull/32) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- update stargate to 2.0.6 [\#31](https://github.com/stargate/jsonapi/pull/31) ([ivansenic](https://github.com/ivansenic))
- Changes for findOne command [\#27](https://github.com/stargate/jsonapi/pull/27) ([maheshrajamani](https://github.com/maheshrajamani))
- Change storage of "shredded" doc to use `doc\_json` field [\#25](https://github.com/stargate/jsonapi/pull/25) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Added the FilterClause [\#23](https://github.com/stargate/jsonapi/pull/23) ([maheshrajamani](https://github.com/maheshrajamani))
- I don't think we need Tuples package at this point \(after removing use in `docAtomicFields`\) [\#21](https://github.com/stargate/jsonapi/pull/21) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- Create and insert review \(Fixes based on review comments on PR16\)  [\#20](https://github.com/stargate/jsonapi/pull/20) ([maheshrajamani](https://github.com/maheshrajamani))
- closes \#8: ignore sgv2 exception mappers, send HTTP 200 on errors  [\#18](https://github.com/stargate/jsonapi/pull/18) ([ivansenic](https://github.com/ivansenic))
- update to stargate 2.0.4, DSE props generic [\#17](https://github.com/stargate/jsonapi/pull/17) ([ivansenic](https://github.com/ivansenic))
- Added Insert One and Create Collection commands [\#16](https://github.com/stargate/jsonapi/pull/16) ([maheshrajamani](https://github.com/maheshrajamani))
- 2 minor javadoc fixes for links \(remove warnings\) [\#15](https://github.com/stargate/jsonapi/pull/15) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- closes \#9: add engine exception, add handlers [\#13](https://github.com/stargate/jsonapi/pull/13) ([ivansenic](https://github.com/ivansenic))
- Initial incomplete implementation of shredding \(doc-\>shredded\) [\#12](https://github.com/stargate/jsonapi/pull/12) ([tatu-at-datastax](https://github.com/tatu-at-datastax))
- use stargate v2.0.3 [\#11](https://github.com/stargate/jsonapi/pull/11) ([ivansenic](https://github.com/ivansenic))
- relates to C2-2197: modeling command resolving and processing [\#10](https://github.com/stargate/jsonapi/pull/10) ([ivansenic](https://github.com/ivansenic))
- relates to C2-2197: setup the command serialization with tests [\#7](https://github.com/stargate/jsonapi/pull/7) ([ivansenic](https://github.com/ivansenic))
- Helm chart for Docs API v3 [\#3](https://github.com/stargate/jsonapi/pull/3) ([versaurabh](https://github.com/versaurabh))

## [5/merge](https://github.com/stargate/jsonapi/tree/5/merge) (2022-11-24)

[Full Changelog](https://github.com/stargate/jsonapi/compare/1df47e590b37b06f00787527300236067a24ef0d...5/merge)

**Merged pull requests:**

- update stargate to v2.0.1 [\#2](https://github.com/stargate/jsonapi/pull/2) ([ivansenic](https://github.com/ivansenic))
- bootstrapping the project based on v2 APIs [\#1](https://github.com/stargate/jsonapi/pull/1) ([ivansenic](https://github.com/ivansenic))

