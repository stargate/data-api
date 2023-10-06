[Full Changelog](https://github.com/stargate/jsonapi/compare/v1.0.0-ALPHA-13...HEAD)

**Closed issues:**

- Reduce index columns [\#505](https://github.com/stargate/jsonapi/issues/505)

## [v1.0.0-ALPHA-13](https://github.com/stargate/jsonapi/tree/v1.0.0-ALPHA-13) (2023-08-16)

[Full Changelog](https://github.com/stargate/jsonapi/compare/v1.0.0-ALPHA-12...v1.0.0-ALPHA-13)

**Closed issues:**

- Create Collection with vector options fail [\#504](https://github.com/stargate/jsonapi/issues/504)

**Merged pull requests:**

- Merge array\_equals and sub\_doc\_equals field into query\_text\_value [\#506](https://github.com/stargate/jsonapi/pull/506) ([maheshrajamani](https://github.com/maheshrajamani))
- Bumping version for next jsonapi release [\#501](https://github.com/stargate/jsonapi/pull/501) ([github-actions[bot]](https://github.com/apps/github-actions))

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

