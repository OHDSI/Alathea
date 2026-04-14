#' This function resolves concept sets in a SQL database and writes the result to the Excel file
#'
#' @description This function resolves concept sets in a SQL database
#' it uses an input of \code{getNodeConcepts()} funcion,
#' it detects
#' 1) non-standard concepts used in concept set expression;
#' 2) added or excluded source concepts due to changed mapping to standard concepts
#' 3) domain changes of included standard concepts
#' The result is written to an excel file with the tab for each check
#'
#'
#' @param connectionDetailsVocab An R object of type\cr\code{connectionDetails} created using the
#'                                     function \code{createConnectionDetails} in the
#'                                     \code{DatabaseConnector} package.
#' @param Concepts_in_cohortSet dataframe which stores cohorts and concept set definitions in a tabular format,
#'                              it should have the following columns:
#'                              "ConceptID","isExcluded","includeDescendants","conceptsetId","conceptsetName","cohortId"
#' @param newVocabSchema        schema containing a new vocabulary version
#' @param oldVocabSchema        schema containing an older vocabulary version
#' @param resultSchema          (optional) schema containing Achilles \code{achilles_result_concept_count} table;
#'                              provides concept usage counts in the output.
#'                              Set to \code{NULL} to run without usage counts (default: \code{NULL})
#' @param excludedNodes         text string with excluded nodes, for example: "9201, 9202, 9203"; 0 by default
#' @param includedSourceVocabs  text string with included source vocabularies, for example: "'ICD10CM', 'ICD9CM', 'HCPCS'"; 0 by default, which is treated as ALL vocabularies
#' @param projName              project name - used to name the output file
#' @param scratchSchema         used to store temp tables in Databricks
#' @param cdmSchema             (optional) CDM schema used for the stats tab; requires patient-level data access.
#'                              Set to \code{NULL} to skip the stats tab (default: \code{NULL})
#' @param outputFolder          path to the folder where the Excel file will be saved; created if it does not exist (default: \code{"results"})
#' \dontrun{
#'  resultToExcel(connectionDetails = YourconnectionDetails,
#'  Concepts_in_cohortSet = Concepts_in_cohortSet, # is returned by getNodeConcepts function
#'  newVocabSchema = "omopVocab_v1", #schema containing newer vocabulary version
#'  oldVocabSchema = "omopVocab_v0", #schema containing older vocabulary version
#'  resultSchema = "achillesresults") #schema with achillesresults
#' }
#' @export



resultToExcel <-function( connectionDetailsVocab,
                          Concepts_in_cohortSet,
                          newVocabSchema,
                          oldVocabSchema,
                          resultSchema = NULL,
                          excludedNodes = NULL,
                          includedSourceVocabs =0,
                          projName  = '',
                          scratchSchema,
                          cdmSchema = NULL,
                          outputFolder = "results"
                          )
{
  #use databaseConnector to run SQL and extract tables into data frames

  options(sqlRenderTempEmulationSchema = scratchSchema)

  #connect to the vocabulary server
  conn <- DatabaseConnector::connect(connectionDetailsVocab)


  #insert Concepts_in_cohortSet into the SQL database where concepts sets will be resolved
  DatabaseConnector::insertTable(connection = conn,
                                 tableName = "#ConceptsInCohortSet",
                                 data = Concepts_in_cohortSet,
                                 dropTableIfExists = TRUE,
                                 createTable = TRUE,
                                 tempTable = T,
                                 bulkLoad = F)

  #get vocabulary versions from both schemas
  vocabVersions <- DatabaseConnector::renderTranslateQuerySql(
    connection = conn,
    "select 'old' as vocabulary_type, vocabulary_version from @oldVocabSchema.vocabulary where vocabulary_id = 'None'
     union all
     select 'new' as vocabulary_type, vocabulary_version from @newVocabSchema.vocabulary where vocabulary_id = 'None'",
    oldVocabSchema = oldVocabSchema,
    newVocabSchema = newVocabSchema,
    snakeCaseToCamelCase = FALSE
  )


  # read SQL from file
  pathToSql <- system.file("sql/sql_server", "AllFromNodes.sql", package = "Alathea")
  InitSql <- read_file(pathToSql)


  #run the SQL creating all tables needed for the output
  DatabaseConnector::renderTranslateExecuteSql (connection = conn,
                                                InitSql,
                                                newVocabSchema = newVocabSchema,
                                                oldVocabSchema = oldVocabSchema,
                                                hasAchilles = !is.null(resultSchema),
                                                resultSchema = if (!is.null(resultSchema)) resultSchema else "na",
                                                excludedNodes = if (!is.null(excludedNodes)) excludedNodes else 0,
                                                includedSourceVocabs = includedSourceVocabs
  )

  #get SQL tables into dataframes

  #comparison on source codes can't be done on SQL, since the SQL render used in DatabaseConnector::renderTranslateQuerySql doesn't support STRING_AGG function
  # so this is done in R
  #source concepts resolved and their mapping in the old vocabulary
  oldMap <- DatabaseConnector::renderTranslateQuerySql(connection = conn,
                                                       "select * from #oldmap", snakeCaseToCamelCase = F)

  #source concepts resolved and their mapping in the new vocabulary
  newMap <- DatabaseConnector::renderTranslateQuerySql(connection = conn,
                                                       "select * from #newmap", snakeCaseToCamelCase = F)

  # aggregate the target concepts into one row so we can compare old and new mapping, newMap

  newMapAgg <-

    newMap %>%
    arrange(concept_id) %>%
    group_by(cohortid, cohortName, conceptsetname, conceptsetid, source_concept_id, action) %>%
    summarise(
      new_mapped_concept_id     = paste(concept_id, collapse = "-"),
      new_mapped_concept_name   = paste(concept_name, collapse = "-"),
      new_mapped_vocabulary_id = paste(vocabulary_id, collapse = "-"),
      new_mapped_concept_code  = paste(concept_code, collapse = "-"),
      .groups = "drop"
    )


  # aggregate the target concepts into one row so we can compare old and new mapping, oldMap

  oldMapAgg <-

    oldMap %>%
    arrange(concept_id) %>%
    group_by(
      cohortid, cohortName,
      conceptsetname, conceptsetid,
      source_concept_id, record_count, action,
      source_concept_name, source_vocabulary_id, source_concept_code
    ) %>%
    summarise(
      old_mapped_concept_id     = paste(concept_id, collapse = "-"),
      old_mapped_concept_name   = paste(concept_name, collapse = "-"),
      old_mapped_vocabulary_id = paste(vocabulary_id, collapse = "-"),
      old_mapped_concept_code  = paste(concept_code, collapse = "-"),
      .groups = "drop"
    )


  # join oldMap and newMap to see the mappings of added or removed source concepts

  mapDif <- oldMapAgg %>%
    inner_join(
      newMapAgg,
      by = c(
        "cohortid",
        "cohortName",
        "conceptsetname",
        "conceptsetid",
        "source_concept_id",
        "action"
      )
    ) %>%
    arrange(desc(record_count))

  #get the non-standard concepts used in concept set definitions
  nonStNodes <- DatabaseConnector::renderTranslateQuerySql(connection = conn,
                                                           "select * from #non_st_Nodes
order by drc desc", snakeCaseToCamelCase = T)


  #get the standard concepts changed domains and their mapped counterparts
  domainChange <-DatabaseConnector::renderTranslateQuerySql(connection = conn,
                                                            "select * from
                                           #resolv_dom_dif order by source_concept_record_count desc",  snakeCaseToCamelCase = T)

  #get stats (optional - requires cdmSchema with patient-level data)
  #############
  if (!is.null(cdmSchema)) {
    pathToSqlStats <- system.file("sql/sql_server", "get_stats.sql", package = "Alathea")
    StatsSql <- read_file(pathToSqlStats)

    #run the SQL getting the statistics
    DatabaseConnector::renderTranslateExecuteSql(connection = conn,
                                                 StatsSql,
                                                 newVocabSchema = newVocabSchema,
                                                 oldVocabSchema = oldVocabSchema,
                                                 includedSourceVocabs = includedSourceVocabs,
                                                 cdmSchema = cdmSchema,
                                                 scratchSchema = scratchSchema
    )

    stats <- DatabaseConnector::renderTranslateQuerySql(connection = conn,
                                                        "select * from @scratchSchema.stats ORDER BY same_persons_no_change * 1.0 / total_persons",
                                                        scratchSchema = scratchSchema,
                                                        snakeCaseToCamelCase = T)
  }


  #drop temp tables (which are physical tables in databricks, so need to be deleted)
  DatabaseConnector::renderTranslateExecuteSql (connection = conn,
                                                "drop table #conceptsincohortset;
drop table #new_vc;
drop table #newmap;
drop table #non_st_nodes;
drop table #old_vc;
drop table #oldmap;
drop table #resolv_dif_sc;
drop table #resolv_dom_dif;
DROP TABLE IF EXISTS @scratchSchema.concepts_in_cohorts;
DROP TABLE IF EXISTS @scratchSchema.concept_diff;
DROP TABLE IF EXISTS @scratchSchema.concept_count_change;
DROP TABLE IF EXISTS @scratchSchema.cohort_vocab_change_summary;
DROP TABLE IF EXISTS @scratchSchema.stats;",
             scratchSchema = scratchSchema
  )

  #disconnect
  DatabaseConnector::disconnect(conn)

  # put the results in excel, each dataframe goes to a separate tab
  wb <- createWorkbook()

  addWorksheet(wb, "vocabularyVersions")
  writeData(wb, "vocabularyVersions", vocabVersions)

  addWorksheet(wb, "nonStNodes")
  writeData(wb, "nonStNodes", nonStNodes)

  addWorksheet(wb, "mapDif")
  writeData(wb, "mapDif", mapDif)

  addWorksheet(wb, "domainChange")
  writeData(wb, "domainChange", domainChange)

  if (!is.null(cdmSchema)) {
    addWorksheet(wb, "stats")
    writeData(wb, "stats", stats)
  }

  if (!dir.exists(outputFolder)) dir.create(outputFolder, recursive = TRUE)
  outputPath <- file.path(outputFolder, paste0(projName, "PhenChange.xlsx"))
  saveWorkbook(wb, outputPath, overwrite = TRUE)
}
