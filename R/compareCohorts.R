#' CompareCohorts
#'
#' This function resolves concept sets of two cohorts run on different vocabulary versions and writes the result to the Excel file
#'
#' @description This function resolves concept sets of two cohorts run on different vocabulary versions and writes the result to the Excel file
#' it uses an input of \code{getNodeConcepts()} funcion,
#' it detects
#' for the updated cohort definition run on a new vocabulary
#' 1) non-standard concepts used in concept set expression;
#' for the old cohort definition run on an old vocabulary vs updated cohort definition run on a new vocabulary:
#' 2) added or excluded source concepts due to changed mapping to standard concepts
#' 3) domain changes of included standard concepts
#' The result is written to an excel file with the tab for each check
#'
#'
#' @param connectionDetailsVocab An R object of type\cr\code{connectionDetails} created using the
#'                                     function \code{createConnectionDetails} in the
#'                                     \code{DatabaseConnector} package.
#' @param Concepts_in_cohortSetOldCht old cohort definition run on an old vocabulary -
#'                                    dataframe which stores cohorts and concept set definitions in a tabular format,
#'                                    it should have the following columns:
#'                                    "ConceptID","isExcluded","includeDescendants","conceptsetId","conceptsetName","cohortId"
#' @param Concepts_in_cohortSetNewCht updated cohort definition run on a new vocabulary -
#'                                    dataframe which stores cohorts and concept set definitions in a tabular format,
#'                                    it should have the following columns:
#'                                    "ConceptID","isExcluded","includeDescendants","conceptsetId","conceptsetName","cohortId"
#' @param newVocabSchema        schema containing a new vocabulary version
#' @param oldVocabSchema        schema containing an older vocabulary version
#' @param resultSchema          (optional) schema containing Achilles \code{achilles_result_cc} table;
#'                              provides concept usage counts in the output.
#'                              Set to \code{NULL} to run without usage counts (default: \code{NULL})
#' @param excludedNodes         text string with excluded nodes, for example: "9201, 9202, 9203"; 0 by default
#' @param includedSourceVocabs  text string with included source vocabularies, for example: "'ICD10CM', 'ICD9CM', 'HCPCS'"; 0 by default, which is treated as ALL vocabularies
#' @param phenotypeUpdates      dataframe with columns \code{old_cohort_id} and \code{new_cohort_id} mapping old cohorts to their updated versions
#' @param scratchSchema         used to store temp tables in Databricks
#' @param projName              project name - used to name the output file
#' @param outputFolder          path to the folder where the Excel file will be saved; created if it does not exist (default: \code{"results"})
#'
#' @examples
#' \dontrun{
#'                              CompareCohorts(connectionDetailsVocab = connectionDetailsVocab,
#'                                            cohorts = cohorts,
#'                                            Concepts_in_cohortSetOldCht = Concepts_in_cohortSetOldCht,
#'                                            Concepts_in_cohortSetNewCht = Concepts_in_cohortSetNewCht,
#'                                            newVocabSchema = newVocabSchema,
#'                                            oldVocabSchema = oldVocabSchema,
#'                                            excludedNodes = excludedVisitNodes,
#'                                            resultSchema = resultSchema)
#'
#' }
#' @export


CompareCohorts <-function( connectionDetailsVocab,
                           cohorts,
                           Concepts_in_cohortSetOldCht,
                           Concepts_in_cohortSetNewCht,
                           phenotypeUpdates,
                          newVocabSchema,
                          oldVocabSchema,
                          resultSchema = NULL,
                          excludedNodes = NULL,
                          includedSourceVocabs = 0,
                          scratchSchema,
                          projName = '',
                          outputFolder = "results"
                          )
{
  #use databaseConnector to run SQL and extract tables into data frames

  options(sqlRenderTempEmulationSchema = scratchSchema)

  #connect to the vocabulary server
  conn <- DatabaseConnector::connect(connectionDetailsVocab)


  #insert Concepts_in_cohortSet into the SQL database where concepts sets will be resolved
  DatabaseConnector::insertTable(connection = conn,
                                 tableName = "#ConceptsInCohortSetOld",
                                 data = Concepts_in_cohortSetOldCht,
                                 dropTableIfExists = TRUE,
                                 createTable = TRUE,
                                 tempTable = T,
                                 bulkLoad = F)
  DatabaseConnector::insertTable(connection = conn,
                                 tableName = "#ConceptsInCohortSetnew",
                                 data = Concepts_in_cohortSetNewCht,
                                 dropTableIfExists = TRUE,
                                 createTable = TRUE,
                                 tempTable = T,
                                 bulkLoad = F)
  DatabaseConnector::insertTable(connection = conn,
                                 tableName = "#cohorts",
                                 data = cohorts,
                                 dropTableIfExists = TRUE,
                                 createTable = TRUE,
                                 tempTable = T,
                                 bulkLoad = F)


  # read SQL from file
 pathToSql <- system.file("sql/sql_server", "AllFromNodes_updated_cohorts.sql", package = "Alathea")


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

  #aggregate the target concepts into one row so we can compare old and new mapping, newMap
  newMapAgg <-
    newMap %>%
    arrange(CONCEPT_ID) %>%
    group_by(COHORTID, CONCEPTSETNAME, CONCEPTSETID, SOURCE_CONCEPT_ID, ACTION) %>%
    summarise(
      NEW_MAPPED_CONCEPT_ID = paste(CONCEPT_ID, collapse = '-'),
      NEW_MAPPED_CONCEPT_NAME = paste(CONCEPT_NAME, collapse = '-'),
      NEW_MAPPED_VOCABULARY_ID = paste(VOCABULARY_ID, collapse = '-'),
      NEW_MAPPED_CONCEPT_CODE = paste(CONCEPT_CODE, collapse = '-')
    )

  #aggregate the target concepts into one row so we can compare old and new mapping, oldMap
  oldMapAgg <-
    oldMap %>%
    arrange(CONCEPT_ID) %>%
    group_by(COHORTID, CONCEPTSETNAME, CONCEPTSETID, SOURCE_CONCEPT_ID, RECORD_COUNT, ACTION,
             SOURCE_CONCEPT_NAME, SOURCE_VOCABULARY_ID, SOURCE_CONCEPT_CODE
    ) %>%
    summarise(
      OLD_MAPPED_CONCEPT_ID = paste(CONCEPT_ID, collapse = '-'),
      OLD_MAPPED_CONCEPT_NAME = paste(CONCEPT_NAME, collapse = '-'),
      OLD_MAPPED_VOCABULARY_ID = paste(VOCABULARY_ID, collapse = '-'),
      OLD_MAPPED_CONCEPT_CODE = paste(CONCEPT_CODE, collapse = '-')
    )


  #join oldMap and newMap where combination of target concepts are different
  mapDif <- oldMapAgg %>%
    inner_join(newMapAgg, by = c("COHORTID", "CONCEPTSETNAME", "CONCEPTSETID", "SOURCE_CONCEPT_ID", "ACTION")) %>%
    filter(if_else(is.na(OLD_MAPPED_CONCEPT_ID), '', OLD_MAPPED_CONCEPT_ID) != if_else(is.na(NEW_MAPPED_CONCEPT_ID), '', NEW_MAPPED_CONCEPT_ID))%>%
    arrange(desc(RECORD_COUNT))

  #append new cohort ids for the easier review
  mapDif <- mapDif %>%
    inner_join(phenotypeUpdates, by = c("COHORTID" = "old_cohort_id"))

  #get the non-standard concepts used in concept set definitions
  nonStNodes <- DatabaseConnector::renderTranslateQuerySql(connection = conn,
                                                           "select * from #non_st_Nodes
order by drc desc", snakeCaseToCamelCase = T)

  #get the standard concepts changed domains and their mapped counterparts
  domainChange <-DatabaseConnector::renderTranslateQuerySql(connection = conn,
                                                            "select * from
                                           #resolv_dom_dif order by source_concept_record_count desc",  snakeCaseToCamelCase = T)
  domainChange <- domainChange %>%
    inner_join(phenotypeUpdates, by = c("cohortid" = "old_cohort_id"))


  #disconnect
  DatabaseConnector::disconnect(conn)

  # put the results in excel, each dataframe goes to a separate tab
  wb <- createWorkbook()

  addWorksheet(wb, "nonStNodes")
  writeData(wb, "nonStNodes", nonStNodes)

  addWorksheet(wb, "mapDif")
  writeData(wb, "mapDif", mapDif)

  addWorksheet(wb, "domainChange")
  writeData(wb, "domainChange", domainChange)

  if (!dir.exists(outputFolder)) dir.create(outputFolder, recursive = TRUE)
  outputPath <- file.path(outputFolder, paste0(projName, "CohortDif.xlsx"))
  saveWorkbook(wb, outputPath, overwrite = TRUE)
}
