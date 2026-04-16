######################################
## Alathea code to run ##
######################################

# install libraries, if not installed
#remotes::install_github("OHDSI/Alathea")
#remotes::install_github("OHDSI/DatabaseConnector")

library (dplyr)
library (openxlsx)
library (readr)
library (tibble)
library (DatabaseConnector)
library (Alathea)

#set the BaseUrl of your Atlas instance
baseUrl <- "https://epi.jnj.com:8443/WebAPI/"

# if security is enabled authorize use of the webapi
ROhdsiWebApi::authorizeWebApi(
  baseUrl = baseUrl,
  authMethod = "windows")


# specify cohorts you want to run the comparison for
Cohorts <- read_delim("Cohorts2026.csv", delim = ",",
                      escape_double = FALSE, trim_ws = TRUE)
cohorts <-c( Cohorts$cohortId)


#set name which will be used for the output
projName='Feb2026test'

#excluded nodes is a text string with nodes you want to exclude from the analysis, it's set to 0 by default
# for example now some CPT4 and HCPCS are mapped to Visit concepts and we didn't implement this in the ETL,
#so we don't want these in the analysis (note, the tool doesn't look at the actual CDM, but on the mappings in the vocabulary)
#this way, the excludedNodes are defined in this way:
excludedVisitNodes <- "9202, 2514435,9203,2514436,2514437,2514434,2514433,9201"

#you can restrict the output by using specific source vocabularies (only those that exist in your data as source concepts)
includedSourceVocabs <- "'ICD10', 'ICD10CM', 'CPT4', 'HCPCS', 'NDC', 'ICD9CM', 'ICD9Proc', 'ICD10PCS', 'ICDO3', 'JMDC', 'LOINC'"

#set connectionDetails,
#you can use keyring to store your credentials,
#see how to configure keyring to use with the example below in ~/Alathea/extras/KeyringSetup.R

# you can also define connectionDetails directly, see the DatabaseConnector documentation https://ohdsi.github.io/DatabaseConnector/

connectionDetails = DatabaseConnector::createConnectionDetails(
  dbms = "spark",
  connectionString = keyring::key_get("databricks", "connection_string"),
  user = "token",
  password = keyring::key_get("databricks", "token")
)

#specify schemas with vocabulary versions you want to compare
 oldVocabSchema<-'vocabulary.v20250827'
 newVocabSchema <-'vocabulary.v20260227'


# (optional) schema containing Achilles achilles_result_concept_count table - adds concept usage counts to output
# to generate the table, see:
# https://github.com/OHDSI/WebAPI/blob/master/src/main/resources/ddl/achilles/achilles_result_concept_count.sql

# set to NULL to run without usage counts
resultSchema <-'vocabulary.jnj_network'
scratchSchema <-'scratch.scratch_ddymshyt'


# (optional) CDM schema for the stats tab - requires access to patient-level data
# set to NULL to skip the stats tab
cdmSchema <-'healthverity_cc.cdm_healthverity_cc_v3616'

#create the dataframe with concept set expressions using the getNodeConcepts function
Concepts_in_cohortSet<-getNodeConcepts(cohorts, baseUrl)

#resolve concept sets, compare the outputs on different vocabulary versions, write results to the Excel file
#for Redshift ask your administrator for a key for bulk load, since the function uploads the data to the database
resultToExcel(connectionDetailsVocab = connectionDetails,
              Concepts_in_cohortSet = Concepts_in_cohortSet,
              newVocabSchema = newVocabSchema,
              oldVocabSchema = oldVocabSchema,
              excludedNodes = excludedVisitNodes,
              resultSchema = resultSchema,
              scratchSchema= scratchSchema,
              includedSourceVocabs = includedSourceVocabs,
              projName = projName,
              cdmSchema = cdmSchema,
              outputFolder = "results"
)
  
#open the excel file
#Windows
shell.exec(normalizePath(file.path("results", paste0(projName, "PhenChange.xlsx"))))

#MacOS
#system(paste("open", normalizePath(file.path("results", paste0(projName, "PhenChange.xlsx")))))
