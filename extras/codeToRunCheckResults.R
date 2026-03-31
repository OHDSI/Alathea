######################################
## Alathea code to run ##
######################################

# install libraries, if not installed
#remotes::install_github("OHDSI/DatabaseConnector")
#remotes::install_github("OHDSI/Alathea")

library (dplyr)
library (openxlsx)
library (readr)
library (tibble)
library (DatabaseConnector)
library (Alathea)

#set the BaseUrl of your Atlas instance
#baseUrl <- "https://yourSecureAtlas.ohdsi.org/"

# if security is enabled authorize use of the webapi
ROhdsiWebApi::authorizeWebApi(
  baseUrl = baseUrl,
  authMethod = "windows")


# specify the old and updated cohorts you want to compare
# this table should have old_cohort_id, new_cohort_id columns with old cohort id and its updated version id
phenotypeUpdates <-read.csv('phenotype_updates.csv')

#excluded nodes is a text string with nodes you want to exclude from the analysis, it's set to 0 by default
# for example now some CPT4 and HCPCS are mapped to Visit concepts and we didn't implement this in the ETL,
#so we don't want these in the analysis (note, the tool doesn't look at the actual CDM, but on the mappings in the vocabulary)
#this way, the excludedNodes are defined in this way:
excludedVisitNodes <- "9202, 2514435,9203,2514436,2514437,2514434,2514433,9201"

#you can restrict the output by using specific source vocabularies (only those that exist in your data as source concepts)
includedSourceVocabs <- "'ICD10', 'ICD10CM', 'CPT4', 'HCPCS', 'NDC', 'ICD9CM', 'ICD9Proc', 'ICD10PCS', 'ICDO3', 'JMDC'"


#set connectionDetails,
#you can use keyring to store your credentials,
#see how to configure keyring to use with the example below in ~/Alathea/extras/KeyringSetup.R

# you can also define connectionDetails directly, see the DatabaseConnector documentation https://ohdsi.github.io/DatabaseConnector/

# connectionDetailsVocab = DatabaseConnector::createConnectionDetails(
#   dbms = keyring::key_get("YourDatabase", "dbms" ),
#   connectionString = keyring::key_get("YourDatabase", "connectionString"),
#   user = keyring::key_get("YourDatabase", "username"),
#   password = keyring::key_get("YourDatabase", "password" )
# )

#specify schemas with vocabulary versions you want to compare
newVocabSchema <-'v20240229' #schema containing a new vocabulary version
oldVocabSchema <-'v20230116' #schema containing an older vocabulary


#get the concept count table
#see to generate here
# https://github.com/OHDSI/WebAPI/blob/master/src/main/resources/ddl/achilles/achilles_result_concept_count.sql
# and store it in the same database as the Vocabulary tables, please specify schema as result schema

resultSchema <-'scratch_ddymshyt' #schema containing Achilles results

cohorts <-phenotypeUpdates %>%filter(old_cohort_vocab_version == 'v20230116') %>%  select(old_cohort_id, new_cohort_id)

#create the dataframe with concept set expressions using the getNodeConcepts function
Concepts_in_cohortSetOldCht<-getNodeConcepts(cohorts$old_cohort_id, baseUrl)
Concepts_in_cohortSetNewCht<-getNodeConcepts(cohorts$new_cohort_id, baseUrl)

#resolve concept sets, compare the outputs on different vocabulary versions, write results to the Excel file
#for Redshift ask your administrator for a key for bulk load, since the function uploads the data to the database
Alathea::CompareCohorts(connectionDetailsVocab = connectionDetailsVocab,
                                              cohorts = cohorts,
              Concepts_in_cohortSetOldCht = Concepts_in_cohortSetOldCht,
               Concepts_in_cohortSetNewCht = Concepts_in_cohortSetNewCht,
              newVocabSchema = newVocabSchema,
              oldVocabSchema = oldVocabSchema,
              excludedNodes = excludedVisitNodes,
              resultSchema = resultSchema
)

#open the excel file
#Windows
shell.exec("CohortDif.xlsx")

#MacOS
#system(paste("open", "CohortDif.xlsx"))
