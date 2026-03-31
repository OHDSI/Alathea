# Utility to compare cohorts run in different vocabulary versions by resolving their concept sets
### Identifies Non-standard concepts used in concept set expressions, compares source codes captured and domain changes among included concepts; 

## Prerequisites: 
### 1. schemas with:
- Vocabulary (OHDSI standardized vocabularies) version the cohort were initially created on
- Vocabulary version you are going to migrate
- achilles_count_cc table (resultSchema)
This table is generated on top of Achilles results, see how to generate it here:
https://github.com/OHDSI/WebAPI/blob/master/src/main/resources/ddl/achilles/achilles_result_concept_count.sql

### 2. Active Atlas intance with cohorts instantiated (you don't need to run them - just create/import cohorts in Atlas)

## Step by Step Example



```r
#install package
remotes::install_github("OHDSI/Alathea")

library (dplyr)
library (openxlsx)
library (readr)
library (tibble)
library (Alathea)

#set the BaseUrl of your Atlas instance
#baseUrl <- "https://yourSecureAtlas.ohdsi.org/"

# if security is enabled authorize use of the webapi
ROhdsiWebApi::authorizeWebApi(
  baseUrl = baseUrl,
  authMethod = "windows")

# specify cohorts you want to run the comparison for
# you can define the cohorts as vector:
cohorts <-c(1, 2, 3)

#specify excluded nodes ("Nodes" are the concepts put in concept set expression)
#it is a text string with nodes you want to exclude from the analysis, it's set to 0 by default
# for example now some CPT4 and HCPCS are mapped to Visit concepts and we didn't implement this in the ETL,
# so we don't want these in the analysis (note, the tool doesn't look at the actual CDM, but on the mappings in the vocabulary, predicting how the ETL will be done)
#this way, the excludedNodes are defined in this way:
excludedNodes <-"9202, 2514435,9203,2514436,2514437,2514434,2514433,9201" # visit concepts

#you can restrict the output by using specific source vocabularies (only those that exist in your data as source concepts and thus plays role in event capture), if variable isn't defined, all vocabularies are included in the analysis
# for example:
includedSourceVocabs <- "'ICD10', 'ICD10CM', 'CPT4', 'HCPCS', 'NDC', 'ICD9CM', 'ICD9Proc', 'ICD10PCS', 'ICDO3', 'JMDC'" 

#set connectionDetails,
#you can use keyring to store your credentials,
#see how to configure keyring: https://github.com/OHDSI/Alathea/blob/modify_output/extras/KeyringSetup.R

# you can also define connectionDetails directly, see the DatabaseConnector documentation: https://ohdsi.github.io/DatabaseConnector/

connectionDetails = DatabaseConnector::createConnectionDetails(
  dbms = keyring::key_get("YourDatabase", "dbms" ),
  connectionString = keyring::key_get("YourDatabase", "connectionString"),
  user = keyring::key_get("YourDatabase", "username"),
  password = keyring::key_get("YourDatabase", "password" )
)

#specify working schemas
newVocabSchema <-'vocab_schema_n1' #schema containing a new vocabulary version
oldVocabSchema <-'vocab_schema_n0' #schema containing an older vocabulary version
resultSchema <-'achilles_results' #schema containing Achilles results

#create the dataframe with concept set expressions using the getNodeConcepts function
Concepts_in_cohortSet<-getNodeConcepts(cohorts, baseUrl)

#resolve concept sets, compare the outputs on different vocabulary versions, write results to the Excel file "PhenChange.xlsx" saved in a session root folder
resultToExcel(connectionDetails = connectionDetails,
              Concepts_in_cohortSet = Concepts_in_cohortSet,
              newVocabSchema = newVocabSchema,
              oldVocabSchema = oldVocabSchema,
              resultSchema = resultSchema,
              excludedNodes = excludedVisitNodes,
              includedSourceVocabs = includedSourceVocabs
              )

#open the excel file
#Windows
shell.exec("PhenChange.xlsx")

#MacOS
#system(paste("open", "PhenChange.xlsx"))


```

# The output description:

Writes an Excel file with a separate tab for each type of comparison.


## Definitions/column names used:

**"Node concept"** is a concept directly used in Concept Set Expression

**"drc"**: descendant record count - total number of occurrences of descendants of a given concept

**"source concept":** related source concept_id. The concept set definition is usually done through standard concepts, but different clinical events might be captured with the same standard concepts if mapping was changed, that's why the tool tracks source concepts related.

**“Action”:** flags whether concept or hierarchy branch is added or removed


## The Excel file has the following tabs:


### 1. nonStNodes

lists non-standard concepts used in the concept set definition.

Note, the concept set definition JSON isn't updated with the vocabulary update, so you will not see concept changes in Atlas.

This way you need to run this tool to see if concepts changed to non-standard.

- For example, the cohort_id 10729 has conceptset =’Malignancies that spread to liver’ which has Node concept = "4324190|History of malignant neoplasm of breast" with descendants included, 

this concept is non-standard and mapped this way:

Maps to "1340204|History of event"

Maps to value "4112853|Malignant tumor of breast".

In this situation you'll get the output below, which gives you the **target concepts you need to use** to capture the same clinical events while using a new vocabulary version.


<table>
  <tr>
   <td>cohortid
   </td>
   <td>10729
   </td>
  </tr>
     <td>cohortname
   </td>
   <td>Malignant neoplasms
   </td>
  </tr>
  <tr>
   <td>conceptsetname
   </td>
   <td>Malignancies that spread to liver
   </td>
  </tr>
  <tr>
   <td>conceptsetid
   </td>
   <td>15
   </td>
  </tr>
  <tr>
   <td>isexcluded
   </td>
   <td>0
   </td>
  </tr>
  <tr>
   <td>includedescendants
   </td>
   <td>1
   </td>
  </tr>
  <tr>
   <td>nodeConceptId
   </td>
   <td>4324190
   </td>
  </tr>
  <tr>
   <td>nodeConceptName
   </td>
   <td>History of malignant neoplasm of breast
   </td>
  </tr>
  <tr>
   <td>drc
   </td>
   <td>20284048
   </td>
  </tr>
  <tr>
   <td><strong>mapsToConceptId</strong>
   </td>
   <td><strong>1340204</strong>
   </td>
  </tr>
  <tr>
   <td><strong>mapsToConceptName</strong>
   </td>
   <td><strong>History of event</strong>
   </td>
  </tr>
  <tr>
   <td><strong>mapsToValueConceptId</strong>
   </td>
   <td><strong>4112853</strong>
   </td>
  </tr>
  <tr>
   <td><strong>mapsToValueConceptName</strong>
   </td>
   <td><strong>Malignant tumor of breast</strong>
   </td>
  </tr>
</table>



### 2. mapDif

Tab shows related source concepts that were added or removed. Mapping in both vocabulary versions is shown. 

This way the user knows why the difference in related source concepts occurs and might modify the concept set expression adding or removing mapped concepts.

- In the example below, events with ICD9CM “Neural hearing loss concept, unilateral” are now captured because of the mapping change. OLD_MAPPED_CONCEPT “Unilateral neural hearing loss” didn’t have the proper hierarchy, and wasn’t captured.


<table>
  <tr>
   <td>COHORTID
   </td>
   <td>12822
   </td>
  </tr>
     <td>COHORTNAME
   </td>
   <td>Nerve disorders
   </td>
  </tr>
  <tr>
   <td>CONCEPTSETNAME
   </td>
   <td>Cranial nerve disorder
   </td>
  </tr>
  <tr>
   <td>CONCEPTSETID
   </td>
   <td>28
   </td>
  </tr>
  <tr>
    <td>SOURCE_CONCEPT_ID
   </td>
   <td>44823107
   </td>
  </tr>
  <tr>
   <td>RECORD_COUNT
   </td>
   <td>7115
   </td>
  </tr>
  <tr>
   <td>ACTION
   </td>
   <td>Added
   </td>
  </tr>
  <tr>
   <td>SOURCE_CONCEPT_NAME
   </td>
   <td>Neural hearing loss, unilateral
   </td>
  </tr>
  <tr>
   <td>SOURCE_VOCABULARY_ID
   </td>
   <td>ICD9CM
   </td>
  </tr>
  <tr>
   <td>SOURCE_CONCEPT_CODE
   </td>
   <td>389.13
   </td>
  </tr>
  <tr>
   <td>OLD_MAPPED_CONCEPT_ID
   </td>
   <td>379831
   </td>
  </tr>
  <tr>
   <td>OLD_MAPPED_CONCEPT_NAME
   </td>
   <td>Unilateral neural hearing loss
   </td>
  </tr>
  <tr>
   <td>OLD_MAPPED_VOCABULARY_ID
   </td>
   <td>SNOMED
   </td>
  </tr>
  <tr>
   <td>OLD_MAPPED_CONCEPT_CODE
   </td>
   <td>425601005
   </td>
  </tr>
  <tr>
   <td>NEW_MAPPED_CONCEPT_ID
   </td>
   <td>381312
   </td>
  </tr>
  <tr>
   <td>NEW_MAPPED_CONCEPT_NAME
   </td>
   <td>Neural hearing loss
   </td>
  </tr>
  <tr>
   <td>NEW_MAPPED_VOCABULARY_ID
   </td>
   <td>SNOMED
   </td>
  </tr>
  <tr>
   <td>NEW_MAPPED_CONCEPT_CODE
   </td>
   <td>73371001
   </td>
  </tr>
</table>


### 3. domainChange

This tab shows included concepts that changed their domain, so the different event table should be used.
To show how these concepts are connected to actual events, source codes with their record counts are shown


<table>
  <tr>
   <td>cohortid
   </td>
   <td>123
   </td>
  </tr>
  <tr>
   <td>cohortname
   </td>
   <td>Altered mental status
   </td>
  </tr>
  <tr>
   <td>conceptsetname
   </td>
   <td>Altered mental status
   </td>
  </tr>
  <tr>
   <td>conceptsetid
   </td>
   <td>1
   </td>
  </tr>
  <tr>
   <td>conceptId
   </td>
   <td>436222
   </td>
  </tr>
  <tr>
   <td>conceptName
   </td>
   <td>Altered mental status
   </td>
  </tr>
  <tr>
   <td>vocabularyId
   </td>
   <td>SNOMED
   </td>
  </tr>
  <tr>
   <td>sourceconceptCode
   </td>
   <td>R41.82
   </td>
  </tr>
  <td>sourceconceptname
   </td>
   <td>Altered mental status, unspecified
   </td>
  </tr>
  <td>sourceVocabularyId
   </td>
   <td>ICD10CM
   </td>
  </tr>
  <tr>
   <td><strong>oldDomainId</strong>
   </td>
   <td><strong>Condition</strong>
   </td>
  </tr>
  <tr>
   <td><strong>newDomainId</strong>
   </td>
   <td><strong>Observation</strong>
   </td>
  </tr>
  <tr>
   <td>sourceConceptRecordCount
   </td>
   <td>88528142
   </td>
  </tr>
</table>

## Checking updated cohorts
 After you created updated versions of cohorts in Atlas, to check if the new cohort on a new vocabulary is resolved in the same way as the old one on the old vocabulary you need to run the "compareCohorts" funcion, it will give result similar to the "PhenChange.xlsx", but two cohorts on two vocabulary versions will be compared
### see example of the code to run in "extras/codeToRunCheckResults.R"
