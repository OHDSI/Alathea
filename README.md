# Alathea — Utility to compare cohorts across OMOP vocabulary versions

Identifies non-standard concepts used in concept set expressions, compares source codes captured, and detects domain changes among included concepts when migrating from one OMOP vocabulary version to another.

## Prerequisites

### 1. Database with vocabulary schemas
- A schema with the **old** OMOP vocabulary version (the version cohorts were originally created on)
- A schema with the **new** OMOP vocabulary version (the version you are migrating to)
- A **scratch schema** with write access — used for intermediate Databricks/Spark tables

### 2. Active Atlas instance
Cohorts must exist in Atlas (you do not need to run them — just create or import cohort definitions).

### 3. Optional: Achilles result schema
A schema containing the `achilles_result_concept_count` table adds concept usage counts (`record_count`, `drc`) to the output.  
To generate this table, see:  
https://github.com/OHDSI/WebAPI/blob/master/src/main/resources/ddl/achilles/achilles_result_concept_count.sql  
Set `resultSchema = NULL` to run without usage counts.

### 4. Optional: CDM schema
Required only for the **stats** tab, which calculates person-level impact of vocabulary changes.  
Set `cdmSchema = NULL` to skip this tab.

---

## Installation

```r
remotes::install_github("OHDSI/Alathea")
remotes::install_github("OHDSI/DatabaseConnector")
```

---

## Step-by-Step Example

```r
library(dplyr)
library(openxlsx)
library(readr)
library(tibble)
library(DatabaseConnector)
library(Alathea)

# Set the BaseUrl of your Atlas instance
baseUrl <- "https://yourAtlas.ohdsi.org/"

# If security is enabled, authorize use of the WebAPI
ROhdsiWebApi::authorizeWebApi(
  baseUrl = baseUrl,
  authMethod = "windows")

# Specify cohorts — IDs and names are fetched directly from Atlas metadata
Cohorts <- ROhdsiWebApi::getCohortDefinitionsMetaData(baseUrl = baseUrl)
cohorts <- Cohorts$id

# Set name used for the output file
projName <- 'MyProject'

# Excluded nodes: concept IDs to exclude from the analysis (e.g. visit concepts
# mapped from CPT4/HCPCS that are not implemented in your ETL)
excludedVisitNodes <- "9202, 2514435, 9203, 2514436, 2514437, 2514434, 2514433, 9201"

# Restrict to source vocabularies present in your data (leave as 0 for all)
includedSourceVocabs <- "'ICD10', 'ICD10CM', 'CPT4', 'HCPCS', 'NDC', 'ICD9CM', 'ICD9Proc', 'ICD10PCS', 'ICDO3', 'JMDC', 'LOINC'"

# Connection details — example using keyring for Databricks
# See extras/KeyringSetup.R to store credentials securely
connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = "spark",
  connectionString = keyring::key_get("databricks", "connection_string"),
  user = "token",
  password = keyring::key_get("databricks", "token")
)

# Schemas for the two vocabulary versions to compare
oldVocabSchema <- 'vocab_schema_old'
newVocabSchema <- 'vocab_schema_new'

# Scratch schema for intermediate tables (required for Databricks/Spark)
scratchSchema <- 'scratch.my_scratch_schema'

# (Optional) Schema with Achilles achilles_result_concept_count — adds usage counts
# Set to NULL to run without usage counts
resultSchema <- 'achilles_results_schema'

# (Optional) CDM schema for the stats tab — requires patient-level data access
# Set to NULL to skip the stats tab
cdmSchema <- 'my_cdm_schema'

# Step 1: fetch concept set expressions from Atlas
Concepts_in_cohortSet <- getNodeConcepts(cohorts, baseUrl)

# Step 2: resolve concept sets across vocabulary versions and write Excel output
# The file is saved to the 'results/' folder (created automatically if missing)
resultToExcel(
  connectionDetailsVocab = connectionDetails,
  Concepts_in_cohortSet  = Concepts_in_cohortSet,
  newVocabSchema         = newVocabSchema,
  oldVocabSchema         = oldVocabSchema,
  excludedNodes          = excludedVisitNodes,
  resultSchema           = resultSchema,
  scratchSchema          = scratchSchema,
  includedSourceVocabs   = includedSourceVocabs,
  projName               = projName,
  cdmSchema              = cdmSchema,
  outputFolder           = "results"
)

# Open the output file
# Windows:
shell.exec(file.path("results", paste0(projName, "PhenChange.xlsx")))
# MacOS:
# system(paste("open", file.path("results", paste0(projName, "PhenChange.xlsx"))))
```

---

## Output Description

Writes an Excel file to the `results/` folder named `{projName}PhenChange.xlsx`, with a separate tab for each type of comparison.

### Definitions

**Node concept** — a concept directly placed in a Concept Set Expression  
**drc** — descendant record count: total occurrences of all descendants of a given concept  
**source concept** — the source-coded event (e.g. ICD10CM code) that maps to a standard concept  
**Action** — flags whether a source concept is `Added` or `Removed` relative to the old vocabulary

---

## Excel Tabs

### 1. vocabularyVersions

Lists the vocabulary versions compared, queried directly from the vocabulary tables.

| vocabulary_type | vocabulary_version |
|---|---|
| old | Vocabularies v5.0 27-AUG-25 |
| new | Vocabularies v5.0 27-FEB-26 |

---

### 2. nonStNodes

Lists non-standard concepts used in concept set definitions (identified against the **new** vocabulary).

Note: concept set definition JSON in Atlas is not automatically updated on vocabulary change, so concept standard-status changes are not visible in Atlas. Use this tab to identify which node concepts have become non-standard and what they now map to.

| Column | Description |
|---|---|
| cohortid / cohortName | Cohort identifier and name |
| conceptsetname / conceptsetid | Concept set within the cohort |
| isexcluded / includedescendants | Logic flags from concept set definition |
| nodeConceptId / nodeConceptName | The non-standard concept used as a node |
| drc | Descendant record count (0 if no Achilles `resultSchema` provided) |
| mapsToConceptId / mapsToConceptName | Standard concept it maps to |
| mapsToValueConceptId / mapsToValueConceptName | Value concept (for measurement/observation mappings) |

---

### 3. mapDif

Shows source concepts (e.g. ICD codes) that were **added** or **removed** due to mapping changes between vocabulary versions. Old and new mappings are shown side by side so you can understand why the difference occurs.

| Column | Description |
|---|---|
| cohortid / cohortName | Cohort identifier and name |
| conceptsetname / conceptsetid | Concept set within the cohort |
| source_concept_id / source_concept_name | The source code (ICD, CPT4, etc.) |
| source_vocabulary_id / source_concept_code | Source vocabulary and code |
| record_count | Number of occurrences in the database (0 if no Achilles `resultSchema` provided) |
| action | `Added` or `Removed` |
| old_mapped_concept_id / old_mapped_concept_name | Standard concept this source mapped to in the old vocabulary |
| new_mapped_concept_id / new_mapped_concept_name | Standard concept this source maps to in the new vocabulary |

---

### 4. domainChange

Shows standard concepts that **changed domain** between vocabulary versions (e.g. Condition → Observation), meaning a different CDM event table would now capture those events. Source codes with record counts are shown to help assess impact.

| Column | Description |
|---|---|
| cohortid / cohortName | Cohort identifier and name |
| conceptsetname / conceptsetid | Concept set within the cohort |
| conceptId / conceptName | The standard concept that changed domain |
| sourceConceptCode / sourceConceptName / sourceVocabularyId | Related source concept |
| oldDomainId | Domain in the old vocabulary |
| newDomainId | Domain in the new vocabulary |
| sourceConceptRecordCount | Number of occurrences in the database (0 if no Achilles `resultSchema` provided) |

---

### 5. stats *(optional — requires `cdmSchema`)*

Quantifies the **patient-level impact** of the vocabulary change. For each cohort, the tool determines which source concepts (ICD, CPT4, etc.) map to the resolved standard concepts under the old vs new vocabulary, classifies each source concept as `same`, `added`, or `removed`, then joins against 6 CDM event tables (`condition_occurrence`, `procedure_occurrence`, `drug_exposure`, `device_exposure`, `measurement`, `observation`) on `*_source_concept_id` to count how many real patients are affected. Results are ordered by `same_persons_no_change / total_persons` ascending, so the most impacted cohorts appear first.

> **Note:** This is not a real subject count change, but an approximation — the calculation does not use the real cohort definition logic (e.g. time windows, entry events, inclusion criteria).

Only produced when `cdmSchema` is provided.

| Column | Description |
|---|---|
| cohort_definition_id / cohortname | Cohort identifier and name |
| total_persons | All persons having any matching source concept in the 6 CDM tables above |
| same_persons | Persons captured by both vocabulary versions (have at least one unchanged source concept, or have both added and removed concepts) |
| same_persons_no_change | Subset of same_persons whose qualifying source concepts are entirely unchanged — no impact from the vocabulary migration |
| same_persons_potential_index_misclassification | same_persons minus same_persons_no_change: persons still captured but with at least one added or removed source concept, meaning the index event date may shift |
| new_persons | Persons captured only under the new vocabulary (all their matching source concepts are `added`) |
| lost_persons | Persons captured only under the old vocabulary (all their matching source concepts are `removed`) |
| same_concepts_count | Number of source concepts unchanged between vocabulary versions |
| removed_concepts_count | Number of source concepts that dropped out in the new vocabulary |
| added_concepts_count | Number of source concepts newly included in the new vocabulary |

**`same_persons_potential_index_misclassification`**: at least one of the concept sets has either added or lost source concepts, but the union of source concepts across all concept sets remains the same overall. This will potentially capture the same clinical events, but defined by different components of the cohort definition with different time constraints, resulting in index date misclassification.

---

## Checking Updated Cohorts

After creating updated cohort versions in Atlas, use the `CompareCohorts()` function to verify that the new cohort on the new vocabulary resolves equivalently to the old cohort on the old vocabulary.

See `extras/codeToRunCheckResults.R` for an example.

## Using the OHDSI Phenotype Library

To run the analysis against all cohorts in the OHDSI Phenotype Library instead of Atlas, see `extras/workWithOHDSIPhenLibrary.R`.
