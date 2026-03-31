--1. get the Node concepts that became non-standard and show their replacements
SELECT
  s.cohortid,
  s.cohortName,
  s.conceptsetname,
  s.conceptsetid,
  s.isexcluded,
  s.includedescendants,
  cn.concept_id AS Node_concept_id,
  cn.concept_name AS node_concept_name,
  {@hasAchilles} ? {coalesce(aro.descendant_record_count, 0)} : {0} AS drc,
  cm.concept_id AS maps_to_concept_id,
  cm.concept_name AS maps_to_concept_name,
  cmv.concept_id AS maps_to_value_concept_id,
  cmv.concept_name AS maps_to_value_concept_name
INTO #non_st_Nodes
FROM #ConceptsInCohortSetNew s
JOIN @newVocabSchema.concept cn ON cn.concept_id = s.conceptid AND cn.standard_concept IS NULL
{@hasAchilles} ? {LEFT JOIN @resultSchema.achilles_result_cc aro ON aro.concept_id = cn.concept_id}
LEFT JOIN @newVocabSchema.concept_relationship cr ON cr.concept_id_1 = cn.concept_id AND cr.relationship_id = 'Maps to' AND cr.invalid_reason IS NULL
LEFT JOIN @newVocabSchema.concept cm ON cm.concept_id = cr.concept_id_2
LEFT JOIN @newVocabSchema.concept_relationship crv ON crv.concept_id_1 = cn.concept_id AND crv.relationship_id = 'Maps to value' AND crv.invalid_reason IS NULL
LEFT JOIN @newVocabSchema.concept cmv ON cmv.concept_id = crv.concept_id_2
;

--2. look at the mapping difference between old cohort on old vocabulary VS new cohort on a new vocabulary
--old vocabulary coverage
SELECT cohortid, conceptsetname, conceptsetid, ca.descendant_concept_id
INTO #old_vc
FROM #ConceptsInCohortSetOld s
JOIN @oldVocabSchema.concept cn ON cn.concept_id = s.conceptid
JOIN @oldVocabSchema.concept_ancestor ca
  ON ca.ancestor_concept_id = cn.concept_id
  AND ((s.includedescendants = 0 AND ca.ancestor_concept_id = ca.descendant_concept_id) OR s.includedescendants <> 0)
WHERE s.isexcluded = 0
  AND s.conceptid NOT IN (@excludedNodes)

EXCEPT

SELECT cohortid, conceptsetname, conceptsetid, ca.descendant_concept_id
FROM #ConceptsInCohortSetOld s
JOIN @oldVocabSchema.concept cn ON cn.concept_id = s.conceptid
JOIN @oldVocabSchema.concept_ancestor ca
  ON ca.ancestor_concept_id = cn.concept_id
  AND ((s.includedescendants = 0 AND ca.ancestor_concept_id = ca.descendant_concept_id) OR s.includedescendants <> 0)
WHERE s.isexcluded = 1
;

--new vocabulary coverage
SELECT cohortid, conceptsetname, conceptsetid, ca.descendant_concept_id
INTO #new_vc
FROM #ConceptsInCohortSetNew s
JOIN @newVocabSchema.concept cn ON cn.concept_id = s.conceptid
JOIN @newVocabSchema.concept_ancestor ca
  ON ca.ancestor_concept_id = cn.concept_id
  AND ((s.includedescendants = 0 AND ca.ancestor_concept_id = ca.descendant_concept_id) OR s.includedescendants <> 0)
WHERE s.isexcluded = 0
  AND s.conceptid NOT IN (@excludedNodes)

EXCEPT

SELECT cohortid, conceptsetname, conceptsetid, ca.descendant_concept_id
FROM #ConceptsInCohortSetNew s
JOIN @newVocabSchema.concept cn ON cn.concept_id = s.conceptid
JOIN @newVocabSchema.concept_ancestor ca
  ON ca.ancestor_concept_id = cn.concept_id
  AND ((s.includedescendants = 0 AND ca.ancestor_concept_id = ca.descendant_concept_id) OR s.includedescendants <> 0)
WHERE s.isexcluded = 1
;

--differences in source concept mappings (Removed vs Added)
WITH old_vc_map AS (
  SELECT cohortid, conceptsetname, conceptsetid, r.concept_id_2 AS source_concept_id
  FROM #old_vc
  JOIN @oldVocabSchema.concept_relationship r
    ON r.concept_id_1 = descendant_concept_id AND r.relationship_id = 'Mapped from' AND r.invalid_reason IS NULL
),
new_vc_map AS (
  SELECT c.old_cohort_id AS cohortid, conceptsetname, conceptsetid, r.concept_id_2 AS source_concept_id
  FROM #new_vc vc
  JOIN #cohorts c ON c.new_cohort_id = vc.cohortid
  JOIN @newVocabSchema.concept_relationship r
    ON r.concept_id_1 = descendant_concept_id AND r.relationship_id = 'Mapped from' AND r.invalid_reason IS NULL
)
SELECT cohortid, conceptsetname, conceptsetid, source_concept_id, 'Removed' AS action
INTO #resolv_dif_sc
FROM (
  SELECT * FROM old_vc_map
  EXCEPT
  SELECT * FROM new_vc_map
) a

UNION ALL

SELECT cohortid, conceptsetname, conceptsetid, source_concept_id, 'Added' AS action
FROM (
  SELECT * FROM new_vc_map
  EXCEPT
  SELECT * FROM old_vc_map
) b
;

--append mappings to see difference in source concepts, previous vocabulary version
-- join of this and #newmap is done in R
SELECT
  dif.cohortid,
  dif.conceptsetname,
  dif.conceptsetid,
  dif.action,
  {@hasAchilles} ? {arc.record_count} : {0} AS record_count,
  dif.source_concept_id,
  cs.concept_name AS source_concept_name,
  cs.vocabulary_id AS source_vocabulary_id,
  cs.concept_code AS source_concept_code,
  c.concept_id,
  c.concept_name,
  c.vocabulary_id,
  c.concept_code
INTO #oldmap
FROM #resolv_dif_sc dif
JOIN @newVocabSchema.concept cs ON cs.concept_id = dif.source_concept_id
LEFT JOIN @oldVocabSchema.concept_relationship cr
  ON cr.concept_id_1 = dif.source_concept_id AND cr.relationship_id = 'Maps to' AND cr.invalid_reason IS NULL
LEFT JOIN @oldVocabSchema.concept c ON c.concept_id = cr.concept_id_2
{@hasAchilles} ? {JOIN @resultSchema.achilles_result_cc arc ON arc.concept_id = cs.concept_id}
{@includedSourceVocabs != 0} ? {WHERE cs.vocabulary_id IN (@includedSourceVocabs)}
;

--append mappings to see difference in source concepts, new vocabulary version
SELECT
  dif.cohortid,
  dif.conceptsetname,
  dif.conceptsetid,
  dif.action,
  dif.source_concept_id,
  c.concept_id,
  c.concept_name,
  c.vocabulary_id,
  c.concept_code
INTO #newmap
FROM #resolv_dif_sc dif
LEFT JOIN @newVocabSchema.concept_relationship cr
  ON cr.concept_id_1 = dif.source_concept_id AND cr.relationship_id = 'Maps to' AND cr.invalid_reason IS NULL
LEFT JOIN @newVocabSchema.concept c ON c.concept_id = cr.concept_id_2
;

--3. domain difference: old vocabulary vs new, target concept comparison
SELECT
  a.cohortid,
  a.conceptsetname,
  a.conceptsetid,
  cn.concept_id,
  cn.concept_name,
  cn.vocabulary_id,
  cs.concept_code AS source_concept_code,
  cs.concept_name AS source_concept_name,
  cs.vocabulary_id AS source_vocabulary_id,
  co.domain_id AS old_domain_id,
  cn.domain_id AS new_domain_id,
  {@hasAchilles} ? {coalesce(aro.record_count, 0)} : {0} AS source_concept_record_count
INTO #resolv_dom_dif
FROM (
  SELECT * FROM #old_vc
  INTERSECT
  SELECT c.old_cohort_id AS cohortid, conceptsetname, conceptsetid, descendant_concept_id
  FROM #new_vc vc
  JOIN #cohorts c ON c.new_cohort_id = vc.cohortid
) a
JOIN @oldVocabSchema.concept co ON co.concept_id = a.descendant_concept_id
JOIN @newVocabSchema.concept cn ON cn.concept_id = a.descendant_concept_id
JOIN @newVocabSchema.concept_relationship cr
  ON cr.relationship_id = 'Mapped from' AND cr.concept_id_1 = cn.concept_id AND cr.invalid_reason IS NULL
JOIN @newVocabSchema.concept cs ON cs.concept_id = cr.concept_id_2
{@hasAchilles} ? {LEFT JOIN @resultSchema.achilles_result_cc aro ON aro.concept_id = cs.concept_id}
WHERE co.domain_id <> cn.domain_id
{@includedSourceVocabs != 0} ? {AND cs.vocabulary_id IN (@includedSourceVocabs)}
;
--1. look at the mapping difference between old cohort on old vocabulary VS new cohort on a new vocabulary
--old vocabulary vs new, source concept comparison
create table #old_vc as
select cohortid, conceptsetname, conceptsetid, ca.descendant_concept_id
from #ConceptsInCohortSetOld s
join @oldVocabSchema.concept cn on cn.concept_id = s.conceptid
join @oldVocabSchema.concept_ancestor ca on ca.ancestor_concept_id = cn.concept_id
and ((includedescendants = 0 and ca.ancestor_concept_id = ca.descendant_concept_id ) or includedescendants != 0)
and isexcluded = 0
--exclude visits nodes
and s.conceptid not in (@excludedNodes)
except
select cohortid, conceptsetname, conceptsetid , ca.descendant_concept_id
from #ConceptsInCohortSetOld s
join @oldVocabSchema.concept cn on cn.concept_id = s.conceptid
join @oldVocabSchema.concept_ancestor ca on ca.ancestor_concept_id = cn.concept_id
and ((includedescendants = 0 and ca.ancestor_concept_id = ca.descendant_concept_id ) or includedescendants != 0)
and isexcluded = 1
;
create table #new_vc
 as
select cohortid, conceptsetname, conceptsetid, ca.descendant_concept_id
from #ConceptsInCohortSetNew s
join @newVocabSchema.concept cn on cn.concept_id = s.conceptid
join @newVocabSchema.concept_ancestor ca on ca.ancestor_concept_id = cn.concept_id
and ((includedescendants = 0 and ca.ancestor_concept_id = ca.descendant_concept_id ) or includedescendants != 0)
and isexcluded = 0
--exclude specific nodes from analysis
and s.conceptid not in (@excludedNodes)
except
select cohortid, conceptsetname, conceptsetid , ca.descendant_concept_id
from #ConceptsInCohortSetNew s
join @newVocabSchema.concept cn on cn.concept_id = s.conceptid
join @newVocabSchema.concept_ancestor ca on ca.ancestor_concept_id = cn.concept_id
and ((includedescendants = 0 and ca.ancestor_concept_id = ca.descendant_concept_id ) or includedescendants != 0)
and isexcluded = 1
;
create table #resolv_dif_sc as 
with
old_vc_map as (
select cohortid, conceptsetname, conceptsetid, r.concept_id_2 as source_concept_id
 from #old_vc
join @oldVocabSchema.concept_relationship r on descendant_concept_id = r.concept_id_1 and r.relationship_id ='Mapped from' and r.invalid_reason is null
)
,
new_vc_map as (
select c.old_cohort_id as cohortid, conceptsetname, conceptsetid,  r.concept_id_2 as source_concept_id
 from #new_vc vc
 join #cohorts c on c.new_cohort_id = vc.cohortid
join @newVocabSchema.concept_relationship r on descendant_concept_id = r.concept_id_1 and r.relationship_id ='Mapped from' and r.invalid_reason is null
)
select *, 'Removed' as action from (
select * from old_vc_map
except
select * from new_vc_map
) a
union all
select *, 'Added' as action from (
select * from new_vc_map
except
select * from old_vc_map
) a
;
--append mappings to see difference in source concepts, previous vocabulary version
-- listagg and join of this and #newmap table will be done in R
--afterwards, these tables are joined in R
create table #oldmap as
with aaa as (select 1 as test)
select cohortid, conceptsetname, conceptsetid, action, {@hasAchilles} ? {arc.record_count} : {0} as record_count ,
source_concept_id, cs.concept_name as source_concept_name, cs.vocabulary_id as source_vocabulary_id, cs.concept_code as source_concept_code,
c.concept_id , c.concept_name, c.vocabulary_id, c.concept_code
from #resolv_dif_sc dif
join @newVocabSchema.concept cs on cs.concept_id = dif.source_concept_id -- to get source_concept_id info
left join @oldVocabSchema.concept_relationship cr on cr.concept_id_1 = dif.source_concept_id and cr.relationship_id ='Maps to' and cr.invalid_reason is null
left join @oldVocabSchema.concept c on c.concept_id = cr.concept_id_2
{@hasAchilles} ? {join @resultSchema.achilles_result_cc arc on arc.concept_id = cs.concept_id}
--look only at specific vocabularies that used by our data
{@includedSourceVocabs !=0} ? {where cs.vocabulary_id in (@includedSourceVocabs)}
;
--append mappings to see difference in source concepts, new vocabulary version
create table #newmap as
with aaa as (select 1 as test)
select dif.*,c.concept_id , c.concept_name, c.vocabulary_id, c.concept_code
from #resolv_dif_sc dif
left join @newVocabSchema.concept_relationship cr on cr.concept_id_1 = dif.source_concept_id and cr.relationship_id ='Maps to' and cr.invalid_reason is null
left join @newVocabSchema.concept c on c.concept_id = cr.concept_id_2
;
--3. domain difference
--old vocabulary vs new, target concept comparison
create table #resolv_dom_dif  as
select cohortid, conceptsetname, conceptsetid, 
 cn.concept_id , cn.concept_name , cn.vocabulary_id ,
 cs.concept_code as source_concept_code, cs.concept_name as source_concept_name, cs.vocabulary_id as source_vocabulary_id,
 co.domain_id as old_domain_id, cn.domain_id as new_domain_id,
 {@hasAchilles} ? {coalesce (aro.record_count, 0)} : {0} as source_concept_record_count
 from (
select * from #old_vc
--compare only rows where same included concepts exist
intersect 
select c.old_cohort_id as cohortid, conceptsetname, conceptsetid, descendant_concept_id from #new_vc vc
join #cohorts c on c.new_cohort_id = vc.cohortid
) a
join @oldVocabSchema.concept co on co.concept_id =descendant_concept_id
join @newVocabSchema.concept cn on cn.concept_id =descendant_concept_id
--get source concepts related to those targets with changed domains
join @newVocabSchema.concept_relationship cr on cr.relationship_id ='Mapped from' and cr.concept_id_1 = cn.concept_id and cr.invalid_reason is null
join @newVocabSchema.concept cs on cs.concept_id = cr.concept_id_2
{@hasAchilles} ? {left join @resultSchema.achilles_result_cc aro on aro.concept_id = cs.concept_id}
where co.domain_id != cn.domain_id
{@includedSourceVocabs !=0}? {and cs.vocabulary_id in (@includedSourceVocabs)}
;