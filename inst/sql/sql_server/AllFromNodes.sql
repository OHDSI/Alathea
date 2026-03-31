-- 1) Get Node concepts that became non-standard and show their replacements
SELECT
  s.cohortid,
  s.cohortName,
  s.conceptsetname,
  s.conceptsetid,
  s.isexcluded,
  s.includedescendants,
  cn.concept_id AS Node_concept_id,
  cn.concept_name AS node_concept_name,
  {@hasAchilles} ? {COALESCE(aro.descendant_record_count, 0)} : {0} AS drc,
  cm.concept_id AS maps_to_concept_id,
  cm.concept_name AS maps_to_concept_name,
  cmv.concept_id AS maps_to_value_concept_id,
  cmv.concept_name AS maps_to_value_concept_name
INTO #non_st_Nodes
FROM #ConceptsInCohortSet AS s
JOIN @newVocabSchema.concept AS cn
  ON cn.concept_id = s.conceptid
 AND cn.standard_concept IS NULL
{@hasAchilles} ? {LEFT JOIN @resultSchema.achilles_result_concept_count AS aro
  ON aro.concept_id = cn.concept_id}
LEFT JOIN @newVocabSchema.concept_relationship AS cr
  ON cr.concept_id_1 = cn.concept_id
 AND cr.relationship_id = 'Maps to'
 AND cr.invalid_reason IS NULL
LEFT JOIN @newVocabSchema.concept AS cm
  ON cm.concept_id = cr.concept_id_2
LEFT JOIN @newVocabSchema.concept_relationship AS crv
  ON crv.concept_id_1 = cn.concept_id
 AND crv.relationship_id = 'Maps to value'
 AND crv.invalid_reason IS NULL
LEFT JOIN @newVocabSchema.concept AS cmv
  ON cmv.concept_id = crv.concept_id_2
;

-- 2) Mapping difference: old vocabulary vs new (source concept comparison)

-- old vocabulary coverage
SELECT *
INTO #old_vc
FROM (
  SELECT
    s.cohortid,
    s.cohortName,
    s.conceptsetname,
    s.conceptsetid,
    ca.descendant_concept_id
  FROM #ConceptsInCohortSet AS s
  JOIN @oldVocabSchema.concept AS cn
    ON cn.concept_id = s.conceptid
  JOIN @oldVocabSchema.concept_ancestor AS ca
    ON ca.ancestor_concept_id = cn.concept_id
   AND (
        (s.includedescendants = 0 AND ca.ancestor_concept_id = ca.descendant_concept_id)
        OR s.includedescendants <> 0
       )
  WHERE s.isexcluded = 0
    AND s.conceptid NOT IN (@excludedNodes)

  EXCEPT

  SELECT
    s.cohortid,
    s.cohortName,
    s.conceptsetname,
    s.conceptsetid,
    ca.descendant_concept_id
  FROM #ConceptsInCohortSet AS s
  JOIN @oldVocabSchema.concept AS cn
    ON cn.concept_id = s.conceptid
  JOIN @oldVocabSchema.concept_ancestor AS ca
    ON ca.ancestor_concept_id = cn.concept_id
   AND (
        (s.includedescendants = 0 AND ca.ancestor_concept_id = ca.descendant_concept_id)
        OR s.includedescendants <> 0
       )
  WHERE s.isexcluded = 1
) AS x
;

-- new vocabulary coverage
SELECT *
INTO #new_vc
FROM (
  SELECT
    s.cohortid,
    s.cohortName,
    s.conceptsetname,
    s.conceptsetid,
    ca.descendant_concept_id
  FROM #ConceptsInCohortSet AS s
  JOIN @newVocabSchema.concept AS cn
    ON cn.concept_id = s.conceptid
  JOIN @newVocabSchema.concept_ancestor AS ca
    ON ca.ancestor_concept_id = cn.concept_id
   AND (
        (s.includedescendants = 0 AND ca.ancestor_concept_id = ca.descendant_concept_id)
        OR s.includedescendants <> 0
       )
  WHERE s.isexcluded = 0
    AND s.conceptid NOT IN (@excludedNodes)

  EXCEPT

  SELECT
    s.cohortid,
    s.cohortName,
    s.conceptsetname,
    s.conceptsetid,
    ca.descendant_concept_id
  FROM #ConceptsInCohortSet AS s
  JOIN @newVocabSchema.concept AS cn
    ON cn.concept_id = s.conceptid
  JOIN @newVocabSchema.concept_ancestor AS ca
    ON ca.ancestor_concept_id = cn.concept_id
   AND (
        (s.includedescendants = 0 AND ca.ancestor_concept_id = ca.descendant_concept_id)
        OR s.includedescendants <> 0
       )
  WHERE s.isexcluded = 1
) AS x
;

-- Differences in source concept mappings (Removed vs Added)
WITH old_vc_map AS (
  SELECT
    v.cohortid,
    v.cohortName,
    v.conceptsetname,
    v.conceptsetid,
    r.concept_id_2 AS source_concept_id
  FROM #old_vc AS v
  JOIN @oldVocabSchema.concept_relationship AS r
    ON v.descendant_concept_id = r.concept_id_1
   AND r.relationship_id = 'Mapped from'
   AND r.invalid_reason IS NULL
),
new_vc_map AS (
  SELECT
    v.cohortid,
    v.cohortName,
    v.conceptsetname,
    v.conceptsetid,
    r.concept_id_2 AS source_concept_id
  FROM #new_vc AS v
  JOIN @newVocabSchema.concept_relationship AS r
    ON v.descendant_concept_id = r.concept_id_1
   AND r.relationship_id = 'Mapped from'
   AND r.invalid_reason IS NULL
)
SELECT
  cohortid,
  cohortName,
  conceptsetname,
  conceptsetid,
  source_concept_id,
  'Removed' AS action
INTO #resolv_dif_sc
FROM (
  SELECT * FROM old_vc_map
  EXCEPT
  SELECT * FROM new_vc_map
) AS a
;

WITH old_vc_map AS (
  SELECT
    v.cohortid,
    v.cohortName,
    v.conceptsetname,
    v.conceptsetid,
    r.concept_id_2 AS source_concept_id
  FROM #old_vc AS v
  JOIN @oldVocabSchema.concept_relationship AS r
    ON v.descendant_concept_id = r.concept_id_1
   AND r.relationship_id = 'Mapped from'
   AND r.invalid_reason IS NULL
),
new_vc_map AS (
  SELECT
    v.cohortid,
    v.cohortName,
    v.conceptsetname,
    v.conceptsetid,
    r.concept_id_2 AS source_concept_id
  FROM #new_vc AS v
  JOIN @newVocabSchema.concept_relationship AS r
    ON v.descendant_concept_id = r.concept_id_1
   AND r.relationship_id = 'Mapped from'
   AND r.invalid_reason IS NULL
)
INSERT INTO #resolv_dif_sc (cohortid, cohortName, conceptsetname, conceptsetid, source_concept_id, action)
SELECT
  cohortid,
  cohortName,
  conceptsetname,
  conceptsetid,
  source_concept_id,
  'Added' AS action
FROM (
  SELECT * FROM new_vc_map
  EXCEPT
  SELECT * FROM old_vc_map
) AS b
;

-- Append mappings to see difference in source concepts (previous vocabulary version)
-- listagg/join work to be done in R
SELECT
  dif.cohortid,
  dif.cohortName,
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
FROM #resolv_dif_sc AS dif
JOIN @newVocabSchema.concept AS cs
  ON cs.concept_id = dif.source_concept_id -- to get source_concept_id info
LEFT JOIN @oldVocabSchema.concept_relationship AS cr
  ON cr.concept_id_1 = dif.source_concept_id
 AND cr.relationship_id = 'Maps to'
 AND cr.invalid_reason IS NULL
LEFT JOIN @oldVocabSchema.concept AS c
  ON c.concept_id = cr.concept_id_2
{@hasAchilles} ? {JOIN @resultSchema.achilles_result_concept_count AS arc
  ON arc.concept_id = cs.concept_id}
WHERE cs.vocabulary_id IN (@includedSourceVocabs)
;

-- Append mappings to see difference in source concepts (new vocabulary version)
SELECT
  dif.*,
  c.concept_id,
  c.concept_name,
  c.vocabulary_id,
  c.concept_code
INTO #newmap
FROM #resolv_dif_sc AS dif
LEFT JOIN @newVocabSchema.concept_relationship AS cr
  ON cr.concept_id_1 = dif.source_concept_id
 AND cr.relationship_id = 'Maps to'
 AND cr.invalid_reason IS NULL
LEFT JOIN @newVocabSchema.concept AS c
  ON c.concept_id = cr.concept_id_2
;

-- 3) Domain difference: old vocabulary vs new (target concept comparison)
SELECT
  a.cohortid,
  a.cohortName,
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
  {@hasAchilles} ? {COALESCE(aro.record_count, 0)} : {0} AS source_concept_record_count
INTO #resolv_dom_dif
FROM (
  SELECT * FROM #old_vc
  INTERSECT
  SELECT * FROM #new_vc
) AS a
JOIN @oldVocabSchema.concept AS co
  ON co.concept_id = a.descendant_concept_id
JOIN @newVocabSchema.concept AS cn
  ON cn.concept_id = a.descendant_concept_id
-- get source concepts related to those targets with changed domains
JOIN @newVocabSchema.concept_relationship AS cr
  ON cr.relationship_id = 'Mapped from'
 AND cr.concept_id_1 = cn.concept_id
 AND cr.invalid_reason IS NULL
JOIN @newVocabSchema.concept AS cs
  ON cs.concept_id = cr.concept_id_2
{@hasAchilles} ? {LEFT JOIN @resultSchema.achilles_result_concept_count AS aro
  ON aro.concept_id = cs.concept_id}
WHERE co.domain_id <> cn.domain_id
  AND cs.vocabulary_id IN (@includedSourceVocabs)
