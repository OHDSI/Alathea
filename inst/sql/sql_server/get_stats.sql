-- tables that will help calculate statistics

DROP TABLE IF EXISTS @scratchSchema.concepts_in_cohorts;

SELECT
    cohortid AS cohort_definition_id,
    'old' AS concept_type,
    c.concept_id
INTO @scratchSchema.concepts_in_cohorts
FROM #old_vc ci
JOIN @oldVocabSchema.concept_relationship r
    ON r.concept_id_1 = ci.descendant_concept_id
   AND r.relationship_id = 'Mapped from'
JOIN @oldVocabSchema.concept c
    ON c.concept_id = r.concept_id_2
   AND c.vocabulary_id IN (@includedSourceVocabs)

UNION ALL

SELECT
    cohortid AS cohort_definition_id,
    'new' AS concept_type,
    c.concept_id
FROM #new_vc ci
JOIN @newVocabSchema.concept_relationship r
    ON r.concept_id_1 = ci.descendant_concept_id
   AND r.relationship_id = 'Mapped from'
JOIN @newVocabSchema.concept c
    ON c.concept_id = r.concept_id_2
   AND c.vocabulary_id IN (@includedSourceVocabs);


DROP TABLE IF EXISTS @scratchSchema.concept_diff; -- this table was generated in postgres vocab server since it has new version of the vocabulary

SELECT
    cohort_definition_id,
    concept_id,
    CASE
        WHEN is_old = 1 AND is_new = 1 THEN 'same'
        WHEN is_old = 1 AND is_new = 0 THEN 'removed'
        WHEN is_old = 0 AND is_new = 1 THEN 'added'
        ELSE 'error'
    END AS concept_type
INTO @scratchSchema.concept_diff
FROM
(
    SELECT
        cohort_definition_id,
        concept_id,
        MAX(CASE WHEN concept_type = 'old' THEN 1 ELSE 0 END) AS is_old,
        MAX(CASE WHEN concept_type = 'new' THEN 1 ELSE 0 END) AS is_new
    FROM @scratchSchema.concepts_in_cohorts
    GROUP BY cohort_definition_id, concept_id
) t1;


DROP TABLE IF EXISTS @scratchSchema.concept_count_change;

SELECT
    cohort_definition_id,
    SUM(is_same)    AS same_concepts_count,
    SUM(is_removed) AS removed_concepts_count,
    SUM(is_added)   AS added_concepts_count
INTO @scratchSchema.concept_count_change
FROM
(
    SELECT
        cohort_definition_id,
        CASE WHEN concept_type = 'same'    THEN 1 ELSE 0 END AS is_same,
        CASE WHEN concept_type = 'removed' THEN 1 ELSE 0 END AS is_removed,
        CASE WHEN concept_type = 'added'   THEN 1 ELSE 0 END AS is_added
    FROM @scratchSchema.concept_diff
) t
GROUP BY cohort_definition_id;


DROP TABLE IF EXISTS @scratchSchema.cohort_vocab_change_summary;

SELECT
    cohort_definition_id,
    SUM(num_persons) AS total_persons,
    SUM(CASE WHEN has_same = 1 OR (has_new = 1 AND has_old = 1) THEN num_persons ELSE 0 END) AS same_persons,
    SUM(CASE WHEN has_same = 1 AND has_new = 0 AND has_old = 0 THEN num_persons ELSE 0 END) AS same_persons_no_change,
    SUM(CASE WHEN has_same = 1 OR (has_new = 1 AND has_old = 1) THEN num_persons ELSE 0 END)
      - SUM(CASE WHEN has_same = 1 AND has_new = 0 AND has_old = 0 THEN num_persons ELSE 0 END) AS same_persons_potential_index_misclassification,
    SUM(CASE WHEN has_same = 0 AND has_new = 1 AND has_old = 0 THEN num_persons ELSE 0 END) AS new_persons,
    SUM(CASE WHEN has_same = 0 AND has_new = 0 AND has_old = 1 THEN num_persons ELSE 0 END) AS lost_persons
INTO @scratchSchema.cohort_vocab_change_summary
FROM
(
    SELECT
        cohort_definition_id,
        has_same,
        has_new,
        has_old,
        COUNT(person_id) AS num_persons
    FROM
    (
        SELECT
            cohort_definition_id,
            person_id,
            MAX(CASE WHEN concept_type = 'same'    THEN 1 ELSE 0 END) AS has_same,
            MAX(CASE WHEN concept_type = 'added'   THEN 1 ELSE 0 END) AS has_new,
            MAX(CASE WHEN concept_type = 'removed' THEN 1 ELSE 0 END) AS has_old
        FROM
        (
            SELECT
                cd1.cohort_definition_id,
                co1.person_id,
                cd1.concept_type
            FROM @cdmSchema.condition_occurrence AS co1
            INNER JOIN @scratchSchema.concept_diff AS cd1
                ON co1.condition_source_concept_id = cd1.concept_id

            UNION ALL

            SELECT
                cd1.cohort_definition_id,
                po1.person_id,
                cd1.concept_type
            FROM @cdmSchema.procedure_occurrence AS po1
            INNER JOIN @scratchSchema.concept_diff AS cd1
                ON po1.procedure_source_concept_id = cd1.concept_id

            UNION ALL

            SELECT
                cd1.cohort_definition_id,
                de1.person_id,
                cd1.concept_type
            FROM @cdmSchema.drug_exposure AS de1
            INNER JOIN @scratchSchema.concept_diff AS cd1
                ON de1.drug_source_concept_id = cd1.concept_id

            UNION ALL

            SELECT
                cd1.cohort_definition_id,
                de1.person_id,
                cd1.concept_type
            FROM @cdmSchema.device_exposure AS de1
            INNER JOIN @scratchSchema.concept_diff AS cd1
                ON de1.device_source_concept_id = cd1.concept_id

            UNION ALL

            SELECT
                cd1.cohort_definition_id,
                m1.person_id,
                cd1.concept_type
            FROM @cdmSchema.measurement AS m1
            INNER JOIN @scratchSchema.concept_diff AS cd1
                ON m1.measurement_source_concept_id = cd1.concept_id

            UNION ALL

            SELECT
                cd1.cohort_definition_id,
                o1.person_id,
                cd1.concept_type
            FROM @cdmSchema.observation AS o1
            INNER JOIN @scratchSchema.concept_diff AS cd1
                ON o1.observation_source_concept_id = cd1.concept_id
        ) t0
        GROUP BY cohort_definition_id, person_id
    ) t1
    GROUP BY cohort_definition_id, has_same, has_new, has_old
) t2
GROUP BY cohort_definition_id;


-- summary output with cohort names

DROP TABLE IF EXISTS @scratchSchema.stats;

SELECT
    s.cohort_definition_id,
    n.cohortname,
    s.total_persons,
    s.same_persons,
    s.same_persons_no_change,
    s.same_persons_potential_index_misclassification,
    s.new_persons,
    s.lost_persons,
    cc.same_concepts_count,
    cc.removed_concepts_count,
    cc.added_concepts_count
INTO @scratchSchema.stats
FROM @scratchSchema.cohort_vocab_change_summary AS s
LEFT JOIN @scratchSchema.cohort_id_names AS n
    ON n.cohortid = s.cohort_definition_id
INNER JOIN @scratchSchema.concept_count_change AS cc
    ON cc.cohort_definition_id = s.cohort_definition_id
ORDER BY s.same_persons_no_change * 1.0 / s.total_persons;
