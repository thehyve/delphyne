measurement_indexes_full = {
    'ix_measurement_measurement_concept_id',
    'ix_measurement_visit_occurrence_id',
    'ix_measurement_person_id'
}
measurement_indexes_without_person_index = {
    'ix_measurement_measurement_concept_id',
    'ix_measurement_visit_occurrence_id'
}

all_specimen_objects = {
    'fk_specimen_anatomic_site_concept_id_concept',
    'fk_specimen_disease_status_concept_id_concept',
    'fk_specimen_person_id_person',
    'fk_specimen_specimen_concept_id_concept',
    'fk_specimen_specimen_type_concept_id_concept',
    'fk_specimen_unit_concept_id_concept',
    'ix_specimen_person_id',
    'ix_specimen_specimen_concept_id',
    'pk_specimen'
}

db_table_objects_full = {
    'fk_care_site_location_id_location',
    'fk_care_site_place_of_service_concept_id_concept',
    'fk_concept_ancestor_ancestor_concept_id_concept',
    'fk_concept_ancestor_descendant_concept_id_concept',
    'fk_concept_class_concept_class_concept_id_concept',
    'fk_concept_concept_class_id_concept_class',
    'fk_concept_domain_id_domain',
    'fk_concept_relationship_concept_id_1_concept',
    'fk_concept_relationship_concept_id_2_concept',
    'fk_concept_relationship_relationship_id_relationship',
    'fk_concept_synonym_concept_id_concept',
    'fk_concept_synonym_language_concept_id_concept',
    'fk_concept_vocabulary_id_vocabulary',
    'fk_condition_era_condition_concept_id_concept',
    'fk_condition_era_person_id_person',
    'fk_condition_occurrence_condition_concept_id_concept',
    'fk_condition_occurrence_condition_source_concept_id_concept',
    'fk_condition_occurrence_condition_status_concept_id_concept',
    'fk_condition_occurrence_condition_type_concept_id_concept',
    'fk_condition_occurrence_person_id_person',
    'fk_condition_occurrence_provider_id_provider',
    'fk_condition_occurrence_visit_detail_id_visit_detail',
    'fk_condition_occurrence_visit_occurrence_id_visit_occurrence',
    'fk_cost_cost_concept_id_concept',
    'fk_cost_cost_source_concept_id_concept',
    'fk_cost_cost_type_concept_id_concept',
    'fk_cost_currency_concept_id_concept',
    'fk_cost_drg_concept_id_concept',
    'fk_cost_payer_plan_period_id_payer_plan_period',
    'fk_cost_person_id_person',
    'fk_cost_revenue_code_concept_id_concept',
    'fk_device_exposure_device_concept_id_concept',
    'fk_device_exposure_device_source_concept_id_concept',
    'fk_device_exposure_device_type_concept_id_concept',
    'fk_device_exposure_person_id_person',
    'fk_device_exposure_provider_id_provider',
    'fk_device_exposure_visit_detail_id_visit_detail',
    'fk_device_exposure_visit_occurrence_id_visit_occurrence',
    'fk_domain_domain_concept_id_concept',
    'fk_dose_era_drug_concept_id_concept',
    'fk_dose_era_person_id_person',
    'fk_dose_era_unit_concept_id_concept',
    'fk_drug_era_drug_concept_id_concept',
    'fk_drug_era_person_id_person',
    'fk_drug_exposure_drug_concept_id_concept',
    'fk_drug_exposure_drug_source_concept_id_concept',
    'fk_drug_exposure_drug_type_concept_id_concept',
    'fk_drug_exposure_person_id_person',
    'fk_drug_exposure_provider_id_provider',
    'fk_drug_exposure_route_concept_id_concept',
    'fk_drug_exposure_visit_detail_id_visit_detail',
    'fk_drug_exposure_visit_occurrence_id_visit_occurrence',
    'fk_drug_strength_amount_unit_concept_id_concept',
    'fk_drug_strength_denominator_unit_concept_id_concept',
    'fk_drug_strength_drug_concept_id_concept',
    'fk_drug_strength_ingredient_concept_id_concept',
    'fk_drug_strength_numerator_unit_concept_id_concept',
    'fk_fact_relationship_domain_concept_id_1_concept',
    'fk_fact_relationship_domain_concept_id_2_concept',
    'fk_fact_relationship_relationship_concept_id_concept',
    'fk_location_history_location_id_location',
    'fk_location_history_relationship_type_concept_id_concept',
    'fk_location_region_concept_id_concept',
    'fk_measurement_measurement_concept_id_concept',
    'fk_measurement_measurement_source_concept_id_concept',
    'fk_measurement_measurement_type_concept_id_concept',
    'fk_measurement_operator_concept_id_concept',
    'fk_measurement_person_id_person',
    'fk_measurement_provider_id_provider',
    'fk_measurement_unit_concept_id_concept',
    'fk_measurement_value_as_concept_id_concept',
    'fk_measurement_visit_detail_id_visit_detail',
    'fk_measurement_visit_occurrence_id_visit_occurrence',
    'fk_note_encoding_concept_id_concept',
    'fk_note_language_concept_id_concept',
    'fk_note_nlp_note_id_note',
    'fk_note_nlp_note_nlp_concept_id_concept',
    'fk_note_nlp_note_nlp_source_concept_id_concept',
    'fk_note_nlp_section_concept_id_concept',
    'fk_note_note_class_concept_id_concept',
    'fk_note_note_type_concept_id_concept',
    'fk_note_person_id_person',
    'fk_note_provider_id_provider',
    'fk_note_visit_detail_id_visit_detail',
    'fk_note_visit_occurrence_id_visit_occurrence',
    'fk_observation_obs_event_field_concept_id_concept',
    'fk_observation_observation_concept_id_concept',
    'fk_observation_observation_source_concept_id_concept',
    'fk_observation_observation_type_concept_id_concept',
    'fk_observation_period_period_type_concept_id_concept',
    'fk_observation_period_person_id_person',
    'fk_observation_person_id_person',
    'fk_observation_provider_id_provider',
    'fk_observation_qualifier_concept_id_concept',
    'fk_observation_unit_concept_id_concept',
    'fk_observation_value_as_concept_id_concept',
    'fk_observation_visit_detail_id_visit_detail',
    'fk_observation_visit_occurrence_id_visit_occurrence',
    'fk_payer_plan_period_contract_concept_id_concept',
    'fk_payer_plan_period_contract_person_id_person',
    'fk_payer_plan_period_contract_source_concept_id_concept',
    'fk_payer_plan_period_payer_concept_id_concept',
    'fk_payer_plan_period_payer_source_concept_id_concept',
    'fk_payer_plan_period_person_id_person',
    'fk_payer_plan_period_plan_concept_id_concept',
    'fk_payer_plan_period_plan_source_concept_id_concept',
    'fk_payer_plan_period_sponsor_concept_id_concept',
    'fk_payer_plan_period_sponsor_source_concept_id_concept',
    'fk_payer_plan_period_stop_reason_concept_id_concept',
    'fk_payer_plan_period_stop_reason_source_concept_id_concept',
    'fk_person_care_site_id_care_site',
    'fk_person_ethnicity_concept_id_concept',
    'fk_person_ethnicity_source_concept_id_concept',
    'fk_person_gender_concept_id_concept',
    'fk_person_gender_source_concept_id_concept',
    'fk_person_location_id_location',
    'fk_person_provider_id_provider',
    'fk_person_race_concept_id_concept',
    'fk_person_race_source_concept_id_concept',
    'fk_procedure_occurrence_modifier_concept_id_concept',
    'fk_procedure_occurrence_person_id_person',
    'fk_procedure_occurrence_procedure_concept_id_concept',
    'fk_procedure_occurrence_procedure_source_concept_id_concept',
    'fk_procedure_occurrence_procedure_type_concept_id_concept',
    'fk_procedure_occurrence_provider_id_provider',
    'fk_procedure_occurrence_visit_detail_id_visit_detail',
    'fk_procedure_occurrence_visit_occurrence_id_visit_occurrence',
    'fk_provider_care_site_id_care_site',
    'fk_provider_gender_concept_id_concept',
    'fk_provider_gender_source_concept_id_concept',
    'fk_provider_specialty_concept_id_concept',
    'fk_provider_specialty_source_concept_id_concept',
    'fk_relationship_relationship_concept_id_concept',
    'fk_relationship_reverse_relationship_id_relationship',
    'fk_source_to_concept_map_source_vocabulary_id_vocabulary',
    'fk_source_to_concept_map_target_concept_id_concept',
    'fk_source_to_concept_map_target_vocabulary_id_vocabulary',
    'fk_specimen_anatomic_site_concept_id_concept',
    'fk_specimen_disease_status_concept_id_concept',
    'fk_specimen_person_id_person',
    'fk_specimen_specimen_concept_id_concept',
    'fk_specimen_specimen_type_concept_id_concept',
    'fk_specimen_unit_concept_id_concept',
    'fk_stem_table_anatomic_site_concept_id_concept',
    'fk_stem_table_concept_id_concept',
    'fk_stem_table_condition_status_concept_id_concept',
    'fk_stem_table_disease_status_concept_id_concept',
    'fk_stem_table_domain_id_domain',
    'fk_stem_table_event_field_concept_id_concept',
    'fk_stem_table_modifier_concept_id_concept',
    'fk_stem_table_operator_concept_id_concept',
    'fk_stem_table_person_id_person',
    'fk_stem_table_provider_id_provider',
    'fk_stem_table_qualifier_concept_id_concept',
    'fk_stem_table_route_concept_id_concept',
    'fk_stem_table_source_concept_id_concept',
    'fk_stem_table_type_concept_id_concept',
    'fk_stem_table_unit_concept_id_concept',
    'fk_stem_table_value_as_concept_id_concept',
    'fk_stem_table_visit_detail_id_visit_detail',
    'fk_stem_table_visit_occurrence_id_visit_occurrence',
    'fk_survey_conduct_assisted_concept_id_concept',
    'fk_survey_conduct_collection_method_concept_id_concept',
    'fk_survey_conduct_person_id_person',
    'fk_survey_conduct_provider_id_provider',
    'fk_survey_conduct_respondent_type_concept_id_concept',
    'fk_survey_conduct_response_visit_occurrence_id_visit_occurrence',
    'fk_survey_conduct_survey_concept_id_concept',
    'fk_survey_conduct_survey_source_concept_id_concept',
    'fk_survey_conduct_timing_concept_id_concept',
    'fk_survey_conduct_validated_survey_concept_id_concept',
    'fk_survey_conduct_visit_detail_id_visit_detail',
    'fk_survey_conduct_visit_occurrence_id_visit_occurrence',
    'fk_visit_detail_admitted_from_concept_id_concept',
    'fk_visit_detail_care_site_id_care_site',
    'fk_visit_detail_discharge_to_concept_id_concept',
    'fk_visit_detail_person_id_person',
    'fk_visit_detail_preceding_visit_detail_id_visit_detail',
    'fk_visit_detail_provider_id_provider',
    'fk_visit_detail_visit_detail_concept_id_concept',
    'fk_visit_detail_visit_detail_parent_id_visit_detail',
    'fk_visit_detail_visit_detail_source_concept_id_concept',
    'fk_visit_detail_visit_detail_type_concept_id_concept',
    'fk_visit_detail_visit_occurrence_id_visit_occurrence',
    'fk_visit_occurrence_admitted_from_concept_id_concept',
    'fk_visit_occurrence_care_site_id_care_site',
    'fk_visit_occurrence_discharge_to_concept_id_concept',
    'fk_visit_occurrence_person_id_person',
    'fk_visit_occurrence_preceding_visit_occurrence_id_visit_a59e',
    'fk_visit_occurrence_provider_id_provider',
    'fk_visit_occurrence_visit_concept_id_concept',
    'fk_visit_occurrence_visit_source_concept_id_concept',
    'fk_visit_occurrence_visit_type_concept_id_concept',
    'fk_vocabulary_vocabulary_concept_id_concept',
    'ix_concept_ancestor_ancestor_concept_id',
    'ix_concept_ancestor_descendant_concept_id',
    'ix_concept_concept_class_id',
    'ix_concept_concept_code',
    'ix_concept_domain_id',
    'ix_concept_relationship_concept_id_1',
    'ix_concept_relationship_concept_id_2',
    'ix_concept_relationship_relationship_id',
    'ix_concept_synonym_concept_id',
    'ix_concept_vocabulary_id',
    'ix_condition_era_condition_concept_id',
    'ix_condition_era_person_id',
    'ix_condition_occurrence_condition_concept_id',
    'ix_condition_occurrence_person_id',
    'ix_condition_occurrence_visit_occurrence_id',
    'ix_cost_person_id',
    'ix_device_exposure_device_concept_id',
    'ix_device_exposure_person_id',
    'ix_device_exposure_visit_occurrence_id',
    'ix_dose_era_drug_concept_id',
    'ix_dose_era_person_id',
    'ix_drug_era_drug_concept_id',
    'ix_drug_era_person_id',
    'ix_drug_exposure_drug_concept_id',
    'ix_drug_exposure_person_id',
    'ix_drug_exposure_visit_occurrence_id',
    'ix_drug_strength_drug_concept_id',
    'ix_drug_strength_ingredient_concept_id',
    'ix_fact_relationship_domain_concept_id_1',
    'ix_fact_relationship_domain_concept_id_2',
    'ix_fact_relationship_relationship_concept_id',
    'ix_measurement_measurement_concept_id',
    'ix_measurement_person_id',
    'ix_measurement_visit_occurrence_id',
    'ix_metadata_metadata_concept_id',
    'ix_note_nlp_note_id',
    'ix_note_nlp_note_nlp_concept_id',
    'ix_note_note_type_concept_id',
    'ix_note_person_id',
    'ix_note_visit_occurrence_id',
    'ix_observation_observation_concept_id',
    'ix_observation_period_person_id',
    'ix_observation_person_id',
    'ix_observation_visit_occurrence_id',
    'ix_payer_plan_period_person_id',
    'ix_procedure_occurrence_person_id',
    'ix_procedure_occurrence_procedure_concept_id',
    'ix_procedure_occurrence_visit_occurrence_id',
    'ix_source_to_concept_map_source_code',
    'ix_source_to_concept_map_source_vocabulary_id',
    'ix_source_to_concept_map_target_concept_id',
    'ix_source_to_concept_map_target_vocabulary_id',
    'ix_specimen_person_id',
    'ix_specimen_specimen_concept_id',
    'ix_stem_table_concept_id',
    'ix_stem_table_person_id',
    'ix_stem_table_visit_occurrence_id',
    'ix_survey_conduct_person_id',
    'ix_visit_detail_person_id',
    'ix_visit_detail_visit_detail_concept_id',
    'ix_visit_occurrence_person_id',
    'ix_visit_occurrence_visit_concept_id',
    'pk_care_site',
    'pk_cdm_source',
    'pk_concept',
    'pk_concept_ancestor',
    'pk_concept_class',
    'pk_concept_relationship',
    'pk_concept_synonym',
    'pk_condition_era',
    'pk_condition_occurrence',
    'pk_cost',
    'pk_device_exposure',
    'pk_domain',
    'pk_dose_era',
    'pk_drug_era',
    'pk_drug_exposure',
    'pk_drug_strength',
    'pk_fact_relationship',
    'pk_location',
    'pk_location_history',
    'pk_measurement',
    'pk_metadata',
    'pk_note',
    'pk_note_nlp',
    'pk_observation',
    'pk_observation_period',
    'pk_payer_plan_period',
    'pk_person',
    'pk_procedure_occurrence',
    'pk_provider',
    'pk_relationship',
    'pk_source_to_concept_map',
    'pk_specimen',
    'pk_stem_table',
    'pk_survey_conduct',
    'pk_visit_detail',
    'pk_visit_occurrence',
    'pk_vocabulary',
}

vocab_table_objects = {
    'fk_concept_ancestor_ancestor_concept_id_concept',
    'fk_concept_ancestor_descendant_concept_id_concept',
    'fk_concept_class_concept_class_concept_id_concept',
    'fk_concept_concept_class_id_concept_class',
    'fk_concept_domain_id_domain',
    'fk_concept_relationship_concept_id_1_concept',
    'fk_concept_relationship_concept_id_2_concept',
    'fk_concept_relationship_relationship_id_relationship',
    'fk_concept_synonym_concept_id_concept',
    'fk_concept_synonym_language_concept_id_concept',
    'fk_concept_vocabulary_id_vocabulary',
    'fk_domain_domain_concept_id_concept',
    'fk_drug_strength_amount_unit_concept_id_concept',
    'fk_drug_strength_denominator_unit_concept_id_concept',
    'fk_drug_strength_drug_concept_id_concept',
    'fk_drug_strength_ingredient_concept_id_concept',
    'fk_drug_strength_numerator_unit_concept_id_concept',
    'fk_relationship_relationship_concept_id_concept',
    'fk_relationship_reverse_relationship_id_relationship',
    'fk_source_to_concept_map_source_vocabulary_id_vocabulary',
    'fk_source_to_concept_map_target_concept_id_concept',
    'fk_source_to_concept_map_target_vocabulary_id_vocabulary',
    'fk_vocabulary_vocabulary_concept_id_concept',
    'ix_concept_ancestor_ancestor_concept_id',
    'ix_concept_ancestor_descendant_concept_id',
    'ix_concept_concept_class_id',
    'ix_concept_concept_code',
    'ix_concept_domain_id',
    'ix_concept_relationship_concept_id_1',
    'ix_concept_relationship_concept_id_2',
    'ix_concept_relationship_relationship_id',
    'ix_concept_synonym_concept_id',
    'ix_concept_vocabulary_id',
    'ix_drug_strength_drug_concept_id',
    'ix_drug_strength_ingredient_concept_id',
    'ix_source_to_concept_map_source_code',
    'ix_source_to_concept_map_source_vocabulary_id',
    'ix_source_to_concept_map_target_concept_id',
    'ix_source_to_concept_map_target_vocabulary_id',
    'pk_concept',
    'pk_concept_ancestor',
    'pk_concept_class',
    'pk_concept_relationship',
    'pk_concept_synonym',
    'pk_domain',
    'pk_drug_strength',
    'pk_relationship',
    'pk_source_to_concept_map',
    'pk_vocabulary',
}
