{{ config(
    materialized='table',
    schema='silver',
    alias='census_g01'
) }}

WITH cleaned AS (
    SELECT
        TRIM(lga_code_2016) AS lga_code_2016,

        -- Total population
        CASE WHEN tot_p_p >= 0 THEN tot_p_p ELSE NULL END AS tot_p_p,
        CASE WHEN tot_p_m >= 0 THEN tot_p_m ELSE NULL END AS tot_p_m,
        CASE WHEN tot_p_f >= 0 THEN tot_p_f ELSE NULL END AS tot_p_f,

        -- Age groups
        COALESCE(age_0_4_yr_m,0) AS age_0_4_yr_m,
        COALESCE(age_0_4_yr_f,0) AS age_0_4_yr_f,
        COALESCE(age_0_4_yr_p,0) AS age_0_4_yr_p,
        COALESCE(age_5_14_yr_m,0) AS age_5_14_yr_m,
        COALESCE(age_5_14_yr_f,0) AS age_5_14_yr_f,
        COALESCE(age_5_14_yr_p,0) AS age_5_14_yr_p,
        COALESCE(age_15_19_yr_m,0) AS age_15_19_yr_m,
        COALESCE(age_15_19_yr_f,0) AS age_15_19_yr_f,
        COALESCE(age_15_19_yr_p,0) AS age_15_19_yr_p,
        COALESCE(age_20_24_yr_m,0) AS age_20_24_yr_m,
        COALESCE(age_20_24_yr_f,0) AS age_20_24_yr_f,
        COALESCE(age_20_24_yr_p,0) AS age_20_24_yr_p,
        COALESCE(age_25_34_yr_m,0) AS age_25_34_yr_m,
        COALESCE(age_25_34_yr_f,0) AS age_25_34_yr_f,
        COALESCE(age_25_34_yr_p,0) AS age_25_34_yr_p,
        COALESCE(age_35_44_yr_m,0) AS age_35_44_yr_m,
        COALESCE(age_35_44_yr_f,0) AS age_35_44_yr_f,
        COALESCE(age_35_44_yr_p,0) AS age_35_44_yr_p,
        COALESCE(age_45_54_yr_m,0) AS age_45_54_yr_m,
        COALESCE(age_45_54_yr_f,0) AS age_45_54_yr_f,
        COALESCE(age_45_54_yr_p,0) AS age_45_54_yr_p,
        COALESCE(age_55_64_yr_m,0) AS age_55_64_yr_m,
        COALESCE(age_55_64_yr_f,0) AS age_55_64_yr_f,
        COALESCE(age_55_64_yr_p,0) AS age_55_64_yr_p,
        COALESCE(age_65_74_yr_m,0) AS age_65_74_yr_m,
        COALESCE(age_65_74_yr_f,0) AS age_65_74_yr_f,
        COALESCE(age_65_74_yr_p,0) AS age_65_74_yr_p,
        COALESCE(age_75_84_yr_m,0) AS age_75_84_yr_m,
        COALESCE(age_75_84_yr_f,0) AS age_75_84_yr_f,
        COALESCE(age_75_84_yr_p,0) AS age_75_84_yr_p,
        COALESCE(age_85ov_m,0) AS age_85ov_m,
        COALESCE(age_85ov_f,0) AS age_85ov_f,
        COALESCE(age_85ov_p,0) AS age_85ov_p,

        -- Census night and other location counts
        COALESCE(counted_census_night_home_m,0) AS counted_census_night_home_m,
        COALESCE(counted_census_night_home_f,0) AS counted_census_night_home_f,
        COALESCE(counted_census_night_home_p,0) AS counted_census_night_home_p,
        COALESCE(count_census_nt_ewhere_aust_m,0) AS count_census_nt_ewhere_aust_m,
        COALESCE(count_census_nt_ewhere_aust_f,0) AS count_census_nt_ewhere_aust_f,
        COALESCE(count_census_nt_ewhere_aust_p,0) AS count_census_nt_ewhere_aust_p,

        -- Indigenous population
        COALESCE(indigenous_psns_aboriginal_m,0) AS indigenous_psns_aboriginal_m,
        COALESCE(indigenous_psns_aboriginal_f,0) AS indigenous_psns_aboriginal_f,
        COALESCE(indigenous_psns_aboriginal_p,0) AS indigenous_psns_aboriginal_p,
        COALESCE(indig_psns_torres_strait_is_m,0) AS indig_psns_torres_strait_is_m,
        COALESCE(indig_psns_torres_strait_is_f,0) AS indig_psns_torres_strait_is_f,
        COALESCE(indig_psns_torres_strait_is_p,0) AS indig_psns_torres_strait_is_p,
        COALESCE(indig_bth_abor_torres_st_is_m,0) AS indig_bth_abor_torres_st_is_m,
        COALESCE(indig_bth_abor_torres_st_is_f,0) AS indig_bth_abor_torres_st_is_f,
        COALESCE(indig_bth_abor_torres_st_is_p,0) AS indig_bth_abor_torres_st_is_p,
        COALESCE(indigenous_p_tot_m,0) AS indigenous_p_tot_m,
        COALESCE(indigenous_p_tot_f,0) AS indigenous_p_tot_f,
        COALESCE(indigenous_p_tot_p,0) AS indigenous_p_tot_p,

        -- Birthplace & language
        COALESCE(birthplace_australia_m,0) AS birthplace_australia_m,
        COALESCE(birthplace_australia_f,0) AS birthplace_australia_f,
        COALESCE(birthplace_australia_p,0) AS birthplace_australia_p,
        COALESCE(birthplace_elsewhere_m,0) AS birthplace_elsewhere_m,
        COALESCE(birthplace_elsewhere_f,0) AS birthplace_elsewhere_f,
        COALESCE(birthplace_elsewhere_p,0) AS birthplace_elsewhere_p,
        COALESCE(lang_spoken_home_eng_only_m,0) AS lang_spoken_home_eng_only_m,
        COALESCE(lang_spoken_home_eng_only_f,0) AS lang_spoken_home_eng_only_f,
        COALESCE(lang_spoken_home_eng_only_p,0) AS lang_spoken_home_eng_only_p,
        COALESCE(lang_spoken_home_oth_lang_m,0) AS lang_spoken_home_oth_lang_m,
        COALESCE(lang_spoken_home_oth_lang_f,0) AS lang_spoken_home_oth_lang_f,
        COALESCE(lang_spoken_home_oth_lang_p,0) AS lang_spoken_home_oth_lang_p,
        COALESCE(australian_citizen_m,0) AS australian_citizen_m,
        COALESCE(australian_citizen_f,0) AS australian_citizen_f,
        COALESCE(australian_citizen_p,0) AS australian_citizen_p,

        -- Education
        COALESCE(age_psns_att_educ_inst_0_4_m,0) AS age_psns_att_educ_inst_0_4_m,
        COALESCE(age_psns_att_educ_inst_0_4_f,0) AS age_psns_att_educ_inst_0_4_f,
        COALESCE(age_psns_att_educ_inst_0_4_p,0) AS age_psns_att_educ_inst_0_4_p,
        COALESCE(age_psns_att_educ_inst_5_14_m,0) AS age_psns_att_educ_inst_5_14_m,
        COALESCE(age_psns_att_educ_inst_5_14_f,0) AS age_psns_att_educ_inst_5_14_f,
        COALESCE(age_psns_att_educ_inst_5_14_p,0) AS age_psns_att_educ_inst_5_14_p,
        COALESCE(age_psns_att_edu_inst_15_19_m,0) AS age_psns_att_edu_inst_15_19_m,
        COALESCE(age_psns_att_edu_inst_15_19_f,0) AS age_psns_att_edu_inst_15_19_f,
        COALESCE(age_psns_att_edu_inst_15_19_p,0) AS age_psns_att_edu_inst_15_19_p,
        COALESCE(age_psns_att_edu_inst_20_24_m,0) AS age_psns_att_edu_inst_20_24_m,
        COALESCE(age_psns_att_edu_inst_20_24_f,0) AS age_psns_att_edu_inst_20_24_f,
        COALESCE(age_psns_att_edu_inst_20_24_p,0) AS age_psns_att_edu_inst_20_24_p,
        COALESCE(age_psns_att_edu_inst_25_ov_m,0) AS age_psns_att_edu_inst_25_ov_m,
        COALESCE(age_psns_att_edu_inst_25_ov_f,0) AS age_psns_att_edu_inst_25_ov_f,
        COALESCE(age_psns_att_edu_inst_25_ov_p,0) AS age_psns_att_edu_inst_25_ov_p,
        COALESCE(high_yr_schl_comp_yr_12_eq_m,0) AS high_yr_schl_comp_yr_12_eq_m,
        COALESCE(high_yr_schl_comp_yr_12_eq_f,0) AS high_yr_schl_comp_yr_12_eq_f,
        COALESCE(high_yr_schl_comp_yr_12_eq_p,0) AS high_yr_schl_comp_yr_12_eq_p,
        COALESCE(high_yr_schl_comp_yr_11_eq_m,0) AS high_yr_schl_comp_yr_11_eq_m,
        COALESCE(high_yr_schl_comp_yr_11_eq_f,0) AS high_yr_schl_comp_yr_11_eq_f,
        COALESCE(high_yr_schl_comp_yr_11_eq_p,0) AS high_yr_schl_comp_yr_11_eq_p,
        COALESCE(high_yr_schl_comp_yr_10_eq_m,0) AS high_yr_schl_comp_yr_10_eq_m,
        COALESCE(high_yr_schl_comp_yr_10_eq_f,0) AS high_yr_schl_comp_yr_10_eq_f,
        COALESCE(high_yr_schl_comp_yr_10_eq_p,0) AS high_yr_schl_comp_yr_10_eq_p,
        COALESCE(high_yr_schl_comp_yr_9_eq_m,0) AS high_yr_schl_comp_yr_9_eq_m,
        COALESCE(high_yr_schl_comp_yr_9_eq_f,0) AS high_yr_schl_comp_yr_9_eq_f,
        COALESCE(high_yr_schl_comp_yr_9_eq_p,0) AS high_yr_schl_comp_yr_9_eq_p,
        COALESCE(high_yr_schl_comp_yr_8_belw_m,0) AS high_yr_schl_comp_yr_8_belw_m,
        COALESCE(high_yr_schl_comp_yr_8_belw_f,0) AS high_yr_schl_comp_yr_8_belw_f,
        COALESCE(high_yr_schl_comp_yr_8_belw_p,0) AS high_yr_schl_comp_yr_8_belw_p,
        COALESCE(high_yr_schl_comp_d_n_g_sch_m,0) AS high_yr_schl_comp_d_n_g_sch_m,
        COALESCE(high_yr_schl_comp_d_n_g_sch_f,0) AS high_yr_schl_comp_d_n_g_sch_f,
        COALESCE(high_yr_schl_comp_d_n_g_sch_p,0) AS high_yr_schl_comp_d_n_g_sch_p,

        -- Dwelling
        COALESCE(count_psns_occ_priv_dwgs_m,0) AS count_psns_occ_priv_dwgs_m,
        COALESCE(count_psns_occ_priv_dwgs_f,0) AS count_psns_occ_priv_dwgs_f,
        COALESCE(count_psns_occ_priv_dwgs_p,0) AS count_psns_occ_priv_dwgs_p,
        COALESCE(count_persons_other_dwgs_m,0) AS count_persons_other_dwgs_m,
        COALESCE(count_persons_other_dwgs_f,0) AS count_persons_other_dwgs_f,
        COALESCE(count_persons_other_dwgs_p,0) AS count_persons_other_dwgs_p

    FROM bronze.census_g01
)

SELECT *
FROM cleaned
WHERE lga_code_2016 IS NOT NULL
