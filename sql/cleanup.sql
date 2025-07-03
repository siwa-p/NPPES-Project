create or replace procedure createtable()
language plpgsql
as $$
BEGIN
    DROP TABLE IF EXISTS nppes_subset cascade;
    create table nppes_subset (
        npi bigint primary key not null,
        entity_type_code integer,
        entity_name varchar,
        practice_address varchar,
        postal_code varchar,
        primary_taxonomy_code varchar  
    );

    INSERT INTO nppes_subset (
        npi,
        entity_type_code,
        entity_name,
        practice_address,
        postal_code,
        primary_taxonomy_code
    )
    SELECT
        np.npi,
        CAST(NULLIF(np.entity_type_code, '') AS INTEGER),
        COALESCE(
            NULLIF(np.provider_organization_name_legal_business_name, ''),
            CONCAT_WS(
                ' ',
                NULLIF(np.provider_name_prefix_text, ''),
                NULLIF(np.provider_first_name, ''),
                NULLIF(np.provider_middle_name, ''),
                NULLIF(np.provider_last_name_legal_name, ''),
                NULLIF(np.provider_name_suffix_text, ''),
                NULLIF(np.provider_credential_text, '')
            ),
            nullif(np.provider_other_organization_name, ''),
            concat_ws(
                ' ',
                nullif(np.provider_other_name_prefix_text, ''),
                nullif(np.provider_other_first_name, ''),
                nullif(np.provider_other_middle_name, ''),
                nullif(np.provider_other_last_name, ''),
                nullif(np.provider_other_name_suffix_text, '')
            )
        ),
        CONCAT_WS(
            ' ',
            NULLIF(np.provider_first_line_business_practice_location_address, ''),
            NULLIF(np.provider_second_line_business_practice_location_address, ''),
            NULLIF(np.provider_business_practice_location_address_city_name, ''),
            NULLIF(np.provider_business_practice_location_address_state_name, '')
        ),
        np.provider_business_practice_location_address_postal_code,
        COALESCE(
            CASE WHEN np.healthcare_provider_primary_taxonomy_switch_1 = 'Y' THEN np.healthcare_provider_taxonomy_code_1 END,
            CASE WHEN np.healthcare_provider_primary_taxonomy_switch_2 = 'Y' THEN np.healthcare_provider_taxonomy_code_2 END,
            CASE WHEN np.healthcare_provider_primary_taxonomy_switch_3 = 'Y' THEN np.healthcare_provider_taxonomy_code_3 END,
            CASE WHEN np.healthcare_provider_primary_taxonomy_switch_4 = 'Y' THEN np.healthcare_provider_taxonomy_code_4 END,
            CASE WHEN np.healthcare_provider_primary_taxonomy_switch_5 = 'Y' THEN np.healthcare_provider_taxonomy_code_5 END,
            CASE WHEN np.healthcare_provider_primary_taxonomy_switch_6 = 'Y' THEN np.healthcare_provider_taxonomy_code_6 END,
            CASE WHEN np.healthcare_provider_primary_taxonomy_switch_7 = 'Y' THEN np.healthcare_provider_taxonomy_code_7 END,
            CASE WHEN np.healthcare_provider_primary_taxonomy_switch_8 = 'Y' THEN np.healthcare_provider_taxonomy_code_8 END,
            CASE WHEN np.healthcare_provider_primary_taxonomy_switch_9 = 'Y' THEN np.healthcare_provider_taxonomy_code_9 END,
            CASE WHEN np.healthcare_provider_primary_taxonomy_switch_10 = 'Y' THEN np.healthcare_provider_taxonomy_code_10 END,
            CASE WHEN np.healthcare_provider_primary_taxonomy_switch_11 = 'Y' THEN np.healthcare_provider_taxonomy_code_11 END,
            CASE WHEN np.healthcare_provider_primary_taxonomy_switch_12 = 'Y' THEN np.healthcare_provider_taxonomy_code_12 END,
            CASE WHEN np.healthcare_provider_primary_taxonomy_switch_13 = 'Y' THEN np.healthcare_provider_taxonomy_code_13 END,
            CASE WHEN np.healthcare_provider_primary_taxonomy_switch_14 = 'Y' THEN np.healthcare_provider_taxonomy_code_14 END,
            CASE WHEN np.healthcare_provider_primary_taxonomy_switch_15 = 'Y' THEN np.healthcare_provider_taxonomy_code_15 END
        )
    FROM nppes np;
end
$$;

CREATE OR REPLACE PROCEDURE create_table_view()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Create complete records view
    CREATE OR REPLACE VIEW view_records AS
    select nps.npi,
        nps.entity_type_code,
        nps.entity_name,
        nps.practice_address,
        nu.classification,
        nu.grouping,
        nu.specialization
    from nppes_subset nps
    left join nucc nu
    on nu.code = nps.primary_taxonomy_code
    order by nps.npi;
    
    RAISE NOTICE 'Created table views';
END;
$$;



select * from view_records