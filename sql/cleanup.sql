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
        zip_code varchar,
        primary_taxonomy_code varchar  
    );

    INSERT INTO nppes_subset (
        npi,
        entity_type_code,
        entity_name,
        practice_address,
        zip_code,
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
        CASE 
            WHEN length(np.provider_business_practice_location_address_postal_code) = 0
                THEN NULL
            WHEN length(np.provider_business_practice_location_address_postal_code) < 9
                THEN LPAD(np.provider_business_practice_location_address_postal_code, 5, '0')
            WHEN length(np.provider_business_practice_location_address_postal_code) = 9
                THEN SUBSTRING(np.provider_business_practice_location_address_postal_code, 1, 5)
            ELSE NULL
        END AS zip_code,
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


create or replace procedure merge_county()
language plpgsql
as $$
begin
    create or replace view view_county as

    WITH fips_with_population AS (
        SELECT f.fipscounty, f.countyname_fips, c.population
        FROM county_pop c
        INNER JOIN fips f
            ON (c.state || c.county) = lpad(f.fipscounty::VARCHAR, 5, '0')
    ),
    ranked_zip AS (
        SELECT z.zip, z.county, f.countyname_fips, f.population,
               rank() OVER (PARTITION BY z.zip ORDER BY f.population DESC) AS rank
        FROM zip z
        INNER JOIN fips_with_population f
            ON z.county = f.fipscounty
    )
    SELECT
        np.npi,
        np.entity_type_code,
        np.entity_name,
        np.practice_address,
        np.zip_code,
        rz.countyname_fips AS county_name,
        rz.population,
        nu.classification,
        nu.grouping,
        nu.specialization
    FROM nppes_subset np
    LEFT JOIN ranked_zip rz
        ON LPAD(rz.zip::VARCHAR, 5, '0') = np.zip_code
       AND rz.rank = 1
    LEFT JOIN nucc nu
        ON nu.code = np.primary_taxonomy_code;
end
$$;

select * from view_county;