
drop table if exists nppes_subset;
CREATE TABLE nppes_subset AS
SELECT
    np."npi",
    cast(nullif(np."entity_type_code", '') as integer) as entity_type_code,
    np."provider_organization_name_legal_business_name",
    np."provider_last_name_legal_name",
    np."provider_first_name",
    np."provider_middle_name",
    np."provider_name_prefix_text",
    np."provider_name_suffix_text",
    np."provider_credential_text",
    np."provider_first_line_business_practice_location_address",
    np."provider_second_line_business_practice_location_address",
    np."provider_business_practice_location_address_city_name",
    np."provider_business_practice_location_address_state_name",
    np."provider_business_practice_location_address_postal_code",
    np."healthcare_provider_taxonomy_code_1",
    np."healthcare_provider_primary_taxonomy_switch_1",
    np."healthcare_provider_taxonomy_code_2",
    np."healthcare_provider_primary_taxonomy_switch_2",
    np."healthcare_provider_taxonomy_code_3",
    np."healthcare_provider_primary_taxonomy_switch_3",
    np."healthcare_provider_taxonomy_code_4",
    np."healthcare_provider_primary_taxonomy_switch_4",
    np."healthcare_provider_taxonomy_code_5",
    np."healthcare_provider_primary_taxonomy_switch_5",
    np."healthcare_provider_taxonomy_code_6",
    np."healthcare_provider_primary_taxonomy_switch_6",
    np."healthcare_provider_taxonomy_code_7",
    np."healthcare_provider_primary_taxonomy_switch_7",
    np."healthcare_provider_taxonomy_code_8",
    np."healthcare_provider_primary_taxonomy_switch_8",
    np."healthcare_provider_taxonomy_code_9",
    np."healthcare_provider_primary_taxonomy_switch_9",
    np."healthcare_provider_taxonomy_code_10",
    np."healthcare_provider_primary_taxonomy_switch_10",
    np."healthcare_provider_taxonomy_code_11",
    np."healthcare_provider_primary_taxonomy_switch_11",
    np."healthcare_provider_taxonomy_code_12",
    np."healthcare_provider_primary_taxonomy_switch_12",
    np."healthcare_provider_taxonomy_code_13",
    np."healthcare_provider_primary_taxonomy_switch_13",
    np."healthcare_provider_taxonomy_code_14",
    np."healthcare_provider_primary_taxonomy_switch_14",
    np."healthcare_provider_taxonomy_code_15",
    np."healthcare_provider_primary_taxonomy_switch_15"
FROM nppes np;


ALTER TABLE nppes_subset ADD COLUMN primary_taxonomy_code varchar(32);

UPDATE nppes_subset
SET primary_taxonomy_code = COALESCE(
    CASE WHEN healthcare_provider_primary_taxonomy_switch_1 = 'Y' THEN healthcare_provider_taxonomy_code_1 END,
    CASE WHEN healthcare_provider_primary_taxonomy_switch_2 = 'Y' THEN healthcare_provider_taxonomy_code_2 END,
    CASE WHEN healthcare_provider_primary_taxonomy_switch_3 = 'Y' THEN healthcare_provider_taxonomy_code_3 END,
    CASE WHEN healthcare_provider_primary_taxonomy_switch_4 = 'Y' THEN healthcare_provider_taxonomy_code_4 END,
    CASE WHEN healthcare_provider_primary_taxonomy_switch_5 = 'Y' THEN healthcare_provider_taxonomy_code_5 END,
    CASE WHEN healthcare_provider_primary_taxonomy_switch_6 = 'Y' THEN healthcare_provider_taxonomy_code_6 END,
    CASE WHEN healthcare_provider_primary_taxonomy_switch_7 = 'Y' THEN healthcare_provider_taxonomy_code_7 END,
    CASE WHEN healthcare_provider_primary_taxonomy_switch_8 = 'Y' THEN healthcare_provider_taxonomy_code_8 END,
    CASE WHEN healthcare_provider_primary_taxonomy_switch_9 = 'Y' THEN healthcare_provider_taxonomy_code_9 END,
    CASE WHEN healthcare_provider_primary_taxonomy_switch_10 = 'Y' THEN healthcare_provider_taxonomy_code_10 END,
    CASE WHEN healthcare_provider_primary_taxonomy_switch_11 = 'Y' THEN healthcare_provider_taxonomy_code_11 END,
    CASE WHEN healthcare_provider_primary_taxonomy_switch_12 = 'Y' THEN healthcare_provider_taxonomy_code_12 END,
    CASE WHEN healthcare_provider_primary_taxonomy_switch_13 = 'Y' THEN healthcare_provider_taxonomy_code_13 END,
    CASE WHEN healthcare_provider_primary_taxonomy_switch_14 = 'Y' THEN healthcare_provider_taxonomy_code_14 END,
    CASE WHEN healthcare_provider_primary_taxonomy_switch_15 = 'Y' THEN healthcare_provider_taxonomy_code_15 END
);

-- drop all taxonomy switch and code columns 
ALTER TABLE nppes_subset
    DROP COLUMN healthcare_provider_taxonomy_code_1,
    DROP COLUMN healthcare_provider_primary_taxonomy_switch_1,
    DROP COLUMN healthcare_provider_taxonomy_code_2,
    DROP COLUMN healthcare_provider_primary_taxonomy_switch_2,
    DROP COLUMN healthcare_provider_taxonomy_code_3,
    DROP COLUMN healthcare_provider_primary_taxonomy_switch_3,
    DROP COLUMN healthcare_provider_taxonomy_code_4,
    DROP COLUMN healthcare_provider_primary_taxonomy_switch_4,
    DROP COLUMN healthcare_provider_taxonomy_code_5,
    DROP COLUMN healthcare_provider_primary_taxonomy_switch_5,
    DROP COLUMN healthcare_provider_taxonomy_code_6,
    DROP COLUMN healthcare_provider_primary_taxonomy_switch_6,
    DROP COLUMN healthcare_provider_taxonomy_code_7,
    DROP COLUMN healthcare_provider_primary_taxonomy_switch_7,
    DROP COLUMN healthcare_provider_taxonomy_code_8,
    DROP COLUMN healthcare_provider_primary_taxonomy_switch_8,
    DROP COLUMN healthcare_provider_taxonomy_code_9,
    DROP COLUMN healthcare_provider_primary_taxonomy_switch_9,
    DROP COLUMN healthcare_provider_taxonomy_code_10,
    DROP COLUMN healthcare_provider_primary_taxonomy_switch_10,
    DROP COLUMN healthcare_provider_taxonomy_code_11,
    DROP COLUMN healthcare_provider_primary_taxonomy_switch_11,
    DROP COLUMN healthcare_provider_taxonomy_code_12,
    DROP COLUMN healthcare_provider_primary_taxonomy_switch_12,
    DROP COLUMN healthcare_provider_taxonomy_code_13,
    DROP COLUMN healthcare_provider_primary_taxonomy_switch_13,
    DROP COLUMN healthcare_provider_taxonomy_code_14,
    DROP COLUMN healthcare_provider_primary_taxonomy_switch_14,
    DROP COLUMN healthcare_provider_taxonomy_code_15,
    DROP COLUMN healthcare_provider_primary_taxonomy_switch_15;



-- create a new column with provider_last_name_legal_name if exists, 
-- if it doesn't take the prefix (if not null), first_name, middle name and last name and concatenate

ALTER TABLE nppes_subset ADD COLUMN provider_full_name varchar(512);

UPDATE nppes_subset
SET provider_full_name = 
    COALESCE(
        NULLIF(provider_organization_name_legal_business_name, ''),
        CONCAT_WS(
                ' ',
                NULLIF(provider_name_prefix_text, ''),
                NULLIF(provider_first_name, ''),
                NULLIF(provider_middle_name, ''),
                NULLIF(provider_last_name_legal_name, ''),
                NULLIF(provider_name_suffix_text, ''),
                NULLIF(provider_credential_text, '')
            )
        );


alter table nppes_subset
    drop column provider_organization_name_legal_business_name,
    drop column provider_last_name_legal_name,
    drop column provider_first_name,
    drop column provider_middle_name,
    drop column provider_name_prefix_text,
    drop column provider_name_suffix_text,
    drop column provider_credential_text


select * from nppes_subset
order by npi
limit 20;


select column_name, data_type
from information_schema.columns
where table_name = 'nppes_subset'
    and table_schema = 'public';