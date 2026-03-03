/**
 * PRODUCTION-SAFE SQL DATA NORMALIZATION (COMPREHENSIVE - ALL 28 STATES + 8 UTs)
 * Target: raw_clean_google_map_data
 */

SET SQL_SAFE_UPDATES = 0;

START TRANSACTION;

-- PHASE 0: REMOVE INVALID ADDRESSES (Numerical only)
-- Deletes rows where the address contains only digits (potentially with spaces)
DELETE FROM raw_clean_google_map_data
WHERE address REGEXP '^[0-9 ]+$';

-- PHASE 1: PREVENT DUPLICATE CONFLICTS
-- We must remove any 'unknown' record that, once normalized, would conflict with an existing record.
DELETE FROM raw_clean_google_map_data
WHERE id IN (
    SELECT target_id FROM (
        SELECT t.id AS target_id
        FROM raw_clean_google_map_data t
        CROSS JOIN (
            -- 28 States
            SELECT 'Andhra Pradesh' AS sname UNION ALL SELECT 'Arunachal Pradesh' UNION ALL 
            SELECT 'Assam' UNION ALL SELECT 'Bihar' UNION ALL SELECT 'Chhattisgarh' UNION ALL 
            SELECT 'Goa' UNION ALL SELECT 'Gujarat' UNION ALL SELECT 'Haryana' UNION ALL 
            SELECT 'Himachal Pradesh' UNION ALL SELECT 'Jharkhand' UNION ALL SELECT 'Karnataka' UNION ALL 
            SELECT 'Kerala' UNION ALL SELECT 'Madhya Pradesh' UNION ALL SELECT 'Maharashtra' UNION ALL 
            SELECT 'Manipur' UNION ALL SELECT 'Meghalaya' UNION ALL SELECT 'Mizoram' UNION ALL 
            SELECT 'Nagaland' UNION ALL SELECT 'Odisha' UNION ALL SELECT 'Punjab' UNION ALL 
            SELECT 'Rajasthan' UNION ALL SELECT 'Sikkim' UNION ALL SELECT 'Tamil Nadu' UNION ALL 
            SELECT 'Telangana' UNION ALL SELECT 'Tripura' UNION ALL SELECT 'Uttar Pradesh' UNION ALL 
            SELECT 'Uttarakhand' UNION ALL SELECT 'West Bengal' UNION ALL
            -- 8 Union Territories
            SELECT 'Andaman and Nicobar Islands' UNION ALL SELECT 'Chandigarh' UNION ALL 
            SELECT 'Dadra and Nagar Haveli and Daman and Diu' UNION ALL SELECT 'Delhi' UNION ALL 
            SELECT 'Jammu and Kashmir' UNION ALL SELECT 'Ladakh' UNION ALL 
            SELECT 'Lakshadweep' UNION ALL SELECT 'Puducherry' UNION ALL
            -- Common Aliases
            SELECT 'Pondicherry'
        ) states
        INNER JOIN raw_clean_google_map_data d ON 
            d.name = t.name AND 
            d.phone_number = t.phone_number AND 
            d.address = t.address AND
            d.city = TRIM(SUBSTRING(t.city, 1, (LENGTH(t.city) - LENGTH(states.sname))))
        WHERE t.state = 'unknown'
        AND t.city LIKE CONCAT('% ', states.sname)
        AND t.id <> d.id
    ) AS conflict_rows
);

-- PHASE 2: PERFORM CLEANING/NORMALIZATION
UPDATE raw_clean_google_map_data t
INNER JOIN (
    -- 28 States
    SELECT 'Andhra Pradesh' AS sname UNION ALL SELECT 'Arunachal Pradesh' UNION ALL 
    SELECT 'Assam' UNION ALL SELECT 'Bihar' UNION ALL SELECT 'Chhattisgarh' UNION ALL 
    SELECT 'Goa' UNION ALL SELECT 'Gujarat' UNION ALL SELECT 'Haryana' UNION ALL 
    SELECT 'Himachal Pradesh' UNION ALL SELECT 'Jharkhand' UNION ALL SELECT 'Karnataka' UNION ALL 
    SELECT 'Kerala' UNION ALL SELECT 'Madhya Pradesh' UNION ALL SELECT 'Maharashtra' UNION ALL 
    SELECT 'Manipur' UNION ALL SELECT 'Meghalaya' UNION ALL SELECT 'Mizoram' UNION ALL 
    SELECT 'Nagaland' UNION ALL SELECT 'Odisha' UNION ALL SELECT 'Punjab' UNION ALL 
    SELECT 'Rajasthan' UNION ALL SELECT 'Sikkim' UNION ALL SELECT 'Tamil Nadu' UNION ALL 
    SELECT 'Telangana' UNION ALL SELECT 'Tripura' UNION ALL SELECT 'Uttar Pradesh' UNION ALL 
    SELECT 'Uttarakhand' UNION ALL SELECT 'West Bengal' UNION ALL
    -- 8 Union Territories
    SELECT 'Andaman and Nicobar Islands' UNION ALL SELECT 'Chandigarh' UNION ALL 
    SELECT 'Dadra and Nagar Haveli and Daman and Diu' UNION ALL SELECT 'Delhi' UNION ALL 
    SELECT 'Jammu and Kashmir' UNION ALL SELECT 'Ladakh' UNION ALL 
    SELECT 'Lakshadweep' UNION ALL SELECT 'Puducherry' UNION ALL
    -- Common Aliases
    SELECT 'Pondicherry'
) states ON t.city LIKE CONCAT('% ', states.sname)
SET 
    -- If Pondicherry, normalize to canonical Puducherry
    t.state = IF(states.sname = 'Pondicherry', 'Puducherry', states.sname),
    t.city = TRIM(SUBSTRING(t.city, 1, (LENGTH(t.city) - LENGTH(states.sname))))
WHERE t.state = 'unknown'
AND t.id IN (
    SELECT id FROM (SELECT id FROM raw_clean_google_map_data WHERE state = 'unknown') tmp
);

COMMIT;

SET SQL_SAFE_UPDATES = 1;
