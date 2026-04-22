SELECT 
    e.email,

    -- Clean first name (NULL safe + remove titles like dr, dr.)
    COALESCE(
        TRIM(
            CASE 
                WHEN LOWER(REPLACE(SUBSTRING_INDEX(MIN(e.customer_name), ' ', 1), '.', '')) 
                     IN ('mr','mrs','ms','dr')
                THEN SUBSTRING_INDEX(SUBSTRING_INDEX(MIN(e.customer_name), ' ', 2), ' ', -1)
                ELSE SUBSTRING_INDEX(MIN(e.customer_name), ' ', 1)
            END
        ),
        ''
    ) AS first_name,

    -- Pick only one pet per user
    COALESCE(MIN(pet.pet_name), '') AS pet_name

FROM (
    -- 🔥 Filter early (BIG performance win)
    SELECT customer_id
    FROM retentionTeam.cx_clinic_base_v2
    WHERE indiranagar_distance <= 5.0
      AND nearest_clinic = 'Indiranagar'
) base

INNER JOIN retentionTeam.vw_cx_email e 
    ON base.customer_id = e.customer_id

INNER JOIN retentionTeam.cx_pet_profile pet 
    ON base.customer_id = pet.customer_id

WHERE e.email IS NOT NULL

GROUP BY e.email;