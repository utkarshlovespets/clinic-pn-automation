SELECT 
    e.email,
    COALESCE(
        TRIM(
            CASE 
                WHEN REPLACE(LOWER(SUBSTRING_INDEX(MIN(e.customer_name), ' ', 1)), '.', '') 
                     IN ('dr','mr','mrs','miss','ms')
                THEN SUBSTRING_INDEX(SUBSTRING_INDEX(MIN(e.customer_name), ' ', 2), ' ', -1)
                ELSE SUBSTRING_INDEX(MIN(e.customer_name), ' ', 1)
            END
        ), 
        'User'
    ) AS first_name,

    COALESCE(MIN(pet.pet_name), '') AS pet_name,
    MIN(pet.dob_month) AS dob_month

FROM retentionTeam.vw_cx_email e

-- 🔥 Filter pets FIRST (current month)
INNER JOIN (
    SELECT customer_id, pet_name, dob_month
    FROM retentionTeam.cx_pet_profile
    WHERE LOWER(dob_month) = LOWER(DATE_FORMAT(CURDATE(), '%M'))
) pet
    ON e.customer_id = pet.customer_id

-- 🔥 Filter Bangalore users
INNER JOIN (
    SELECT DISTINCT customer_id
    FROM retentionTeam.vw_cx_pins
    WHERE pincode_city = 'Bangalore'
) p
    ON e.customer_id = p.customer_id

WHERE e.email IS NOT NULL

GROUP BY e.email;