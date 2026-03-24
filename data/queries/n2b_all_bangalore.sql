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
    MIN(pet.pet_name) AS pet_name
FROM retentionTeam.vw_cx_pins p
LEFT JOIN retentionTeam.vw_cx_email e
    ON p.customer_id = e.customer_id
LEFT JOIN retentionTeam.cx_pet_profile pet
    ON p.customer_id = pet.customer_id
WHERE p.pincode_city = 'Bangalore'
    AND e.email IS NOT NULL
GROUP BY e.email;