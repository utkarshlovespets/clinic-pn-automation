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
    COALESCE(MIN(pet.pet_name), '') AS pet_name
FROM retentionTeam.vw_cx_email e
INNER JOIN retentionTeam.vw_cx_pins p
    ON e.customer_id = p.customer_id
LEFT JOIN retentionTeam.cx_pet_profile pet
    ON e.customer_id = pet.customer_id
WHERE p.pincode IN (
    '560043','560005','560033','560113','560045','560084'
)
GROUP BY e.email;