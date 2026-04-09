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

-- Exclusions
LEFT JOIN healthcare.clinic_orders c
    ON e.email = c.contact_email
LEFT JOIN healthcare.ahs_appointments a
    ON e.email = a.owner_email 
    AND a.booking_revenue IS NOT NULL

WHERE p.pincode_city = 'Bangalore'
    AND e.email IS NOT NULL
    AND c.contact_email IS NULL
    AND a.owner_email IS NULL

GROUP BY e.email;