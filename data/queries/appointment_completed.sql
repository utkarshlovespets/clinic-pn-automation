SELECT 
    base.email,
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
FROM (
    -- Combine both datasets
    SELECT contact_email AS email 
    FROM healthcare.clinic_orders 
    WHERE contact_email IS NOT NULL

    UNION   -- removes duplicates automatically

    SELECT owner_email AS email 
    FROM healthcare.ahs_appointments 
    WHERE booking_revenue IS NOT NULL
) base

-- Join to get customer details
LEFT JOIN retentionTeam.vw_cx_email e
    ON base.email = e.email

LEFT JOIN retentionTeam.cx_pet_profile pet
    ON e.customer_id = pet.customer_id

GROUP BY base.email;