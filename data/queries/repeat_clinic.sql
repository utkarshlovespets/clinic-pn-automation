SELECT 
    COALESCE(co.contact_email, '') AS email,

    COALESCE(
        TRIM(
            CASE 
                WHEN LOWER(REPLACE(SUBSTRING_INDEX(MIN(ve.customer_name), ' ', 1), '.', '')) 
                     IN ('mr', 'mrs', 'ms', 'dr')
                THEN SUBSTRING_INDEX(SUBSTRING_INDEX(MIN(ve.customer_name), ' ', 2), ' ', -1)
                ELSE SUBSTRING_INDEX(MIN(ve.customer_name), ' ', 1)
            END
        ),
        ''
    ) AS first_name,

    COALESCE(MIN(pp.pet_name), '') AS pet_name

FROM healthcare.clinic_orders co

LEFT JOIN retentionTeam.vw_cx_email ve 
    ON co.contact_email = ve.email

LEFT JOIN retentionTeam.cx_pet_profile pp 
    ON co.contact_email = pp.email

WHERE co.contact_email IS NOT NULL

GROUP BY co.contact_email;