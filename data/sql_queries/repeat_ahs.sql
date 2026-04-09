SELECT 
    ah.owner_email AS email,

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

FROM healthcare.ahs_appointments ah

LEFT JOIN retentionTeam.vw_cx_email ve 
    ON ah.owner_email = ve.email

LEFT JOIN retentionTeam.cx_pet_profile pp 
    ON ah.owner_email = pp.email

WHERE ah.owner_email IS NOT NULL
  AND ah.booking_revenue IS NOT NULL

GROUP BY ah.owner_email;