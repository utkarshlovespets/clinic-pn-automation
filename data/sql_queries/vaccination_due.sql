SELECT
    final.email,
    MIN(final.first_name) AS first_name,
    COALESCE(MIN(final.pet_name), '') AS pet_name
FROM (

    -- 🔵 Set A: Pet Profile (Vaccination based)
    SELECT
        p.email,
        TRIM(
            CASE
                WHEN e.customer_name IS NULL THEN ''
                ELSE SUBSTRING_INDEX(TRIM(e.customer_name), ' ', 1)
            END
        ) AS first_name,
        p.pet_name
    FROM retentionTeam.cx_pet_profile p
    LEFT JOIN retentionTeam.vw_cx_email e
        ON p.email = e.email
    WHERE
        p.last_vaccination_date IS NOT NULL
        AND MONTH(p.last_vaccination_date) = MONTH(CURDATE())
        AND YEAR(p.last_vaccination_date) <> YEAR(CURDATE())

    UNION

    -- 🟢 Set B: Clinic Orders (Vaccination customers)
    SELECT
        c.contact_email AS email,
        TRIM(
            CASE
                WHEN e.customer_name IS NULL THEN ''
                ELSE SUBSTRING_INDEX(TRIM(e.customer_name), ' ', 1)
            END
        ) AS first_name,
        c.patient_name AS pet_name
    FROM healthcare.clinic_orders c
    LEFT JOIN retentionTeam.vw_cx_email e
        ON c.contact_email = e.email
    WHERE
        c.vaccination_count >= 1
        AND MONTH(c.order_created_at) = MONTH(CURDATE())

    UNION

    -- 🟡 Set C: AHS Appointments (At-Home Vaccination)
    SELECT
        a.owner_email AS email,
        TRIM(
            CASE
                WHEN e.customer_name IS NULL THEN ''
                ELSE SUBSTRING_INDEX(TRIM(e.customer_name), ' ', 1)
            END
        ) AS first_name,
        a.patient_name AS pet_name
    FROM healthcare.ahs_appointments a
    LEFT JOIN retentionTeam.vw_cx_email e
        ON a.owner_email = e.email
    WHERE
        a.serviceType = 'vaccination'
        AND a.home_visit_revenue IS NOT NULL
        AND MONTH(a.appointmentDate) = MONTH(CURDATE())

) final
GROUP BY final.email;