SELECT 
    final.email,
    
    -- Extract first name safely
    TRIM(
        CASE 
            WHEN LOCATE(' ', final.customer_name) > 0 
            THEN SUBSTRING_INDEX(final.customer_name, ' ', 1)
            ELSE final.customer_name
        END
    ) AS first_name,

    COALESCE(final.pet_name, '') AS pet_name

FROM (

    -- 🔵 PART 1: PET PROFILE (Vaccination data)
    SELECT 
        p.email,
        e.customer_name,
        p.pet_name
    FROM retentionTeam.cx_pet_profile p

    LEFT JOIN retentionTeam.vw_cx_email e 
        ON p.customer_id = e.customer_id

    WHERE 
        p.last_vaccination_date IS NOT NULL
        AND MONTH(p.last_vaccination_date) = MONTH(CURRENT_DATE())
        AND YEAR(p.last_vaccination_date) <> YEAR(CURRENT_DATE())


    UNION


    -- 🟢 PART 2: CLINIC ORDERS (Vaccination visits)
    SELECT 
        c.contact_email AS email,
        e.customer_name,
        c.patient_name AS pet_name
    FROM healthcare.clinic_orders c

    LEFT JOIN retentionTeam.vw_cx_email e 
        ON c.customer_id = e.customer_id

    WHERE 
        c.vaccination_count >= 1
        AND c.first_clinic_order IS NOT NULL
        AND MONTH(c.first_clinic_order) = MONTH(CURRENT_DATE())
        AND YEAR(c.first_clinic_order) <> YEAR(CURRENT_DATE())

) final

WHERE final.email IS NOT NULL;