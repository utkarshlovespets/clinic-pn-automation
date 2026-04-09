SELECT 
    e.email,

    -- Clean first name (NULL safe + removes titles)
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

    -- One pet name per user
    COALESCE(MIN(pet.pet_name), '') AS pet_name

FROM retentionTeam.vw_cx_pins p

INNER JOIN retentionTeam.cx_pet_profile pet 
    ON p.customer_id = pet.customer_id

INNER JOIN retentionTeam.vw_cx_email e 
    ON p.customer_id = e.customer_id

WHERE p.pincode_city = 'Bangalore' 
  AND pet.breed IN (
      'Pug','French Bulldog','Pekingese','German Shepherd',
      'Labrador Retriever','Golden Retriever','Rottweiler',
      'Great Dane','Dachshund','Doberman'
  )
  AND e.email IS NOT NULL

GROUP BY e.email;