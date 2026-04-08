SELECT 
    p.customer_id, 
    e.customer_name, 
    ph.phone,
    p.pincode, 
    pet.breed
FROM retentionTeam.vw_cx_pins p
INNER JOIN retentionTeam.cx_pet_profile pet 
    ON p.customer_id = pet.customer_id
INNER JOIN retentionTeam.vw_cx_phones ph 
    ON p.customer_id = ph.customer_id
INNER JOIN retentionTeam.vw_cx_email e 
    ON p.customer_id = e.customer_id
WHERE p.pincode_city = 'Bangalore' 
  AND pet.breed IN (
      'Bichon Frise',
      'Shih Tzu',
      'Poodle',
      'Maltese',
      'Lhasa Apso',
      'Pomeranian',
      'Cocker Spaniel',
      'Yorkshire Terrier',
      'Persian',
      'Himalayan'
  );