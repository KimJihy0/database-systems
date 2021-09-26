SELECT name
FROM Pokemon
WHERE type = 'Water' AND
	id NOT IN (
      	SELECT before_id
      	FROM Evolution
     	)
ORDER BY name;