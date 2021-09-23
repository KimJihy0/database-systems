SELECT name
FROM Pokemon, Evolution
WHERE before_id > after_id AND
	before_id = id
ORDER BY name;