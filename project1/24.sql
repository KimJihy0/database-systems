SELECT name
FROM Pokemon
WHERE name LIKE 'A%' OR name LIKE 'a%' OR
	name LIKE 'E%' OR name LIKE 'e%' OR
    name LIKE 'I%' OR name LIKE 'i%' OR
    name LIKE 'O%' OR name LIKE 'o%' OR
    name LIKE 'U%' OR name LIKE 'u%'
ORDER BY name;