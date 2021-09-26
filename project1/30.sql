SELECT name
FROM Pokemon
WHERE name LIKE '%s' OR name LIKE '%S'
ORDER BY name;