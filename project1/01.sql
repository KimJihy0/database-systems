SELECT name
FROM Trainer as T, CatchedPokemon as C
WHERE T.id = C.owner_id
GROUP BY C.owner_id
HAVING count(*) > 2
ORDER BY count(*);