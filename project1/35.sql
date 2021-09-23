SELECT T.name, COUNT(*)
FROM Trainer as T, CatchedPokemon as C
WHERE T.id = C.owner_id
GROUP BY T.name
ORDER BY T.name;