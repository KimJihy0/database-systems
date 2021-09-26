SELECT DISTINCT T.name
FROM Trainer as T, CatchedPokemon as C1, CatchedPokemon as C2
WHERE C1.id <> C2.id AND
	C1.owner_id = C2.owner_id AND
    C1.pid = C2.pid AND
    T.id = C1.owner_id
ORDER BY T.name;