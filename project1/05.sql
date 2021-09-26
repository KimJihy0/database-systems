SELECT AVG(level)
FROM Trainer as T, CatchedPokemon as C
WHERE T.id = C.owner_id AND
	T.name = 'Red';