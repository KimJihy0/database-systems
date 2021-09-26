SELECT SUM(C.level)
FROM Trainer as T, CatchedPokemon as C
WHERE T.name = 'Matis' AND
	C.owner_id = T.id;