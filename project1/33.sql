SELECT T.name
FROM Trainer as T, Pokemon as P, CatchedPokemon as C
WHERE P.type = 'Psychic' AND
	C.pid = P.id AND
    T.id = C.owner_id
ORDER BY T.name;