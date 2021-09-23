SELECT AVG(C.level)
FROM Pokemon as P, CatchedPokemon as C
WHERE P.type = 'Water' AND
	C.pid = P.id;