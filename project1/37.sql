SELECT SUM(C.level)
FROM Pokemon as P, CatchedPokemon as C
WHERE P.id = C.pid AND
	P.type = 'Fire';