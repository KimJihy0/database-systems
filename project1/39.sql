SELECT P.name
FROM Pokemon as P, Gym as G, CatchedPokemon as C
WHERE G.city = 'Rainbow City' AND
	C.owner_id = G.leader_id AND
    P.id = C.pid
ORDER BY P.name;