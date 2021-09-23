SELECT C.nickname
FROM Gym as G, Pokemon as P, CatchedPokemon as C
WHERE G.city = 'Sangnok City' AND
	C.owner_id = G.leader_id AND
    P.id = C.pid AND
    P.type = 'Water'
ORDER BY C.nickname;