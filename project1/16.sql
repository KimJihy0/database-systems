SELECT T.name
FROM City as C, Trainer as T, Gym as G
WHERE C.description = 'Amazon' AND
	G.city = C.name AND
    T.id = G.leader_id;