SELECT T.name
FROM Trainer as T, Gym as G
WHERE G.city = 'Brown City' AND
	T.id = G.leader_id;