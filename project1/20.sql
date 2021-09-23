SELECT T.name
FROM Trainer as T, Gym as G
WHERE T.id = G.leader_id
ORDER BY T.name;