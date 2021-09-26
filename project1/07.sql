SELECT C.nickname
FROM Trainer AS T, CatchedPokemon AS C,
	(SELECT T.hometown AS h, MAX(level) AS m
	FROM Trainer AS T, CatchedPokemon AS C
	WHERE T.id = C.owner_id
	GROUP BY T.hometown
	)
    as N
WHERE T.id = C.owner_id AND
	C.level = N.m AND
	T.hometown = N.h
ORDER BY C.nickname;