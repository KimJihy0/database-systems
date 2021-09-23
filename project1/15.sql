SELECT T.name
FROM Trainer as T, Pokemon as P, CatchedPokemon as C
WHERE T.hometown = 'Sangnok City' AND
	C.owner_id = T.id AND
    P.id = C.pid AND
    P.name LIKE 'P%'
ORDER BY T.name;