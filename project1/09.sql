SELECT T.name, C.nickname
FROM Trainer AS T, CatchedPokemon AS C,
  (SELECT owner_id, MAX(level) AS m
  FROM CatchedPokemon AS C
  GROUP BY owner_id
  HAVING COUNT(*) >= 4) as N
WHERE T.id = N.owner_id AND
  T.id = C.owner_id AND
  C.level = N.m
ORDER BY C.nickname;