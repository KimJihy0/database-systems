SELECT T.name, P.name, COUNT(*)
FROM Trainer as T, Pokemon as P, CatchedPokemon as C
WHERE T.id in
  (SELECT T.id
  FROM Trainer as T, Pokemon as P, CatchedPokemon as C
  WHERE T.id = C.owner_id AND
    C.pid = P.id
  GROUP BY T.id
  HAVING COUNT(DISTINCT P.type) = 1) AND
  T.id = C.owner_id AND
  C.pid = P.id
GROUP BY T.name, P.name
ORDER BY T.name;