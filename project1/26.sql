SELECT N2.name, N2.sum_level
FROM
  (SELECT MAX(sum_level) AS max_level
   FROM (SELECT T.name, SUM(C.level) AS sum_level
         FROM Trainer as T, CatchedPokemon as C
         WHERE T.id = C.owner_id
         GROUP BY T.id) AS NN
   ) AS N1,
  (SELECT T.name, SUM(C.level) AS sum_level
   FROM Trainer as T, CatchedPokemon as C
   WHERE T.id = C.owner_id
   GROUP BY T.id) AS N2
WHERE N2.sum_level = N1.max_level;