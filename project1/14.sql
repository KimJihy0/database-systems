SELECT hometown
FROM (
  SELECT hometown, COUNT(*) AS c
  FROM Trainer
  GROUP BY hometown
) AS N, (
  SELECT MAX(c) AS m
  FROM (
    SELECT hometown, COUNT(*) AS c
    FROM Trainer 
    GROUP BY hometown
  ) AS N
) as M
WHERE c = M.m;