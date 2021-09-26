SELECT P.name
FROM Evolution as E, Pokemon as P
WHERE E.before_id NOT IN
  (SELECT after_id
   FROM Evolution
  ) AND
  E.after_id = P.id
ORDER BY P.name;