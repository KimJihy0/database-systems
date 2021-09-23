SELECT name
FROM Pokemon
WHERE id in
  (SELECT C.pid
  FROM CatchedPokemon AS C, Trainer AS T
  WHERE T.id = C.owner_id AND
    T.hometown = 'Sangnok City'
  ) AND
  id in
  (SELECT C.pid
  FROM CatchedPokemon AS C, Trainer AS T
  WHERE T.id = C.owner_id AND
    T.hometown = 'Blue City'
  )
ORDER BY name;