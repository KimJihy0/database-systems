SELECT P.type, count(*)
FROM Pokemon as P, CatchedPokemon as C
WHERE P.id = C.pid
GROUP BY P.type
ORDER BY P.type;