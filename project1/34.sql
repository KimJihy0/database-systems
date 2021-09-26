SELECT P2.name
FROM Pokemon as P1, Pokemon as P2, Evolution as E1, Evolution as E2
WHERE P1.name = 'Charmander' AND
	E1.before_id = P1.id AND
    E2.before_id = E1.after_id AND
    P2.id = E2.after_id;