USE StarWars

-- What are the names of the characters in Episode 1?
SELECT Name FROM person 
INNER JOIN filmsPersons
ON person.ID = filmsPersons.personID 
INNER JOIN films 
ON films.EpisodeID = filmsPersons.filmsID 
WHERE films.EpisodeID = 1
ORDER BY Name;

-- How many characters are in each film ordered from greatest to least?
SELECT films.Title, COUNT(*) AS Number_of_People FROM films 
INNER JOIN filmsPersons 
ON films.ID = filmsPersons.filmsID 
INNER JOIN person 
ON person.ID = filmsPersons.personID 
GROUP BY films.Title
ORDER BY Number_of_People DESC;

-- What is the cost of the most expensive starship flown by each species?
SELECT species.Name AS Species, MAX(Cost) AS Cost FROM species
INNER JOIN person 
ON person.Species = species.ID 
INNER JOIN starshipPerson
ON starshipPerson.personID = person.ID 
INNER JOIN starship 
ON starship.ID = starshipPerson.starshipID 
group by species.Name;

-- What is the homeworld of each character in Episode 2 ordered by alphabetical planet name?
SELECT person.Name, planet.Name AS Planet FROM planet
INNER JOIN person 
ON planet.ID = person.Homeworld 
INNER JOIN filmsPersons
ON filmsPersons.personID = person.ID 
INNER JOIN films
ON films.ID = filmsPersons.filmsID
WHERE films.EpisodeID = 2
ORDER BY planet.Name;

-- What is the manufacturer of the vehicle driven by Wedge Antilles?
SELECT manufacturer.Name FROM manufacturer
INNER JOIN manufacturerVehicle
ON manufacturerVehicle.manufacturerID = manufacturer.ID 
INNER JOIN vehicle
ON vehicle.ID = manufacturerVehicle.vehicleID
inner join vehiclePerson
ON vehiclePerson.vehicleID = vehicle.ID 
INNER JOIN person 
ON person.ID = vehiclePerson.personID
WHERE person.Name = "Wedge Antilles";

-- What is the terrain and climate of the homeworld of each species of each starship pilot in Episode 3?
SELECT DISTINCT person.Name as Pilot, climate.Description AS Climate, terrain.Description AS Terrain FROM films
INNER JOIN filmsStarships
ON filmsStarships.filmsID = films.ID 
INNER JOIN starship
ON starship.ID = filmsStarships.starshipID
INNER JOIN starshipPerson
ON starshipPerson.starshipID = starship.ID 
INNER JOIN person 
ON person.ID = starshipPerson.personID
INNER JOIN species
ON species.ID = person.Species
INNER JOIN planet
ON planet.ID = species.Homeworld
INNER JOIN planetClimate
ON planetClimate.planetID = planet.ID 
INNER JOIN climate
ON climate.ID = planetClimate.climateID
INNER JOIN planetTerrain
ON planetTerrain.planetID = planet.ID 
INNER JOIN terrain 
ON terrain.ID = planetTerrain.terrainID
WHERE films.EpisodeID = 3;

-- How many films has each producer produced?
SELECT producer.Name, count(producer.Name) AS Amount FROM producer
INNER JOIN producerFilms
ON producerFilms.producerID = producer.ID 
Inner JOIN films 
ON films.ID = producerFilms.filmsID group by producer.Name;

-- What is the name, height, mass, and eye color of all the Skywalkers?
SELECT person.Name, person.Height, person.Mass, eyeColor.Color AS EyeColor FROM person 
INNER JOIN eyeColor 
ON eyeColor.ID = person.eyeColor
WHERE person.Name LIKE "%Skywalker"
ORDER BY person.Name DESC;


 