"""
Course: CSCI 265: Database Systems
Assignment: Final Project
Author: Mark Morykan
"""

import os
from dotenv import load_dotenv
import mysql.connector
import requests
from collections import defaultdict
from datetime import datetime


base_url = 'https://swapi.dev/api/'


def connectToMySQL():
    """Connect to StarWars database"""
    load_dotenv()  # Load mysql credentials as environment variables
    try:
        cnx = mysql.connector.connect(password=os.getenv('PASSWORD'), user=os.getenv('USERNAME'), database='StarWars')
    except mysql.connector.Error as err:
        print(f"Failed connecting to database: {err}")
        exit(1)

    cursor = cnx.cursor()
    return cursor, cnx


def get_response(endpoint):
    """Get all the endpoint's data from the REST API by paginating through the API's results"""
    response_list = []
    url = base_url + endpoint
    response = requests.get(url)
    data = response.json()
    response_list += data['results']
    while data['next']:
        response = requests.get(data['next'])
        data = response.json()
        response_list += data['results']

    return response_list


def convert_into_two_column_entity(cursor, table_name, column_name, collection_of_values, do_insertion=True):
    """Inserts into a basic two column entity, where the first column is the id column"""
    relation_dict = {}
    values = []
    sql = f"INSERT INTO {table_name} ({column_name}) VALUES (%s);"
    for id_, type_ in enumerate(collection_of_values, 1):
        values.append([type_])
        relation_dict[id_] = collection_of_values[type_]

    if do_insertion:
        cursor.executemany(sql, values)

    return relation_dict


def insert_into_relationship(cursor, table_name, dict_of_relationship):
    """Insert data into a many-to-many relationship"""
    sql = f"INSERT INTO {table_name} VALUES (%s, %s);"
    sub_arguments = [(foreign2_id, foreign1_id) for foreign1_id in dict_of_relationship for foreign2_id in dict_of_relationship[foreign1_id]]

    cursor.executemany(sql, sub_arguments)


def get_info(object_, desired_columns):
    """Returns a list of info from the API response object to be stored in the database"""
    info = []
    url = object_['url']
    id_ = int(url.split('/')[-2])
    info.append(id_)
    for column in desired_columns:
        value = object_[column]
        if value: 
            value = value.lower()
            if value != 'unknown' and value != 'n/a' and value != 'indefinite' and value != 'none':
                value = value.replace(',', '')
                info.append(value if not value.isdigit() else int(value))
            else:
                info.append(None)
        else:
            info.append(None)

    return info


def separate_listy_strings_into_dicts(object_, id_, list_fields, *dicts):
    """Associates an attribute name with a list of ids"""
    for i in range(len(list_fields)):
        string_to_list_object = object_[list_fields[i]]
        if string_to_list_object != 'unknown':
            object_types = string_to_list_object.split(', ')
            for object_type in object_types:
                dicts[i][object_type].append(id_)


def insert_into_planets(cursor):
    """Insert data into the planet table"""
    terrain = defaultdict(list)
    climate = defaultdict(list)
    desired_columns = ['name', 'rotation_period', 'orbital_period', 'diameter', 'gravity', 'surface_water', 'population']
    sql = "INSERT INTO planet (ID, Name, RotationPeriod, OrbitalPeriod, Diameter, Gravity, SurfaceWater, Population) " + \
          "VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"
    value_list = []
    response = get_response('planets/')
    for planet in response:
        info = get_info(planet, desired_columns)
        value_list.append(info)

        separate_listy_strings_into_dicts(planet, info[0], ['climate', 'terrain'], climate, terrain)
        
    cursor.executemany(sql, value_list)
    return terrain, climate


def insert_into_terrain(cursor, terrain):
    return convert_into_two_column_entity(cursor, 'terrain', 'Description', terrain)


def insert_into_climate(cursor, climate):
    return convert_into_two_column_entity(cursor, 'climate', 'Description', climate)


def insert_into_planet_terrain_rship(cursor, relation_dict):
    insert_into_relationship(cursor, 'planetTerrain', relation_dict)


def insert_into_planet_climate_rship(cursor, relation_dict):
    insert_into_relationship(cursor, 'planetClimate', relation_dict)


def insert_into_species(cursor):
    """Insert data into the species table"""
    hair_colors = defaultdict(list)
    eye_colors = defaultdict(list)
    skin_colors = defaultdict(list)

    columns = ['name', 'classification', 'designation', 'average_height', 'average_lifespan', 'homeworld', 'language']
    sql = "INSERT INTO species (ID, Name, Classification, Designation, AverageHeight, AverageLifespan, Homeworld, Language) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"
    value_list = []

    response = get_response('species/')

    for species in response:
        info = get_info(species, columns)

        homeworld = info[-2]
        info[-2] = int(homeworld.split('/')[-2]) if homeworld else None  # Gets the planet ID out of the API url homeworld

        value_list.append(info)
        separate_listy_strings_into_dicts(species, info[0], ['hair_colors', 'eye_colors', 'skin_colors'], hair_colors, eye_colors, skin_colors)
        
    cursor.executemany(sql, value_list)
    return hair_colors, eye_colors, skin_colors


def insert_into_eye_color(cursor, eye_colors):
    return convert_into_two_column_entity(cursor, 'eyeColor', 'Color', eye_colors)

def insert_into_eye_color_species_rship(cursor, relation_dict):
    insert_into_relationship(cursor, 'speciesEyeColor', relation_dict)


def insert_into_persons(cursor):
    """Insert data into the person table"""
    hair_colors = defaultdict(list)
    skin_colors = defaultdict(list)
    desired_columns = ['name', 'height', 'mass', 'birth_year', 'gender', 'homeworld']
    sql = "INSERT INTO person (ID, Name, Height, Mass, EyeColor, BirthYear, Gender, Homeworld, Species) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);"

    values_list = []
    response = get_response('people/')

    for person in response:
        info = get_info(person, desired_columns)

        eye_color = person['eye_color'].lower()
        cursor.execute(f"SELECT ID FROM eyeColor WHERE Color = '{eye_color}'")
        color_row = cursor.fetchone()
        info.insert(4, int(color_row[0]) if color_row else None)

        homeworld = info[-1]
        info[-1] = int(homeworld.split('/')[-2]) if homeworld else None  # Gets the planet ID out of the API url homeworld
        # info[-1] = None  # Delete this line when ready

        species = person['species']
        info.append(int(species[0].split('/')[-2]) if species else 1)  # Human species isn't attached to characters in the API

        values_list.append(info)
        separate_listy_strings_into_dicts(person, info[0], ['hair_color', 'skin_color'], hair_colors, skin_colors)

    cursor.executemany(sql, values_list)
    return hair_colors, skin_colors


def insert_into_hair_colors(cursor, hair_colors):
    convert_into_two_column_entity(cursor, 'hairColor', 'Color', hair_colors)


def insert_into_skin_colors(cursor, skin_colors):
    convert_into_two_column_entity(cursor, 'skinColor', 'Color', skin_colors)


def insert_into_species_hair_rship(cursor, relation_dict):
    insert_into_relationship(cursor, 'speciesHairColor', relation_dict)


def insert_into_person_hair_rship(cursor, relation_dict):
    insert_into_relationship(cursor, 'personHairColor', relation_dict)


def insert_into_species_skin_rship(cursor, relation_dict):
    insert_into_relationship(cursor, 'speciesSkinColor', relation_dict)


def insert_into_person_skin_rship(cursor, relation_dict):
    insert_into_relationship(cursor, 'personSkinColor', relation_dict)


def insert_into_starship(cursor):
    """Insert data into the starship table"""
    pilots = defaultdict(list)
    manufacturers = defaultdict(list)
    desired_columns = ['name', 'model', 'cost_in_credits', 'length', 'max_atmosphering_speed', 'crew', 'passengers', 
                       'cargo_capacity', 'consumables', 'hyperdrive_rating', 'MGLT', 'starship_class']
    sql = "INSERT INTO starship (ID, Name, Model, Cost, Length, MaxSpeed, Crew, Passengers, CargoCapacity, Consumables, HyperdriveRating, MGLT, Class) " + \
          "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"

    values_list = []
    response = get_response('starships/')

    for starship in response:
        info = get_info(starship, desired_columns)
        info[5] = float(info[5]) if info[5] and str(info[5])[-1].isdigit() else None
        info[-3] = float(info[-3]) if info[-3] else None

        values_list.append(info)
        persons = starship['pilots']
        if persons:
            for person in persons:
                pilots[info[0]].append(int(person.split('/')[-2]))
        

        separate_listy_strings_into_dicts(starship, info[0], ['manufacturer'], manufacturers)


    cursor.executemany(sql, values_list)
    return pilots, manufacturers


def insert_into_pilots_rship(cursor, pilots):
    insert_into_relationship(cursor, 'starshipPerson', pilots)


def insert_into_vehicle(cursor):
    """Insert data into the vehicle table"""
    drivers = defaultdict(list)
    manufacturers = defaultdict(list)
    desired_columns = ['name', 'model', 'cost_in_credits', 'length', 'max_atmosphering_speed', 'crew', 'passengers', 
                       'cargo_capacity', 'consumables', 'vehicle_class']
    sql = "INSERT INTO vehicle (ID, Name, Model, Cost, Length, MaxSpeed, Crew, Passengers, CargoCapacity, Consumables, Class) " + \
          "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"

    values_list = []
    response = get_response('vehicles/')

    for vehicle in response:
        info = get_info(vehicle, desired_columns)
        info[5] = float(info[5]) if info[5] and str(info[5])[-1].isdigit() else None
        info[-3] = float(info[-3]) if info[-3] else None

        values_list.append(info)
        persons = vehicle['pilots']
        if persons:
            for person in persons:
                drivers[info[0]].append(int(person.split('/')[-2]))
        

        separate_listy_strings_into_dicts(vehicle, info[0], ['manufacturer'], manufacturers)


    cursor.executemany(sql, values_list)
    return drivers, manufacturers


def insert_into_drivers_rship(cursor, drivers):
    insert_into_relationship(cursor, 'vehiclePerson', drivers)


def insert_into_manufacturers(cursor, manufacturers):
    convert_into_two_column_entity(cursor, 'manufacturer', 'Name', manufacturers)


def insert_into_manufacturers_starship_rship(cursor, manufacturers):
    insert_into_relationship(cursor, 'manufacturerStarship', manufacturers)


def insert_into_manufacturers_vehicle_rship(cursor, manufacturers):
    insert_into_relationship(cursor, 'manufacturerVehicle', manufacturers)


def store_relationship_ids(object_, fieldname, id_, relation_dict):
    collection = object_[fieldname]
    for url in collection:
        relation_dict[id_].append(int(url.split('/')[-2]))


def insert_into_films(cursor):
    """Insert data into the films table"""
    persons = defaultdict(list)
    species = defaultdict(list)
    planets = defaultdict(list)
    starships = defaultdict(list)
    vehicles = defaultdict(list)
    producers = defaultdict(list)

    desired_columns = ['title', 'opening_crawl', 'director', 'release_date']
    sql = "INSERT INTO films (ID, EpisodeID, Title, OpeningCrawl, Director, ReleaseDate) " + \
          "VALUES (%s, %s, %s, %s, %s, %s);"

    values_list = []
    response = get_response('films/')

    for film in response:
        info = get_info(film, desired_columns)
        info.insert(1, film['episode_id'])
        info[-1] = datetime.strptime(info[-1], '%Y-%m-%d')
        values_list.append(info)

        store_relationship_ids(film, 'characters', info[0], persons)
        store_relationship_ids(film, 'planets', info[0], planets)
        store_relationship_ids(film, 'species', info[0], species)
        store_relationship_ids(film, 'starships', info[0], starships)
        store_relationship_ids(film, 'vehicles', info[0], vehicles)
        
        separate_listy_strings_into_dicts(film, info[0], ['producer'], producers)

    cursor.executemany(sql, values_list)
    return persons, species, planets, starships, vehicles, producers


def insert_into_persons_films_rship(cursor, persons):
    insert_into_relationship(cursor, 'filmsPersons', persons)


def insert_into_species_films_rship(cursor, species):
    insert_into_relationship(cursor, 'filmsSpecies', species)


def insert_into_planets_films_rship(cursor, planets):
    insert_into_relationship(cursor, 'filmsPlanets', planets)


def insert_into_starships_films_rship(cursor, starships):
    insert_into_relationship(cursor, 'filmsStarships', starships)


def insert_into_vehicles_films_rship(cursor, vehicles):
    insert_into_relationship(cursor, 'filmsVehicles', vehicles)


def insert_into_producers(cursor, producers):
    return convert_into_two_column_entity(cursor, 'producer', 'Name', producers)


def insert_into_producers_films_rship(cursor, producers):
    insert_into_relationship(cursor, 'producerFilms', producers)


def main():
    cursor, connection = connectToMySQL()
    terrain, climate = insert_into_planets(cursor)
    terrain_and_planets = insert_into_terrain(cursor, terrain)
    climate_and_planets = insert_into_climate(cursor, climate)
    insert_into_planet_terrain_rship(cursor, terrain_and_planets)
    insert_into_planet_climate_rship(cursor, climate_and_planets)

    species_hair_colors, eye_colors, species_skin_colors = insert_into_species(cursor)
    species_and_hair_colors = convert_into_two_column_entity(cursor, 'speciesHairColor', 'Color', species_hair_colors, False)
    species_and_skin_colors = convert_into_two_column_entity(cursor, 'speciesSkinColor', 'Color', species_skin_colors, False)
    eye_colors_and_species = insert_into_eye_color(cursor, eye_colors)
    insert_into_eye_color_species_rship(cursor, eye_colors_and_species)

    person_hair_colors, person_skin_colors = insert_into_persons(cursor)
    person_and_hair_colors = convert_into_two_column_entity(cursor, 'personHairColor', 'Color', person_hair_colors, False)
    person_and_skin_colors = convert_into_two_column_entity(cursor, 'personSkinColor', 'Color', person_skin_colors, False)

    insert_into_hair_colors(cursor, {**species_hair_colors, **person_hair_colors})
    insert_into_skin_colors(cursor, {**species_skin_colors, **person_skin_colors})
    insert_into_species_hair_rship(cursor, species_and_hair_colors)
    insert_into_person_hair_rship(cursor, person_and_hair_colors)
    insert_into_species_skin_rship(cursor, species_and_skin_colors)
    insert_into_person_skin_rship(cursor, person_and_skin_colors)


    pilots, manufacturers_starship = insert_into_starship(cursor)
    insert_into_pilots_rship(cursor, pilots)
    starship_manufacturers = convert_into_two_column_entity(cursor, 'manufacturerStarship', 'Name', manufacturers_starship, False)

    drivers, manufacturers_vehicle = insert_into_vehicle(cursor)
    insert_into_drivers_rship(cursor, drivers)
    vehicle_manufacturers = convert_into_two_column_entity(cursor, 'manufacturerVehicle', 'Name', manufacturers_vehicle, False)

    insert_into_manufacturers(cursor, {**manufacturers_starship, **manufacturers_vehicle})
    insert_into_manufacturers_starship_rship(cursor, starship_manufacturers)
    insert_into_manufacturers_vehicle_rship(cursor, vehicle_manufacturers)

    persons, species, planets, starships, vehicles, producers = insert_into_films(cursor)
    insert_into_persons_films_rship(cursor, persons)
    insert_into_species_films_rship(cursor, species)
    insert_into_planets_films_rship(cursor, planets)
    insert_into_starships_films_rship(cursor, starships)
    insert_into_vehicles_films_rship(cursor, vehicles)

    producers_and_films = insert_into_producers(cursor, producers)
    insert_into_producers_films_rship(cursor, producers_and_films)

    connection.commit()
    cursor.close()
    connection.close()

if __name__ == '__main__':
    main()
