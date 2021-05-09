"""
Course: CSCI 265: Database Systems
Assignment: Final Project
Author: Mark Morykan
"""

import os
from dotenv import load_dotenv
import mysql.connector


def connectToMySQL():
    load_dotenv()
    cnx = mysql.connector.connect(password=os.getenv('PASSWORD'), user=os.getenv('USERNAME'))
    cursor = cnx.cursor()
    return cursor, cnx


def createDatabase(cursor, DB_NAME):
    '''
    :param cursor: instance of the connection to the database
    :param DB_NAME: name of the database to create
    Creates the database at cursor with the given name.
    '''
    try:
        cursor.execute(
            f"CREATE DATABASE {DB_NAME} DEFAULT CHARACTER SET 'utf8'")
    except mysql.connector.Error as err:
        print(f"Failed creating database: {err}")
        exit(1)


def climate_terrain_color_manufacturer_producer_table(table_name, second_column):
    return f"CREATE TABLE {table_name} " + \
           "(ID INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " + \
           f"{second_column} VARCHAR(1024)" + \
           ");"


def create_relationship(table_name, ref_table1, ref_table2):
    first_id_column = ref_table1 + 'ID'
    second_id_column = ref_table2 + 'ID'
    return f"CREATE TABLE {table_name} " + \
           f"({first_id_column} INT NOT NULL, " + \
           f"{second_id_column} INT NOT NULL, " + \
           f"PRIMARY KEY ({first_id_column}, {second_id_column}), " + \
           f"FOREIGN KEY ({first_id_column}) REFERENCES {ref_table1+'(ID)'} ON DELETE CASCADE, " + \
           f"FOREIGN KEY ({second_id_column}) REFERENCES {ref_table2+'(ID)'} ON DELETE CASCADE" + \
            ");"


def starship_vehicle_table(table_name):
    sql = f"CREATE TABLE {table_name} " + \
           "(ID INT NOT NULL PRIMARY KEY, " + \
           "Name VARCHAR(50), " + \
           "Model VARCHAR(50), " + \
           "Cost BIGINT, " + \
           "Length INT, " + \
           "MaxSpeed DECIMAL(10, 2), " + \
           "Crew VARCHAR(20), " + \
           "Passengers INT, " + \
           "CargoCapacity BIGINT, " + \
           "Consumables VARCHAR(50), " + \
           "Class VARCHAR(50)"
    
    if table_name == 'starship':
        sql += ", HyperdriveRating DECIMAL(2, 1), MGLT INT"
    sql += ');'

    return sql


def create_planet_table():  # Might have to change population type
    return "CREATE TABLE planet " + \
          "(ID INT NOT NULL PRIMARY KEY, " + \
          "Name VARCHAR(50), " + \
          "RotationPeriod INT, " + \
          "OrbitalPeriod INT, " + \
          "Diameter INT, " + \
          "Gravity VARCHAR(50), " + \
          "SurfaceWater INT, " + \
          "Population BIGINT" + \
          ");"


def create_climate_table():
    return climate_terrain_color_manufacturer_producer_table('climate', 'Description')


def create_terrain_table():
    return climate_terrain_color_manufacturer_producer_table('terrain', 'Description')


def create_planet_climate_rship():
    return create_relationship('planetClimate', 'planet', 'climate')


def create_planet_terrain_rship():
    return create_relationship('planetTerrain', 'planet', 'terrain')


def create_species_table():
    return "CREATE TABLE species " + \
           "(ID INT NOT NULL PRIMARY KEY, " + \
           "Name VARCHAR(50), " + \
           "Classification VARCHAR(20), " + \
           "Designation VARCHAR(20), " + \
           "AverageHeight INT, " + \
           "AverageLifespan INT, " + \
           "Language VARCHAR(50), " + \
           "Homeworld INT, " + \
           "FOREIGN KEY (Homeworld) REFERENCES planet(ID) ON DELETE CASCADE" + \
           ");"


def create_hair_color_table():
    return climate_terrain_color_manufacturer_producer_table('hairColor', 'Color')


def create_eye_color_table():
    return climate_terrain_color_manufacturer_producer_table('eyeColor', 'Color')


def create_skin_color_table():
    return climate_terrain_color_manufacturer_producer_table('skinColor', 'Color')


def create_species_hair_color_rship():
    return create_relationship('speciesHairColor', 'species', 'hairColor')


def create_species_eye_color_rship():
    return create_relationship('speciesEyeColor', 'species', 'eyeColor')


def create_species_skin_color_rship():
    return create_relationship('speciesSkinColor', 'species', 'skinColor')


def create_person_table():
    return "CREATE TABLE person " + \
           "(ID INT NOT NULL PRIMARY KEY, " + \
           "Name VARCHAR(50), " + \
           "Height INT, " + \
           "Mass INT, " + \
           "EyeColor INT, " + \
           "BirthYear VARCHAR(10), " + \
           "Gender VARCHAR(30), " + \
           "Homeworld INT, " + \
           "Species INT, " + \
           "FOREIGN KEY (EyeColor) REFERENCES eyeColor(ID) ON DELETE CASCADE, " + \
           "FOREIGN KEY (Homeworld) REFERENCES planet(ID) ON DELETE CASCADE, " + \
           "FOREIGN KEY (Species) REFERENCES species(ID) ON DELETE CASCADE" + \
           ");"


def create_person_hair_color_rship():
    return create_relationship('personHairColor', 'person', 'hairColor')


def create_person_skin_color_rship():
    return create_relationship('personSkinColor', 'person', 'skinColor')


def create_starship_table():
    return starship_vehicle_table('starship')


def create_starship_person_rship():
    return create_relationship('starshipPerson', 'person', 'starship')


def create_vehicle_table():
    return starship_vehicle_table('vehicle')


def create_vehicle_person_rship():
    return create_relationship('vehiclePerson', 'person', 'vehicle')


def create_manufacturer_table():
    return climate_terrain_color_manufacturer_producer_table('manufacturer', 'Name')


def create_manufacturer_starship_rship():
    return create_relationship('manufacturerStarship', 'starship', 'manufacturer')


def create_manufacturer_vehicle_rship():
    return create_relationship('manufacturerVehicle', 'vehicle', 'manufacturer')


def create_films_table():
    return "CREATE TABLE films " + \
           "(ID INT NOT NULL PRIMARY KEY, " + \
           "EpisodeID INT, " + \
           "Title VARCHAR(30), " + \
           "OpeningCrawl VARCHAR(1024), " + \
           "Director VARCHAR(30), " + \
           "ReleaseDate DATE" + \
           ");"


def create_films_persons_rship():
    return create_relationship('filmsPersons', 'person', 'films')


def create_films_species_rship():
    return create_relationship('filmsSpecies', 'species', 'films')


def create_films_planets_rship():
    return create_relationship('filmsPlanets', 'planet', 'films')


def create_films_starships_rship():
    return create_relationship('filmsStarships', 'starship', 'films')


def create_films_vehicles_rship():
    return create_relationship('filmsVehicles', 'vehicle', 'films')


def create_producer_table():
    return climate_terrain_color_manufacturer_producer_table('producer', 'Name')


def create_producer_films_rship():
    return create_relationship('producerFilms', 'films', 'producer')



def create_tables(cursor):
    create_table_statements = [
        create_planet_table(),
        create_climate_table(),
        create_terrain_table(),

        create_planet_climate_rship(),
        create_planet_terrain_rship(),

        create_species_table(),
        create_hair_color_table(),
        create_eye_color_table(),
        create_skin_color_table(),

        create_species_hair_color_rship(),
        create_species_eye_color_rship(),
        create_species_skin_color_rship(),

        create_person_table(),
        create_person_hair_color_rship(),
        create_person_skin_color_rship(),

        create_starship_table(),
        create_starship_person_rship(),

        create_vehicle_table(),
        create_vehicle_person_rship(),

        create_manufacturer_table(),
        create_manufacturer_starship_rship(),
        create_manufacturer_vehicle_rship(),
        
        create_films_table(),
        create_films_persons_rship(),
        create_films_species_rship(),
        create_films_planets_rship(),
        create_films_starships_rship(),
        create_films_vehicles_rship(),

        create_producer_table(),
        create_producer_films_rship(),
    ]

    for statement in create_table_statements:
        cursor.execute(statement)


def main():
    DB_NAME = 'StarWars'
    cursor, connection = connectToMySQL()
    createDatabase(cursor, DB_NAME)
    cursor.execute(f"USE {DB_NAME}")
    create_tables(cursor)


if __name__ == '__main__':
    main()
