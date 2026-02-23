"""This module defines enumerations used across the application."""

from enum import Enum, IntEnum


class Industry(str, Enum):
    """Enumeration of industries for client classification."""

    blank = ""
    automotive = "automotive"
    food_and_beverage = "food_and_beverage"
    personal_care = "personal_care"
    retail = "retail"
    beauty = "beauty"
    telecommunications = "telecommunications"
    aerospace = "aerospace"
    pharmaceuticals = "pharmaceuticals"
    water_and_waste_management = "water_and_waste_management"
    banking = "banking"
    healthcare = "healthcare"
    energy = "energy"
    real_estate = "real_estate"
    education = "education"
    electronics_and_electrical_equipment = "electronics_and_electrical_equipment"
    media_and_entertainment = "media_and_entertainment"
    transportation_and_logistics = "transportation_and_logistics"
    tourism_and_hospitality = "tourism_and_hospitality"
    insurance = "insurance"
    legal_services = "legal_services"
    consulting_services = "consulting_services"
    chemical_manufacturing = "chemical_manufacturing"
    construction = "construction"
    e_commerce = "e_commerce"
    fashion = "fashion"
    defense = "defense"
    consumer_healthcare = "consumer_healthcare"
    beverages = "beverages"
    industrial_safety_ppe = "industrial_safety_ppe"
    animal_feed_additive = "animal_feed_additive"
    consumer_food_retail = "consumer_food_retail"
    skincare_and_hygiene = "skincare_and_hygiene"
    jewelry = "jewelry"
    steel_transformation = "steel_transformation"
    industrial_minerals = "industrial_minerals"
    agriculture = "agriculture"
    energy_distribution = "energy_distribution"
    professional_food_retail = "professional_food_retail"
    home_furnitures_retail = "home_furnitures_retail"
    delivery_and_supply_chain = "delivery_and_supply_chain"
    beauty_consumer_goods = "beauty_consumer_goods"


class ExperimentType(IntEnum):
    """Enumeration of experiment types for classifying different experiment categories."""

    EXPERIMENT = 0
    TOPIC_DEEP_DIVE = 1
    DIGITAL_TWIN = 2
    PATENT_TOPIC_ASSOCIATION = 3


class DatasetProcessStatus(IntEnum):
    """Enumeration of dataset processing statuses to track the state of dataset processing."""

    NOT_STARTED = 0
    IN_PROGRESS = 1
    FINISHED = 2
    FAILED = 3
