from datetime import datetime
from enum import Enum, IntEnum
from typing import List, Optional
from uuid import UUID, uuid4

from sqlalchemy import JSON, Column
from sqlmodel import Field, SQLModel


class PublicModel(SQLModel):
    """Base SQLModel that enforces the 'public' database schema.

    Intended to be subclassed by application models so they inherit the table
    configuration placing tables in the "public" schema.

    Usage:
        class MyModel(PublicModel, table=True):
            id: Optional[int] = Field(default=None, primary_key=True)
            ...

    Attributes:
        __table_args__ (dict): Table options set to {"schema": "public"}.
    """

    __table_args__ = {"schema": "public"}


class Industry(str, Enum):
    BLANK = ""
    AUTOMOTIVE = "automotive"
    FOOD_AND_BEVERAGE = "food_and_beverage"
    PERSONAL_CARE = "personal_care"
    RETAIL = "retail"
    BEAUTY = "beauty"
    TELECOMMUNICATIONS = "telecommunications"
    AEROSPACE = "aerospace"
    PHARMACEUTICALS = "pharmaceuticals"
    WATER_AND_WASTE_MANAGEMENT = "water_and_waste_management"
    BANKING = "banking"
    HEALTHCARE = "healthcare"
    ENERGY = "energy"
    REAL_ESTATE = "real_estate"
    EDUCATION = "education"
    ELECTRONICS_AND_ELECTRICAL_EQUIPMENT = "electronics_and_electrical_equipment"
    MEDIA_AND_ENTERTAINMENT = "media_and_entertainment"
    TRANSPORTATION_AND_LOGISTICS = "transportation_and_logistics"
    TOURISM_AND_HOSPITALITY = "tourism_and_hospitality"
    INSURANCE = "insurance"
    LEGAL_SERVICES = "legal_services"
    CONSULTING_SERVICES = "consulting_services"
    CHEMICAL_MANUFACTURING = "chemical_manufacturing"
    CONSTRUCTION = "construction"
    E_COMMERCE = "e_commerce"
    FASHION = "fashion"
    DEFENSE = "defense"
    CONSUMER_HEALTHCARE = "consumer_healthcare"
    BEVERAGES = "beverages"
    INDUSTRIAL_SAFETY_PPE = "industrial_safety_ppe"
    ANIMAL_FEED_ADDITIVE = "animal_feed_additive"
    CONSUMER_FOOD_RETAIL = "consumer_food_retail"
    SKINCARE_AND_HYGIENE = "skincare_and_hygiene"
    JEWELRY = "jewelry"
    STEEL_TRANSFORMATION = "steel_transformation"
    INDUSTRIAL_MINERALS = "industrial_minerals"
    AGRICULTURE = "agriculture"
    ENERGY_DISTRIBUTION = "energy_distribution"
    PROFESSIONAL_FOOD_RETAIL = "professional_food_retail"
    HOME_FURNITURES_RETAIL = "home_furnitures_retail"
    DELIVERY_AND_SUPPLY_CHAIN = "delivery_and_supply_chain"
    BEAUTY_CONSUMER_GOODS = "beauty_consumer_goods"


class Client(SQLModel, table=True):
    __tablename__ = "cs_interface_client"
    __table_args__ = {"schema": "public"}

    id: Optional[int] = Field(default=None, primary_key=True)
    company: str = Field(default="D&M")
    customer_id: str
    last_updated: datetime = Field(default_factory=datetime.utcnow)
    hidden: bool = False
    # industry: Optional[Industry] = Field(default=None)
    industry: Optional[str] = Field(default=None)
    tier_id: Optional[int] = Field(default=None, foreign_key="service_tier.id")
    website: Optional[str] = None
    company_profile_image_s3_uri: Optional[str] = None


class Sow(SQLModel, table=True):
    __tablename__ = "cs_interface_sow"
    __table_args__ = {"schema": "public"}

    id: Optional[int] = Field(default=None, primary_key=True)
    client_id: int = Field(foreign_key="client.id")
    name: str
    description: Optional[str] = None
    sow_id: Optional[str] = None
    sow_status: str = Field(default="disabled")
    last_updated: datetime = Field(default_factory=datetime.utcnow)
    hidden: bool = False
    feed_group_id: Optional[int] = None
    geography_id: Optional[str] = Field(default=None, foreign_key="geography.geography_id")
    tier_id: Optional[int] = Field(default=None, foreign_key="service_tier.id")
