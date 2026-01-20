"""
Punjab Property Tax Data Generator (Multi-Tenant)
Generates realistic synthetic data for Punjab property tax tables using Faker library.

Tables generated per tenant:
- eg_pt_property: Property records
- eg_pt_owner: Property owners (1-2 per property)
- eg_pt_unit: Property units (1-5 per property)
- eg_pt_address: Property addresses (1:1)
- eg_demand: Demand records (FY 2015-2025, 10 years)
- egbs_demand_detail: Demand details (6-12 tax heads per demand)

Entity Relationships:
- Property -> Owner (1:N, max 2)
- Property -> Unit (1:N, max 5)
- Property -> Address (1:1)
- Property -> Demand (1:N, 10 years)
- Demand -> Detail (1:N, 6-12 tax heads)

Tax Head Codes (6 always included):
- PT_TAX (always)
- PT_FIRE_CESS (always)
- PT_CANCER_CESS (always)
- PT_TIME_PENALTY (always)
- PT_TIME_INTEREST (always)
- PT_ROUND_OFF (always)

Usage:
    python punjab_bulk_data_generator.py                    # Generate for all tenants
    python punjab_bulk_data_generator.py --tenant phagwara  # Generate for specific tenant
    python punjab_bulk_data_generator.py --list-tenants     # List available tenants
"""

from faker import Faker
import uuid
import random
import json
from datetime import datetime, timedelta
import time
import csv
import os
import logging
import argparse

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Faker with Indian locale
fake = Faker('en_IN')

# === TENANT CONFIGURATIONS ===
# Add new tenants here with their specific settings
TENANT_CONFIGS = {
    "phagwara": {
        "tenant_id": "pb.phagwara",
        "city": "Phagwara",
        "district": "Kapurthala",
        "property_prefix": "PT-1014",
        "property_start_index": 700000,
        "num_properties": 10000,
        "pincodes": ["144401", "144409", ""],
        "localities": [
            "PLC14", "PLC20", "PLC22", "PLC30", "PLC31", "PLC37", "PLC38", "PLC50", "PLC51",
            "PLC52", "PLC54", "PLC57", "PLC61", "PLC68", "PLC72", "PLC74", "PLC79", "PLC81",
            "PLC82", "PLC83", "PLC84", "PLC98", "PLC99", "PLC103", "PLC105", "PLC112", "PLC113",
            "PLC114", "PLC131", "PLC137", "PLC142", "PLC144", "PLC145", "PLC151", "PLC152",
            "PLC154", "PLC155", "PLC159", "PLC160", "PLC163", "PLC166", "PLC178", "PLC181",
            "PLC185", "PLC187", "PLC191", "PLC200", "PLC210"
        ],
        "building_names": [
            "Handa City Centre", "Green Park", "Doctor Colony", "Rangalia Mohalla",
            "Saraba Nagar", "Guru Teg Bahadur Nagar", "Greater Kailash Colony",
            "Bedian Mohalla", "Joshian Mohalla", "Thatiyara Mohalla", ""
        ]
    },
    "jalandhar": {
        "tenant_id": "pb.jalandhar",
        "city": "Jalandhar",
        "district": "Jalandhar",
        "property_prefix": "PT-1001",
        "property_start_index": 800000,
        "num_properties": 10000,
        "pincodes": ["144001", "144002", "144003", "144004", ""],
        "localities": [
            "JLC01", "JLC02", "JLC03", "JLC04", "JLC05", "JLC06", "JLC07", "JLC08",
            "JLC09", "JLC10", "JLC11", "JLC12", "JLC13", "JLC14", "JLC15", "JLC16",
            "JLC17", "JLC18", "JLC19", "JLC20", "JLC21", "JLC22", "JLC23", "JLC24"
        ],
        "building_names": [
            "Model Town", "Civil Lines", "Guru Nanak Pura", "Mota Singh Nagar",
            "Shaheed Bhagat Singh Nagar", "New Jawahar Nagar", "Lajpat Nagar",
            "Basti Sheikh", "Leather Complex", "Industrial Area", ""
        ]
    },
    "ludhiana": {
        "tenant_id": "pb.ludhiana",
        "city": "Ludhiana",
        "district": "Ludhiana",
        "property_prefix": "PT-1002",
        "property_start_index": 900000,
        "num_properties": 10000,
        "pincodes": ["141001", "141002", "141003", "141004", "141008", ""],
        "localities": [
            "LLC01", "LLC02", "LLC03", "LLC04", "LLC05", "LLC06", "LLC07", "LLC08",
            "LLC09", "LLC10", "LLC11", "LLC12", "LLC13", "LLC14", "LLC15", "LLC16",
            "LLC17", "LLC18", "LLC19", "LLC20", "LLC21", "LLC22", "LLC23", "LLC24"
        ],
        "building_names": [
            "Model Town", "Civil Lines", "Sarabha Nagar", "BRS Nagar",
            "Dugri", "Pakhowal Road", "Ferozepur Road", "GT Road",
            "Industrial Area A", "Industrial Area B", "Focal Point", ""
        ]
    },
    "amritsar": {
        "tenant_id": "pb.amritsar",
        "city": "Amritsar",
        "district": "Amritsar",
        "property_prefix": "PT-1003",
        "property_start_index": 600000,
        "num_properties": 10000,
        "pincodes": ["143001", "143002", "143003", "143004", "143005", ""],
        "localities": [
            "ALC01", "ALC02", "ALC03", "ALC04", "ALC05", "ALC06", "ALC07", "ALC08",
            "ALC09", "ALC10", "ALC11", "ALC12", "ALC13", "ALC14", "ALC15", "ALC16",
            "ALC17", "ALC18", "ALC19", "ALC20", "ALC21", "ALC22", "ALC23", "ALC24"
        ],
        "building_names": [
            "Ranjit Avenue", "Green Avenue", "Lawrence Road", "Mall Road",
            "GT Road", "Majitha Road", "Batala Road", "Circular Road",
            "Golden Temple Area", "Hall Bazaar", "Katra Jaimal Singh", ""
        ]
    },
    "patiala": {
        "tenant_id": "pb.patiala",
        "city": "Patiala",
        "district": "Patiala",
        "property_prefix": "PT-1004",
        "property_start_index": 500000,
        "num_properties": 10000,
        "pincodes": ["147001", "147002", "147003", "147004", ""],
        "localities": [
            "PTC01", "PTC02", "PTC03", "PTC04", "PTC05", "PTC06", "PTC07", "PTC08",
            "PTC09", "PTC10", "PTC11", "PTC12", "PTC13", "PTC14", "PTC15", "PTC16"
        ],
        "building_names": [
            "Model Town", "Urban Estate", "Punjabi Bagh", "New Lal Bagh",
            "Tripuri Town", "Rajpura Road", "Sangrur Road", "Sirhind Road",
            "Bahadurgarh Road", "Leela Bhawan", ""
        ]
    },
    "abohar": {
        "tenant_id": "pb.abohar",
        "city": "Abohar",
        "district": "Fazilka",
        "property_prefix": "PT-1005",
        "property_start_index": 400000,
        "num_properties": 5000,
        "pincodes": ["152116", ""],
        "localities": [
            "ABC01", "ABC02", "ABC03", "ABC04", "ABC05", "ABC06", "ABC07", "ABC08",
            "ABC09", "ABC10", "ABC11", "ABC12"
        ],
        "building_names": [
            "Model Town", "Civil Lines", "Grain Market Area", "Bus Stand Road",
            "Railway Road", "Sukhchain Nagar", ""
        ]
    },
    "bathinda": {
        "tenant_id": "pb.bathinda",
        "city": "Bathinda",
        "district": "Bathinda",
        "property_prefix": "PT-1006",
        "property_start_index": 300000,
        "num_properties": 8000,
        "pincodes": ["151001", "151002", "151003", ""],
        "localities": [
            "BTC01", "BTC02", "BTC03", "BTC04", "BTC05", "BTC06", "BTC07", "BTC08",
            "BTC09", "BTC10", "BTC11", "BTC12", "BTC13", "BTC14", "BTC15", "BTC16"
        ],
        "building_names": [
            "Model Town", "Civil Lines", "Rose Garden", "Thermal Colony",
            "Guru Nanak Dev Nagar", "Giani Zail Singh Nagar", "Goniana Road", ""
        ]
    },
    "mohali": {
        "tenant_id": "pb.mohali",
        "city": "Mohali",
        "district": "SAS Nagar",
        "property_prefix": "PT-1007",
        "property_start_index": 200000,
        "num_properties": 10000,
        "pincodes": ["160055", "160059", "160062", "160071", ""],
        "localities": [
            "MHC01", "MHC02", "MHC03", "MHC04", "MHC05", "MHC06", "MHC07", "MHC08",
            "MHC09", "MHC10", "MHC11", "MHC12", "MHC13", "MHC14", "MHC15", "MHC16",
            "MHC17", "MHC18", "MHC19", "MHC20"
        ],
        "building_names": [
            "Phase 1", "Phase 2", "Phase 3A", "Phase 3B", "Phase 4", "Phase 5",
            "Phase 6", "Phase 7", "Phase 8", "Phase 9", "Phase 10", "Phase 11",
            "Sector 70", "Sector 71", "IT City", "Aerocity", ""
        ]
    }
}

# === Global Configuration ===
OUTPUT_BASE_DIR = "../generated_bulk_punjab_data"

# === Reference Data (common across tenants) ===

# Property types
PROPERTY_TYPES = [
    "BUILTUP.INDEPENDENTPROPERTY",
    "BUILTUP.SHAREDPROPERTY",
    "VACANT"
]
PROPERTY_TYPE_WEIGHTS = [90, 5, 5]

# Ownership categories
OWNERSHIP_CATEGORIES = [
    "INDIVIDUAL.SINGLEOWNER",
    "INDIVIDUAL.MULTIPLEOWNERS"
]
OWNERSHIP_WEIGHTS = [70, 30]

# Usage categories
USAGE_CATEGORIES = [
    "RESIDENTIAL",
    "NONRESIDENTIAL.COMMERCIAL",
    "NONRESIDENTIAL.INDUSTRIAL",
    "MIXED"
]
USAGE_WEIGHTS = [60, 20, 5, 15]

# Creation reasons
CREATION_REASONS = ["CREATE", "UPDATE", "MUTATION"]
CREATION_REASON_WEIGHTS = [20, 70, 10]

# Channels
CHANNELS = ["CFC_COUNTER", "MIGRATION", "SYSTEM"]
CHANNEL_WEIGHTS = [50, 40, 10]

# Sources
SOURCES = ["MUNICIPAL_RECORDS", "WATER_CHARGES"]
SOURCE_WEIGHTS = [90, 10]

# Owner types
OWNER_TYPES = ["NONE", "WIDOW","DEFENSE","HANDICAPPED","FREEDOMFIGHTER"]
OWNER_TYPE_WEIGHTS = [70, 10, 10, 5, 5]

# Relationships
RELATIONSHIPS = ["Father", "Husband", "FATHER"]
RELATIONSHIP_WEIGHTS = [50, 45, 5]

# Unit types
UNIT_TYPES = ["OTHERCOMMERCIAL", "OTHERINDUSTRIAL", "SCHOOL","WAREHOUSE","MANUFACTURINGFACILITY","HOTELS","MALLS","PVTHOSPITAL"]
UNIT_TYPE_WEIGHTS = [30, 20, 10, 10, 10, 10, 5, 5,]

# Usage category for units
UNIT_USAGE_CATEGORIES = [
    "RESIDENTIAL",
    "NONRESIDENTIAL.COMMERCIAL.OTHERCOMMERCIALSUBMINOR.OTHERCOMMERCIAL",
    "NONRESIDENTIAL.INDUSTRIAL.OTHERINDUSTRIALSUBMINOR.OTHERINDUSTRIAL",
    "NONRESIDENTIAL.INSTITUTIONAL.EDUCATIONAL.SCHOOL",
    "NONRESIDENTIAL.INDUSTRIAL.WAREHOUSE.WAREHOUSE",
    "NONRESIDENTIAL.COMMERCIAL.MEDICALFACILITY.PVTHOSPITAL",
    "NONRESIDENTIAL.COMMERCIAL.HOTELS.HOTELS"
]
UNIT_USAGE_WEIGHTS = [40, 30, 10, 5, 5, 5, 5]

# Occupancy types
OCCUPANCY_TYPES = ["SELFOCCUPIED", "RENTED", "UNOCCUPIED", "PG"]
OCCUPANCY_WEIGHTS = [60, 15, 20, 5]

# Tax head codes - always included marked with True
TAX_HEAD_CODES = {
    "PT_TAX": True,
    "PT_FIRE_CESS": True,
    "PT_CANCER_CESS": True,
    "PT_TIME_PENALTY": True,
    "PT_TIME_INTEREST": True,
    "PT_ROUNDOFF": True,
    "PT_UNIT_USAGE_EXEMPTION": False,
    "PT_OWNER_EXEMPTION": False,
    "PT_EDUCATION_CESS": False,
    "PT_LIBRARY_CESS": False,
    "PT_SEWERAGE_CESS": False,
    "PT_TIME_REBATE": False,
    "PT_ADVANCE_CARRYFORWARD": False
}

# Fiscal years (FY 2015 to FY 2025)
FISCAL_YEARS = []
for year in range(2015, 2026):
    start_ts = int(datetime(year, 4, 1).timestamp() * 1000)
    end_ts = int(datetime(year + 1, 3, 31, 23, 59, 59).timestamp() * 1000)
    FISCAL_YEARS.append({
        "year": f"{year}-{str(year+1)[2:]}",
        "from": start_ts,
        "to": end_ts
    })

# Common user IDs
USER_IDS = [
    "84bcc12e-de5a-4ae7-a177-bd80d4011127",
    "24686674-451c-4edf-8e76-958fdf759fd2",
    "4c59e315-7438-48e9-8582-eac9f94512dc",
    "c3b2c310-87c6-45d7-b261-727c8a842617",
    "dc676349-1439-4268-a9bc-cd4beaf159f0",
    "4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65",
    "ed7ff6ba-7823-4639-bc22-bfc3ea919518",
    "7c89c026-63d3-425b-942d-cb51b0be9e68"
]


def generate_uuid():
    return str(uuid.uuid4())


def random_weighted_choice(choices, weights):
    return random.choices(choices, weights=weights, k=1)[0]


def random_timestamp(start_year=2019, end_year=2025):
    """Generate random timestamp in milliseconds"""
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    delta = end - start
    random_days = random.randint(0, delta.days)
    random_date = start + timedelta(days=random_days,
                                     hours=random.randint(0, 23),
                                     minutes=random.randint(0, 59),
                                     seconds=random.randint(0, 59))
    return int(random_date.timestamp() * 1000)


def generate_property_id(tenant_config, index):
    """Generate property ID in format PT-XXXX-XXXXXX"""
    return f"{tenant_config['property_prefix']}-{tenant_config['property_start_index'] + index}"


def generate_acknowledgement_number(created_time):
    """Generate acknowledgement number based on date"""
    dt = datetime.fromtimestamp(created_time / 1000)
    random_num = random.randint(1000000, 9999999)
    return f"AC-{dt.year}-{dt.month:02d}-{dt.day:02d}-{random_num}"


def generate_old_property_id(tenant_config):
    """Generate old property ID format"""
    city_prefix = tenant_config['city'][:2].upper()
    if random.random() < 0.7:
        area_code = random.choice(["0", "5", "6", "4", "3", "2", "1"])
        sub_code = random.choice(["A", "B", "C", ""])
        num = random.randint(100, 9999)
        return f"{city_prefix}{area_code}{random.randint(0,9)}{sub_code}{num:04d}"
    return ""


def generate_land_area():
    """Generate realistic land area in sq yards"""
    area_ranges = [
        (10, 100, 30),
        (100, 300, 40),
        (300, 500, 20),
        (500, 2000, 10)
    ]
    range_choice = random_weighted_choice(
        [(r[0], r[1]) for r in area_ranges],
        [r[2] for r in area_ranges]
    )
    return round(random.uniform(range_choice[0], range_choice[1]), 2)


def generate_carpet_area(land_area):
    """Generate carpet area based on land area"""
    return round(land_area * random.uniform(0.3, 0.9), 2)


def generate_tax_amount(usage_category, land_area):
    """Generate realistic tax amount based on property characteristics"""
    base_rate = {
        "RESIDENTIAL": 2.0,
        "NONRESIDENTIAL.COMMERCIAL": 5.0,
        "NONRESIDENTIAL.INDUSTRIAL": 4.0,
        "MIXED": 3.5
    }.get(usage_category, 2.5)
    return round(land_area * base_rate * random.uniform(0.8, 1.2), 2)


def generate_arv():
    """Generate Annual Rental Value"""
    if random.random() < 0.3:
        return round(random.uniform(3600, 1200000), 2)
    return None


def generate_property(tenant_config, index, created_time):
    """Generate a single property record"""
    property_uuid = generate_uuid()
    property_id = generate_property_id(tenant_config, index)
    user_id = random.choice(USER_IDS)

    usage_category = random_weighted_choice(USAGE_CATEGORIES, USAGE_WEIGHTS)
    creation_reason = random_weighted_choice(CREATION_REASONS, CREATION_REASON_WEIGHTS)

    land_area = generate_land_area()
    superbuiltuparea = round(land_area * random.uniform(0, 0.5), 2) if random.random() < 0.2 else 0

    additional_details = {}
    if random.random() < 0.5:
        additional_details["inflammable"] = random.choice([True, False, None])
    if random.random() < 0.3:
        additional_details["heightAbove36Feet"] = False
    if usage_category in ["NONRESIDENTIAL.COMMERCIAL", "MIXED"]:
        additional_details["businessName"] = random.choice(["SHOP", "COMM", "Commercial", ""])
    if random.random() < 0.3:
        additional_details["yearConstruction"] = f"{random.randint(2000, 2023)}-{random.randint(14, 25)}"
    if creation_reason == "UPDATE":
        additional_details["previousPropertyUuid"] = generate_uuid()

    return {
        "id": property_uuid,
        "propertyid": property_id,
        "tenantid": tenant_config["tenant_id"],
        "surveyid": "0000000000" if random.random() < 0.1 else "",
        "accountid": random.choice(USER_IDS),
        "oldpropertyid": generate_old_property_id(tenant_config) if random.random() < 0.3 else "",
        "status": "ACTIVE",
        "acknowldgementnumber": generate_acknowledgement_number(created_time),
        "propertytype": random_weighted_choice(PROPERTY_TYPES, PROPERTY_TYPE_WEIGHTS),
        "ownershipcategory": random_weighted_choice(OWNERSHIP_CATEGORIES, OWNERSHIP_WEIGHTS),
        "usagecategory": usage_category,
        "creationreason": creation_reason,
        "nooffloors": random.randint(1, 5),
        "landarea": f"{land_area:.2f}",
        "superbuiltuparea": f"{superbuiltuparea:.2f}" if superbuiltuparea > 0 else "",
        "linkedproperties": "",
        "source": random_weighted_choice(SOURCES, SOURCE_WEIGHTS),
        "channel": random_weighted_choice(CHANNELS, CHANNEL_WEIGHTS),
        "createdby": user_id,
        "lastmodifiedby": user_id,
        "createdtime": created_time,
        "lastmodifiedtime": created_time + random.randint(0, 100000),
        "additionaldetails": json.dumps(additional_details) if additional_details else "null"
    }


def generate_owner(tenant_config, property_uuid, property_ownership, owner_index, created_time):
    """Generate an owner record"""
    user_id = generate_uuid()
    owner_user_id = random.choice(USER_IDS)

    if property_ownership == "INDIVIDUAL.SINGLEOWNER":
        ownership_percentage = 100
    else:
        ownership_percentage = random.choice([50, 33, 34, 25, 75])

    return {
        "ownerinfouuid": generate_uuid(),
        "tenantid": tenant_config["tenant_id"],
        "propertyid": property_uuid,
        "userid": user_id,
        "status": "ACTIVE",
        "isprimaryowner": "",
        "ownertype": random_weighted_choice(OWNER_TYPES, OWNER_TYPE_WEIGHTS),
        "ownershippercentage": ownership_percentage,
        "institutionid": "",
        "relationship": random_weighted_choice(RELATIONSHIPS, RELATIONSHIP_WEIGHTS),
        "createdby": owner_user_id,
        "createdtime": created_time,
        "lastmodifiedby": owner_user_id,
        "lastmodifiedtime": created_time + random.randint(0, 100000),
        "additionaldetails": ""
    }


def generate_unit(tenant_config, property_uuid, property_usage, floor_no, created_time):
    """Generate a unit record"""
    user_id = random.choice(USER_IDS)

    if property_usage == "RESIDENTIAL":
        unit_usage = "RESIDENTIAL"
        unit_type = random.choice(["false", ""])
    elif property_usage in ["NONRESIDENTIAL.COMMERCIAL", "NONRESIDENTIAL.INDUSTRIAL"]:
        unit_usage = "NONRESIDENTIAL.COMMERCIAL.OTHERCOMMERCIALSUBMINOR.OTHERCOMMERCIAL"
        unit_type = "OTHERCOMMERCIAL"
    else:
        unit_usage = random_weighted_choice(UNIT_USAGE_CATEGORIES, UNIT_USAGE_WEIGHTS)
        unit_type = random_weighted_choice(UNIT_TYPES, UNIT_TYPE_WEIGHTS)

    occupancy_type = random_weighted_choice(OCCUPANCY_TYPES, OCCUPANCY_WEIGHTS)
    carpet_area = generate_carpet_area(random.uniform(20, 200))
    arv = generate_arv() if occupancy_type == "RENTED" else None

    additional_details = {}
    if occupancy_type in ["RENTED", "PG"]:
        additional_details["rentedformonths"] = 12
        additional_details["usageForDueMonths"] = random.choice(["SELFOCCUPIED", "UNOCCUPIED"])

    return {
        "id": generate_uuid(),
        "tenantid": tenant_config["tenant_id"],
        "propertyid": property_uuid,
        "floorno": floor_no,
        "unittype": unit_type,
        "usagecategory": unit_usage,
        "occupancytype": occupancy_type,
        "occupancydate": 0,
        "carpetarea": "",
        "builtuparea": f"{carpet_area:.2f}",
        "plintharea": "",
        "superbuiltuparea": "",
        "arv": f"{arv:.2f}" if arv else "",
        "constructiontype": "",
        "constructiondate": "",
        "dimensions": "null",
        "active": random.choice(["t", "f"]) if random.random() < 0.1 else "t",
        "createdby": user_id,
        "createdtime": created_time,
        "lastmodifiedby": user_id,
        "lastmodifiedtime": created_time + random.randint(0, 100000),
        "additionaldetails": json.dumps(additional_details) if additional_details else "null"
    }


def generate_address(tenant_config, property_uuid, created_time):
    """Generate an address record"""
    user_id = random.choice(USER_IDS)

    door_no = ""
    if random.random() < 0.4:
        door_no = f"{random.randint(1, 500)}"
        if random.random() < 0.3:
            door_no = f"B-{random.choice(['XII', 'XLIV', 'XXXII'])}/{door_no}"

    return {
        "tenantid": tenant_config["tenant_id"],
        "id": generate_uuid(),
        "propertyid": property_uuid,
        "doorno": door_no,
        "plotno": "",
        "buildingname": random.choice(tenant_config["building_names"]) if random.random() < 0.2 else "",
        "street": "",
        "landmark": "",
        "city": tenant_config["city"],
        "pincode": random.choice(tenant_config["pincodes"]),
        "locality": random.choice(tenant_config["localities"]),
        "district": tenant_config["district"],
        "region": "",
        "state": "Punjab",
        "country": "India",
        "latitude": "0.000000",
        "longitude": "0.0000000",
        "createdby": user_id,
        "createdtime": created_time,
        "lastmodifiedby": user_id,
        "lastmodifiedtime": created_time + random.randint(0, 100000),
        "additionaldetails": "null"
    }


def generate_demand(tenant_config, property_id, fiscal_year, payer_id, created_time):
    """Generate a demand record"""
    user_id = random.choice(USER_IDS)

    return {
        "id": generate_uuid(),
        "consumercode": property_id,
        "consumertype": "BUILTUP",
        "businessservice": "PT",
        "payer": payer_id,
        "taxperiodfrom": fiscal_year["from"],
        "taxperiodto": fiscal_year["to"],
        "createdby": user_id,
        "createdtime": created_time,
        "lastmodifiedby": user_id,
        "lastmodifiedtime": created_time + random.randint(0, 1000000000),
        "tenantid": tenant_config["tenant_id"],
        "minimumamountpayable": "100.00",
        "status": "ACTIVE",
        "additionaldetails": "null",
        "billexpirytime": random.choice(["", "0"]),
        "ispaymentcompleted": random.choice(["t", "f"]),
        "fixedbillexpirydate": ""
    }


def generate_demand_details(tenant_config, demand_id, usage_category, land_area, created_time, fiscal_year, is_property_defaulter):
    """Generate demand detail records (6-12 tax heads)

    Args:
        is_property_defaulter: Boolean indicating if this property is a defaulter.
                               Penalty/interest only apply to defaulter properties.
    """
    user_id = random.choice(USER_IDS)
    details = []

    mandatory_heads = [k for k, v in TAX_HEAD_CODES.items() if v]
    optional_heads = [k for k, v in TAX_HEAD_CODES.items() if not v]

    num_optional = random.randint(0, min(6, len(optional_heads)))
    selected_optional = random.sample(optional_heads, num_optional)

    all_heads = mandatory_heads + selected_optional
    base_tax = generate_tax_amount(usage_category, land_area)

    # Calculate years overdue for this fiscal year
    fy_start_year = int(fiscal_year["year"][:4])
    current_year = 2025
    years_overdue = current_year - fy_start_year

    # Penalty/interest only applies if:
    # 1. Property is a defaulter AND
    # 2. The fiscal year is at least 1 year old (no penalty for current year even for defaulters)
    apply_penalty_interest = is_property_defaulter and years_overdue >= 1

    for tax_head in all_heads:
        tax_amount = 0.0
        collection_amount = 0.0

        if tax_head == "PT_TAX":
            tax_amount = base_tax
        elif tax_head == "PT_FIRE_CESS":
            tax_amount = round(base_tax * 0.05, 2) if random.random() < 0.5 else 0
        elif tax_head == "PT_CANCER_CESS":
            tax_amount = round(base_tax * 0.02, 2)
        elif tax_head == "PT_TIME_PENALTY":
            # Penalty: 10-25% of base tax for defaulters, increases with years overdue
            if apply_penalty_interest:
                penalty_rate = min(0.10 + (years_overdue * 0.05), 0.25)  # Max 25%
                tax_amount = round(base_tax * penalty_rate * random.uniform(0.8, 1.2), 2)
            else:
                tax_amount = 0
        elif tax_head == "PT_TIME_INTEREST":
            # Interest: 12-18% per annum on base tax for defaulters
            if apply_penalty_interest:
                annual_interest_rate = random.uniform(0.12, 0.18)
                # Interest compounds based on years overdue
                interest_amount = base_tax * annual_interest_rate * min(years_overdue, 5)
                tax_amount = round(interest_amount * random.uniform(0.9, 1.1), 2)
            else:
                tax_amount = 0
        elif tax_head == "PT_ROUNDOFF":
            tax_amount = round(random.uniform(-0.5, 0.5), 2)
        elif tax_head == "PT_TIME_REBATE":
            # Rebate only for non-defaulters who paid on time
            if not is_property_defaulter and random.random() < 0.4:
                tax_amount = round(-base_tax * random.uniform(0.05, 0.10), 2)
            else:
                tax_amount = 0
        elif tax_head in ["PT_UNIT_USAGE_EXEMPTION", "PT_OWNER_EXEMPTION"]:
            # Some exemptions for special categories
            if random.random() < 0.15:
                tax_amount = round(-base_tax * random.uniform(0.10, 0.50), 2)
            else:
                tax_amount = 0
        elif tax_head in ["PT_EDUCATION_CESS", "PT_LIBRARY_CESS", "PT_SEWERAGE_CESS"]:
            tax_amount = round(base_tax * random.uniform(0.01, 0.03), 2)
        elif tax_head == "PT_ADVANCE_CARRYFORWARD":
            # Some properties may have advance payments
            if random.random() < 0.1:
                tax_amount = round(-base_tax * random.uniform(0.5, 1.5), 2)
            else:
                tax_amount = 0

        # Collection amount logic
        # Defaulters have lower collection rates
        if is_property_defaulter:
            if random.random() < 0.4:  # 40% partial/full collection for defaulters
                collection_amount = round(tax_amount * random.uniform(0.3, 1.0), 2)
            else:
                collection_amount = 0
        else:
            if random.random() < 0.85:  # 85% collection rate for non-defaulters
                collection_amount = tax_amount

        details.append({
            "id": generate_uuid(),
            "demandid": demand_id,
            "taxheadcode": tax_head,
            "taxamount": f"{tax_amount:.2f}",
            "collectionamount": f"{collection_amount:.2f}",
            "createdby": user_id,
            "createdtime": created_time,
            "lastmodifiedby": user_id,
            "lastmodifiedtime": created_time + random.randint(0, 1000000000),
            "tenantid": tenant_config["tenant_id"],
            "additionaldetails": ""
        })

    return details


def write_csv(output_dir, filename, data, fieldnames):
    """Write data to CSV file"""
    filepath = os.path.join(output_dir, filename)
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    logger.info(f"Written {len(data)} records to {filename}")


def generate_tenant_data(tenant_name, tenant_config):
    """Generate all data for a single tenant"""
    start_time = time.time()

    # Create tenant-specific output directory
    output_dir = os.path.join(OUTPUT_BASE_DIR, tenant_name)
    os.makedirs(output_dir, exist_ok=True)

    num_properties = tenant_config["num_properties"]
    logger.info(f"Generating data for tenant: {tenant_name} ({tenant_config['tenant_id']})")
    logger.info(f"Number of properties: {num_properties}")

    properties = []
    owners = []
    units = []
    addresses = []
    demands = []
    demand_details = []

    for i in range(num_properties):
        if (i + 1) % 1000 == 0:
            logger.info(f"[{tenant_name}] Processing property {i + 1}/{num_properties}")

        created_time = random_timestamp(2019, 2025)

        prop = generate_property(tenant_config, i, created_time)
        properties.append(prop)

        property_uuid = prop["id"]
        property_id = prop["propertyid"]
        ownership_category = prop["ownershipcategory"]
        usage_category = prop["usagecategory"]
        land_area = float(prop["landarea"])
        num_floors = int(prop["nooffloors"])

        # Determine if this property is a defaulter (35% of properties)
        # This is a property-level attribute - consistent across all demands for this property
        is_property_defaulter = random.random() < 0.35

        if ownership_category == "INDIVIDUAL.SINGLEOWNER":
            num_owners = 1
        else:
            num_owners = random.randint(1, 2)

        payer_id = None
        for j in range(num_owners):
            owner = generate_owner(tenant_config, property_uuid, ownership_category, j, created_time)
            owners.append(owner)
            if j == 0:
                payer_id = owner["userid"]

        num_units = random.randint(1, min(5, num_floors + 2))
        for j in range(num_units):
            floor_no = j if j < num_floors else random.randint(0, num_floors - 1)
            unit = generate_unit(tenant_config, property_uuid, usage_category, floor_no, created_time)
            units.append(unit)

        address = generate_address(tenant_config, property_uuid, created_time)
        addresses.append(address)

        for fy in FISCAL_YEARS:
            demand_created_time = random_timestamp(int(fy["year"][:4]), int(fy["year"][:4]) + 1)
            demand = generate_demand(tenant_config, property_id, fy, payer_id, demand_created_time)
            demands.append(demand)

            details = generate_demand_details(tenant_config, demand["id"], usage_category, land_area, demand_created_time, fy, is_property_defaulter)
            demand_details.extend(details)

    logger.info(f"[{tenant_name}] Writing CSV files...")

    # Get city name for file suffix
    city_name = tenant_name.lower()

    property_fields = [
        "id", "propertyid", "tenantid", "surveyid", "accountid", "oldpropertyid",
        "status", "acknowldgementnumber", "propertytype", "ownershipcategory",
        "usagecategory", "creationreason", "nooffloors", "landarea", "superbuiltuparea",
        "linkedproperties", "source", "channel", "createdby", "lastmodifiedby",
        "createdtime", "lastmodifiedtime", "additionaldetails"
    ]
    write_csv(output_dir, f"eg_pt_property_{city_name}.csv", properties, property_fields)

    owner_fields = [
        "ownerinfouuid", "tenantid", "propertyid", "userid", "status", "isprimaryowner",
        "ownertype", "ownershippercentage", "institutionid", "relationship",
        "createdby", "createdtime", "lastmodifiedby", "lastmodifiedtime", "additionaldetails"
    ]
    write_csv(output_dir, f"eg_pt_owner_{city_name}.csv", owners, owner_fields)

    unit_fields = [
        "id", "tenantid", "propertyid", "floorno", "unittype", "usagecategory",
        "occupancytype", "occupancydate", "carpetarea", "builtuparea", "plintharea",
        "superbuiltuparea", "arv", "constructiontype", "constructiondate", "dimensions",
        "active", "createdby", "createdtime", "lastmodifiedby", "lastmodifiedtime",
        "additionaldetails"
    ]
    write_csv(output_dir, f"eg_pt_unit_{city_name}.csv", units, unit_fields)

    address_fields = [
        "tenantid", "id", "propertyid", "doorno", "plotno", "buildingname",
        "street", "landmark", "city", "pincode", "locality", "district",
        "region", "state", "country", "latitude", "longitude",
        "createdby", "createdtime", "lastmodifiedby", "lastmodifiedtime", "additionaldetails"
    ]
    write_csv(output_dir, f"eg_pt_address_{city_name}.csv", addresses, address_fields)

    demand_fields = [
        "id", "consumercode", "consumertype", "businessservice", "payer",
        "taxperiodfrom", "taxperiodto", "createdby", "createdtime",
        "lastmodifiedby", "lastmodifiedtime", "tenantid", "minimumamountpayable",
        "status", "additionaldetails", "billexpirytime", "ispaymentcompleted",
        "fixedbillexpirydate"
    ]
    write_csv(output_dir, f"eg_demand_{city_name}.csv", demands, demand_fields)

    detail_fields = [
        "id", "demandid", "taxheadcode", "taxamount", "collectionamount",
        "createdby", "createdtime", "lastmodifiedby", "lastmodifiedtime",
        "tenantid", "additionaldetails"
    ]

    detail_dir = os.path.join(output_dir, "egbs_demand_detail_v1")
    os.makedirs(detail_dir, exist_ok=True)

    chunk_size = 50000
    for idx, i in enumerate(range(0, len(demand_details), chunk_size)):
        chunk = demand_details[i:i + chunk_size]
        chunk_file = os.path.join(detail_dir, f"output_{idx}.csv")
        with open(chunk_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=detail_fields)
            writer.writeheader()
            writer.writerows(chunk)
        logger.info(f"[{tenant_name}] Written {len(chunk)} records to output_{idx}.csv")

    elapsed_time = time.time() - start_time
    minutes = int(elapsed_time // 60)
    seconds = int(elapsed_time % 60)

    logger.info("=" * 60)
    logger.info(f"[{tenant_name}] Data generation complete!")
    logger.info(f"Properties: {len(properties)}")
    logger.info(f"Owners: {len(owners)}")
    logger.info(f"Units: {len(units)}")
    logger.info(f"Addresses: {len(addresses)}")
    logger.info(f"Demands: {len(demands)}")
    logger.info(f"Demand Details: {len(demand_details)}")
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"Time: {minutes}m {seconds}s")
    logger.info("=" * 60)

    return {
        "tenant": tenant_name,
        "properties": len(properties),
        "owners": len(owners),
        "units": len(units),
        "addresses": len(addresses),
        "demands": len(demands),
        "demand_details": len(demand_details),
        "time_seconds": elapsed_time
    }


def main():
    """Main function with command-line argument parsing"""
    global OUTPUT_BASE_DIR

    parser = argparse.ArgumentParser(
        description="Punjab Property Tax Data Generator (Multi-Tenant)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python punjab_bulk_data_generator.py                      # Generate for all tenants
  python punjab_bulk_data_generator.py --tenant phagwara    # Generate for phagwara only
  python punjab_bulk_data_generator.py --tenant phagwara --num-properties 5000
  python punjab_bulk_data_generator.py --list-tenants       # List available tenants
        """
    )

    parser.add_argument(
        "--tenant", "-t",
        type=str,
        help="Generate data for a specific tenant only (e.g., phagwara, jalandhar)"
    )

    parser.add_argument(
        "--num-properties", "-n",
        type=int,
        help="Override the number of properties to generate (applies to specified tenant)"
    )

    parser.add_argument(
        "--list-tenants", "-l",
        action="store_true",
        help="List all available tenants and their configurations"
    )

    parser.add_argument(
        "--output-dir", "-o",
        type=str,
        default=OUTPUT_BASE_DIR,
        help=f"Base output directory (default: {OUTPUT_BASE_DIR})"
    )

    args = parser.parse_args()

    # Update global output directory if specified
    OUTPUT_BASE_DIR = args.output_dir

    # List tenants if requested
    if args.list_tenants:
        print("\nAvailable Tenants:")
        print("=" * 80)
        print(f"{'Tenant':<15} {'Tenant ID':<20} {'City':<15} {'Properties':<12} {'Prefix'}")
        print("-" * 80)
        for name, config in TENANT_CONFIGS.items():
            print(f"{name:<15} {config['tenant_id']:<20} {config['city']:<15} {config['num_properties']:<12} {config['property_prefix']}")
        print("=" * 80)
        return

    # Determine which tenants to process
    if args.tenant:
        tenant_name = args.tenant.lower()
        if tenant_name not in TENANT_CONFIGS:
            logger.error(f"Unknown tenant: {tenant_name}")
            logger.info(f"Available tenants: {', '.join(TENANT_CONFIGS.keys())}")
            return
        tenants_to_process = {tenant_name: TENANT_CONFIGS[tenant_name].copy()}

        # Override num_properties if specified
        if args.num_properties:
            tenants_to_process[tenant_name]["num_properties"] = args.num_properties
    else:
        tenants_to_process = TENANT_CONFIGS.copy()

    # Create base output directory
    os.makedirs(OUTPUT_BASE_DIR, exist_ok=True)

    # Generate data for each tenant
    total_start_time = time.time()
    results = []

    for tenant_name, tenant_config in tenants_to_process.items():
        result = generate_tenant_data(tenant_name, tenant_config)
        results.append(result)

    # Print final summary
    total_elapsed = time.time() - total_start_time
    total_minutes = int(total_elapsed // 60)
    total_seconds = int(total_elapsed % 60)

    print("\n" + "=" * 80)
    print("FINAL SUMMARY")
    print("=" * 80)
    print(f"{'Tenant':<15} {'Properties':<12} {'Owners':<12} {'Units':<12} {'Demands':<12} {'Details':<12}")
    print("-" * 80)

    total_props = total_owners = total_units = total_addresses = total_demands = total_details = 0
    for r in results:
        print(f"{r['tenant']:<15} {r['properties']:<12} {r['owners']:<12} {r['units']:<12} {r['demands']:<12} {r['demand_details']:<12}")
        total_props += r['properties']
        total_owners += r['owners']
        total_units += r['units']
        total_addresses += r['addresses']
        total_demands += r['demands']
        total_details += r['demand_details']

    print("-" * 80)
    print(f"{'TOTAL':<15} {total_props:<12} {total_owners:<12} {total_units:<12} {total_demands:<12} {total_details:<12}")
    print("=" * 80)
    print(f"Output base directory: {OUTPUT_BASE_DIR}")
    print(f"Total time: {total_minutes}m {total_seconds}s")
    print("=" * 80)


if __name__ == "__main__":
    main()
