import sys
import json

import pandas as pd
import pymongo

# get_ipython().magic('matplotlib inline')
# mongo connection
config_file = open(sys.argv[1]).read()
config = json.loads(config_file)

categories = pd.read_csv(config["categories_fixed_path"])
categories.index = categories.name.str.replace("\"", "")
datafile = config["datapath"]
dataset = pd.read_csv(datafile)
client = pymongo.MongoClient(config["primary_node"])

art = ["Arts & Entertainment", "Arcade", "Art Gallery", "Museum", "Bowling Alley", "Casino", "Comedy Club",
       "Concert Hall", "General Entertainment",
       "Historic Site", "Movie Theater", "Music Venue", "Performing Arts Venue", "Pool Hall", "Public Art", "Racetrack",
       "Stadium", "Theme Park"]
edu = ["College & University"]
food = ["Food", "BBQ Joint", "Bagel Shop", "Bakery", "Breakfast Spot", "Brewery", "Burger Joint", "Burrito Place",
        "Cafe", "Coffee Shop",
        "Cupcake Shop", "Deli / Bodega", "Dessert Shop", "Diner", "Distillery", "Donut Shop", "Fast Food Restaurant",
        "Fish & Chips Shop",
        "Food Truck", "Fried Chicken Joint", "Gastropub", "Hot Dog Joint", "Ice Cream Shop", "Juice Bar",
        "Mac & Cheese Joint",
        "Pizza Place", "Ramen / Noodle House", "Restaurant", "Salad Place", "Sandwich Place", "Snack Place",
        "Soup Place",
        "Steakhouse", "Sushi Restaurant", "Taco Place", "Tea Room", "Winery", "Wings Joint", "Frozen Yogurt"]
prof = ["Professional & Other Places", "Animal Shelter", "Auditorium", "Building", "Convention Center", "Event Space",
        "Factory",
        "Fair", "Funeral Home", "Government Building", "Library", "Medical Center", "Military Base", "Non-Profit",
        "Office", "Parking",
        "Post Office", "Radio Station", "School", "Spiritual Center", "Voting Booth"]
night = ["Nightlife Spot", "Bar", "Pub", "Club House"]
outdoors = ["Other Great Outdoors", "Sport", "Park"]
shop = ["Shop & Service", "Antique Shop", "Astrologer", "Adult Boutique", "Arts & Crafts Store", "Automotive Shop",
        "ATM", "Betting Shop",
        "Big Box Store", "Baby Store", "Bank", "Bike Shop", "Board Shop", "Bookstore",
        "Bridal Shop", "Camera Store", "Candy Store", "Car Dealership", "Car Wash", "Carpet Store", "Clothing Store",
        "Check Cashing Service",
        "Chocolate Shop", "Convenience Store", "Cosmetics Shop", "Costume Shop",
        "Credit Union", "Daycare", "Department Store", "Design Studio", "Discount Store", "Dive Shop", "Dry Cleaner",
        "Drugstore / Pharmacy", "EV Charging Station", "Electronics Store",
        "Fabric Shop", "Financial or Legal Service",
        "Fireworks Store",
        "Fishing Store",
        "Flea Market",
        "Flower Shop",
        "Food & Drink Shop",
        "Frame Store",
        "Fruit & Vegetable Store",
        "Furniture / Home Store",
        "Gaming Cafe",
        "Garden Center",
        "Gas Station / Garage",
        "Gift Shop", "Gym / Fitness Center", "Hardware Store",
        "Herbs & Spices Store",
        "Hobby Shop",
        "Hunting Supply",
        "IT Services",
        "Internet Cafe", "Jewelry Store",
        "Knitting Store",
        "Laundromat",
        "Laundry Service",
        "Lawyer",
        "Leather Goods Store",
        "Locksmith",
        "Luggage Store",
        "Mall", "Market", "Miscellaneous Shop",
        "Mattress Store",
        "Mobile Phone Shop", "Motorcycle Shop", "Music Store", "Nail Salon",
        "Newsstand",
        "Optical Shop",
        "Other Repair Shop",
        "Outdoor Supply Store",
        "Outlet Store",
        "Paper / Office Supplies Store",
        "Pawn Shop",
        "Perfume Shop",
        "Pet Service", "Pet Store", "Photography Lab",
        "Piercing Parlor",
        "Pop-Up Shop",
        "Print Shop",
        "Real Estate Office", "Record Shop", "Recording Studio",
        "Recycling Facility", "Salon / Barbershop",
        "Shipping Store",
        "Shoe Repair",
        "Smoke Shop",
        "Smoothie Shop",
        "Souvenir Shop",
        "Spa", "Sporting Goods Shop", "Stationery Store", "Storage Facility", "Tailor Shop",
        "Tanning Salon", "Tattoo Parlor", "Thrift / Vintage Store", "Toy / Game Store", "Travel Agency",
        "Used Bookstore", "Video Game Store", "Video Store",
        "Warehouse Store",
        "Watch Repair Shop",
]
transport = ["Travel & Transport", "Hotel", "Subway", "Taxi", "Bus Stop", "Train Station", "Tram", "Platform"]
tags = art + edu + food + prof + night + outdoors + shop + transport


def add_amenity(tags, value):
    if value == "bar":
        tags["Bar"] = True
        tags["Nightlife Spot"] = True
    elif value == "pub":
        tags["Pub"] = True
        tags["Nightlife Spot"] = True
    elif value == "Club House":
        tags["Nightlife Spot"] = True
        tags["Club House"] = True
    elif value == "fuel":
        tags["Shop & Service"] = True
        tags["Gas Station / Garage"] = True
    elif value == "place_of_worship":
        tags["Spiritual Center"] = True
        tags["Professional & Other Places"] = True
    elif value == "post_office":
        tags["Post Office"] = True
        tags["Professional & Other Places"] = True
    elif value == "bank":
        tags["Shop & Service"] = True
        tags["Bank"] = True
    elif value == "library":
        tags["Professional & Other Places"] = True
        tags["Library"] = True
    elif value == "cinema":
        tags["Arts & Entertainment"] = True
        tags["Movie Theater"] = True
    elif value == "atm":
        tags["Shop & Service"] = True
        tags["ATM"] = True
    elif value == "restaurant":
        tags["Food"] = True
        tags["Restaurant"] = True
    elif value == "fast_food" or value == "food_court":
        tags["Food"] = True
        tags["Fast Food Restaurant"] = True
    elif value == "cafe":
        tags["Food"] = True
        tags["Cafe"] = True
    elif value == "nightclub":
        tags["Nightlife Spot"] = True
        tags["Club House"] = True
    elif value == "pharmacy":
        tags["Shop & Service"] = True
        tags["Drugstore / Pharmacy"] = True
    elif value == "kindergarten":
        tags["Shop & Service"] = True
        tags["Daycare"] = True
    elif value == "university" or value == "college":
        tags["College & University"] = True
    elif value == "parking":
        tags["Professional & Other Places"] = True
        tags["Parking"] = True
    elif value == "car_wash":
        tags["Shop & Service"] = True
        tags["Car Wash"] = True
    elif value == "police":
        tags["Professional & Other Places"] = True
        tags["Government Building"] = True
    elif value == "fountain":
        tags["Other Great Outdoors"] = True
    elif value == "courthouse":
        tags["Government Building"] = True
    elif value == "theatre":
        tags["Arts & Entertainment"] = True
        tags["Performing Arts Venue"] = True
    elif value == "veterinary":
        tags["Pet Service"] = True
        tags["Shop & Service"] = True
    elif value == "collection office":
        tags["Professional & Other Places"] = True
        tags["Office"] = True
    elif value == "marketplace":
        tags["Shop & Service"] = True
        tags["Market"] = True
    elif value == "hospital" or value == "dentist" or value == "clinic" or value == "doctors":
        tags["Professional & Other Places"] = True
        tags["Medical Center"] = True
    elif "school" in value:
        tags["College & University"] = True
    elif value == "beauty":
        tags["Shop & Service"] = True


def add_atm(tags, value):
    if value == "yes":
        tags["ATM"] = True
        tags["Shop & Service"] = True


def add_craft(tags, value):
    tags["Arts & Crafts Store"] = True
    tags["Shop & Service"] = True


def add_leisure(tags, value):
    if value == "sports_centre":
        tags["Shop & Service"] = True
        tags["Gym / Fitness Center"] = True


def add_office(tags, value):
    tags["Office"] = True
    tags["Professional & Other Places"] = True


def add_transport(tags, value):
    if value == "subway" or value == 'subway_entrance':
        tags["Travel & Transport"] = True
        tags["Subway"] = True
    if value == "tram_stop":
        tags["Travel & Transport"] = True
        tags["Tram"] = True
    if value == "platform":
        tags["Travel & Transport"] = True
        tags["Train Station"] = True
        tags["Platform"] = True
    if value == "bus_stop" or value == "stop_position" or value == "station":
        tags["Travel & Transport"] = True
        tags["Bus Stop"] = True


def add_shop(tags, value):
    tags["Shop & Service"] = True
    if value == "convenience":
        tags["Convenience Store"] = True
    elif value == "supermarket":
        tags["Food & Drink Shop"] = True
    elif value == "clothes":
        tags["Clothing Store"] = True
    elif value == "hairdresser":
        tags["Salon / Barbershop"] = True
    elif value == "florist":
        tags["Flower Shop"] = True
    elif value == "shoes":
        tags["Clothing Store"] = True
    elif value == "beauty":
        tags["Shop & Service"] = True
    elif value == "mobile_phone":
        tags["Mobile Phone Shop"] = True
    elif value == "furniture":
        tags["Furniture / Home Store"] = True
    elif value == "jewelry":
        tags["Jewelry Store"] = True
    elif value == "hardware":
        tags["Hardware Store"] = True
    elif value == "alcohol":
        tags["Food & Drink Shop"] = True
    elif value == "electronics" or value == "computer":
        tags["Electronics Store"] = True
    elif value == "books":
        tags["Bookstore"] = True
    elif value == "pet":
        tags["Pet Store"] = True
    elif value == "car":
        tags["Car Dealership"] = True
    elif value == "bakery":
        tags["Bakery"] = True
    elif value == "sports":
        tags["Sporting Goods Shop"] = True
    elif value == "toys":
        tags["Toy / Game Store"] = True
    elif value == "gift":
        tags["Gift Shop"] = True
    elif value == "travel_agency":
        tags["Travel Agency"] = True


def add_tourism(tags, value):
    if value == "hotel" or value == "hostel":
        tags["Travel & Transport"] = True
        tags["Hotel"] = True
    if value == "museum":
        tags["Arts & Entertainment"] = True
        tags["Museum"] = True
    if value == "artwork":
        tags["Arts & Entertainment"] = True
        tags["Art Gallery"] = True
    if value == "attraction":
        tags["Arts & Entertainment"] = True


def add_tags(place_tags, osm_place):
    if not (pd.isnull(osm_place["amenity"])):
        add_amenity(place_tags, osm_place["amenity"])
    if not (pd.isnull(osm_place["atm"])):
        add_atm(place_tags, osm_place["atm"])
    if not (pd.isnull(osm_place["craft"])):
        add_craft(place_tags, osm_place["craft"])
    if not (pd.isnull(osm_place["leisure"])):
        add_leisure(place_tags, osm_place["leisure"])
    if not (pd.isnull(osm_place["office"])):
        add_office(place_tags, osm_place["office"])
    if not (pd.isnull(osm_place["transport"])):
        add_transport(place_tags, osm_place["transport"])
    if not (pd.isnull(osm_place["shop"])):
        add_shop(place_tags, osm_place["shop"])
    if not (pd.isnull(osm_place["tourism"])):
        add_tourism(place_tags, osm_place["tourism"])


def get_id(name):
    return categories.ix[name]["fullCategory"]


def transform(place):
    place_tags = {tag: False for tag in tags}
    add_tags(place_tags, place)
    place_name = place["name"]
    tagsList = [tag for tag in tags if place_tags[tag]]
    result = {
        "_id": place.id,
        "location":  [place.lon, place.lat],
        "name": place_name
    }
    if pd.notnull(place["url"]):
        result["url"] = place["url"]
    if pd.notnull(place["phone"]):
        result["contact_phone"] = place["phone"]
    if pd.notnull(place["operator"]):
        result["operator"] = place["operator"]
    result["category"] = {
        "categories_names": tagsList,
        "categories_ids": [get_id(category) for category in tagsList]
    }
    return result


#first prepare of data
dataset["transport"] = dataset["highway"].combine_first(dataset["transport"])
dataset["transport"] = dataset["transport"].combine_first(dataset["station"])
dataset["transport"] = dataset["transport"].combine_first(dataset["railway"])
dataset["transport"] = dataset["transport"].combine_first(dataset["public_transport"])
dataset["transport"].unique()
dataset["operator"] = dataset["network"].combine_first(dataset["operator"])

queries = [transform(dataset.ix[i]) for i in range(dataset.shape[0])]

db = client[config["database"]]
collection = db[config["collection"]]

#clear collection
collection.remove({})
collection.insert(queries)
collection.ensure_index([("location","2d")])

