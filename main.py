import sqlite3
import numpy as np
import pandas as pd

from NBHD_Price import *
from Review_200 import *
from SG_Map import *

desired_width = 320
pd.set_option('display.width', desired_width)
pd.set_option("display.max_columns", None)

raw_bnb = pd.read_csv("https://raw.githubusercontent.com/RossKoff/DSA8002_Coursework/main/listings.csv")

conn = sqlite3.connect("SingaporeBnB_Coursework_40078242.db")

c = conn.cursor()

"""
Creates instances of the 3 defined classes. Raw_data, c and conn have already been stated.
Table names have been created but feel free to change them to whatever you want.
"""
x = NeighbourhoodPricing(raw_bnb, c, conn, 'NBHD_Pricing')
y = HostReviews(raw_bnb, c, conn, 'Host_Reviews')
z = MapArea(raw_bnb, c, conn, 'Singapore_Bnb_Map')

"""
Creates the data tables for each instance and puts them into our database
"""
# x.create_data_table()
# y.create_data_table()
# z.create_data_table()

"""
Calls the functions defined within our NeighbourhoodPricing class
"""
# x.rooms_per_neighbourhood()
# x.price_summary_per_neighbourhood()
# x.max_room_price_per_neighbourhood_group()

"""
Calls the function defined within our HostReviews class, test function also allows us to check the sum total reviews for hosts with over 200 reviews
"""
# y.over_200_reviews()
# test_over_200_reviews(y.over_200_reviews, 8155)

"""
creates a scatter graph map for the locations of the bnbs in Singapore
"""
# z.create_map()

"""
deletes the data tables from the database for each instance
"""
# x.delete_data_table()
# y.delete_data_table()
# z.delete_data_table()







