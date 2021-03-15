import sqlite3
import numpy as np
import pandas as pd

import matplotlib.pyplot as plt
from DB_Manage import *


class NeighbourhoodPricing(DBManagement):
    """
    class created to extract and look at the data in relation to the pricing of bnbs in regards to the neighbourhood
    group that they are from

    column names have already been defined within the class

    When making an instance of this class 4 arguments need to be passed: raw data, c, conn, data_table name

    """

    columns_required = ['neighbourhood_group', 'neighbourhood', 'room_type', 'price']
    table_columns = 'neighbourhood_group text, neighbourhood text, room_type text, price int'

    def __init__(self, dataframe, c, conn, db_table_name):
        self.dataframe = dataframe
        self.c = c
        self.conn = conn
        self.db_table_name = db_table_name

        nbhd_price_dataframe = self.dataframe.copy()
        nbhd_price_dataframe = nbhd_price_dataframe[self.columns_required]
        self.nbhd_price_dataframe = nbhd_price_dataframe

    def create_data_table(self):
        """
        calls the DBManagement class and creates a new data table based on the values passed into NeighbourhoodPricing
        class
        :return:
        """

        DBManagement.__init__(self, self.c, self.conn, self.db_table_name, self.table_columns,
                              self.nbhd_price_dataframe)
        DBManagement.db_table_creation(self)
        statement = "\n{tab} DataTable created!\n".format(tab=self.db_table_name)
        print(statement)

    def delete_data_table(self):
        """
        calls the DBManagement class and creates a new data table based on the values passed into NeighbourhoodPricing
        class
        :return:
        """
        DBManagement.db_table_deletion(self, self.db_table_name)
        statement = "\n{tab} DataTable deleted!\n".format(tab=self.db_table_name)
        print(statement)

    def rooms_per_neighbourhood(self):
        """
        prints a dataframe that shows the amount of rooms when grouped by: neighbourhood_group, neighbourhood
        and room_type
        :return:
        """
        print("\n")
        print(self.nbhd_price_dataframe.groupby(['neighbourhood_group', 'neighbourhood', 'room_type'])
              ['price'].size().reset_index(name='number_of_rooms'))

    def price_summary_per_neighbourhood(self):
        """
        gives a summary dataframe for the price of rooms within each neighbourhood_group

        also creates a histogram graph which states how many rooms there are available at a given price for each
        neighbourhood_group
        :return:
        """
        print("\n")
        print(self.nbhd_price_dataframe.groupby(self.nbhd_price_dataframe["neighbourhood_group"])[["price"]].describe())

        x1 = self.nbhd_price_dataframe.loc[self.nbhd_price_dataframe.neighbourhood_group == 'Central Region', 'price']
        x2 = self.nbhd_price_dataframe.loc[self.nbhd_price_dataframe.neighbourhood_group == 'East Region', 'price']
        x3 = self.nbhd_price_dataframe.loc[self.nbhd_price_dataframe.neighbourhood_group == 'North Region', 'price']
        x4 = self.nbhd_price_dataframe.loc[self.nbhd_price_dataframe.neighbourhood_group == 'North-East Region', 'price']
        x5 = self.nbhd_price_dataframe.loc[self.nbhd_price_dataframe.neighbourhood_group == 'West Region', 'price']

        plt.figure(figsize=(10, 6))

        kwargs = dict(alpha=0.75, bins=500)

        plt.hist(x1, **kwargs, color='b', label='Central Region', ec='black')
        plt.hist(x5, **kwargs, color='c', label='West Region', ec='black')
        plt.hist(x2, **kwargs, color='g', label='East Region', ec='black')
        plt.hist(x4, **kwargs, color='r', label='North-East Region', ec='black')
        plt.hist(x3, **kwargs, color='y', label='North Region', ec='black')

        plt.gca().set(title='Histogram showing amount of rooms at given values per NeighbourHood Group',
                      xlabel="Room Price (SGD)", ylabel='Number of Rooms')
        plt.xlim(0, 1000)
        plt.legend()
        plt.show()

    def max_room_price_per_neighbourhood_group(self):
        """
        Creates a pivot table showing the max room price for each room type per neighbourhood_group

        this is then visually shown on a bar graph
        :return:
        """
        print("\n")
        pivot_table = self.nbhd_price_dataframe.pivot_table(index="neighbourhood_group",
                                                            columns="room_type", values="price", aggfunc='max')
        print(pivot_table)
        pivot_table.plot(kind='bar', figsize=[10, 10], stacked=False, colormap='spring')
        plt.gca().set(title='Max Room Price of each Room Type per Neighbourhood Group', ylabel='Price (SGD)')
        plt.show()