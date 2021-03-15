import sqlite3
import numpy as np
import pandas as pd

import matplotlib.pyplot as plt
import seaborn as sns
from DB_Manage import *


class MapArea(DBManagement):

    columns_required = ['neighbourhood_group', 'neighbourhood', 'latitude', 'longitude']
    table_columns = 'neighbourhood_group text, neighbourhood text, latitude real, longitude real'

    def __init__(self, dataframe, c, conn, db_table_name):
        self.dataframe = dataframe
        self.c = c
        self.conn = conn
        self.db_table_name = db_table_name

        map_area_dataframe = self.dataframe.copy()
        map_area_dataframe = map_area_dataframe[self.columns_required]
        self.map_area_dataframe = map_area_dataframe

    def create_data_table(self):
        """
        calls the DBManagement class and creates a new data table based on the values passed into NeighbourhoodPricing
        class
        :return:
        """
        DBManagement.__init__(self, self.c, self.conn, self.db_table_name, self.table_columns, self.map_area_dataframe)
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

    def create_map(self):
        """
        Uses the longitude and latitude values to create a pseudo map area for the bnb's in Singapore

        an image of a map of Singapore has been layered behind this to try and make it more visible where these
        bnbs are located

        :return:
        """
        plt.figure(figsize=(10, 6))
        img = plt.imread('singapore.png')
        plt.imshow(img, alpha=0.75, extent=[103.5925, 104.1108, 1.15025, 1.495], zorder=1)
        sns.scatterplot(x=self.map_area_dataframe['longitude'], y=self.map_area_dataframe['latitude'],
                        hue=self.map_area_dataframe['neighbourhood_group'], zorder=2)
        plt.gca().set(title='Recreation of map area using BnB locations', xlabel='longitude', ylabel='latitude')
        plt.show()