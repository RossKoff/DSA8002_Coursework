import sqlite3
import numpy as np
import pandas as pd

from DB_Manage import *


class HostReviews(DBManagement):
    """
    class created to extract and look at the data in terms of bnb reviews and the hosts associated with them

    column names have already been defined within the class

    When making an instance of this class 4 arguments need to be passed: raw data, c, conn, data_table name
    """

    columns_required = ['host_id', 'host_name', 'number_of_reviews', 'last_review', 'reviews_per_month']
    table_columns = 'host_id int, host_name text, number_of_reviews int, last_review text, reviews_per_month real'

    def __init__(self, dataframe, c, conn, db_table_name):
        self.dataframe = dataframe
        self.c = c
        self.conn = conn
        self.db_table_name = db_table_name
        self.most_review_dataframe = None

        host_review_dataframe = self.dataframe.copy()
        host_review_dataframe = host_review_dataframe[self.columns_required]
        self.host_review_dataframe = host_review_dataframe

    def create_data_table(self):
        """
        calls the DBManagement class and creates a new data table based on the values passed into NeighbourhoodPricing
        class
        :return:
        """
        DBManagement.__init__(self, self.c, self.conn, self.db_table_name,
                              self.table_columns, self.host_review_dataframe)
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

    def over_200_reviews(self):
        """
        takes the dataframe, removes all null reviews and lists only those people who have over 200 reviews on
        their bnbs

        the database is then updated to mirror this new dataframe.

        the sum value of all the latest reviews is then calculated, calling the test function for this method and
        passing the value 8155 will check whether this is true or not.
        :return:
        """
        print("\nRemoving all NULL values and BnB data with less than 200 reviews,\nUpdating dataframe and data table "
              "in database:\n")
        self.most_review_dataframe = self.host_review_dataframe.dropna()
        self.most_review_dataframe = \
            self.most_review_dataframe.loc[self.most_review_dataframe['number_of_reviews'] > 200]
        print(self.most_review_dataframe.sort_values(by=['host_id', 'number_of_reviews']))

        delete_null_reviews = """DELETE FROM {tab} WHERE last_review IS NULL 
        OR number_of_reviews < 200""".format(tab=self.db_table_name)
        self.c.execute(delete_null_reviews)
        self.conn.commit()

        sum_total = self.most_review_dataframe['number_of_reviews'].sum()
        return sum_total


def test_over_200_reviews(test_function, expected):
    """
    testing the over_200_reviews function
    :param test_function:
    :param expected:
    :return:
    """
    actual = test_function()

    if actual == expected:
        print('\nTest Passed\n')
    else:
        print("\nTest Failed\n")