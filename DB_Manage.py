import sqlite3
import numpy as np
import pandas as pd


class DBManagement:
    """
    Class used for management of database.

    """

    def __init__(self, c, conn, db_table_name, table_columns, dataframe):
        """
        defines the variables passed into the function

        :param c:                   cursor used to reference connector
        :param conn:                connector used to access database
        :param db_table_name:       name of data table being created or deleted from the database
        :param table_columns:       name of columns to be extracted from dataframe and added into database
        :param dataframe:           dataframe to be passed and manipulated into the database
        """
        self.c = c
        self.conn = conn
        self.db_table_name = db_table_name
        self.table_columns = table_columns
        self.dataframe = dataframe

    def db_table_creation(self):
        """
        creates a new data table within our database
        :return:
        """

        self.c.execute("SELECT count(name) from sqlite_master WHERE type = 'table' AND name = ?", (self.db_table_name,))
        if self.c.fetchone()[0] == 1:
            sql1 = """DELETE FROM {tab2}""".format(tab2=self.db_table_name)
            self.c.execute(sql1)
        else:
            sql = """CREATE TABLE {tab}({columns})""".format(tab=self.db_table_name, columns=self.table_columns)
            self.c.execute(sql)

        self.conn.commit()

        self.dataframe.to_sql(self.db_table_name, self.conn, if_exists='append', index=False)

    def db_table_deletion(self, db_table_name):
        """
        deletes the specified data table from the database
        :param db_table_name:
        :return:
        """

        self.db_table_name = db_table_name
        dt = "DROP TABLE {tab}".format(tab=self.db_table_name)
        self.c.execute(dt)
        self.conn.commit()