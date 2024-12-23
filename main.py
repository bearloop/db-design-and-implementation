import sqlite3
import datetime


# --------------------------------------------------------------
class DBOperations:

  # basic seed data (to be added optionally by the user)
  pilots_seed_data = [
      ("p1", "Adam", "Jones", "Air Grand", "1984-12-21", "2007-03-03"),
      ("p2", "James", "Black", "Apple Air", "1981-09-01", "2003-09-03"),
      ("p3", "William", "Spencer", "Air Grand", "1974-12-11", "1996-06-25"),
      ("p4", "Christina", "Gloves", "Apple Air", "1988-05-11", "2011-07-09"),
      ("p5", "Edward", "Vladic", "Clearview", "1994-06-07", "2009-11-23"),
      ("p6", "Silvie", "Koolash", "Clearview", "1991-08-26", "2010-01-07"),
      ("p7", "John", "Porter", "Clearview", "1992-04-15", "2015-10-13"),
  ]

  aircrafts_seed_data = [
      ("a1", "Boeing", "747", 200, 8),
      ("a2", "Airbus", "320", 120, 6),
      ("a3", "Airbus", "320", 120, 6),
      ("a4", "Airbus", "320", 120, 6),
      ("a5", "Boeing", "747", 200, 8),
  ]

  flights_seed_data = [
      ("f1", "Heathrow Airport", "Charles de Gaulle Airport", "2024-05-24",
       "Landed", "a2"),
      ("f2", "Athens International Airport",
       "Leonardo da Vinci–Fiumicino Airport", "2024-05-24", "Landed", "a4"),
      ("f3", "Los Angeles International Airport", "Heathrow Airport",
       "2024-05-24", "Delayed", "a3"),
      ("f4", "Haneda Airport", "Charles de Gaulle Airport", "2024-05-25",
       "Cancelled", "a1"),
      ("f5", "Heathrow Airport", "Athens International Airport", "2024-05-25",
       "Cancelled", "a5"),
      ("f6", "Heathrow Airport", "Los Angeles International Airport",
       "2024-05-25", "Delayed", "a4"),
      ("f7", "Heathrow Airport", "Haneda Airport", "2024-05-25", "Delayed",
       "a3"),
      ("f8", "Athens International Airport",
       "Leonardo da Vinci–Fiumicino Airport", "2024-05-25", "Landed", "a2"),
      ("f9", "Haneda Airport", "Heathrow Airport", "2024-05-26", "Scheduled",
       "a1"),
      ("f10", "Los Angeles International Airport", "Charles de Gaulle Airport",
       "2024-05-26", "Scheduled", "a5"),
  ]

  operated_by_seed_data = [("f1", "p1"), ("f1", "p2"), ("f2", "p3"),
                           ("f2", "p4"), ("f2", "p5"), ("f3", "p6"),
                           ("f3", "p7"), ("f4", "p1"), ("f4", "p2"),
                           ("f5", "p3"), ("f6", "p4"), ("f7", "p5"),
                           ("f7", "p6"), ("f7", "p7"), ("f8", "p1"),
                           ("f9", "p2"), ("f9", "p3"), ("f10", "p4"),
                           ("f10", "p5"), ("f10", "p6")]

  # tables that have been deleted
  deleted_tables = dict()
  # tables available
  avail_tables = dict()

  # table names
  PILOTS = "Pilots"
  AIRCRAFTS = "Aircrafts"
  FLIGHTS = "Flights"
  OPERATED_BY = "OperatedBy"

  # ------ Create statements
  # Create the "Pilots" table which stores pilot-specific information
  table_pilot = """
                    Pilots (
                      PilotID TEXT NOT NULL PRIMARY KEY,
                      FirstName TEXT NOT NULL,
                      LastName TEXT NOT NULL,
                      School TEXT NOT NULL,
                      BirthDate TEXT NOT NULL,
                      ProfSince TEXT NOT NULL
                    )
                """
  sql_create_pilot_table_firsttime = """ CREATE TABLE IF NOT EXISTS """ + table_pilot

  sql_create_pilot_table = "CREATE TABLE " + table_pilot

  # Create the "Aircraft" table which stores aircraft-specific data
  table_aircraft = """
                      Aircrafts (
                        AircraftID TEXT NOT NULL PRIMARY KEY,
                        Manufacturer TEXT NOT NULL,
                        Model TEXT NOT NULL,
                        MaxPassengers INTEGER NOT NULL,
                        CrewSize INTEGER NOT NULL
                      )
                    """
  sql_create_aircraft_table_firsttime = """ CREATE TABLE IF NOT EXISTS """ + table_aircraft

  sql_create_aircraft_table = "CREATE TABLE " + table_aircraft

  # Create the "Flights" table which stores flight information
  table_flights = """
                      Flights (
                          FlightID TEXT NOT NULL,
                          Origin TEXT NOT NULL,
                          Destination TEXT NOT NULL,
                          Departure TEXT NOT NULL,
                          Status TEXT NOT NULL,
                          AircraftID TEXT NOT NULL,
                          FOREIGN KEY(AircraftID) REFERENCES Aircrafts(AircraftID)
                      )
                   """

  sql_create_flights_table_firsttime = """ CREATE TABLE IF NOT EXISTS """ + table_flights

  sql_create_flights_table = "CREATE TABLE " + table_flights

  # Create the "OperatedBy" table which represents the relationship connecting Pilots and Flights
  table_operated_by = """
                          OperatedBy (
                            FlightID TEXT NOT NULL,
                            PilotID TEXT NOT NULL,
                            PRIMARY KEY (FlightID, PilotID),
                            FOREIGN KEY (PilotID) REFERENCES Pilots(PilotID),
                            FOREIGN KEY (FlightID) REFERENCES Flights( FlightID)

                          )
                      """
  sql_create_operated_by_table_firsttime = """ CREATE TABLE IF NOT EXISTS """ + table_operated_by

  sql_create_operated_by_table = "CREATE TABLE " + table_operated_by

  # store create statements for diect reference
  create_tables_dict = {
      PILOTS: sql_create_pilot_table,
      AIRCRAFTS: sql_create_aircraft_table,
      FLIGHTS: sql_create_flights_table,
      OPERATED_BY: sql_create_operated_by_table
  }

  # store PK for each table
  prim_key_tables_dict = {
      PILOTS: "PilotID",
      AIRCRAFTS: "AircraftID",
      FLIGHTS: "FlightID"
  }

  # ------ Insert statements
  sql_insert_pilots = """
                        INSERT INTO Pilots(PilotID, FirstName, LastName, School, BirthDate, ProfSince)
                             VALUES (?, ?, ?, ?, ?, ?)
                        """

  sql_insert_aircrafts = """
                        INSERT INTO Aircrafts(AircraftID, Manufacturer, Model, MaxPassengers, CrewSize)
                             VALUES (?, ?, ?, ?, ?)
                        """

  sql_insert_flights = """
                        INSERT INTO Flights ( FlightID, Origin, Destination, Departure, Status, AircraftID)
                             VALUES (?, ?, ?, ?, ?, ?)
                        """

  sql_insert_operated_by = """
                          INSERT INTO OperatedBy ( FlightID, PilotID )
                              VALUES (?, ?)
                        """

  # ------ Generic select all statement: the execute function call adds the table name
  sql_select_all = "SELECT * FROM "

  # ------ search statement
  sql_search_flights_data = """SELECT * FROM Flights WHERE FlightID = ?"""
  sql_search_pilots_data = """SELECT * FROM Pilots WHERE FlightID = ?"""
  sql_search_aircrafts_data = """SELECT * FROM Aircrafts WHERE FlightID = ?"""

  # ------ Update statements - note that for OperatedBy which has only contains a composite primary key referencing the primary keys of Flights and Pilots
  # .. there is no update statement, rather the process to "update" should be to first delete an existing record and then insert the new one
  sql_update_pilots_data = """UPDATE Pilots SET FirstName = ?, LastName = ?, \
                                      School = ?, BirthDate = ?, ProfSince = ? WHERE PilotID = ?"""

  sql_update_aircrafts_data = "UPDATE Aircrafts SET Manufacturer = ?, Model = ?,\
                                      MaxPassengers = ?, CrewSize = ? WHERE AircraftID = ?"

  sql_update_flights_data = """UPDATE Flights SET Origin = ?, Destination = ?, \
                                      Departure = ?, Status = ?, AircraftID = ? WHERE FlightID = ?"""

  # ------ Delete statements
  sql_delete_flights_data = """  DELETE FROM Flights WHERE FlightID = ? """
  sql_delete_pilots_data = """  DELETE FROM Pilots WHERE PilotID = ? """
  sql_delete_aircrafts_data = """  DELETE FROM Aircrafts WHERE AircraftID = ? """
  sql_delete_operatedby_data = """  DELETE FROM OperatedBy WHERE FlightID = ? AND PilotID = ?"""

  # ------ Drop statements: the execute function call adds the table name
  sql_drop_table_query = "DROP TABLE IF EXISTS "

  # ------ Select table names
  sql_get_table_names_query = """SELECT name FROM sqlite_master WHERE type='table';"""

  def __init__(self):
    try:
      self.conn = sqlite3.connect("AirDB.db")
      self.cur = self.conn.cursor()

      # create tables if they do not exist
      # self.cur.execute(self.sql_drop_table_query + self.PILOTS)
      self.cur.execute(self.sql_create_pilot_table_firsttime)

      # self.cur.execute(self.sql_drop_table_query + self.AIRCRAFTS)
      self.cur.execute(self.sql_create_aircraft_table_firsttime)

      # self.cur.execute(self.sql_drop_table_query + self.FLIGHTS)
      self.cur.execute(self.sql_create_flights_table_firsttime)

      # self.cur.execute(self.sql_drop_table_query + self.OPERATED_BY)
      self.cur.execute(self.sql_create_operated_by_table)

      self.conn.commit()

    except Exception as e:
      print(e)
    finally:
      self.conn.close()

  def get_connection(self):
    '''
    Establish a connection to the database
    '''
    self.conn = sqlite3.connect("AirDB.db")
    self.cur = self.conn.cursor()

  # ---- Create and delete tables
  def bulk_import_seed_data(self):
    '''
    Function that allows the user to "bulk import" some basic seed data into each table
    '''
    try:
      # get connection
      self.get_connection()

      # fill tables with seed data
      self.cur.executemany(self.sql_insert_pilots, self.pilots_seed_data)
      self.cur.executemany(self.sql_insert_aircrafts, self.aircrafts_seed_data)
      self.cur.executemany(self.sql_insert_flights, self.flights_seed_data)
      self.cur.executemany(self.sql_insert_operated_by,
                           self.operated_by_seed_data)
      self.conn.commit()

      print("Seed data loaded successfully.")

    except Exception as e:
      print(
          "This operation is not allowed as the table is not empty. Please drop and create all tables if you must use seed data."
      )
      print("Error message:", e)
    finally:
      self.conn.close()

  def update_deleted_tables_dict(self):
    '''
    Support function to update the dictionary that stores the names of deleted tables
    '''
    # execute the query to select table names
    self.cur.execute(self.sql_get_table_names_query)
    # fetch table names and store as dict
    self.avail_tables = {
        str(ind): val[0]
        for ind, val in enumerate(self.cur.fetchall(), start=1)
    }

    # identify which items are actually deleted
    temp_items = []
    for val in self.deleted_tables.values():
      if val not in list(self.avail_tables.values()):
        temp_items.append(val)

    # create a new placeholder dictionary to store those items
    temp_dict = dict()
    for ind, val in enumerate(temp_items, start=1):
      temp_dict[str(ind)] = val

    # update deleted tables dictionary
    self.deleted_tables = temp_dict

  def get_available_tables(self):
    '''
    Print out a list of currently available DB tables
    '''
    # get connection
    self.get_connection()
    # execute the query to select table names
    self.cur.execute(self.sql_get_table_names_query)
    # fetch table names and store as dict
    self.avail_tables = {
        str(ind): val[0]
        for ind, val in enumerate(self.cur.fetchall(), start=1)
    }

    if len(self.avail_tables) == 0:
      print("No tables exist in the database.")
    else:
      print("AirDB tables:")
      for key in self.avail_tables.keys():
        print(key, "-", self.avail_tables[key])

  def get_deleted_tables(self):
    '''
    Print out a list of currently deleted tables
    '''
    if len(self.deleted_tables) == 0:
      print("All AirDB tables are currently available.")
    else:
      print("Deleted tables:")
      for key in self.deleted_tables.keys():
        print(key, "-", self.deleted_tables[key])

  def select_deleted_table_to_reinstate(self):
    '''
    Support function to help select a DB table out of those that have been dropped by the user
    '''
    if len(self.deleted_tables) == 0:
      return -1
    # print out the names and ask user to select one
    print("Select one of the deleted tables listed below to reinstate:")
    for key in self.deleted_tables.keys():
      print(key, "-", self.deleted_tables[key])
    # ask the user to select a table by providing a table "ID"
    invalid_input = True
    while invalid_input:
      # ask for input
      usr_input = input("Enter table ID, or 'r' to return to the menu: ")
      if usr_input == 'r':
        return -2
      # validate input
      if usr_input in self.deleted_tables.keys():
        invalid_input = False

        return self.deleted_tables[usr_input]
      # print message if not valid
      else:
        print("Please select the table ID. Valid IDs:",
              [int(i) for i in list(self.deleted_tables.keys())])

  def select_existing_table_to_operate(self):
    '''
    Support function to help select a DB table
    '''
    try:
      # make connection to the db
      self.get_connection()
      # execute the query to select table names
      self.cur.execute(self.sql_get_table_names_query)
      # fetch table names and store as dict
      self.avail_tables = {
          str(ind): val[0]
          for ind, val in enumerate(self.cur.fetchall(), start=1)
      }
      # check if there are any tables on the db
      if len(self.avail_tables) == 0:
        return -1
      # print out the names and ask user to select one
      print("Existing AirDB tables:")
      for key in self.avail_tables.keys():
        print(key, "-", self.avail_tables[key])

      # ask the user to select a table by providing a table "ID"
      invalid_input = True
      while invalid_input:
        # ask for input
        usr_input = input("Enter table ID, or 'r' to return to the menu: ")
        if usr_input == 'r':
          return -2
        # validate input
        if usr_input in self.avail_tables.keys():
          invalid_input = False
          return self.avail_tables[usr_input]
        # print message if not valid
        else:
          print("Please select the table ID. Valid IDs:",
                [int(i) for i in list(self.avail_tables.keys())])

    except Exception as e:
      print(e)

  def create_table(self):
    '''
    Select and create a table
    '''
    try:
      self.get_connection()
      selected_table = self.select_deleted_table_to_reinstate()
      if selected_table == -2:
        print('Returning to main menu')

      elif selected_table == -1:
        print("""No tables have been deleted. \n
              You have to delete a table first to proceed with this operation."""
              )
      else:
        self.cur.execute(self.create_tables_dict[selected_table])
        self.conn.commit()
        print(f"Table {selected_table} created successfully.\n")

        # as the deleted tables dict would still store the table created above, it must be updated
        self.update_deleted_tables_dict()

    except Exception as e:
      print(e)
    finally:
      self.conn.close()

  def drop_table(self):
    '''
    Select and remove a table
    '''
    try:
      self.get_connection()
      selected_table = self.select_existing_table_to_operate()
      if selected_table == -2:
        print('Returning to main menu')

      elif selected_table == -1:
        print("""No tables exist. \n
              You have to create a table first to proceed with this operation."""
              )
      else:
        self.cur.execute(self.sql_drop_table_query + selected_table)
        self.conn.commit()
        print(f"Table {selected_table} removed successfully.\n")

        # Store the removed table to the deleted tables dictionary
        # curr_num_of_del_tables = len(self.deleted_tables)
        self.deleted_tables[selected_table] = selected_table
        # as the deleted tables dict would still store the table created above, it must be updated
        self.update_deleted_tables_dict()

    except Exception as e:
      print(e)
    finally:
      self.conn.close()

  # ---- Display all and search by ID
  def pretty_print(self, columns, records):
    '''
      Parse records and print out the table in a "prettyfied format"
      '''
    columns = [tuple(i[0] for i in columns)]
    records = columns + records

    # calc max length
    max_col_length = max([len(i) for i in columns[0]]) + 2

    # create pretty table to print
    table = ""
    for row in records:
      new_row = ""
      for cell in row:
        cell = str(cell)
        if len(cell) <= max_col_length:
          new_row += cell + " " * (max_col_length - len(cell)) + "|"
        else:
          new_row += cell[:max_col_length - 2] + '..|'
      table += new_row + "\n"

    print("\n")
    print(table)

  def select_all(self):
    '''
    Select table and print out every record
    '''
    try:
      self.get_connection()
      selected_table = self.select_existing_table_to_operate()
      if selected_table == -2:
        print('Returning to main menu')

      elif selected_table == -1:
        print("""No tables exist. \n
              You have to create a table first to proceed with this operation."""
              )
      else:
        print("Table selected:", selected_table)
        result = self.cur.execute(self.sql_select_all + selected_table)
        records = result.fetchall()
        if len(records) > 0:
          self.pretty_print(columns=result.description, records=records)
        else:
          print(f"Table {selected_table} is empty.")

    except Exception as e:
      print(e)
    finally:
      self.conn.close()

  def search_data(self):
    '''
    This function asks the user to select which table they wish to search based on the table's primary key
    '''
    try:
      self.get_connection()
      selected_table = self.select_existing_table_to_operate()
      if selected_table == -2:
        print('Returning to main menu')

      elif selected_table == -1:
        print("""No tables exist. \n
              You have to create a table first to proceed with this operation."""
              )
      else:
        print("Table selected:", selected_table)
        if selected_table != self.OPERATED_BY:
          pk_column = self.prim_key_tables_dict[selected_table]
          query = f""" SELECT * FROM {selected_table} WHERE {pk_column} = ? """
          pk_id = input(f"Enter {pk_column}, or r to return: ")
          if pk_id == 'r':
            return -1

          else:
            result = self.cur.execute(query, (pk_id, ))

        else:
          query = f""" SELECT * FROM {selected_table} WHERE FlightID = ? AND PilotID = ?"""
          pk1_id = input(f"Enter FlightID, or r to return: ")
          if pk1_id == 'r':
            return -1
          pk2_id = input(f"Enter PilotID, or r to return: ")
          if pk2_id == 'r':
            return -1

          result = self.cur.execute(query, (pk1_id, pk2_id))

        records = result.fetchall()
        if len(records) > 0:
          self.pretty_print(columns=result.description, records=records)
        else:
          print(f"No record was found.")

    except Exception as e:
      print(
          "\nOperation terminated. Please see the message above for further information.\n"
      )
      print(e)
    finally:
      self.conn.close()

  def search_data_by_non_pk(self):
    '''
    This function asks the user to select which table they wish to search based on the table's non-primary key attributes
    '''
    try:
      self.get_connection()
      selected_table = self.select_existing_table_to_operate()
      if selected_table == -2:
        print('Returning to main menu')

      elif selected_table == -1:
        print("""No tables exist. \n
              You have to create a table first to proceed with this operation."""
              )
      else:
        print("Table selected:", selected_table)
        if selected_table != self.OPERATED_BY:
          pk_column = input(f"Enter column name, or r to return: ")
          if pk_column == 'r':
            return -1
          query = f""" SELECT * FROM {selected_table} WHERE {pk_column} = ? """
          pk_id = input(f"Enter {pk_column} value, or r to return: ")
          if pk_id == 'r':
            return -1

          else:
            result = self.cur.execute(query, (pk_id, ))
            records = result.fetchall()
            if len(records) > 0:
              self.pretty_print(columns=result.description, records=records)
            else:
              print(f"No record was found.")

        else:
          print("Use PK-based search for", self.OPERATED_BY)
          raise ValueError

    except Exception as e:
      print(
          "\nOperation terminated as an error occurred. Please make sure you've specified a table column name that is available in the selected table.\n"
      )
      print(e)
    finally:
      self.conn.close()

  # ---- Insert, update, delete
  def insert_data(self):
    '''
    This function asks the user to select which table they wish to insert data into and then asks 
    for their input and inserts a new record to the selected table
    '''
    try:
      self.get_connection()
      selected_table = self.select_existing_table_to_operate()
      if selected_table == -2:
        print('Returning to main menu')

      elif selected_table == -1:
        print("""No tables exist. \n
              You have to create a table first to proceed with this operation."""
              )
      else:

        if selected_table == self.FLIGHTS:
          flight = FlightInfo(cursor=self.cur, conn=self.conn)
          if flight.insert_record_by_id(self.sql_insert_flights) == -1:
            raise ValueError

        elif selected_table == self.PILOTS:
          pilot = PilotsInfo(cursor=self.cur, conn=self.conn)
          if pilot.insert_record_by_id(self.sql_insert_pilots) == -1:
            raise ValueError

        elif selected_table == self.AIRCRAFTS:
          aircraft = AircraftsInfo(cursor=self.cur, conn=self.conn)
          if aircraft.insert_record_by_id(self.sql_insert_aircrafts) == -1:
            raise ValueError

        elif selected_table == self.OPERATED_BY:
          operatedBy = OperatedByInfo(cursor=self.cur, conn=self.conn)
          if operatedBy.insert_record_by_id(self.sql_insert_operated_by) == -1:
            raise ValueError

        print(f"Inserted data to {selected_table} successfully.")

    except Exception as e:
      print(
          "\nOperation terminated. Please see the message above for further information.\n"
      )
      print(e)
    finally:
      self.conn.close()

  def update_data(self):
    '''
    This function asks the user to select which table they wish to update and then asks 
    for their input to change the record of the selected table
    '''
    try:

      self.get_connection()
      selected_table = self.select_existing_table_to_operate()
      if selected_table == -2:
        print('Returning to main menu')

      elif selected_table == -1:
        print("""No tables exist. \n
              You have to create a table first to proceed with this operation."""
              )
      else:

        if selected_table == self.FLIGHTS:
          flight = FlightInfo(cursor=self.cur, conn=self.conn)
          if flight.update_record_by_id(self.sql_update_flights_data) == -1:
            raise ValueError

        elif selected_table == self.PILOTS:
          pilot = PilotsInfo(cursor=self.cur, conn=self.conn)
          if pilot.update_record_by_id(self.sql_update_pilots_data) == -1:
            raise ValueError

        elif selected_table == self.AIRCRAFTS:
          aircraft = AircraftsInfo(cursor=self.cur, conn=self.conn)
          if aircraft.update_record_by_id(
              self.sql_update_aircrafts_data) == -1:
            raise ValueError

        elif selected_table == self.OPERATED_BY:
          print(
              "OperatedBy contains a composite PK referencing Pilots and Flights tables."
          )
          print("You should delete the PK record and insert a new one.")
          raise ValueError

        print(f"Updated data in {selected_table} successfully.")

    except Exception as e:
      print(e)
      print(
          "\nOperation terminated. Please see the message above for further information.\n"
      )

    finally:
      self.conn.close()

  def delete_data(self):
    '''
    This function deletes a record based on the primary key of the selected table
    '''
    try:

      self.get_connection()
      selected_table = self.select_existing_table_to_operate()
      if selected_table == -2:
        print('Returning to main menu')

      elif selected_table == -1:
        print("""No tables exist. \n
              You have to create a table first to proceed with this operation."""
              )
      else:

        if selected_table == self.FLIGHTS:
          flight = FlightInfo(cursor=self.cur, conn=self.conn)
          if flight.delete_record_by_id(self.sql_delete_flights_data) == -1:
            raise ValueError

        elif selected_table == self.PILOTS:
          pilot = PilotsInfo(cursor=self.cur, conn=self.conn)
          if pilot.delete_record_by_id(self.sql_delete_pilots_data) == -1:
            raise ValueError

        elif selected_table == self.AIRCRAFTS:
          aircraft = AircraftsInfo(cursor=self.cur, conn=self.conn)
          if aircraft.delete_record_by_id(
              self.sql_delete_aircrafts_data) == -1:
            raise ValueError

        elif selected_table == self.OPERATED_BY:
          operatedBy = OperatedByInfo(cursor=self.cur, conn=self.conn)
          if operatedBy.delete_record_by_id(
              self.sql_delete_operatedby_data) == -1:
            raise ValueError

        print(f"Deleted data from {selected_table} successfully.")

    except Exception as e:
      print(e)
      print(
          "\nOperation terminated. Please see the message above for further information.\n"
      )

    finally:
      self.conn.close()

  # ---- Calculate summary stats
  def calc_summary_stat(self, qid):
    q1 = '''SELECT Status, COUNT(Status) AS 'Number of Flights'
           FROM Flights 
           GROUP BY Status
           ORDER BY COUNT(Status) DESC
           '''
    q2 = '''SELECT Status, ROUND(SUM(MaxPassengers),1) AS 'Total Passengers Capacity'
           FROM Aircrafts, Flights
           WHERE Flights.AircraftID = Aircrafts.AircraftID 
           GROUP BY Status
           ORDER BY ROUND(SUM(MaxPassengers),1) DESC
           '''
    q3 = '''SELECT PilotID,
                  CONCAT(LastName,' ', FirstName) AS Name,
                  ROUND((julianday('now') - julianday(ProfSince ))/365,1) AS ProfExperienceYears
           FROM Pilots
           ORDER BY ProfExperienceYears DESC
           '''
    q4 = '''SELECT Pilots.PilotID, Status, COUNT(Flights.FlightID) AS "Number of Flights"
            FROM Flights, Pilots, OperatedBy
            WHERE Flights.FlightID = OperatedBy.FlightID
              AND Pilots.PilotID = OperatedBy.PilotID
            GROUP BY Pilots.PilotID, Status

         '''
    map_queries = {1: q1, 2: q2, 3: q3, 4: q4}
    try:
      self.get_connection()
      result = self.cur.execute(map_queries[qid], )
      records = result.fetchall()
      if len(records) > 0:
        self.pretty_print(columns=result.description, records=records)
      else:
        print(f"No records were found.")
    except Exception as e:
      print("\nAn error occurred. Operation terminated.\n")
      print(e)
    finally:
      self.conn.close()


# --------------------------------------------------------------
class PilotsInfo:

  def __init__(self, cursor, conn=None):
    self.pilot_id = ''
    self.first_name = ''
    self.last_name = ''
    self.school = ''
    self.birth_date = ''
    self.prof_since = ''
    self.cursor = cursor
    self.conn = conn

  # table interaction functions
  def delete_record_by_id(self, sql_delete_pilots_data):
    '''
    Deletes a Pilots table record by its ID
    '''
    pilot_id = input("Enter PilotID, or r to return: ")
    if pilot_id == 'r':
      return -1
    else:
      id_in_table = self.accepted_pilot_id(pilot_id) == False
      if id_in_table:
        self.cursor.execute(sql_delete_pilots_data, (pilot_id, ))
        result = self.cursor.rowcount
        print("Rows affected:", str(result))
        self.conn.commit()
      else:
        print('Pilot ID does not exist.')
        return -1

  def update_record_by_id(self, sql_update_pilots_data):
    '''
    Updates a Pilots table record by its ID
    '''
    pilot_id = input("Enter PilotID, or r to return: ")
    if pilot_id == 'r':
      return -1
    else:
      id_in_table = self.accepted_pilot_id(pilot_id) == False
      if id_in_table:
        print('Pilot ID found.')
        if self.set_first_name(input("Enter First Name: ")):
          if self.set_last_name(input("Enter Last Name: ")):
            if self.set_school(input("Enter Air School Name: ")):
              if self.set_birth_date(input("Enter Birth Date: ")):
                if self.set_prof_since(input("Enter Prof Since Date: ")):
                  pilot = str(self.__str__()) + "\n" + pilot_id
                  # print(pilot)
                  self.cursor.execute(sql_update_pilots_data,
                                      tuple(pilot.split("\n"))[1:])
                  result = self.cursor.rowcount
                  print("Rows affected:", str(result))
                  self.conn.commit()

                else:
                  return -1
              else:
                return -1
            else:
              return -1
          else:
            return -1
        else:
          return -1
      else:
        print('Pilot ID does not exist.')
        return -1

  def insert_record_by_id(self, sql_insert_pilots):
    '''
      Insert new record to Pilots table
      '''
    pilot_id = input("Enter PilotID, or r to return: ")
    if pilot_id == 'r':
      return -1
    else:
      if self.set_pilot_id(pilot_id):
        if self.set_first_name(input("Enter First Name: ")):
          if self.set_last_name(input("Enter Last Name: ")):
            if self.set_school(input("Enter Air School Name: ")):
              if self.set_birth_date(input("Enter Birth Date: ")):
                if self.set_prof_since(input("Enter Prof Since Date: ")):
                  # print(tuple(str(self.__str__()).split("\n")))
                  self.cursor.execute(sql_insert_pilots,
                                      tuple(str(self.__str__()).split("\n")))
                  result = self.cursor.rowcount
                  print("Rows affected:", str(result))
                  self.conn.commit()
                else:
                  return -1
              else:
                return -1
            else:
              return -1
          else:
            return -1
        else:
          return -1
      else:
        return -1

  # validation functions
  def accepted_pilot_id(self, pilot_id):
    '''
    Validate that the pilot id is not already being used on the Pilots table
    '''
    existing_ids = self.cursor.execute("SELECT PilotID FROM Pilots").fetchall()
    existing_ids = [i[0] for i in existing_ids]
    if pilot_id not in existing_ids:
      return True
    else:
      return False

  def accepted_birth_date(self, birth_date):
    '''
    Validate that the input is a date respecting the "YYYY-MM-DD" format
    '''
    try:
      # confirm format
      datetime.date.fromisoformat(birth_date)
      # confirm that the pilot is between 18 and 70 yo
      days = (datetime.datetime.now() -
              datetime.datetime.strptime(birth_date, '%Y-%m-%d')).days
      years = days / 365.24
      if (years >= 18) & (70 >= years):
        return True
      else:
        return False
    except:
      return False

  def accepted_prof_since_date(self, prof_since):
    '''
    Validate the prof since date
    '''
    try:
      # confirm format
      datetime.date.fromisoformat(prof_since)
      # confirm that the pilot turned pro at least 18 years following their birth
      turned_pro = datetime.datetime.strptime(prof_since, '%Y-%m-%d')
      birth = datetime.datetime.strptime(self.birth_date, '%Y-%m-%d')
      days = (turned_pro - birth).days
      years = days / 365.24
      # confirm that the prof date is not set in the future
      days_from_now = (datetime.datetime.now() -
                       datetime.datetime.strptime(prof_since, '%Y-%m-%d')).days
      if (years >= 18) & (days_from_now > 0):
        return True
      else:
        return False
    except:
      return False

  # setter functions
  def set_pilot_id(self, pilot_id):
    if self.accepted_pilot_id(pilot_id) & (pilot_id != "\n"):
      self.pilot_id = pilot_id
      return True
    else:
      print("Invalid Pilot ID.")
      return False

  def set_first_name(self, first_name):
    if (len(first_name) > 1) & (not first_name.isnumeric()) & (first_name
                                                               != "\n"):
      self.first_name = first_name
      return True
    else:
      print(
          "Pilot names must have at least two characters and be non-numeric.")
      return False

  def set_last_name(self, last_name):
    if (len(last_name) > 1) & (not last_name.isnumeric()) & (last_name
                                                             != "\n"):
      self.last_name = last_name
      return True
    else:
      print(
          "Pilot names must have at least two characters and be non-numeric.")
      return False

  def set_school(self, school):
    if (len(school) > 1) & (not school.isnumeric()) & (school != "\n"):
      self.school = school
      return True
    else:
      print(
          "Air school names must have at least two characters and be non-numeric."
      )
      return False

  def set_birth_date(self, birth_date):
    if self.accepted_birth_date(birth_date):
      self.birth_date = birth_date
      return True
    else:
      print(
          "Invalid birth date. The pilot must be between 18 and 70 years old.")
      return False

  def set_prof_since(self, prof_since):
    if self.accepted_prof_since_date(prof_since):
      self.prof_since = prof_since
      return True
    else:
      print(
          "Invalid ''prof since'' date. The pilot must have been at least 18 years old when they started working as professionals."
      )
      return False

  def __str__(self):
    return self.pilot_id + "\n"\
        + self.first_name + "\n"\
        + self.last_name + "\n"\
        + self.school + "\n"\
        + self.birth_date + "\n"\
        + self.prof_since


# --------------------------------------------------------------
class AircraftsInfo:

  def __init__(self, cursor, conn=None):
    self.aircraft_id = ''
    self.manufacturer = ''
    self.model = ''
    self.max_passengers = ''
    self.crew = ''
    self.cursor = cursor
    self.conn = conn

  # table interaction functions

  def delete_record_by_id(self, sql_delete_aircrafts_data):
    '''
    Deletes an Aircrafts table record by its ID
    '''
    aircraft_id = input("Enter AircraftID, or r to return: ")
    if aircraft_id == 'r':
      return -1
    else:
      id_in_table = self.accepted_aircraft_id(aircraft_id) == False
      if id_in_table:
        self.cursor.execute(sql_delete_aircrafts_data, (aircraft_id, ))
        result = self.cursor.rowcount
        print("Rows affected:", str(result))
        self.conn.commit()
      else:
        print('Aircraft ID does not exist.')
        return -1

  def update_record_by_id(self, sql_update_aircrafts_data):
    '''
    Updates an Aircrafts table record by its ID
    '''
    aircraft_id = input("Enter AircraftID, or r to return: ")
    if aircraft_id == 'r':
      return -1
    else:
      id_in_table = self.accepted_aircraft_id(aircraft_id) == False
      if id_in_table:
        print('Aircraft ID found.')
        if self.set_manufacturer(input("Enter Manufacturer: ")):
          if self.set_model(input("Enter Model: ")):
            if self.set_max_passengers(input("Enter Max Passengers: ")):
              if self.set_crew(input("Enter Crew Size: ")):
                pilot = str(self.__str__()) + "\n" + aircraft_id
                # print(pilot)
                self.cursor.execute(sql_update_aircrafts_data,
                                    tuple(pilot.split("\n"))[1:])
                result = self.cursor.rowcount
                print("Rows affected:", str(result))
                self.conn.commit()

              else:
                return -1
            else:
              return -1
          else:
            return -1
        else:
          return -1
      else:
        print('Aircraft ID does not exist.')
        return -1

  def insert_record_by_id(self, sql_insert_aircrafts):
    '''
      Insert new record to Aircrafts table
      '''
    aircraft_id = input("Enter AircraftID, or r to return: ")
    if aircraft_id == 'r':
      return -1
    else:
      if self.set_aircraft_id(aircraft_id):
        if self.set_manufacturer(input("Enter Manufacturer: ")):
          if self.set_model(input("Enter Model: ")):
            if self.set_max_passengers(input("Enter Max Passengers: ")):
              if self.set_crew(input("Enter Crew Size: ")):
                # print(tuple(str(self.__str__()).split("\n")))
                self.cursor.execute(sql_insert_aircrafts,
                                    tuple(str(self.__str__()).split("\n")))
                result = self.cursor.rowcount
                print("Rows affected:", str(result))
                self.conn.commit()

              else:
                return -1
            else:
              return -1
          else:
            return -1
        else:
          return -1
      else:
        return -1

  # validation functions
  def accepted_aircraft_id(self, aircraft_id):
    '''
    Validate that the aircraft id is not already being used on the Aircrafts table
    '''
    existing_ids = self.cursor.execute(
        "SELECT AircraftID FROM Aircrafts").fetchall()
    existing_ids = [i[0] for i in existing_ids]
    if aircraft_id not in existing_ids:
      return True
    else:
      return False

  # setter functions
  def set_aircraft_id(self, aircraft_id):
    if self.accepted_aircraft_id(aircraft_id) & (aircraft_id != "\n"):
      self.aircraft_id = aircraft_id
      return True
    else:
      print("Invalid Aircraft ID.")
      return False

  def set_manufacturer(self, manufacturer):
    if (len(manufacturer) > 1) & (not manufacturer.isnumeric()) & (manufacturer
                                                                   != "\n"):
      self.manufacturer = manufacturer
      return True
    else:
      print(
          "Manufacturer names must have at least two characters and be non-numeric."
      )
      return False

  def set_model(self, model):
    if model != "\n":
      self.model = model
      return True
    return False

  def set_max_passengers(self, max_passengers):
    try:
      if (int(max_passengers) >= 20) & (int(max_passengers) <= 500):
        self.max_passengers = str(int(max_passengers))
        return True
      else:
        print("Max passengers value must be numeric and between 20 and 500.")
        return False
    except:
      print("Max passengers value must be numeric and between 20 and 500.")
      return False

  def set_crew(self, crew):
    try:
      if (int(crew) >= 2) & (int(crew) <= 10):
        self.crew = str(int(crew))
        return True
      else:
        print("Cabin crew value must be numeric and between 2 and 10.")
        return False
    except:
      print("Cabin crew value must be numeric and between 2 and 10.")
      return False

  def __str__(self):
    return self.aircraft_id + "\n"\
        + self.manufacturer + "\n"\
        + self.model + "\n"\
        + self.max_passengers + "\n"\
        + self.crew


# --------------------------------------------------------------
class OperatedByInfo:

  def __init__(self, cursor, conn=None):
    self.flight_id = ''
    self.pilot_id = ''
    self.cursor = cursor
    self.conn = conn

  # table interaction functions
  def delete_record_by_id(self, sql_delete_operated_by):
    '''
    Delete record from OperatedBy table
    '''
    flight_id = input("Enter FlightID, or r to return: ")
    if flight_id == 'r':
      return -1

    else:
      pilot_id = input("Enter PilotID, or r to return: ")
      if pilot_id == 'r':
        return -1

      contains_flight_id = (not self.accepted_flight_id(flight_id))
      contains_pilot_id = (not self.accepted_pilot_id_for_given_flight_id(
          flight_id, pilot_id))
      contains_both = contains_flight_id & contains_pilot_id

      if contains_both:
        self.set_pilot_id(pilot_id)
        self.set_flight_id(flight_id)
        # print(tuple(str(self.__str__()).split("\n")))
        self.cursor.execute(sql_delete_operated_by,
                            tuple(str(self.__str__()).split("\n")))
        result = self.cursor.rowcount
        print("Rows affected:", str(result))
        self.conn.commit()

      else:
        print("Composite PK does not exist.")
        return -1

  def insert_record_by_id(self, sql_insert_operated_by):
    '''
      Insert new record to OperatedBy table
      '''
    flight_id = input("Enter FlightID, or r to return: ")
    if flight_id == 'r':
      return -1
    else:
      pilot_id = input("Enter PilotID, or r to return: ")
      if pilot_id == 'r':
        return -1

      contains_flight_id = (not self.accepted_flight_id(flight_id))
      contains_pilot_id = (not self.accepted_pilot_id_for_given_flight_id(
          flight_id, pilot_id))
      contains_both = contains_flight_id & contains_pilot_id

      if (not contains_both) & (flight_id != pilot_id):
        self.set_pilot_id(pilot_id)
        self.set_flight_id(flight_id)
        # print(tuple(str(self.__str__()).split("\n")))
        self.cursor.execute(sql_insert_operated_by,
                            tuple(str(self.__str__()).split("\n")))
        result = self.cursor.rowcount
        print("Rows affected:", str(result))
        self.conn.commit()

      else:
        print("Composite PK already exists.")
        return -1

  # validate
  # def accepted_pilot_id(self, pilot_id):
  #   '''
  #   Validate that the pilot id is not already being used on the Pilots table
  #   '''
  #   existing_ids = self.cursor.execute(
  #       "SELECT PilotID FROM OperatedBy").fetchall()
  #   existing_ids = [i[0] for i in existing_ids]
  #   if pilot_id not in existing_ids:
  #     return True
  #   else:
  #     return False

  def accepted_pilot_id_for_given_flight_id(self, flight_id, pilot_id):
    '''
    Validate that the flight id is not already being used on the Flights table
    '''
    existing_ids = self.cursor.execute(
        "SELECT PilotID FROM OperatedBy WHERE FlightID = ? AND PilotID = ?",
        (flight_id, pilot_id)).fetchall()
    existing_ids = [i[0] for i in existing_ids]

    if pilot_id not in existing_ids:
      return True
    else:
      return False

  def accepted_flight_id(self, flight_id):
    '''
    Validate that the flight id is not already being used on the Flights table
    '''
    existing_ids = self.cursor.execute(
        "SELECT FlightID FROM OperatedBy").fetchall()
    existing_ids = [i[0] for i in existing_ids]
    if flight_id not in existing_ids:
      return True
    else:
      return False

  # setters
  def set_pilot_id(self, pilot_id):
    self.pilot_id = pilot_id

  def set_flight_id(self, flight_id):
    self.flight_id = flight_id

  def __str__(self):
    return self.flight_id + "\n" + self.pilot_id


# --------------------------------------------------------------
class FlightInfo:

  def __init__(self, cursor, conn=None):
    self.flight_id = ''
    self.flight_origin = ''
    self.flight_destination = ''
    self.flight_departure = ''
    self.status = ''
    self.aircraft_id = ''
    self.cursor = cursor
    self.conn = conn
    self.valid_status_list = ["Cancelled", "Landed", "Delayed", "Scheduled"]

  # table interaction functions

  def delete_record_by_id(self, sql_delete_flights_data):
    '''
    Deletes a Flights table record by its ID
    '''
    flight_id = input("Enter FlightID, or r to return: ")
    if flight_id == 'r':
      return -1
    else:
      id_in_table = self.accepted_flight_id(flight_id) == False
      if id_in_table:
        self.cursor.execute(sql_delete_flights_data, (flight_id, ))
        result = self.cursor.rowcount
        print("Rows affected:", str(result))
        self.conn.commit()
      else:
        print('Flight ID does not exist.')
        return -1

  def update_record_by_id(self, sql_update_flights_data):
    '''
    Updates a Flights table record by its ID
    '''
    flight_id = input("Enter FlightID, or r to return: ")
    if flight_id == 'r':
      return -1
    else:
      id_in_table = self.accepted_flight_id(flight_id) == False
      if id_in_table:
        print('Flight ID found.')
        if self.set_flight_origin(input("Enter Origin Airport: ")):
          if self.set_flight_destination(input("Enter Destination Airport: ")):
            if self.set_flight_departure(input("Enter Departure Date: ")):
              if self.set_status(input("Enter Flight Status: ")):
                if self.set_aircraft_id(input("Enter AircraftID: ")):
                  flight = str(self.__str__()) + "\n" + flight_id
                  # print(flight)
                  self.cursor.execute(sql_update_flights_data,
                                      tuple(flight.split("\n"))[1:])
                  result = self.cursor.rowcount
                  print("Rows affected:", str(result))
                  self.conn.commit()

                else:
                  return -1
              else:
                return -1
            else:
              return -1
          else:
            return -1
        else:
          return -1
      else:
        print('Flight ID does not exist.')
        return -1

  def insert_record_by_id(self, sql_insert_flights):
    '''
      Insert new record to Flights table
      '''
    flight_id = input("Enter FlightID, or r to return: ")
    if flight_id == 'r':
      return -1
    else:
      if self.set_flight_id(flight_id):
        if self.set_flight_origin(input("Enter Origin Airport: ")):
          if self.set_flight_destination(input("Enter Destination Airport: ")):
            if self.set_flight_departure(input("Enter Departure Date: ")):
              if self.set_status(input("Enter Flight Status: ")):
                if self.set_aircraft_id(input("Enter AircraftID: ")):
                  # print(tuple(str(self.__str__()).split("\n")))
                  self.cursor.execute(sql_insert_flights,
                                      tuple(str(self.__str__()).split("\n")))
                  result = self.cursor.rowcount
                  print("Rows affected:", str(result))
                  self.conn.commit()
                else:
                  return -1
              else:
                return -1
            else:
              return -1
          else:
            return -1
        else:
          return -1
      else:
        return -1

  # validation functions
  def accepted_departure_date(self, departure):
    '''
    Validate that the departure input is a date respecting the "YYYY-MM-DD" format
    '''
    try:
      datetime.date.fromisoformat(departure)
      return True
    except:
      return False

  def accepted_flight_id(self, flight_id):
    '''
    Validate that the flight id is not already being used on the Flights table
    '''
    existing_ids = self.cursor.execute(
        "SELECT FlightID FROM Flights").fetchall()
    existing_ids = [i[0] for i in existing_ids]
    if flight_id not in existing_ids:
      return True
    else:
      return False

  def accepted_flight_destination(self, destination):
    '''
    Validate that the destination is not the same airport as the origin airport
    '''
    return self.flight_origin != destination

  def accepted_flight_status(self, status):
    '''
    Validate that the flight status is a valid term
    '''
    return status in self.valid_status_list

  def accepted_flight_aircraft_id(self, aircraft_id):
    '''
    Validate that the aircraft id exists in the Aircrafts table
    '''
    existing_ids = self.cursor.execute(
        "SELECT AircraftID FROM Aircrafts").fetchall()
    existing_ids = [i[0] for i in existing_ids]
    return aircraft_id in existing_ids

  # setter functions
  def set_flight_id(self, flight_id):
    if self.accepted_flight_id(flight_id) & (flight_id != "\n"):
      self.flight_id = flight_id
      return True
    else:
      print("Invalid Flight ID.")
      return False

  def set_flight_origin(self, flight_origin):
    if flight_origin != "\n":
      self.flight_origin = flight_origin
      return True
    else:
      return False

  def set_flight_destination(self, flight_destination):
    if self.accepted_flight_destination(flight_destination) & (
        flight_destination != "\n"):
      self.flight_destination = flight_destination
      return True
    else:
      print("Flight origin and destination cannot be the same.")
      return False

  def set_flight_departure(self, flight_departure):
    if self.accepted_departure_date(flight_departure) & (flight_departure
                                                         != "\n"):
      self.flight_departure = flight_departure
      return True
    else:
      print("Flight departure date must follow the ''YYYY-MM-DD'' format.")
      return False

  def set_status(self, status):
    if self.accepted_flight_status(status) & (status != "\n"):
      self.status = status
      return True
    else:
      print("Status must be a label from the following values:",
            self.valid_status_list)
      return False

  def set_aircraft_id(self, aircraft_id):
    if self.accepted_flight_aircraft_id(aircraft_id) & (aircraft_id != "\n"):
      self.aircraft_id = aircraft_id
      return True
    else:
      print("The Aircraft ID must be available in the Aircrafts table")
      return False

  def __str__(self):
    return self.flight_id + "\n"\
        + self.flight_origin + "\n"\
        + self.flight_destination + "\n"\
        + self.flight_departure + "\n"\
        + self.status + "\n"\
        + self.aircraft_id


# --------------------------------------------------------------
db_ops = DBOperations()

while True:
  print("\n Menu:")
  print("**********")
  print('\n----- Database management and review:')
  print("  1. Create a table")
  print("  2. Drop a table")
  print("  3. View available table names")
  print("  4. View deleted table names")
  print("  5. Optional bulk data import")
  print('\n----- Insert, update, delete data:')
  print("  6. Insert table record")
  print("  7. Update table values based on PK")
  print("  8. Delete table record based on PK")
  print('\n----- Inspect tables:')
  print("  9. View table (SEL *)")
  print(" 10. Search table record based on PK")
  print(" 11. Search table record based on Non-PK attributes")
  print('\n----- Calculate summary stats:')
  print(" 12. Number of flights by status")
  print(" 13. Max passengers capacity by flight status")
  print(" 14. Pilots professional experience in years")
  print(" 15. Number of flights by pilot and flight status")
  print('\n----- ')
  print(" Type 0 to exit the program\n")

  try:
    __choose_menu = int(input("Enter your choice: "))
    print("\n")
  except:
    __choose_menu = None
    print(
        "Please use numeric input to interact with the database or to exit the program.\n"
    )

  # ---
  if __choose_menu == 1:
    db_ops.create_table()
  elif __choose_menu == 2:
    db_ops.drop_table()
  elif __choose_menu == 3:
    db_ops.get_available_tables()
  elif __choose_menu == 4:
    db_ops.get_deleted_tables()
  elif __choose_menu == 5:
    db_ops.bulk_import_seed_data()
  # ---
  elif __choose_menu == 6:
    db_ops.insert_data()
  elif __choose_menu == 7:
    db_ops.update_data()
  elif __choose_menu == 8:
    db_ops.delete_data()
  # ---
  elif __choose_menu == 9:
    db_ops.select_all()
  elif __choose_menu == 10:
    db_ops.search_data()
  elif __choose_menu == 11:
    db_ops.search_data_by_non_pk()
  # ---
  elif __choose_menu == 12:
    db_ops.calc_summary_stat(1)
  elif __choose_menu == 13:
    db_ops.calc_summary_stat(2)
  elif __choose_menu == 14:
    db_ops.calc_summary_stat(3)
  elif __choose_menu == 15:
    db_ops.calc_summary_stat(4)
  elif __choose_menu == 16:
    db_ops.calc_summary_stat(4)

  elif __choose_menu == 0:
    print("Goodbye..\n")
    exit(0)
  elif __choose_menu is None:
    pass
  else:
    print("Invalid Choice\n")

# -- END -- 2024 May 26, 11:01 UTC
