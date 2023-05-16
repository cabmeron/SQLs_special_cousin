# Author: Cameron McCoy
# Date: 04/06/2023
# History: V1.1


# user = constants containing keyword for commands
# hf = helper functions for responding to specific commands

import shlex
import user
import os
import hf
import re
import sys

resident_DB = None
user_command = None
table_list = [None]

numModifications = 0
tuple_modifications_global_count = 0

BreakFlag = 0
isLocked = 1
userMadeLock = 0
CommandsToExecuteOnCommit = []


# user input string parsing


def query_extraction(unecessary_strings):

    user_query = re.compile('(;|{})'.format(re.escape(unecessary_strings)))
    final_string = user_query.sub('', user_command)
    return final_string


# while loop for continuous funciton
while (user_command != user.EXIT and user_command != user.EXIT2) and user_command != user.EXIT3 and user_command != user.EXIT4:
    user_command = input(user.SHELL)
    if (';' not in user_command and user_command != user.EXIT and user_command != user.EXIT2 and user_command != user.EXIT3 and user_command != user.EXIT4):
        print(user.ERROR_sc)
    # pathway if user input contains "CREATE DB"
    elif (user.CREATE_DB in user_command):
        db_Name = query_extraction(user.CREATE_DB)
        db_exists = hf.doesDatabaseExist(db_Name)
        if (db_exists):
            print(f"!Failed to create '{db_Name}' because it already exists.")
        else:
            hf.createDatabase(db_Name)
            resident_DB = db_Name
            print(f"Database {db_Name} created.")
    # pathway if user input contains "create db"
    elif (user.CREATE_DB2 in user_command):
        db_Name = query_extraction(user.CREATE_DB2)
        db_exists = hf.doesDatabaseExist(db_Name)
        if (db_exists):
            print(f"!Failed to create '{db_Name}' because it already exists.")
        else:
            hf.createDatabase(db_Name)
            resident_DB = db_Name
            print(f"Database {db_Name} created.")
    # pathway if user input contains "DROP DATABASE"
    elif (user.DELETE_DB in user_command):
        db_Name = query_extraction(user.DELETE_DB)
        db_exists = hf.doesDatabaseExist(db_Name)
        if (db_exists):
            hf.deleteDatabase(db_Name)
            print(f"Database {db_Name} deleted.")
        else:
            print(f"!Failed to delete '{db_Name}' because it does not exist.")
    # pathway if user input contains "drop database"
    elif (user.DELETE_DB2 in user_command):
        db_Name = query_extraction(user.DELETE_DB2)
        db_exists = hf.doesDatabaseExist(db_Name)
        if (db_exists):
            hf.deleteDatabase(db_Name)
            print(f"Database {db_Name} deleted.")
        else:
            print(f"!Failed to delete '{db_Name}' because it does not exist.")
    # pathway if user input contains "CREATE TABLE"
    elif (user.CREATE_TB in user_command):
        full_input = query_extraction(user.CREATE_TB)
        table_name = full_input.split()[0]
        table_data = full_input.replace(table_name, "")
        table_attributes_final = table_data[2:-1].split(',')

        if (resident_DB != None):
            if hf.doesTableExist2(table_name, resident_DB) == 0:
                with open(os.path.join(resident_DB, "tables.txt"), "a") as f:
                    f.write(table_name + "\n")
                os.path.join(resident_DB, f'{table_name}.txt')
                filename = resident_DB + '/' + table_name + '.txt'
                with open(filename, 'w') as f:
                    f.write(' |'.join(table_attributes_final))
                print(f"Created table {table_name}")
            else:
                print(
                    f"Could not create table {table_name} because it already exists")
        else:
            print("Error: Unspecified database for table creation")
    # pathway if user input contains "create table"
    elif (user.CREATE_TB2 in user_command):
        full_input = query_extraction(user.CREATE_TB2)
        print(f"Full Input: {full_input}")
        table_pattern = re.compile(r"(\w+)\s*\((.+)\)")
        match = table_pattern.search(full_input)
        table_name = match.group(1)
        table_data = match.group(2)
        table_attributes_final = [attr.strip()
                                  for attr in table_data.split(',')]
        print(f"Table Name: {table_name}")
        print(f"Table Data: {table_data}")
        print(f"Final Table Attributes: {table_attributes_final}")

        if (resident_DB != None):
            if hf.doesTableExist2(table_name, resident_DB) == 0:
                with open(os.path.join(resident_DB, "tables.txt"), "a") as f:
                    f.write(table_name + "\n")
                os.path.join(resident_DB, f'{table_name}.txt')
                filename = resident_DB + '/' + table_name + '.txt'
                with open(filename, 'w') as f:
                    f.write(' |'.join(table_attributes_final))
                print(f"Created table {table_name}")
            else:
                print(
                    f"!Failed to create table {table_name} because it already exists")
        else:
            print("Error: Unspecified database for table creation")
    # pathway if user input contains "DROP TABLE"
    elif (user.DELETE_TB in user_command):
        table_name = query_extraction(user.DELETE_TB).replace(" ", "")
        if (resident_DB != None):
            if (hf.doesTableExist2(table_name, resident_DB)):
                with open(resident_DB + "/tables.txt", "r") as f:
                    tables = f.readlines()
                with open(resident_DB + "/tables.txt", "w") as f:
                    for table in tables:
                        if table.strip() != table_name:
                            f.write(table)

                file_path = os.path.join(resident_DB, f"{table_name}.txt")
                if os.path.exists(file_path):
                    os.remove(file_path)
                    print(f"Table {table_name} deleted.")
                else:
                    print(
                        f"!Failed to delete {table_name} because it does not exist.")

            else:
                print(
                    f"!Failed to delete {table_name} because it does not exist.")
        else:
            print("Error: Unspecified database for table deletion")
    # pathway if user input contains "drop table"
    elif (user.DELETE_TB2 in user_command):
        table_name = query_extraction(user.DELETE_TB2).replace(" ", "")

        if (resident_DB != None):
            if (hf.doesTableExist(table_name, resident_DB)):

                file_path = os.path.join(resident_DB, f"{table_name}.txt")
                if os.path.exists(file_path):
                    os.remove(file_path)
                    print(f"Table {table_name} deleted.")
                else:
                    print(
                        f"!Failed to delete {table_name} because it does not exist.")

            else:
                print(
                    f"!Failed to delete {table_name} because it does not exist.")
        else:
            print("Error: Unspecified database for table deletion")
    # pathway if user input contains "SELECT * FROM"
    elif (user.SELECT_STAR in user_command):
        table_name = query_extraction(
            user.SELECT_STAR).replace(" ", "")
        if resident_DB != None:
            if (hf.doesTableExist2(table_name, resident_DB)):
                file_path = f'{resident_DB}/{table_name}.txt'
                if os.path.getsize(file_path) > 0:
                    f = open(f'{resident_DB}/{table_name}.txt', 'r')
                    for line in f:
                        if line.strip():
                            print(line.strip())

            else:
                print(
                    f"!Failed to query table {table_name} because it does not exist.")
        else:
            print("Error: Unspecified database for selection deletion")

    elif (user.SELECT_STAR2 in user_command):
        if ("." in user_command.upper()):
            hf.join_table_master(user_command, resident_DB)
        else:

            table_name = query_extraction(
                user.SELECT_STAR2).replace(" ", "")
            if resident_DB != None:

                if (hf.doesTableExist2(table_name, resident_DB)):
                    file_path = f'{resident_DB}/{table_name}.txt'
                    if os.path.getsize(file_path) > 0:
                        f = open(f'{resident_DB}/{table_name}.txt', 'r')
                        for line in f:
                            if line.strip():
                                print(line.strip())

                        f.close()
                else:
                    print(
                        f"!Failed to query table {table_name} because it does not exist.")
        # else:
            # print("Error: Unspecified database for selection deletion")

    elif (user.SELECTA in user_command):
        hf.multi_select_no_star(user_command, resident_DB)

        # pathway if user input contains "ALTER TABLE"
    elif (user.ALTER_TABLE in user_command):
        full_input = query_extraction(user.ALTER_TABLE)
        table_name = full_input.split()[0]
        command = full_input.split()[1]
        alterAddition = full_input.replace(
            table_name, "").replace(command, "")[2:]

        if resident_DB != None:
            if hf.doesTableExist(table_name, resident_DB):
                f = open(f'{resident_DB}/{table_name}.txt', 'a')
                f.write(f" | {alterAddition}")
                f.close()
                print(f"Table {table_name} modified.")
            else:
                print(
                    f"!Failed to modify {table_name} because it does not exist.")
        else:
            print("Error: Unspecified database for table modification")
    # pathway if user input contains "USE"
    elif (user.USE in user_command):
        db_Name = query_extraction(user.USE)
        db_exists = hf.doesDatabaseExist(db_Name)
        if (db_exists):
            resident_DB = db_Name
            print(f"Using database {db_Name}")
        else:
            print(f"!Failed to use {db_Name} because it does not exist")
    # pathway if user input contains "use"
    elif (user.USE2 in user_command):
        db_Name = query_extraction(user.USE2)
        db_exists = hf.doesDatabaseExist(db_Name)
        if (db_exists):
            resident_DB = db_Name
            print(f"Using database {db_Name}")
        else:
            print(f"!Failed to use {db_Name} because it does not exist")
    # pathway for user insertions calling helper
    elif (user.INSERTION in user_command):
        if userMadeLock == 0:
            print("Is Locked Function (Line 262 main.py)")
            isLocked = hf.checkLock(resident_DB) if (
                resident_DB != None) else 1
        print(f"user_c: {user_command}, resident_DB: {resident_DB}, T_M_G_C: {tuple_modifications_global_count}, isLocked: {isLocked}, user_Made_Lock: {userMadeLock}, CommandsToExecuteOnCommit: {CommandsToExecuteOnCommit}")

        hf.insertTuple2(user_command, resident_DB, tuple_modifications_global_count,
                        isLocked, userMadeLock, CommandsToExecuteOnCommit)
    # pathway for user update functionality
    elif (user.UPDATE_TB2 in user_command):
        if resident_DB != None:
            hf.updateTuple2(user_command, resident_DB, isLocked,
                            userMadeLock, CommandsToExecuteOnCommit)

    # pathway for user delete from functionality
    elif (user.DELETE_FROM in user_command):
        hf.deleteTuple(user_command, resident_DB)

    elif (user.TRANSACT_START in user_command.upper()):
        userMadeLock = hf.makeLock(resident_DB)
        print("Transaction start")

    elif (user.COMMIT in user_command.upper()):
        if userMadeLock:
            hf.releaseLock(resident_DB, CommandsToExecuteOnCommit)
            print("Transaction Committed")
        else:
            print("Transaction Aborted")
        userMadeLock = 0


quit()
