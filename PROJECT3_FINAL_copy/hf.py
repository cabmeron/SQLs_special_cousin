# Author: Cameron McCoy
# Date: 04/06/2023
# History: V1.1

import subprocess
import os
import user
import re
import csv
import json

the_Great_Operators = ['>', '<', '=', '!=']


def createDatabase(db_Name):
    subprocess.run(["mkdir", db_Name], check=False)


def doesDatabaseExist(db_Name):
    db_names_unavailable = subprocess.run(
        ["ls", "|", "grep ", db_Name], capture_output=True, text=True)
    if db_Name in db_names_unavailable.stdout:
        return 1
    else:
        return 0


def deleteDatabase(db_Name):
    subprocess.run(["rm", "-r", db_Name])


def doesTableExist(table_name, current_db):
    if table_name in subprocess.run(['ls', current_db,  '|', 'grep', table_name], capture_output=True, text=True).stdout:
        return 1
    else:
        return 0


def doesTableExist2(table_name, current_db):
    table_list_file = os.path.join(current_db, "tables.txt")
    if os.path.exists(table_list_file):
        with open(table_list_file, "r") as f:
            tables = f.read().splitlines()
            return table_name in tables
    else:
        return False


def query_extraction2(unecessary_strings, user_command):

    user_query = re.compile('(;|{})'.format(re.escape(unecessary_strings)))
    final_string = user_query.sub('', user_command)
    return final_string


def insertTuple(user_input_string, resident_DB, tuple_modifications_global_count):
    user_I_string = query_extraction2(user.INSERTION, user_input_string)

    table_name = user_I_string.split()[0]
    remaining_string = user_I_string.replace(table_name, "").replace(
        " values", "")

    table_attributes = [attr.strip()
                        for attr in remaining_string[1:-1].split(",")]

    if resident_DB is None:
        print("No active database selected!")
        return tuple_modifications_global_count

    if not doesTableExist(table_name, resident_DB):
        print(
            f"Could not add values to {table_name} because it does not exist.")
        return tuple_modifications_global_count

    filename = f"{resident_DB}/{table_name}.txt"
    with open(filename, 'a') as f:
        f.write('\n')

        f.write(" |".join(table_attributes))
    tuple_modifications_global_count += 1
    print(f"{tuple_modifications_global_count} new record inserted into {table_name}.")
    return tuple_modifications_global_count

# poor hashing of operands


def operand_extractor(operand_string):
    operand = None
    if (operand_string == '<'):
        operand = 1
    elif (operand_string == '>'):
        operand = 2
    elif (operand_string == '!='):
        operand = 3
    elif (operand_string == '='):
        operand = 4

    return operand


def updateTuple(user_input_string, resident_DB):
    user_input_parsed = query_extraction2(user.UPDATE_TB2, user_input_string)
    table_name = user_input_parsed.split()[0]
    setColumn = user_input_parsed.split()[2]
    setRecord = user_input_parsed.split()[4]
    whereColumn = user_input_parsed.split()[6]
    whereRecord = user_input_parsed.split()[8]

    if (resident_DB != None):
        if doesTableExist(table_name, resident_DB) == 1:

            filename = resident_DB + '/' + table_name + '.txt'
            f = open(filename, 'r')
            temporary_file = f.readlines()
            f.close()

            file_line_count = 0
            modified_file_lines = 0
            num_col_where = 0
            setColumnNum = 0

            for line in temporary_file:
                if (file_line_count == 0):
                    columnList = [s.strip() for s in line.split('|')]
                    for i, item in enumerate(columnList):
                        if whereColumn in item:
                            num_col_where = i

                    for j, item2 in enumerate(columnList):
                        if setColumn in item2:
                            setColumnNum = j

                if (file_line_count > 0 and line != '\n'):
                    tuple_data_ = line.split('|')
                    file_col_data = tuple_data_[num_col_where].strip()

                    if (file_col_data == whereRecord):
                        if (file_col_data == whereRecord):
                            tuple_data_[setColumnNum] = setRecord + ' '
                            modified_line = '|'.join(tuple_data_)
                            temporary_file[file_line_count] = modified_line + '\n'
                            modified_file_lines += 1
                file_line_count += 1

            if modified_file_lines > 0:
                f = open(filename, 'w')
                f.writelines(temporary_file)
                f.close()

            print(f"{modified_file_lines} record(s) updated.")


def deleteTuple(user_command, resident_DB):
    user_input_string = query_extraction2(user.DELETE_FROM, user_command)
    table_name = user_input_string.split()[0].capitalize()
    whereColumn = user_input_string.split()[2]
    whereRecord = user_input_string.split()[4]

    operand = operand_extractor(user_input_string.split()[3])

    if (resident_DB != None):
        if doesTableExist(table_name, resident_DB) == 1:
            filename = resident_DB + '/' + table_name + '.txt'
            f = open(filename, 'r')
            tempFile = f.readlines()
            f.close()

            current_count = 0
            modified_file_lines = 0
            num_col_for_where = 0

            for line in tempFile:
                if (current_count == 0):
                    columnList = [s.strip() for s in line.split('|')]
                    for i, item in enumerate(columnList):
                        if whereColumn in item:
                            num_col_for_where = i

                if (current_count > 0 and line != '\n'):
                    tupleDetails = line.split()
                    print(tupleDetails)

                    modified_file_lines = file_updater(
                        modified_file_lines, operand, tupleDetails, num_col_for_where, whereRecord, tempFile, current_count)

                current_count += 1
            with open(filename, "w") as f:
                f.write("")

            f = open(filename, 'w')
            for line in tempFile:
                print(line)
                if (line != None):
                    f.write(line)
            f.close()

            print(f"{modified_file_lines} record(s) removed in {table_name}.")


# additional helper function for deleteTuple()
def file_updater(mods, operand, tupleDetails, num_col_for_where, w_Record, tempFile, file_line_count):
    if (operand == 4):
        file_col_data = tupleDetails[num_col_for_where]
        if (type(file_col_data) is str):
            file_col_data = file_col_data.replace("|", "").strip()
            if (file_col_data == w_Record):
                tempFile[file_line_count] = None
                mods += 1

        elif (type(tupleDetails[num_col_for_where]) is not str):
            file_col_data = file_col_data.replace("|", "").strip()
            if ((file_col_data) == float(w_Record)):
                tempFile[file_line_count] = None
                mods += 1

    elif (operand == 2):
        file_col_data = tupleDetails[num_col_for_where]
        file_col_data = file_col_data.replace("|", "").strip()
        if ((float(file_col_data)) > float(w_Record)):
            tempFile[file_line_count] = None
            mods += 1

    elif (operand == 1):
        file_col_data = tupleDetails[num_col_for_where]
        file_col_data = file_col_data.replace("|", "").strip()
        if (float(file_col_data) < float(w_Record)):
            tempFile[file_line_count] = None
            mods += 1

    return mods


def multi_select_no_star(user_command, resident_DB):
    user_input_select = query_extraction2(user.SELECTA, user_command)

    selected_Columns = user_input_select.replace(",", "").split()
    operand = operator_loop_extractor(selected_Columns)
    selected_Columns = selected_Columns[:selected_Columns.index("from")]

    table_name = user_input_select.split()[len(selected_Columns)+1]

    w_Column = user_input_select.split()[len(selected_Columns)+3]
    w_Record = user_input_select.split()[len(selected_Columns)+5]

    file_path = f"{resident_DB}/{table_name}.txt"

    data = read_data_from_file(file_path)

    filtered_data = [row for row in data if filter_condition(
        row, w_Column, w_Record, operand)]

    print_selected_key_values(filtered_data, selected_Columns)


def operator_loop_extractor(user_input_string):
    for i, element in enumerate(user_input_string):
        if element in the_Great_Operators:
            operator_index = i
            return user_input_string[i]


# can be more modular when provided schema in text file
def read_data_from_file(file_path):
    with open(file_path, 'r') as file:
        lines = file.readlines()[1:]
        headers = ['pid', 'name', 'price']
        data = []
        for line in lines:
            if line.strip():  # Check if the line is not empty or whitespace
                row = dict(zip(headers, line.strip().split('|')))
                row['pid'] = int(row['pid'])
                row['name'] = row['name'].strip("'")
                row['price'] = float(row['price'])
                data.append(row)
    return data


def filter_condition(row, whereColumn, where_records, operand):
    value = None
    if whereColumn == 'pid':
        value = int(row[whereColumn])

        try:
            where_records = int(where_records)
        except ValueError:
            return False
    elif whereColumn == 'price':
        value = float(row[whereColumn])

        try:
            where_records = float(where_records)
        except ValueError:
            return False
    else:
        value = row[whereColumn].strip("'")

    if isinstance(value, str):

        value = value.replace("'", "")
        value = value.strip()

    if isinstance(where_records, str):
        where_records = where_records.replace("'", "")
        where_records = where_records.strip()
    if operand == '!=':
        return value != where_records
    elif operand == '>':
        return value > where_records
    elif operand == '<':
        return value < where_records
    elif operand == '=':
        return value == where_records
    else:
        return False


# helper function for mult_select
def print_selected_key_values(data, keys):
    for row in data:
        for key in keys:
            print(f"{key}: {row[key]}", end=" | ")
        print()


# helper function for table joins
def join_table_master(user_command, resident_db):

    # pre-processing start
    joinType = 0
    escape_identifier = 1
    if (user.LOJ in user_command.upper()):
        joinType = 1

    uppercase_select = query_extraction2(user.SELECT_STAR, user_command)
    select_values = query_extraction2(user.SELECT_STAR2, uppercase_select)
    select_values = select_values.replace(
        user.IJ, "").replace(user.LOJ2, "")
    word_command_pair = select_values.replace(",", "").split()

    table1Name = word_command_pair[0]
    table2Name = word_command_pair[2]
    comparisonOperator = operand_extractor(word_command_pair[6])

    T1 = []
    T2 = []
    T_Join = []
    list_of_T_names = [T1, T2]

    table_name_strings = [table1Name, table2Name]

    # checking for valid DB and Table before capturing file lines
    if resident_db != None:
        for x in range(0, 2):
            if doesTableExist2(table_name_strings[x], resident_db):
                f = open(f'{resident_db}/{table_name_strings[x]}.txt', 'r')
                for line in f:

                    list_of_T_names[x].append(line)

                f.close()
            else:
                print(
                    f"Unable to process query because Table: {table_name_strings[x]} because it does not exist.")
                escape_identifier = 0
    else:
        print("Current DB either does not exist or is not in use! (USE DB_NAME;).")

    if (escape_identifier == 1):
        table1Column = T1[0].index(word_command_pair[5].split(".")[1])
        table2Column = T2[0].index(word_command_pair[7].split(".")[1])

        # Comparison and Creation of final output relative to join type and comparison operator
        def master_join_hf():
            def process_row(t1_row, t2_row):
                if comparisonOperator == 2:
                    return t2_row.split("|")[table2Column] > t1_row.split("|")[table1Column]
                elif comparisonOperator == 4:
                    return t2_row.split("|")[table2Column] == t1_row.split("|")[table1Column]
                elif comparisonOperator == 3:
                    return t2_row.split("|")[table2Column] != t1_row.split("|")[table1Column]
                elif comparisonOperator == 1:
                    return t2_row.split("|")[table2Column] < t1_row.split("|")[table1Column]
                return False

            T1[0] = T1[0].rstrip('\n')
            T2[0] = T2[0].rstrip('\n')
            T_Join.append(f"{T1[0]}|{T2[0]}")

            for t1 in range(1, len(T1)):
                T1[t1] = T1[t1].rstrip("\n")
                found_match = False

                for t2 in range(1, len(T2)):
                    T2[t2] = T2[t2].rstrip('\n')
                    if process_row(T1[t1], T2[t2]):
                        T_Join.append(f'{T1[t1]} | {T2[t2]}')
                        found_match = True

                # left outer join logic
                if not found_match and joinType == 1:
                    T_Join.append(f"{T1[t1]}||")

            for row in T_Join:
                print(row)

        master_join_hf()


# start of locking mechanism code:

def makeLock(workingDB):
    if checkLock(workingDB):
        return 0
    else:
        # List all files in the workingDB directory and filter for .txt files
        tablesToLock = [file for file in os.listdir(
            workingDB) if file.endswith('.txt')]

        print(f"Tables to lock {tablesToLock}")

        for name in tablesToLock:
            lock_file_path = os.path.join(workingDB, f"{name}.lock")
            print(f"Creating lock file: {lock_file_path}")

            try:
                with open(lock_file_path, 'w') as lock_file:
                    pass
            except IOError as e:
                print(f"Error creating lock file: {lock_file_path}\n{str(e)}")

        return 1

        # Create lock files for each table


def checkLock(workingDB):
    if ".lock" in subprocess.run(['ls', workingDB, '|', 'grep ".lock"'], capture_output=True, text=True).stdout:
        # print("Locks found!")
        return 1
    else:
        return 0


def releaseLock(workingDB, c):
    for cmd in c:
        os.system(cmd)
    os.system(f"rm {workingDB}/*.lock")
    # print("Locks removed!")


def insertTuple2(user_input_string, resident_DB, tuple_modifications_global_count, isLocked, u, c):
    user_I_string = query_extraction2(user.INSERTION, user_input_string)

    table_name = user_I_string.split()[0]
    remaining_string = user_I_string.replace(table_name, "").replace(
        " values", "")

    table_attributes = [attr.strip()
                        for attr in remaining_string[1:-1].split(",")]

    print(f"Table attributes: {table_attributes}")

    for item in range(len(table_attributes)):
        if item == 0:
            table_attributes[item] = table_attributes[item].replace('(', '')
            print(table_attributes[item])

    print(f"Remaining string: {remaining_string}")

    def appendToFile():
        f = open(filename, 'a')
        f.write('\n')
        # Writes list to file with pipe delimiter
        f.write(" | ".join(table_attributes))
        f.close()

    print(f"U: {u}")
    print(f"Is Locked: {isLocked}")

    if resident_DB != None:
        if doesTableExist(table_name, resident_DB):
            if isLocked == 0:
                if u:
                    os.system(
                        f"cp {resident_DB}/{table_name}.txt {resident_DB}/{table_name}.new.txt")
                    filename = resident_DB + '/' + table_name + '.new.txt'
                    appendToFile()
                    c.append(f"rm {resident_DB}/{table_name}.txt")
                    c.append(
                        f"mv {resident_DB}/{table_name}.new.txt {resident_DB}/{table_name}.txt")
                else:
                    filename = resident_DB + '/' + table_name + '.txt'
                    appendToFile()
            else:
                print(f"Table {table_name} is locked!")

        else:
            print(
                f"Could not add values to {table_name} because itdoes not exist")

    else:
        print("Please specify which database to use!")

    # if resident_DB is None:
    #     print("No active database selected!")
    #     return tuple_modifications_global_count

    # if not doesTableExist(table_name, resident_DB):
    #     print(
    #         f"Could not add values to {table_name} because it does not exist.")
    #     return tuple_modifications_global_count

    # filename = f"{resident_DB}/{table_name}.txt"
    # with open(filename, 'a') as f:
    #     f.write('\n')

    #     f.write(" |".join(table_attributes))
    # tuple_modifications_global_count += 1
    # print(f"{tuple_modifications_global_count} new record inserted into {table_name}.")
    # return tuple_modifications_global_count


def updateTuple2(user_input_string, resident_DB, isLocked, u, c):
    user_input_parsed = query_extraction2(user.UPDATE_TB2, user_input_string)
    table_name = user_input_parsed.split()[0]
    setColumn = user_input_parsed.split()[2]
    setRecord = user_input_parsed.split()[4]
    whereColumn = user_input_parsed.split()[6]
    whereRecord = user_input_parsed.split()[8]

    def overwriteFile():
        f = open(filename, 'w')
        for line in temporary_file:
            f.write(line)
        f.close()

    if (resident_DB != None):
        if doesTableExist(table_name, resident_DB) == 1:
            if isLocked == 0:
                filename = resident_DB + '/' + table_name + '.txt'
                f = open(filename, 'r')
                temporary_file = f.readlines()
                f.close()

                file_line_count = 0
                modified_file_lines = 0
                num_col_where = 0
                setColumnNum = 0

                for line in temporary_file:
                    if (file_line_count == 0):
                        columnList = [s.strip() for s in line.split('|')]
                        for i, item in enumerate(columnList):
                            if whereColumn in item:
                                num_col_where = i

                        for j, item2 in enumerate(columnList):
                            if setColumn in item2:
                                setColumnNum = j

                    if (file_line_count > 0 and line != '\n'):
                        tuple_data_ = line.split('|')
                        file_col_data = tuple_data_[num_col_where].strip()

                        if (file_col_data == whereRecord):
                            if (file_col_data == whereRecord):
                                tuple_data_[setColumnNum] = setRecord + ' '
                                modified_line = '|'.join(tuple_data_)
                                temporary_file[file_line_count] = modified_line + '\n'
                                modified_file_lines += 1
                    file_line_count += 1

                # if modified_file_lines > 0:
                #     f = open(filename, 'w')
                #     f.writelines(temporary_file)
                #     f.close()

                print(f"{modified_file_lines} record(s) updated.")

                if u:
                    print("Line 581")
                    filename = resident_DB + '/' + table_name + '.new.txt'
                    print(f"Filename: {filename}")
                    os.system(f"touch {filename}")
                    overwriteFile()
                    c.append(f"rm {resident_DB}/{table_name}.txt")
                    c.append(
                        f"mv {resident_DB}/{table_name}.new.txt {resident_DB}/{table_name}.txt")
                else:
                    # {workingDB}/{tName}.txt
                    os.system(f'truncate -s 0 {filename}')
                    overwriteFile()
                print(f"{modified_file_lines} record(s) modified in {table_name}.")
            else:
                print(f"Table {table_name} is Locked!")
        else:
            print(
                f"Could not update values in {table_name} because it does not exist.")
    else:
        print("Please specify database")
