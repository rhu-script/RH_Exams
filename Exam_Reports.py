#!/usr/bin/python2.7
## This Script is to Process testing records
##
## Author: Ahmed Sammoud
## Date:   July, 2016
##
## Company: Red Hat, Red Hat University , Intern_Team_2016
##
## Description :
##              - This Script main purpose is to be the main module for the Testing Stats
##              - The following is The scope of the project:
##                                    - Get information from testing csv file and store it in db
##                                    - Form a User interface to query the DB.
##                                    - Automate the update procedure.
##
##              - The Script runs in the following phases:        
##              --            1- Main module runs and start Either: Report      /OR/
##                               Import CSV file into db                       /OR/
##                              Both ** Run import then generate report. 
##                
##              - Future work is to download the Information from the site and import it (Automate)
##

import ConfigParser
import argparse

from csv_import import *
from db_store import *

## Moved to Config file for the script.
# DB = "mongodb://localhost:27017"
DB = ""

# Setup the Reportng level for the script.
logging.basicConfig(level=logging.DEBUG)

# Name of the Main Program section
logger = logging.getLogger("__Main__")


# Please change the name of the main function to something else,
# This code needs to be divided into different methods. -- Reporting part needs some work

def main():
    ########################################
    #
    # Initiating Command-Line Parsing
    #

    parser = argparse.ArgumentParser(prog='Testing_Stat',
                                     description='Exam Stats Storing/Reporting tool:')

    parser.add_argument('-import_csv', nargs=1, metavar=('File_Name'),
                        help="tell script to import from a csv into local db ")

    parser.add_argument('-all', nargs=1, metavar=('File_Name'), help="Import from csv and generate a report")

    parser.add_argument('-report', nargs=2, metavar=("File_Name","Report_Type"),
                        help='''Generate a Report from Local DB. Options [ALL,Site,Country,Exams,RedHatters]
                        ''')

    ############################################
    # Config File :
    # Reading values from config file.
    Config = ConfigParser.ConfigParser()
    Config.read("Test_Stat.conf")
    options = {}

    # Main files option
    main_opt = Config.options("Main")
    for opt in main_opt:
        try:
            options[opt.upper()] = Config.get("Main", opt)
        except:
            logger.error("Exception %s", options[opt])

    DB = options["DB"].strip()

    ######################################3
    # Actual Processing Init
    #
    args = parser.parse_args()
    Test_Csv = None
    Test_DB = None
    pp = None

    ###############################################
    # Checking and processing different Options

    # Same as above, plus generate a report
    if args.all is not None:
        logger.debug("Get Data from csv file, Store it in db, Generate a report .. Arguments Passed:" + str(args.all))
        file = args.all[0]

        Test_Csv = CSV_Import(file)
        Test_DB = Test_Store(DB)

        # get list
        list = Test_Csv.getlist()

        # store list
        Test_DB.Store(list, "Test_Results")

        Test_DB = Test_Store(DB)
        # Output CSV Report
        T_csv = CSV_Import(filename="Testing_Report.csv", perm="wb")
        T_csv.store_Rows(Report(Test_DB))


    # Generate a report
    # Options to the report capabilities : "Date, Date range, specific reporting.. etc."
    elif args.report is not None:

        logger.debug("Generate a Report From DB into csv ....  Arguments Passed: " + str(args.report))
        # Output CSV Report
        file = args.report[0]
        type = args.report[1]

        # Check for the Report Type and Generates it
        if type == "ALL":

            Test_DB = Test_Store(DB)
            T_csv = CSV_Import(file, perm="wb")
            T_csv.store_Rows(Report(Test_DB))

        elif type == "Site":
            Test_DB = Test_Store(DB)
            T_csv = CSV_Import(file, perm="wb")
            T_csv.store_Rows(Report_Exams_Site(Test_DB))

        elif type == "Country":
            Test_DB = Test_Store(DB)
            T_csv = CSV_Import(file, perm="wb")
            T_csv.store_Rows(Report_Exams_Country(Test_DB))

        elif type == "RedHatters" :
            Test_DB = Test_Store(DB)
            T_csv = CSV_Import(file, perm="wb")
            T_csv.store_Rows( Report_RedHatter_Info(Test_DB) )

        elif type == "Exams" :
            Test_DB = Test_Store(DB)
            T_csv = CSV_Import(file, perm="wb")
            list = []
            logger.debug("Reporting Number of Examinees per Exam")
            result = Test_DB.Get_NumUsers_Exam()
            list.append(["Number of Examinees per Exam"])
            list.append(["Exam Name", "Number of Exam Takers"])

            for i in result:
                list.append([str(i['_id']), str(i['Number'])])
            T_csv.store_Rows( list)

    # Import a CSV amd store it in DB
    elif args.import_csv is not None:
        logger.debug("CSV Import.....  Arguments Passed: " + str(args.import_csv))

        file = args.import_csv[0]

        Test_Csv = CSV_Import(file)
        Test_DB = Test_Store(DB)

        # get list
        list = Test_Csv.getlist()

        # store list
        Test_DB.Store(list, "Test_Results")

    # When all else fails , Show help message
    else:
        parser.print_help()


# list Report( Test_Store Object)
# Function that will Generate a list of all reports pulled from DB
# Returns a list of all Information in the DB.

def Report(Test_DB):
    list = []

    # Report on All

    logger.debug("Reporting Number of users (RedHat and Others)")
    result = Test_DB.Get_NumUsers_Type()
    list.append(["Number of User (Red Hatters  and Others) "])
    for i in result:
        list.append(["Red Hatter" if i['_id'] == True else "Other", str(i['Number'])])

    list.append(["_" * 100])
    list.append([" " * 100])

    logger.debug("Reporting Number of Exams per Country")
    result = Test_DB.Get_NumExams_Country()
    list.append(["Number of Exams per Country"])
    list.append(["Country", "Number of Exam Takers"])
    for i in result:
        list.append([str(i['_id']), str(i['Number'])])

    list.append(["_" * 100])
    list.append([" " * 100])

    logger.debug("Reporting Number of Exams per site")
    result = Test_DB.Get_NumExams_Site()
    list.append(["Number of Exams per Site"])
    list.append(["Site", "Number of Exams"])
    for i in result:
        list.append([i['_id'], str(i['Number'])])

    list.append(["_" * 100])
    list.append([" " * 100])

    logger.debug("Reporting Number of Exams on KoaLA")
    result = Test_DB.Get_NumExams_KoaLA()
    list.append(["Number of Exams on KoaLA"])
    for i in result:
        list.append(["On KoaLA" if i['_id'] == True else "Regular", i['Number']])

    list.append(["_" * 100])
    list.append([" " * 100])

    logger.debug("Reporting Number of Examinees per Exam")
    result = Test_DB.Get_NumUsers_Exam()
    list.append(["Number of Examinees per Exam"])
    list.append(["Exam Name", "Number of Exam Takers"])

    for i in result:
        list.append([str(i['_id']), str(i['Number'])])

    list.append(["_" * 100])
    list.append([" " * 100])

    logger.debug("Reporting Number of Exams per User")
    result = Test_DB.Get_NumExam_Users()
    list.append(["Number of Exams per User"])
    list.append(["Name", "Number of Exams"])
    for i in result:
        list.append([i['_id'].encode('ascii', 'replace'), i['Exams']])

    list.append(["_" * 100])
    list.append([" " * 100])

    logger.debug("Reporting Number of Exams per Date")
    result = Test_DB.Get_NumExam_Date()
    list.append(["Number of Exams per Date "])
    for i in result:
        date_E = i['_id']
        list.append([str(date_E["month"] + '/' + date_E["day"] + '/' + date_E["year"]), i['Exams']])


    return list


def Report_Exams_Num(Test_DB):
    list = []

    # Report on All
    logger.debug("Reporting Number of Examinees per Exam")
    result = Test_DB.Get_NumUsers_Exam()
    list.append(["Number of Examinees per Exam"])
    list.append(["Exam Name", "Number of Exam Takers"])
    for i in result:
        list.append([str(i['_id']), str(i['Number'])])

    return list


def Report_RedHatter_Info(Test_DB):
    list = []

    # Report on RedHatters Exam

    logger.debug("Reporting RedHatter Info ")
    result = Test_DB.Get_RedHatter_Info()
    list.append(["Site","Country","Number"])
    for i in result:
        list.append([ i["_id"], ', '.join(i["Countries"]) , i["Number"]])

    return list

def Report_RedHatters_Num(Test_DB):
    list = []

    # Report on RedHatters Num

    logger.debug("Reporting Number of users (RedHat and Others)")
    result = Test_DB.Get_NumUsers_Type()
    list.append(["Number of User (Red Hatters  and Others) "])
    for i in result:
        list.append(["Red Hatter" if i['_id'] == True else "Other", str(i['Number'])])

    return list


def Report_Exams_Country(Test_DB):
    list = []

    # Report on Exams per country

    logger.debug("Reporting Number of Exams per Country")
    result = Test_DB.Get_NumExams_Country()
    list.append(["Number of Exams per Country"])
    list.append(["Country", "Number of Exam Takers"])
    for i in result:
        list.append([str(i['_id']), str(i['Number'])])

    list.append(["_" * 100])
    list.append([" " * 100])

    return list


def Report_Exams_Site(Test_DB):
    list = []
    logger.debug("Reporting Number of Exams per site")
    result = Test_DB.Get_NumExams_Site()
    list.append(["Number of Exams per Site"])
    list.append(["Site", "Number of Exams"])
    for i in result:
        list.append([i['_id'], str(i['Number'])])

    return list


def Report_NumExaminee_Exam(Test_DB):
    list = []

    logger.debug("Reporting Number of Examinees per Exam")
    result = Test_DB.Get_NumUsers_Exam()
    list.append(["Number of Examinees per Exam"])
    list.append(["Exam Name", "Number of Exam Takers"])

    for i in result:
        list.append([str(i['_id']), str(i['Number'])])

    return list

def Report_User(Test_DB,User):
    list = []
    logger.debug("Reporting Specific Examinee data :" + str(User))
    result = Test_DB.Get_User_Info(User)

    list.append(["Examinees Data "])
    list.append(["Name", User])
    list.append(["Exam", "Date", "Status", "User Email", "Site", "Site_Info", "City", "Country", "KoaLA", "RedHat ?"])

    for i in result:
        list.append([i["Exam"], str(i["Date"]["month"] + '/' + i["Date"]["day"] + '/' + i["Date"]["year"]), i["Status"],
                 i["Email"], i["Site"], i["Site_Info"], i["City"], i["Country"], i["KoaLA"], i["RedHat"]])

    list.append(["_" * 100])
    list.append([" " * 100])

    return list

def Report_Exam(Test_DB,Exam):
    list = []
    logger.debug("Reporting Specific Exam data :" + str(Exam))
    result = Test_DB.Get_Exam_Info(Exam)

    list.append(["Exam Data for :" + str(Exam)])
    list.append(["Date", "Number of Tests", "Country", "Status", "Site"])

    for i in result:
        date_E = i['_id']
        list.append([str(date_E["month"] + '/' + date_E["day"] + '/' + date_E["year"]), i["Number of Tests"],
         ', '.join(i["Countries"]),', '.join(i["Status"]), ', '.join(i["Sites"])])

    list.append(["_" * 100])
    list.append([" " * 100])
    return list


def Report_Site(Test_DB,Site):
    list = []
    logger.debug("Reporting Specific Exam data :" + str(Site))
    result = Test_DB.Get_Site_Info(Site)

    list.append(["Site Data for :" + str(Site)])
    list.append(["Date", "Number of Tests", "Exam", "Status"])

    for i in result:
        date_E = i['_id']
        list.append([str(date_E["month"] + '/' + date_E["day"] + '/' + date_E["year"]), i["Number of Tests"],
         ', '.join(i["Exams"]), ', '.join(i["Status"])])

    list.append(["_" * 100])
    list.append([" " * 100])
    return list




main()
