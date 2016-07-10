#!/usr/bin/python2.7
## This Script is to read CSV File and process the records to be imported into db.
##
## Author: Ahmed Sammoud
## Date:   June, 2016
##
## Company: Red Hat, Red Hat University , Intern_Team_2016
##
## Description :
##              - This Script main purpose is to read in ".csv" file and extract the needed information for data analysis.
##              - The Script runs in three phases:        
##              --            1- It uses the csv package and start reading Rows from the file in the Starter module.
##              --            2- It uses the Extract_Info function to extract the needed info from columns in each row.
##              --            3- The Process_Data function is used to :
##                                1- clean up the data from each columns
##                                2-populate the needed data structures.
##              --            
##              --    Starter --> Extract_Info --> Process_Data  
##



import csv
import re
import logging
import pprint


class CSV_Import:
    # A function that selects only the needed Columns and puts it in a new list.

    def __init__(self, filename="test_data.csv", perm='rb'):

        # Open CSV File
        CSV_F = open(filename, perm)

        if perm == 'rb':
            # Starting the reader, One Row at a time. Puts it into a list.
            self.Reader = csv.reader(CSV_F)
            self.Reader.next()  # skip header rows
            self.Reader.next()  # skip header rows
        else:
            # Starting the Writer
            self.Writer = csv.writer(CSV_F)

        # Setting name for logger information
        self.logger = logging.getLogger('___CSV_IMPORT__')
        self.logger.debug("CSV_Import intiailized")

    def __Extract_Data__(self, Row):
        Info = {}  # list of values from csv

        ## Data Format in the Exam .csv file
        ## Entries in order (Col) :: Info (Notes)
        ## - Exam Name           (A - 0)  :: Strings no Exam code.
        ## - Duration            (C - 2)  :: In Minutes
        ## - Examinee name       (D - 3)  :: Whole String (First M Last)
        ## - Examinee email      (E - 4)  :: email (Tag if redhat email)
        ## - Status of Exam      (F - 5)  :: Status on how test go (completed,Error,No Show) (Not Scores)
        ## - Country             (G - 6)  :: Country Name
        ## - Site / Office       (H - 7)  :: Site Name (Include info such as: Employees only, Retired, City Name, On KoaLA )
        ## - Date of Test        (K -10)  :: Date (There are 3 Date columns, I choose K since its more consisted: Extract Month,Year)
        ## Data Dictionary Key : entries
        ## Exam       :    Exam Name
        ## Duration   :    time In Minutes
        ## Name       :    Examinee Name
        ## Email      :    Examinee Email
        ## RedHat     :    True or False
        ## Status     :    Completed, System_Error, No Show
        ## Country    :    Conuntry of Test Location
        ## City       :    City of Location  -- Not Always Available
        ## Site       :    Office Name
        ## Site_Info  :    Employees Only/Retired
        ## KoaLa      :    True/False
        ## Date       :    {month,day,year}


        Info["Exam"] = str(Row[0].strip())
        Info["Duration"] = str(Row[2].strip())
        Info["Name"] = str(Row[3].strip())
        Info["Email"] = str(Row[4].strip())

        # This needs some review, since Only checks for RedHat emails.
        Info["RedHat"] = False if re.search('[a-zA-Z0-9_.]*@redhat.com', Info["Email"]) == None else True
        Info["Status"] = str(Row[5].strip())
        Info["Country"] = str(Row[6].strip())

        if "employees only" in str(Row[7].strip().lower()):
            Info["Site_Info"] = "Employees Only"
        elif "retired" in str(Row[7].strip().lower()):
            Info["Site_Info"] = "Retired"
        else:
            Info["Site_Info"] = "N/A"

        ## There are a lot of cases with this field
        ## This is Might start with:
        ## Employees Only: Red Hat- City -KOALA, Empoyees Only: Red Hat -Office - City - KOALA,or Employees Only: City

        Site = str(Row[7].strip())
        Info["test"] = Site
        if Site.startswith("Employees Only".upper()):
            Info["Site"] = "Red Hat"

            if "KOALA" in Site:
                Info["City"] = re.search('(?<=-)[\s\w-]*(?=-KOALA)', Site).group(0) if re.search(
                    "(?<=-)[\s\w-]*(?=-KOALA)", Site) is not None else "N/A"
            else:
                Info["City"] = re.search('(?<=-)[\s\w-]*', Site).group(0)

        ## Else examples : Office - City  or Office - City - KOALA or Office
        else:
            Info["Site"] = re.search('[\w|\D]*(?=-)*', Site).group(0)

            if re.search('(?<=-)[\s\w]*(?=KOALA)*', Site) != None:
                Info["City"] = re.search('(?<=-)[\s\w]*(?=KOALA)*', Site).group(0)
            else:
                Info["City"] = "N/A"

        # Is This test on a KOALA ?
        Info["KoaLA"] = False if re.search('[a-zA-Z0-9_.-]*KOALA', Site) is None else True

        # Extract Date .
        datetmp = re.search('\d*/\d*/\d*', Row[10].strip()).group(0).split("/")
        month = datetmp[0]
        day = datetmp[1]
        year = datetmp[2]
        Info["Date"] = dict(month=month, day=day, year=year)

        return Info

    def getlist(self):

        list = []

        for Row in self.Reader:
            if len(Row) > 0:
                Info = self.__Extract_Data__(Row)
                list.append(Info)

        return list

    def store_Rows(self, Rows):
        for Row in Rows:
            self.Writer.writerow(Row)

    def store_Row(self, Row):
        self.Writer.writerow(Row)


'''
T = CSV_Import("test.csv")
list = T.getlist()
P = pprint.PrettyPrinter()
P.pprint(list)
'''
