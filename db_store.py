#!/usr/bin/python2.7

## This Script is to Process Testing data and store the records into a DB
## The main DB will be Mongodb.
##
## The Script is an interface to simplify the storing and retrieving process.
## 
## Author: Ahmed Sammoud
## Date:   July, 2016
##
## Company: Red Hat, Red Hat University , Intern_Team_2016
##
## Description :
##              - This Script main purpose is to provide an interface to the DB for Exam info
##
##

import logging
import datetime
import re
from unittest import result

from pymongo import *


# Enumeration for values.
# For python 2.7
def enum(**name):
    return type("Enum", (), name)


# This class is an interface for the DB operations on the Testing Data
# Mainly the DB should be on Mongodb, but in case different DB was use, this should provide the isolation needed.

# Test_Store : Main methods are
#  - Store : Takes in the name of collection to store the record in .. This name should be selected from the DB_Table item. "See Table Overview below ".
#
#     An initial overview on the collections on mongo
#      - Test_Results    :Collection of information about Exams
#    

# See Documentation below for more specific information,.
class Test_Store:
    DB_Table = enum(Results="Test_Results")

    def __init__(self, DB_URI):

        # Mongo Connection
        self.DB_Connection = MongoClient(DB_URI)

        # Mongo db
        self.DB = self.DB_Connection['RH_Tests']

        # Setting name for logger information
        self.logger = logging.getLogger('___Store__')
        self.logger.debug("Test_Store initialized")

    # Store information on the corresponding collection,
    # db: Must be one of the DB_Table values
    # returns an object of type

    def Store(self, record, db):
        Coll = None
        result = None

        if db == "Test_Results":
            Coll = self.DB['Test_Results']
        else:
            Coll = None

        if (db == "Test_Results"):
            if Coll != None:
                for r in record:
                    # This will Upserts
                    self.logger.debug("Test_Results: Updating/Inserting : " + str(r["Name"]))
                    result = Coll.update(r, {"$set": r}, upsert=True)
                    self.logger.debug("Test_Results Site / Test" + r["Site"] + ">>>>>" + str(r["test"]))
                    self.logger.debug("Test_Results .. Updated/Inserted")

        return result

    # Get_NumExam_Date : Return Number of Exams by date
    # username: username
    #
    def Get_NumExam_Date(self):
        Coll = self.DB['Test_Results']

        self.logger.debug('Get Number of Exams per Date')
        result = Coll.aggregate([{"$group": {"_id": "$Date", "Exams": {"$sum": 1}}}])
        self.logger.debug("# of found records :" + str(result.alive) + " In Collection:" + Coll.full_name)
        return result

    # Get_Exams_Date : Return list of Exams in each date
    # username: username
    #
    def Get_Exams_Date(self):
        Coll = self.DB['Test_Results']

        self.logger.debug("Get Exams by Date")
        result = Coll.aggregate([{"$group": {"_id": "$Date", "Exams": {"$addToSet": "$Exam"}}}])
        self.logger.debug("# of found records :" + str(result.alive) + " In Collection:" + Coll.full_name)
        return result

    # Get_NumExam_User : Return information for specific user name
    # useername: user name
    #
    def Get_NumExam_User(self, Name):
        Coll = self.DB['Test_Results']

        self.logger.debug("Get Number of Exams For :" + str(Name))
        result = Coll.aggregate([{"$match": {"Name": Name}}, {"$group": {"_id": "$Name", "Exams": {"$sum": 1}}}])
        self.logger.debug("# of found records :" + str(result.alive) + " In Collection:" + Coll.full_name)
        return result

    # Get_NumExam_Users : Returns number of exams per users
    #
    def Get_NumExam_Users(self):
        Coll = self.DB['Test_Results']
        self.logger.debug("Get Number of Exams For Users")
        result = Coll.aggregate([{"$group": {"_id": "$Name", "Exams": {"$sum": 1}}}])
        self.logger.debug("# of found records :" + str(result.alive) + " In Collection:" + Coll.full_name)
        return result

    # Get_NumUsers_Type
    # Returns dictionary type with Two Entries
    # _id : True , Number : #  for the Number of Redhat users
    # _id : False, Number : #  for non Redhat users.
    def Get_NumUsers_Type(self):
        self.logger.debug("Get Number of Redhat Users ")
        Coll = self.DB['Test_Results']
        result = Coll.aggregate([{"$group": {"_id": "$RedHat", "Number": {"$sum": 1}}}])
        self.logger.debug("# of found records :" + str(result.alive) + " In Collection:" + Coll.full_name)
        return result

    # Get_RedHatter_Info
    # Returns cursor type .
    #
    #
    def Get_RedHatter_Info(self):
        self.logger.debug("Get RedHatters Info ")
        Coll = self.DB['Test_Results']
        result = Coll.aggregate([{"$match":{"RedHat":True}},{"$group": {"_id": "$Site", "Countries": {"$addToSet": "$Country"},"Number": {"$sum": 1}}}])
        self.logger.debug("# of found records :" + str(result.alive) + " In Collection:" + Coll.full_name)
        return result

    # Get_NumExams_Country
    # Returns dictionary type with entries per Country
    # _id : Country , Number : #  for the Number of Exam takers
    #
    def Get_NumExams_Country(self):
        self.logger.debug("Get Number of Exams per Country ")
        Coll = self.DB['Test_Results']
        result = Coll.aggregate([{"$group": {"_id": "$Country", "Number": {"$sum": 1}}}])
        self.logger.debug("# of found records :" + str(result.alive) + " In Collection:" + Coll.full_name)
        return result

    # Get_NumExams_Site
    # Returns dictionary type with Entries Per Site
    # _id : Site , Number : #  for the Number of  exams
    # 
    def Get_NumExams_Site(self):
        self.logger.debug("Get Number of Exams per Site ")
        Coll = self.DB['Test_Results']
        result = Coll.aggregate([{"$group": {"_id": "$Site", "Number": {"$sum": 1}}}])
        self.logger.debug("# of found records :" + str(result.alive) + " In Collection:" + Coll.full_name)
        return result

    # Get_NumExams_KoaLA
    # Returns dictionary type with Entries for KoaLa
    # _id : True , Number : #  for the Number of KoaLA
    # _id : False, Number : #
    def Get_NumExams_KoaLA(self):
        self.logger.debug("Get Number of Exams with KoaLA ")
        Coll = self.DB['Test_Results']
        result = Coll.aggregate([{"$group": {"_id": "$KoaLA", "Number": {"$sum": 1}}}])
        self.logger.debug("# of found records :" + str(result.alive) + " In Collection:" + Coll.full_name)
        return result

    # Get_NumUsers_Exam
    # Returns dictionary type with Number of users Entries Per Exam
    # _id : Exam , Number : #  for the Number of Redhat users
    def Get_NumUsers_Exam(self):
        self.logger.debug("Get Number of Exam takers per Exam ")
        Coll = self.DB['Test_Results']
        result = Coll.aggregate([{"$group": {"_id": "$Exam", "Number": {"$sum": 1}}}])
        self.logger.debug("# of found records :" + str(result.alive) + " In Collection:" + Coll.full_name)
        return result

    # Get Specific Name/User info : Is he a Redhatter, His exams .
    def Get_User_Info(self, User):
        list = []
        self.logger.debug("Get Exam taker Data :" + str(User))
        Coll = self.DB['Test_Results']
        result = Coll.aggregate([{"$match": {"Name": User}}])

        self.logger.debug("# of found records :" + str(result.alive) + " In Collection:" + Coll.full_name)
        return result

    # Get info on a Specific Exam: Number of test , Sites, Countries, Dates
    def Get_Exam_Info(self, Exam):
        list = []
        self.logger.debug("Get Exam taker Data :" + str(Exam))
        Coll = self.DB['Test_Results']
        result = Coll.aggregate([{"$match": {"Exam": Exam}}, {
            "$group": {"_id": "$Date", "Number of Tests": {"$sum": 1}, "Countries": {"$addToSet": "$Country"},
                       "Status": {"$addToSet": "$Status"}, "Sites": {"$addToSet": "$Site"}}}])

        self.logger.debug("# of found records :" + str(result.alive) + " In Collection:" + Coll.full_name)
        return result

    # Get info of a Site : Exam, number of Exams ...etc,
    def Get_Site_Info(self, Site):
        list = []
        self.logger.debug("Get Site Data :" + str(Site))
        Coll = self.DB['Test_Results']

        result = Coll.aggregate([{"$match": {"Site": Site}},{"$group": {"_id": "$Date", "Number of Tests": {"$sum": 1},"Exams": {"$addToSet": "$Exam"}, "Status": {"$addToSet": "$Status"} }}])

        self.logger.debug("# of found records :" + str(result.alive) + " In Collection:" + Coll.full_name)
        return result
