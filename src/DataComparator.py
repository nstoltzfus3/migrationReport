import queue
from typing import List
import csv
import os

class DataComparator:
    """
    Top level class for comparing data from two different relational databases and producing report files
    from the resultant information. The report will detail three different types of migration errors
    between two databases:
    1. Corruption Errors - This occurs when an element in the database has had its data removed, but its
                            primary key is left intact.
    2. Copy Omission Errors - This occurs when an element in the pre-migration database does not get
                            properly copied over to the post-migration database.
    3. Creation Errors - This is when a false or invalid row is generated in the new database and is found to
                            have no element with a similar primary key from the original database.

    Note: This data comparator will not work if primary keys cannot be trusted (i.e. can be corrupted
    or duplicated).
    """

    def __init__(self):
        self.totalTableSizeA = 0
        self.totalTableSizeB = 0

        self.corruptionErrors = []
        self.creationErrors = []
        self.copyOmissionErrors = []

        self.numCorruptionErrors = 0
        self.numCreationErrors = 0
        self.numCopyOmissionErrors = 0

        self.leftoverA = {}
        self.leftoverB = {}

        self.largestAPrimaryKey = 0
        self.largestBPrimaryKey = 0

        self.incomingA = []
        self.incomingB = []

    def compareData(self, a, b):
        """
        Compares one row of data between 2 databases
        :param a: the older database.
        :param b: the post migration database.
        :return: if the information is equivalent.
        """
        # we assume primary keys are equal and occupy the zeroeth position,
        # if not, there must have been a missing element from the new database
        for i in range(1,len(a)):
            # start at 1 because we assume primary keys exist at index 0
            # we use database A's data as index because the migration likely appended or expanded
            # the information. We add this as a stretch goal to make it more generalized.
            if (a[i] != b[i]):
                # this is a corruption error
                return False
        return True

    def comparePrimaryKeys(self, a, b):
        # pretty straightforward if the Pks are at index 0.
        return a[0] == b[0]

    # TODO: move error tracking to the report compilation class.

    def addCopyOmissionError(self, data):
        '''
        Adds a copy omission error to the error tracking of the data comparator.
        :param data: a single row from a database that was not correctly copied.
        :return:
        '''
        self.copyOmissionErrors.append(data)
        self.numCopyOmissionErrors += 1

    def addCreationError(self, data):
        '''
        Adds a creation error to the error tracking of the data comparator.
        :param data: a single row from a database that was created during migration, but is false.
        :return:
        '''
        self.creationErrors.append(data)
        self.numCreationErrors += 1

    def addCorruptionError(self, a, b): # gets its own interface because its multiple data sources.
        '''
        Adds a corruption error to the error tracking of the data comparator.
        :param a: The row from the original database.
        :param b: The row containing the corrupted data from the post-migration database.
        :return:
        '''
        self.corruptionErrors.append([a, b])
        self.numCorruptionErrors += 1

    def flushA(self):
        '''
        Helper function that flushes the remaining results from LeftOverA's row tracking information,
        into the CopyOmissionError pipeline. This is called when it is impossible for future data from
        B to match any of the information in LeftOverA.

        We do this to allow for future memory optimization
        if there were large amounts of errors and we needed to write them to a file.
        :return:
        '''
        for key in self.leftoverA:  # things leftover in A must be CopyOmissionErrors
            self.addCopyOmissionError(self.leftoverA[key])
        self.leftoverA.clear()  # we flush A, everything was a CopyOmissionError
        self.largestAPrimaryKey = None

    def flushB(self):
        '''
        Helper function that flushes the remaining results from LeftOverB's row tracking information,
        into the CreationError pipeline. This is called when it is impossible for future data from
        A to match any of the information in LeftOverB.

        We do this to allow for future memory optimization
        if there were large amounts of errors and we needed to write them to a file.
        :return:
        '''
        for key in self.leftoverB:  # things leftover in A must be Creation Errors
            self.addCreationError(self.leftoverB[key])
        self.leftoverB.clear()  # we flush B, everything was a creation error.
        self.largestBPrimaryKey = None

    def prepareDataChunks(self, alist:List, blist:List):
        '''
        Loads chunks of information as lists of tuples into hashmaps for processing. Each tuple is a row
        in the corresponding database.

        Flushes any leftover rows from previous iterations into their corresponding error pipelines.

        Automatically calls the next step in the process: ProcessDataChunks.
        :param alist: data loaded from database A.
        :param blist: data loaded from database B.
        :return:
        '''
        # we can flush the leftover A data if the largest primary key in A is smaller than the smallest primary Key
        # being offered by the data coming from B.
        # We have a guarantee that no data remaining in that hashmap can match anything coming from B.
        self.totalTableSizeA += len(alist)
        self.totalTableSizeB += len(blist)

        if (self.largestAPrimaryKey):
            if (len(blist) > 0):
                if (blist[0][0] > self.largestAPrimaryKey):
                    self.flushA()

        # We likewise do the same for B.
        if (self.largestBPrimaryKey):
            if (len(alist) > 0):
                if (alist[0][0] > self.largestBPrimaryKey):
                    self.flushB()

        # setup the new largest primary keys that will exist in each of the hashmaps.
        if (len(alist) > 0):
            self.largestAPrimaryKey = alist[len(alist) - 1][0]
        if (len(blist) > 0):
            self.largestBPrimaryKey = blist[len(blist) - 1][0]

        # we now add the data into their respective hashmaps.
        for row in alist:
            # we use the primary key as the map key
            self.leftoverA[row[0]] = row
        for row in blist:
            self.leftoverB[row[0]] = row

        self.processDataChunks(self.leftoverA, self.leftoverB)

    def processDataChunks(self, aData:dict, bData:dict):
        '''
        Identifies any corruption errors within the corresponding data chunks via a hashmap lookup
        then individual column comparison.
        :param aData: a dictionary of row data from database A.
        :param bData: a dictionary of row data from database B.
        :return:
        '''
        # we expect aData and bData to generally be the same size, but at the end of the function.
        # we expect them to differ, therefore, we check each time.
        poplist = []
        for primaryKey in aData:
            if (primaryKey in bData):
                # if there is a match, check data, then pop
                if (not self.compareData(aData[primaryKey], bData[primaryKey])):
                    # the primary keys do not match, we can increment corruption errors and add to the report.
                    self.addCorruptionError(aData[primaryKey], bData[primaryKey])
                    # if the primary key is not in bData, we can just leave it in the hashmap to see if it will match a
                    # future primary key.
                # whether or not the internal data matches, we can pop it from our leftover hashmaps.
                poplist.append(primaryKey)

        # remove the matches
        for primaryKey in poplist:
            aData.pop(primaryKey)
            bData.pop(primaryKey)


    def finish(self):
        '''
        Flushes any remaining mismatches in the LeftOver hashmaps into their respective error counters.
        :return:
        '''
        self.flushA()
        self.flushB()

        # print("Corruption Error: %d" % self.numCorruptionErrors)
        # print("Creation Error: %d" % self.numCreationErrors)
        # print("Copy Omission Error: %d" % self.numCopyOmissionErrors)

    def produceReports(self):
        '''
        Generate reports according to the gathered error data within the data comparator object.
        :return: 
        '''

        if (not 'reports' in os.listdir('./')):
            os.makedirs('../reports/')
        reportDir = "../reports/"
        with open(reportDir + "migration-report.txt", 'w') as f:
            f.write('*' * 80 + '\n')
            f.write("Migration Report\n")
            f.write('*' * 80 + '\n\n')

            f.write("Total Rows Pre-migration:\t%d\n" % self.totalTableSizeA)
            f.write("Total Rows Post-migration:\t%d\n\n" % self.totalTableSizeB)

            f.write("Total Migration Errors:" + '\t'*7 + "%d" % (self.numCorruptionErrors +
                                                                 self.numCreationErrors +
                                                                 self.numCopyOmissionErrors))
            f.write("\t%2.2f" % ((self.numCorruptionErrors + self.numCreationErrors + self.numCopyOmissionErrors) *
                                 100.0 / self.totalTableSizeA ) + '%\n')

            f.write("\tTotal Rows Corrupted During Migration: \t\t%d\n" % self.numCorruptionErrors)
            f.write("\tTotal Omitted Rows From Original: \t\t\t%d\n" % self.numCopyOmissionErrors)
            f.write("\tTotal False Rows Created in Migrated: \t\t%d\n" % self.numCreationErrors)

        with open(reportDir + "corruption-errors.csv", 'w') as f:
            csvwriter = csv.writer(f, delimiter=',')
            for corruptedSet in self.corruptionErrors:
                csvwriter.writerow(corruptedSet[0])
                csvwriter.writerow(corruptedSet[1])

        with open(reportDir + "creation-errors.csv", 'w') as f:
            csvwriter = csv.writer(f, delimiter=',')
            for creationError in self.creationErrors:
                csvwriter.writerow(creationError)

        with open(reportDir + "omission-errors.csv", 'w') as f:
            csvwriter = csv.writer(f, delimiter=',')
            for omissionError in self.copyOmissionErrors:
                csvwriter.writerow(omissionError)












