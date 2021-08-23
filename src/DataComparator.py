import queue
from typing import List

class DataComparator:

    def __init__(self):
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
        self.copyOmissionErrors.append(data)
        self.numCopyOmissionErrors += 1

    def addCreationError(self, data):
        self.creationErrors.append(data)
        self.numCreationErrors += 1

    def addCorruptionError(self, a, b): # gets its own interface because its multiple data sources.
        self.corruptionErrors.append([a, b])
        self.numCorruptionErrors += 1

    def prepareDataChunks(self, alist:List, blist:List):
        # we can flush the leftover A data if the largest primary key in A is smaller than the smallest primary Key
        # being offered by the data coming from B.
        # We have a guarantee that no data remaining in that hashmap can match anything coming from B.
        if (self.largestAPrimaryKey):
            if (blist[0][0] > self.largestAPrimaryKey):
                for key in self.leftoverA: # things leftover in A must be CopyOmissionErrors
                    self.addCopyOmissionError(self.leftoverA[key])
                self.leftoverA.clear() # we flush A, everything was a CopyOmissionError
                self.largestAPrimaryKey = None

        # We likewise do the same for B.
        if (self.largestBPrimaryKey):
            if (alist[0][0] > self.largestBPrimaryKey):
                for key in self.leftoverB: # things leftover in A must be Creation Errors
                    self.addCreationError(self.leftoverB[key])
                self.leftoverB.clear() # we flush A, everything was a CopyOmissionError
                self.largestBPrimaryKey = None

        # setup the new largest primary keys that will exist in each of the hashmaps.
        self.largestAPrimaryKey = alist[len(alist) - 1][0]
        self.largestBPrimaryKey = blist[len(blist) - 1][0]

        # we now add the data into their respective hashmaps.
        for row in alist:
            # we use the primary key as the map key
            self.leftoverA[row[0]] = row
        for row in blist:
            self.leftoverB[row[0]] = row



    def processDataChunks(self, aData:dict, bData:dict):
        # we expect aData and bData to generally be the same size, but at the end of the function
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








