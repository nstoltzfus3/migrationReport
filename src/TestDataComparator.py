from DataComparator import DataComparator
import unittest
import copy

import random

class TestDataComparator(unittest.TestCase):


    def setUp(self):
        self.DataComparator = DataComparator()

    def compute(self, alist, blist):
        self.DataComparator.prepareDataChunks(alist, blist)
        self.DataComparator.finish()

    def assertErrorCount(self, numCorruptionErrors, numCopyOmissionErrors, numCreationErrors):
        self.assertEqual(self.DataComparator.numCopyOmissionErrors, numCopyOmissionErrors)
        self.assertEqual(self.DataComparator.numCreationErrors, numCreationErrors)
        self.assertEqual(self.DataComparator.numCorruptionErrors, numCorruptionErrors)

    def testEmptyData(self):
        alist = []
        blist = []
        self.compute(alist, blist)
        self.assertErrorCount(0,0,0)

    def testOneOmissionError(self):
        alist = [(1, 2)]
        blist = []
        self.compute(alist, blist)

        self.assertErrorCount(0, 1, 0)

    def testOneCreationError(self):
        alist = []
        blist = [(1, 2)]
        self.compute(alist, blist)

        self.assertErrorCount(0, 0, 1)

    def testOneCorruptionError(self):
        alist = [(1, 1)]
        blist = [(1, 2)]
        self.compute(alist, blist)
        self.assertErrorCount(1,0,0)

    def test100CorruptionError(self):
        alist = self.setupAData()
        blist = self.setupBData(alist, 100, 0, 0)
        self.compute(alist, blist)
        self.assertErrorCount(0, 0, 100)

    def test100OmissionErrors(self):
        alist = self.setupAData()
        blist = self.setupBData(alist, 0, 100, 0)
        self.compute(alist, blist)
        self.assertErrorCount(0, 100, 0)

    def test100OmissionErrors(self):
        alist = self.setupAData()
        blist = self.setupBData(alist, 0, 100, 0)
        self.compute(alist, blist)
        self.assertErrorCount(100, 0, 0)

    def test100CreationErrors(self):
        alist = self.setupAData()
        blist = self.setupBData(alist, 0, 0, 100)
        self.compute(alist, blist)
        self.assertErrorCount(0, 100, 0)

    def setupAData(self):
        aPrimaryKeys = []

        count = 0
        for i in range(random.randint(1000, 10000)): # database length is somewhere between 1 and 1 million.
            aPrimaryKeys.append(count) # we still have unique and sorted database Primary Keys however.
            count += 1

        aData = []
        rowWidth = random.randint(2,20)
        for pk in aPrimaryKeys:
            row = self.generateRandomRow(pk, rowWidth)
            aData.append(row)

        return aData

    def setupBData(self, aData, corruptionErrors, copyOmissionErrors, creationErrors):
        # avoid any referencing issues
        bData = copy.deepcopy(aData)

        # get a sampling of indexes to introduce errors into.
        samples = random.sample(range(len(bData)), corruptionErrors + copyOmissionErrors + creationErrors)

        # the samples are randomized so we can just go in order.
        for createError in range(corruptionErrors):
            bData[samples[createError]] = self.getCorruptionError(bData, samples[createError])

        start = corruptionErrors
        end = copyOmissionErrors + corruptionErrors
        for omissError in range(start, end):
            del bData[samples[omissError]]

        start = end
        end = end + creationErrors
        # this number is used to ensure we still have ordered pks in a convenient way.
        delta = 0.00000000001
        for createError in range(start, end):
            bData.insert(createError, (bData[createError][0]+delta, bData[createError][1:]))




        return bData

    def generateRandomRow(self, pk, rowWidth):
        row = [pk]
        for i in range(rowWidth):
            # simulates creating data in the row
            row.append(random.randint(1, 1000000))
        row = tuple(row)
        return row

    def getCorruptionError(self, aData, index):
        corruptedRow = list([aData[index][0]]) # gets correct pk from old data.
        for column in aData[index]:
            corruptedRow.append(column + 1)
        return corruptedRow











if __name__ == "__main__":
    unittest.main()