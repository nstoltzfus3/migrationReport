from DataComparator import DataComparator
import unittest
import copy

import random

class TestDataComparator(unittest.TestCase):


    def setUp(self):
        self.DataComparator = DataComparator()

    def computeSingleChunk(self, alist, blist):
        self.DataComparator.prepareDataChunks(alist, blist)
        self.DataComparator.finish()

    def computeChunk(self, alist, blist):
        self.DataComparator.prepareDataChunks(alist, blist)

    def assertErrorCount(self, numCorruptionErrors, numCopyOmissionErrors, numCreationErrors):
        self.assertEqual(numCorruptionErrors, self.DataComparator.numCorruptionErrors)
        self.assertEqual(numCopyOmissionErrors, self.DataComparator.numCopyOmissionErrors)
        self.assertEqual(numCreationErrors, self.DataComparator.numCreationErrors)

    ########################## Begin Single Chunk Verification Tests

    def testEmptyData(self):
        alist = []
        blist = []
        self.computeSingleChunk(alist, blist)
        self.assertErrorCount(0,0,0)

    def testOneOmissionError(self):
        alist = [(1, 2)]
        blist = []
        self.computeSingleChunk(alist, blist)
        self.assertErrorCount(0, 1, 0)

    def testOneCreationError(self):
        alist = []
        blist = [(1, 2)]
        self.computeSingleChunk(alist, blist)
        self.assertErrorCount(0, 0, 1)

    def testOneCorruptionError(self):
        alist = [(1, 1)]
        blist = [(1, 2)]
        self.computeSingleChunk(alist, blist)
        self.assertErrorCount(1,0,0)

    def testOneOmissionErrorWithAuto(self):
        alist = [(1, 2)]
        blist = self.setupBData(alist, 0, 1, 0)
        self.computeSingleChunk(alist, blist)
        self.assertErrorCount(0, 1, 0)

    def testOneCreationErrorWithAuto(self):
        alist = []
        blist = self.setupBData(alist, 0, 0, 1)
        self.computeSingleChunk(alist, blist)
        self.assertErrorCount(0, 0, 1)

    def testOneCorruptionErrorWithAuto(self):
        alist = [(1, 1)]
        blist = self.setupBData(alist, 1, 0, 0)
        self.computeSingleChunk(alist, blist)
        self.assertErrorCount(1,0,0)

    def testOneCorruptionErrorLargeDb(self):
        alist = self.setupAData()
        blist = self.setupBData(alist, 1, 0, 0)
        self.computeSingleChunk(alist, blist)
        self.assertErrorCount(1, 0, 0)

    def testOneOmissionErrorLargeDb(self):
        alist = self.setupAData()
        blist = self.setupBData(alist, 0, 1, 0)
        self.computeSingleChunk(alist, blist)
        self.assertErrorCount(0, 1, 0)

    def testOneCreationErrorLargeDb(self):
        alist = self.setupAData()
        blist = self.setupBData(alist, 0, 0, 1)
        self.computeSingleChunk(alist, blist)
        self.assertErrorCount(0, 0, 1)

    def test100CorruptionError(self):
        alist = self.setupAData()
        blist = self.setupBData(alist, 100, 0, 0)
        self.computeSingleChunk(alist, blist)
        self.assertErrorCount(100, 0, 0)

    def test100OmissionErrors(self):
        alist = self.setupAData()
        blist = self.setupBData(alist, 0, 100, 0)
        self.computeSingleChunk(alist, blist)
        self.assertErrorCount(0, 100, 0)

    def test100CreationErrors(self):
        alist = self.setupAData()
        blist = self.setupBData(alist, 0, 0, 100)
        self.computeSingleChunk(alist, blist)
        self.assertErrorCount(0, 0, 100)

    def testAllCorruptionErrors(self):
        alist = self.setupAData()
        blist = self.setupBData(alist, len(alist) - 1, 0, 0)
        self.computeSingleChunk(alist, blist)
        self.assertErrorCount(len(alist) - 1, 0, 0)

    def testAllOmissionErrors(self):
        alist = self.setupAData()
        blist = self.setupBData(alist, 0, len(alist) - 1, 0)
        self.computeSingleChunk(alist, blist)
        self.assertErrorCount(0, len(alist) - 1, 0)

    def testAllCreationErrors(self):
        alist = self.setupAData()
        blist = self.setupBData(alist, 0, 0, len(alist))
        self.assertEqual(2 * len(alist), len(blist))
        self.computeSingleChunk(alist, blist)
        self.assertErrorCount(0, 0, len(alist))

    def testCorruptionOmissionUnit(self):
        alist = [(1, 2), (2, 3)]
        blist = self.setupBData(alist, 1, 1, 0)
        self.computeSingleChunk(alist, blist)
        self.assertErrorCount(1, 1, 0)

    def testOmissionCreationUnit(self):
        alist = [(1, 2), (2, 3)]
        blist = self.setupBData(alist, 0, 1, 1)
        self.computeSingleChunk(alist, blist)
        self.assertErrorCount(0, 1, 1)

    def testCorruptionCreationUnit(self):
        alist = [(1, 2), (2, 3)]
        blist = self.setupBData(alist, 1, 0, 1)
        self.computeSingleChunk(alist, blist)
        self.assertErrorCount(1, 0, 1)

    def testCorruptionOmission50(self):
        alist = self.setupAData(200)
        blist = self.setupBData(alist, 50, 50, 0)
        self.computeSingleChunk(alist, blist)
        self.assertErrorCount(50, 50, 0)

    def testOmissionCreation50(self):
        alist = self.setupAData()
        blist = self.setupBData(alist, 0, 50, 50)
        self.computeSingleChunk(alist, blist)
        self.assertErrorCount(0, 50, 50)

    def testCorruptionCreation50(self):
        alist = self.setupAData()
        blist = self.setupBData(alist, 50, 0, 50)
        self.computeSingleChunk(alist, blist)
        self.assertErrorCount(50, 0, 50)

    def testAllErrors100(self):
        alist = self.setupAData()
        blist = self.setupBData(alist, 100, 100, 100)
        self.computeSingleChunk(alist, blist)
        self.assertErrorCount(100, 100, 100)

    def IndividualHeavy(self):
        for i in range(100):
            self.DataComparator = DataComparator()
            alist = self.setupAData()
            blist = self.setupBData(alist, len(alist) - 1, 0, 0)
            self.computeSingleChunk(alist, blist)
            self.assertErrorCount(len(alist) - 1, 0, 0)

        for i in range(100):
            self.DataComparator = DataComparator()
            alist = self.setupAData()
            blist = self.setupBData(alist, 0, len(alist) - 1, 0)
            self.computeSingleChunk(alist, blist)
            self.assertErrorCount(0, len(alist) - 1, 0)

        for i in range(100):
            self.DataComparator = DataComparator()
            alist = self.setupAData()
            blist = self.setupBData(alist, 0, 0, len(alist) - 1)
            self.assertEqual(2 * len(alist) - 1, len(blist))
            self.computeSingleChunk(alist, blist)
            self.assertErrorCount(0, 0, len(alist) - 1)

    ################################# End Single Chunk Verification Tests

    ################################# Begin Multi Chunk Verification Tests
    """
    These test the ability of the data comparator to take in multiple consecutive chunks of data at a time
    and only evaluate the errors that could arise, while discarding all information that cannot
    have errors.
    """
    def testNoErrorsMulti(self):
        alist = []
        blist = []
        self.computeChunk(alist, blist)
        alist = [(1, 1)]
        blist = [(1, 1)]
        self.computeChunk(alist, blist)
        self.DataComparator.finish()
        self.assertErrorCount(0,0,0)

    def testOneOmissionErrorMulti(self):
        alist = [(1, 2)]
        blist = []
        self.computeChunk(alist, blist)
        alist = []
        blist = []
        self.computeChunk(alist, blist)
        self.DataComparator.finish()
        self.assertErrorCount(0, 1, 0)

    def testOneCreationErrorMulti(self):
        alist = []
        blist = [(1, 2)]
        self.computeChunk(alist, blist)
        alist = []
        blist = []
        self.computeChunk(alist, blist)
        self.DataComparator.finish()
        self.assertErrorCount(0, 0, 1)

    def testOneCorruptionErrorMulti(self):
        alist = [(1, 1)]
        blist = [(1, 2)]
        self.computeChunk(alist, blist)
        alist = []
        blist = []
        self.computeChunk(alist, blist)
        self.DataComparator.finish()
        self.assertErrorCount(1,0,0)

    def testOneCorruptionErrorMulti(self):
        alist = [(1, 1)]
        blist = [(1, 2)]
        self.computeChunk(alist, blist)
        alist = [(2, 2)]
        blist = [(2, 2)]
        self.computeChunk(alist, blist)
        self.DataComparator.finish()
        self.assertErrorCount(1,0,0)


    def setupAData(self, n=None):
        aPrimaryKeys = []

        count = 0
        if not n:
            for i in range(random.randint(1000, 10000)): # database length is somewhere between 1 and 1 million.
                aPrimaryKeys.append(count) # we still have unique and sorted database Primary Keys however.
                count += 1
        else:
            for i in range(n): # database length is somewhere between 1 and 1 million.
                aPrimaryKeys.append(count) # we still have unique and sorted database Primary Keys however.
                count += 1

        aData = []
        rowWidth = random.randint(2, 20)
        for pk in aPrimaryKeys:
            row = self.generateRandomRow(pk, rowWidth)
            aData.append(row)

        return aData

    def setupBData(self, aData, corruptionErrors, copyOmissionErrors, creationErrors):
        # avoid any referencing issues
        bData = copy.deepcopy(aData)

        # get a sampling of indexes to introduce errors into.
        if (corruptionErrors + copyOmissionErrors + creationErrors < len(bData)):
            samples = random.sample(range(len(bData)), corruptionErrors + copyOmissionErrors + creationErrors)
        else:
            samples = range(corruptionErrors + copyOmissionErrors + creationErrors)

        # the samples are randomized so we can just go in order.
        # we sort the samples because its tedious to avoid deleting corruptions.
        # The datacomparator works with a hashmap anyway so ordering shouldnt matter.
        samples = sorted(samples)

        # Corruption Errors
        for createError in range(corruptionErrors):
            bData[samples[createError]] = self.getCorruptionError(bData, samples[createError])

        # Omission Errors
        start = corruptionErrors
        end = copyOmissionErrors + corruptionErrors
        for omissError in range(start, end):
            if (samples[omissError] < len(bData)):
                del bData[samples[omissError]]
            else:
                bData.pop()

        # Creation Errors
        start = end
        end = end + creationErrors
        # this number is used to ensure we still have ordered pks in a convenient way.
        delta = 0.0000001
        offset = delta
        for createErrorInd in sorted(samples[start:]):
            if len(bData) > createErrorInd:
                bData.insert(createErrorInd + 1, self.generateRandomRow(bData[createErrorInd][0] + offset, len(bData[0])))
            else:
                # if our random insert location was later (or too many samples got removed) then we can
                # just append our created element.
                if (len(bData) > 0):
                    bData.append(self.generateRandomRow(bData[len(bData) - 1][0] + offset, len(bData[0])))
                else:
                    bData.append(self.generateRandomRow(len(aData) + 1, len(aData[0]) if len(aData) > 0 else 0))
            offset += delta

        return bData

    def generateRandomRow(self, pk, rowWidth):
        row = [pk]
        for i in range(rowWidth - 1):
            # simulates creating data in the row
            row.append(random.randint(1, 1000000))
        row = tuple(row)
        return row

    def getCorruptionError(self, aData, index):
        corruptedRow = list([aData[index][0]]) # gets correct pk from old data.
        for column in aData[index][1:]:
            corruptedRow.append(column + 1)
        return corruptedRow











if __name__ == "__main__":
    unittest.main()