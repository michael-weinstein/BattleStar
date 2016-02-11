#!/usr/bin/env python3

'''
BattleStar written by Michael Weinstein, 2016
University of California, Los Angeles, Daniel Cohn laboratory and Collaboratory
email: [myfirstname].[mylastname] AT ucla.edu
'''

class CheckArgs():  #class that checks arguments and ultimately returns a validated set of arguments to the main program
    
    def __init__(self):
        import argparse
        import os
        parser = argparse.ArgumentParser()
        parser.add_argument("-f", "--fileList", help = "List of files to process, separated by a comma.")
        parser.add_argument("-t", "--tempdir", help = "Holds the name of the temporary directory we are using.")
        parser.add_argument("-v", "--verbose", help = "Run in verbose mode (indicate progress, etc.)", action = 'store_true')
        parser.add_argument("-m", "--emptyCellMarker", help = "Marker for blank cells.", default = "")

        rawArgs = parser.parse_args()
        if rawArgs.tempdir:
            if os.path.isdir:
                self.tempdir = rawArgs.tempdir
            else:
                raise RuntimeError("Temporary directory not found: " + rawArgs.tempdir)
        else:
            raise RuntimeError("No temporary directory specified.  This is not designed to run without one.")
        if not rawArgs.fileList:
            raise RuntimeError("A file list must be specified")
        fileList = rawArgs.fileList
        fileList = fileList.split(",")
        for file in fileList:
            if not os.path.isfile(file):
                raise RuntimeError("Filtered data file not found: " + file)
        self.fileList = fileList
        self.verbose = rawArgs.verbose
        self.emptyCellMarker = rawArgs.emptyCellMarker
        
def filterFile(fileName, acceptedLocusTree, acceptedCoordinateIndex):
    import pickle
    file = open(fileName, 'rb')
    rawData = pickle.load(file)
    file.close()
    filteredData = [args.emptyCellMarker] * len(acceptedCoordinateIndex.keys())
    progress = 0
    accepted = 0
    for line in rawData:
        contig, position, ratio = line
        group = position // 1000000
        try:
            acceptedLocus = acceptedLocusTree[contig][group][position]
        except KeyError:
            acceptedLocus = False
        if acceptedLocus:
            accepted += 1
            coordinates = contig + ":" + str(position)
            filteredData[acceptedCoordinateIndex[coordinates]] = ratio
        progress += 1
        if args.verbose:
            if progress % 10000 == 0:
                print(str(accepted) + " of " + str(progress) + " loci in this sample were adequately represented in other samples.                  ", end = "\r")
    if args.verbose:
        print(str(accepted) + " of " + str(progress) + " loci in this sample were adequately represented in other samples.                  ")
    return filteredData

def getAcceptedLocusTree():
    import os
    import pickle
    fileName = args.tempdir + os.sep + "loci" + os.sep + "acceptedLoci.pkl"
    if not os.path.isfile(fileName):
        raise RuntimeError("Unable to find accepted locus tree file: " + fileName)
    file = open(fileName, 'rb')
    acceptedLocusTree = pickle.load(file)
    file.close()
    return acceptedLocusTree

def getAcceptedCoordiateList():
    import os
    import pickle
    coordinateListFileName = args.tempdir + os.sep + 'loci' + os.sep + 'acceptedCoordinateList.pkl'
    coordinateListFile = open(coordinateListFileName,'rb')
    coordinateList = pickle.load(coordinateListFile)
    coordinateListFile.close()
    return coordinateList

def getAcceptedCoordinateIndex():
    import os
    import pickle
    coordinateIndexFileName = args.tempdir + os.sep + 'loci' + os.sep + 'acceptedCoordinateIndex.pkl'
    coordinateIndexFile = open(coordinateIndexFileName,'rb')
    coordinateIndex = pickle.load(coordinateIndexFile)
    return coordinateIndex

def main():
    import pickle
    import datetime
    import os
    start = datetime.datetime.now()
    global args
    args = CheckArgs()
    acceptedLocusTree = getAcceptedLocusTree()
    acceptedCoordinateList = getAcceptedCoordiateList()
    acceptedCoordinateIndex = getAcceptedCoordinateIndex()
    progress = 1
    filteredData = {}
    for file in args.fileList:
        if args.verbose and len(args.fileList) > 1:
            print("Processing file " + str(progress) + " of " + str(len(args.fileList)) + ".     ")
            progress += 1
        filteredData[file.split(os.sep)[-1].split(".")[0]] = filterFile(file, acceptedLocusTree, acceptedCoordinateIndex)
    filteredFileName = args.tempdir + os.sep + "filter2" + os.sep + file.split(os.sep)[-1]
    filteredFile = open(filteredFileName, 'wb')
    pickle.dump(filteredData, filteredFile)
    filteredFile.close()
    if args.verbose:
        runtime = datetime.datetime.now() - start
        print("Locus representation filtering completed in " + str(runtime))
        
main()
        
        
    