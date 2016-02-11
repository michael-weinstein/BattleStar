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
        parser.add_argument("-l", "--fileList", help = "Pass a pickle containing a list of files to operate on.")
        parser.add_argument("-t", "--tempdir", help = "Holds the name of the temporary directory we are using.")
        parser.add_argument("-v", "--verbose", help = "Run in verbose mode (indicate progress, etc.)", action = 'store_true')
        rawArgs = parser.parse_args()
        if rawArgs.tempdir:
            if os.path.isdir:
                self.tempdir = rawArgs.tempdir
            else:
                raise RuntimeError("Temporary directory not found: " + rawArgs.tempdir)
        else:
            raise RuntimeError("No temporary directory specified.  This is not designed to run without one.")
        self.verbose = rawArgs.verbose
        if rawArgs.fileList:
            fileList = rawArgs.fileList.split(",")
            for fileName in fileList:
                if not os.path.isfile(fileName):
                    raise RuntimeError("File list item not found: " + fileName)
            self.fileList = fileList
        else:
            raise RuntimeError("This program needs a file list to run.  None was given.")
        
def addLociFromFile(file):  #locusList should come in as a global variable.  Doing this to try and be kind to memory.
    import pickle
    dataFile = open(file, 'rb')
    inputList = pickle.load(dataFile)
    dataFile.close()
    for line in inputList:
        contig, position, group = line  #assuming it comes in with this form, which it should.  The group should just be the megabase number of the locus and should have been calculated by the parallel processes
        try:
            locusList[contig][group][position] += 1
        except KeyError:
            try:
                locusList[contig][group][position] = 1
            except KeyError:
                try:
                    locusList[contig][group] = {}
                    locusList[contig][group][position] = 1
                except KeyError:
                    locusList[contig] = {}
                    locusList[contig][group] = {}
                    locusList[contig][group][position] = 1

def filteredLocusTree(minimumObservationCount):  #again, assuming locus list comes in as a global variable
    filteredLocusList = {}
    for contig in list(locusList.keys()):
        for group in list(locusList[contig].keys()):
            for position in list(locusList[contig][group].keys()):
                if locusList[contig][group][position] >= minimumObservationCount:
                    try:
                        filteredLocusList[contig][group][position] = True
                    except KeyError:
                        try:
                            filteredLocusList[contig][group] = {}
                            filteredLocusList[contig][group][position] = True
                        except KeyError:
                            filteredLocusList[contig] = {}
                            filteredLocusList[contig][group] = {}
                            filteredLocusList[contig][group][position] = True
    return filteredLocusList

def getListOfFiles():
    import os
    locusFileDirectory = args.tempdir + os.sep + "loci" + os.sep
    rawLocusFiles = os.listdir(locusFileDirectory)
    locusFiles = []
    for file in rawLocusFiles:
        if file.endswith(".loci.pkl"):
            locusFiles.append(locusFileDirectory + file)
    return locusFiles

def main():
    import pickle
    import datetime
    import os
    start = datetime.datetime.now()
    global args
    args = CheckArgs()
    global locusList
    locusList = {}
    progress = 0
    for file in args.fileList:
        if args.verbose:
            print("Processed " + str(progress) + " of " + str(len(args.fileList)) + " files.     ", end = "\r")
            progress += 1
        addLociFromFile(file)
    if args.verbose:
        print("Processed all individual sample files.                                   ")
    gatheredLociFileName = args.tempdir + os.sep + "lociGather" + os.sep + args.fileList[0].split(os.sep)[-1] + ".andFriends.scatter.pkl"
    gatheredLociFile = open(gatheredLociFileName, 'wb')
    pickle.dump(locusList, gatheredLociFile)
    gatheredLociFile.close()
    if args.verbose:
        runtime = datetime.datetime.now() - start
        print("Per-sample locus representation count(s) completed in " + str(runtime))
        
main()
    
    
        