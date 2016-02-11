#!/usr/bin/env python3

'''
BattleStar written by Michael Weinstein, 2016
University of California, Los Angeles, Daniel Cohn laboratory and Collaboratory
email: [myfirstname].[mylastname] AT ucla.edu
'''

global pythonInterpreterAbsolutePath
pythonInterpreterAbsolutePath = "/u/local/apps/python/3.4.3/bin/python3"

class CheckArgs():  #class that checks arguments and ultimately returns a validated set of arguments to the main program
    
    def __init__(self):
        import argparse
        import os
        parser = argparse.ArgumentParser()
        parser.add_argument("-t", "--tempdir", help = "Force the program to try using a temporary directory.  The directory should not already exist.")
        parser.add_argument("-v", "--verbose", help = "Run in verbose mode (indicate progress, etc.)", action = 'store_true')
        parser.add_argument("-f", "--fileList", help = "List of files to operate on, separated by commas")
        rawArgs = parser.parse_args()
        if not rawArgs.tempdir:
            raise RuntimeError("A temporary directory must be passed as an argument.  None was found.")
        if rawArgs.tempdir:
            if not os.path.isdir:
                raise RuntimeError("Specified temporary directory was not found.  " + rawArgs.tempdir)
            self.tempdir = rawArgs.tempdir
        else:
            raise RuntimeError("A temporary directory argument must be passed, but none was seen.")
        self.verbose = rawArgs.verbose
        fileList = rawArgs.fileList
        if not fileList:
            raise RuntimeError("No file list given, nothing to operate on.")
        self.fileList = fileList.split(",")

def addDataFromFile(file):
    import os
    import pickle
    if not os.path.isfile(file):
        raise RuntimeError("Tried to open data file, but it does not exist. " + file)
    inputFile = open(file,'rb')
    #if args.verbose:
        #print("Loading pickle")
    data = pickle.load(inputFile)
    inputFile.close()
    #if args.verbose:
        #print("Loaded pickle.")
    for key in list(data.keys()):
        #if args.verbose:
        #    print("Key: " + key + ":" + str(len(data[key])))
        combinedData[key] = data[key]

def main():
    import datetime
    import os
    import pickle
    import random
    start = datetime.datetime.now()
    global args
    args = CheckArgs()
    global combinedData
    combinedData = {}
    progress = 0
    for file in args.fileList:
        if args.verbose:
            print("Added data from " + str(progress) + " of " + str(len(args.fileList)) + " files.      ", end = "\r")
        addDataFromFile(file)
        progress += 1
    if args.verbose:
        print("Added data from all " + str(len(args.fileList)) + " files.                                                   ")
    outputFileName = args.tempdir + os.sep + "finalParts" + os.sep + args.fileList[0].split(os.sep)[-1] +".andFriends.scatter.pkl"
    outputFile = open(outputFileName, 'wb')
    pickle.dump(combinedData, outputFile)
    outputFile.close()
    if args.verbose:
        runtime = datetime.datetime.now() - start
        print("Partial build process complete in " + str(runtime))
        
main()
    