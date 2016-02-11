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
        parser.add_argument("-t", "--tempdir", help = "Holds the name of the temporary directory we are using.")
        parser.add_argument("-v", "--verbose", help = "Run in verbose mode (indicate progress, etc.)", action = 'store_true')
        parser.add_argument("-o", "--outputFile", help = "Specify the output file name.", default = "output.txt")
        parser.add_argument("-k", "--pickleOut", help = "Output to a Pandas pickle instead of a delimited text file.", action = 'store_true')
        rawArgs = parser.parse_args()
        if rawArgs.tempdir:
            if os.path.isdir:
                self.tempdir = rawArgs.tempdir
            else:
                raise RuntimeError("Temporary directory not found: " + rawArgs.tempdir)
        else:
            raise RuntimeError("No temporary directory specified.  This is not designed to run without one.")
        self.verbose = rawArgs.verbose
        self.outputFile = rawArgs.outputFile  #this should already be sanitized from battlestar
        self.pickleOut = rawArgs.pickleOut

def getListOfFiles():
    import os
    sampleFileDirectory = args.tempdir + os.sep + "filter2" + os.sep
    rawSampleFiles = os.listdir(sampleFileDirectory)
    sampleFiles = []
    for file in rawSampleFiles:
        if file.endswith(".data.pkl"):
            sampleFiles.append(sampleFileDirectory + os.sep + file)
    return sampleFiles
        
def makeScatterCombiningList(fileList):
    scatterJobs = len(fileList)//5
    scatterFileList = []
    for i in range(0,scatterJobs):
        scatterFileList.append([])
    for i in range(0,len(fileList)):
        scatterFileList[i % scatterJobs].append(fileList[i])
    return scatterFileList

def runScatterJobs(scatterFileList):
    import os
    if not os.path.isdir("schedulerOutput"):
        os.mkdir("schedulerOutput")
    wrapperRunnerName = args.tempdir + os.sep + "bashFiles" + os.sep + "wrapper.sh"
    wrapperRunner = open(wrapperRunnerName, 'w')
    wrapperRunner.write("#!/bin/bash\n")
    wrapperRunner.write(pythonInterpreterAbsolutePath + " raptor.py " + "--tempdir " + args.tempdir + " --clockOutDir " + args.tempdir + os.sep + "finalPartsClockOut")
    wrapperRunner.close()
    clockOutFlush(args.tempdir + os.sep + "finalPartsClockOut")
    for i in range(0,len(scatterFileList)):
        arguments = {"--fileList" : ",".join(scatterFileList[i]),
                     "--tempdir" : args.tempdir}
        argumentList = []
        for key in list(arguments.keys()):
            if arguments[key]:
                argumentList.append(key + " " + arguments[key])
        argumentString = " ".join(argumentList)
        if i == 0:
            if args.verbose:
                argumentString += " -v"
            thisNodesJob = pythonInterpreterAbsolutePath + " baseStar.py " + argumentString
        else:
            bashFileName = args.tempdir + os.sep + "bashFiles" + os.sep + str(i) + ".sh"
            bashFile = open(bashFileName, 'w')
            bashFile.write("#!/bin/bash\n")
            bashFile.write(pythonInterpreterAbsolutePath + " baseStar.py " + argumentString)
            bashFile.close()
    jobRange = "1-" + str(len(scatterFileList) - 1) + " "
    command = "qsub -cwd -V -N Toasters -l h_data=4G,time=23:59:00 -e " + os.getcwd() +  "/schedulerOutput/ -o " + os.getcwd() + "/schedulerOutput/ " + "-t " + jobRange + wrapperRunnerName
    if args.verbose:
        print("BASH " + command)
    os.system(command)
    if args.verbose:
        print("Starting the job on this node.")
        print("BASH " + thisNodesJob)
    os.system(thisNodesJob)
    if args.verbose:
        print("Monitoring other nodes counting jobs.")
    monitorJobs(args.tempdir + os.sep + "finalPartsClockOut", len(scatterFileList))
    
def monitorJobs(clockOutDirectory, jobCount):
    import os
    import time
    if args.verbose:
        print("Monitoring jobs on other nodes.")
    done = False
    completed = 1
    jobList = []
    for i in range(1,jobCount):
        jobList.append([i,False])
    while not done:
        if args.verbose:
            print("Completed " + str(completed) + " of " + str(jobCount) + " jobs.       ", end = "\r")
        for i in range(0,len(jobList)):
            if not jobList[i][1]:
                if os.path.isfile(clockOutDirectory + os.sep + str(jobList[i][0])):
                    jobList[i][1] = True
                    completed += 1
                    if completed == len(jobList) + 1:
                        done = True
                        print()
                        break
        time.sleep(1)
    if args.verbose:
        print("Completed " + str(completed) + " of " + str(jobCount) + " jobs.       ", end = "\r")
        
def gatherFiles():
    import os
    if args.verbose:
        print("Collecting job data from other nodes.")
    scatterFileDirectory = args.tempdir + os.sep + "finalParts"
    rawFiles = os.listdir(scatterFileDirectory)
    filteredFiles = []
    for file in rawFiles:
        if file.endswith(".scatter.pkl"):
            filteredFiles.append(scatterFileDirectory + os.sep + file)
    if args.verbose:
        print("Found " + str(len(filteredFiles)) + " sets of results from other jobs.")
    return filteredFiles

def combineGatheredData(fileList):  #assumes that locusList will come in as a global to avoid moving around a big dataset too much
    import os
    import pickle
    progress = 0
    for fileName in fileList:
        print("Processed " + str(progress) + " of " + str(len(fileList)) + " data files.       ", end = "\r" )
        if not os.path.isfile(fileName):
            raise RuntimeError("Expected to find a data file, but it was not found. " + fileName)
        file = open(fileName, 'rb')
        data = pickle.load(file)
        file.close()
        for key in list(data.keys()):
            try:
                throwAway = fullDataSet[key]
            except KeyError:
                pass
            else:
                raise RuntimeError("Sample name collision.  Two samples being entered with name " + key)
            fullDataSet[key] = data[key]
        progress += 1
    if args.verbose:
        print("All files processed.                                                        ")
    
def clockOutFlush(clockOutDir):
    import os
    import shutil
    shutil.rmtree(clockOutDir)
    os.mkdir(clockOutDir)
    
def addDataFromFile(file):
    import os
    import pickle
    if not os.path.isfile(file):
        raise RuntimeError("Tried to open data file, but it does not exist. " + file)
    inputFile = open(file,'rb')
    data = pickle.load(inputFile)
    inputFile.close()
    for key in list(data.keys()):
        fullDataSet[key] = data[key]

def getAcceptedCoordiateList():
    import os
    import pickle
    coordinateListFileName = args.tempdir + os.sep + 'loci' + os.sep + 'acceptedCoordinateList.pkl'
    coordinateListFile = open(coordinateListFileName,'rb')
    coordinateList = pickle.load(coordinateListFile)
    coordinateListFile.close()
    return coordinateList

def main():
    import pickle
    import datetime
    import os
    import pandas
    start = datetime.datetime.now()
    global args
    args = CheckArgs()
    global fullDataSet
    fullDataSet = {}
    fileList = getListOfFiles()
    acceptedcoordinateList = getAcceptedCoordiateList()
    if len(fileList) < 20:
        progress = 0
        for file in fileList:
            if args.verbose:
                print("Processed " + str(progress) + " of " + str(len(fileList)) + " files.     ", end = "\r")
                progress += 1
            addLociFromFile(file)
    else:
        scatterCombiningList = makeScatterCombiningList(fileList)
        runScatterJobs(scatterCombiningList)
        jobFileList = gatherFiles()
        combineGatheredData(jobFileList)
    if args.verbose:
        print("Collected all data.  Forming table.")
    fullDataSet = pandas.DataFrame(fullDataSet, index = acceptedcoordinateList)
    if args.verbose:
        print("Sample data:")
        print(fullDataSet)
    if args.verbose:
        if args.pickleOut:
            print("Saving Pandas pickle of all data. This may take several minutes.")
        else:
            print("Saving table of all data. This may take several minutes.")
    startWrite = datetime.datetime.now()
    if args.pickleOut:
        fullDataSet.to_pickle(args.outputFile)
    else:
        fullDataSet.to_csv(args.outputFile, sep = "\t")
    if args.verbose:
        writeTime = datetime.datetime.now() - startWrite
        print("Table saved in " + str(writeTime) + ".")
    acceptedLociFileName = args.tempdir + os.sep + "loci" + os.sep + "acceptedLoci.pkl"
    if args.verbose:
        runtime = datetime.datetime.now() - start
        print("Full dataset build completed in " + str(runtime))
        
main()
    
    
        