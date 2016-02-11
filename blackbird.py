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
        parser.add_argument("-r", "--minRepresentation", help = "Minimum percent of the time a specific locus needs to have been reported in samples", default = 75, type = int)
        parser.add_argument("-t", "--tempdir", help = "Holds the name of the temporary directory we are using.")
        parser.add_argument("-v", "--verbose", help = "Run in verbose mode (indicate progress, etc.)", action = 'store_true')
        rawArgs = parser.parse_args()
        self.minRepresentationPercent = float(rawArgs.minRepresentation/100)
        if rawArgs.tempdir:
            if os.path.isdir:
                self.tempdir = rawArgs.tempdir
            else:
                raise RuntimeError("Temporary directory not found: " + rawArgs.tempdir)
        else:
            raise RuntimeError("No temporary directory specified.  This is not designed to run without one.")
        self.verbose = rawArgs.verbose

def getListOfFiles():
    import os
    locusFileDirectory = args.tempdir + os.sep + "loci" + os.sep
    rawLocusFiles = os.listdir(locusFileDirectory)
    locusFiles = []
    for file in rawLocusFiles:
        if file.endswith(".loci.pkl"):
            locusFiles.append(locusFileDirectory + file)
    return locusFiles
        
def makeScatterCountingList(fileList):
    scatterJobs = len(fileList)//10
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
    wrapperRunner.write(pythonInterpreterAbsolutePath + " raptor.py " + "--tempdir " + args.tempdir + " --clockOutDir " + args.tempdir + os.sep + "lociClockOut")
    wrapperRunner.close()
    clockOutFlush(args.tempdir + os.sep + "lociClockOut")
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
            thisNodesJob = pythonInterpreterAbsolutePath + " toaster.py " + argumentString
        else:
            bashFileName = args.tempdir + os.sep + "bashFiles" + os.sep + str(i) + ".sh"
            bashFile = open(bashFileName, 'w')
            bashFile.write("#!/bin/bash\n")
            bashFile.write(pythonInterpreterAbsolutePath + " toaster.py " + argumentString)
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
    monitorJobs(args.tempdir + os.sep + "lociClockOut", len(scatterFileList))
    
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
    scatterFileDirectory = args.tempdir + os.sep + "lociGather"
    rawFiles = os.listdir(scatterFileDirectory)
    filteredFiles = []
    for file in rawFiles:
        if file.endswith(".scatter.pkl"):
            filteredFiles.append(scatterFileDirectory + os.sep + file)
    if args.verbose:
        print("Found " + str(len(filteredFiles)) + " sets of results from other jobs.")
    return filteredFiles

def combineGatheredCounts(fileList):  #assumes that locusList will come in as a global to avoid moving around a big dataset too much
    import os
    import pickle
    progress = 0
    for fileName in fileList:
        print("Processed " + str(progress) + " of " + str(len(fileList)) + " data files.       ", end = "\r" )
        file = open(fileName, 'rb')
        scatteredData = pickle.load(file)
        file.close()
        for contig in list(scatteredData.keys()):
            for group in list(scatteredData[contig].keys()):
                for position in list(scatteredData[contig][group].keys()):
                    try:
                        locusList[contig][group][position] += scatteredData[contig][group][position]
                    except KeyError:
                        try:
                            locusList[contig][group][position] = scatteredData[contig][group][position]
                        except KeyError:
                            try:
                                locusList[contig][group][position] += scatteredData[contig][group][position]
                            except KeyError:
                                try:
                                    locusList[contig][group] = {}
                                    locusList[contig][group][position] = scatteredData[contig][group][position]
                                except KeyError:
                                    locusList[contig] = {}
                                    locusList[contig][group] = {}
                                    locusList[contig][group][position] = scatteredData[contig][group][position]
        progress += 1
    if args.verbose:
        print("All files processed.                                                        ")
    
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
    if args.verbose:
        print("Generating a list of loci represented in at least " + str(int(args.minRepresentationPercent*100)) + " percent of samples.")
    filteredLocusList = {}
    screenedLoci = 0
    acceptedLoci = 0
    for contig in list(locusList.keys()):
        for group in list(locusList[contig].keys()):
            for position in list(locusList[contig][group].keys()):
                screenedLoci += 1
                if locusList[contig][group][position] >= minimumObservationCount:
                    acceptedLoci += 1
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
                if args.verbose:
                    if screenedLoci % 10000 == 0:
                        print(str(acceptedLoci) + " of " + str(screenedLoci) + " loci have acceptable representation.                  ", end = "\r")
    if args.verbose:
        print(str(acceptedLoci) + " of " + str(screenedLoci) + " loci have acceptable representation.                  ")
    return filteredLocusList

def clockOutFlush(clockOutDir):
    import os
    import shutil
    shutil.rmtree(clockOutDir)
    os.mkdir(clockOutDir) 

def main():
    import pickle
    import datetime
    import os
    start = datetime.datetime.now()
    global args
    args = CheckArgs()
    global locusList
    locusList = {}
    fileList = getListOfFiles()
    if len(fileList) < 40:
        progress = 0
        for file in fileList:
            if args.verbose:
                print("Processed " + str(progress) + " of " + str(len(fileList)) + " files.     ", end = "\r")
                progress += 1
            addLociFromFile(file)
    else:
        scatterCountingList = makeScatterCountingList(fileList)
        runScatterJobs(scatterCountingList)
        jobFileList = gatherFiles()
        combineGatheredCounts(jobFileList)
    acceptedLocusTree = filteredLocusTree(len(fileList) * args.minRepresentationPercent)
    del locusList
    if args.verbose:
        print("Saving list of accepted loci.")
    acceptedLociFileName = args.tempdir + os.sep + "loci" + os.sep + "acceptedLoci.pkl"
    acceptedLociFile = open(acceptedLociFileName, 'wb')
    pickle.dump(acceptedLocusTree, acceptedLociFile)
    acceptedLociFile.close()
    if args.verbose:
        runtime = datetime.datetime.now() - start
        print("Multi-sample locus representation count completed in " + str(runtime))
        
main()
    
    
        