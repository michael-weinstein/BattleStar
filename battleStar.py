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
        parser.add_argument("-d", "--directory", help = "Directory of files to work with.")
        parser.add_argument("-c", "--minCoverage", help = "Minimum coverage", default = 10, type = int)
        parser.add_argument("-t", "--tempdir", help = "Force the program to try using a temporary directory.  The directory should not already exist.")
        parser.add_argument("--noCleanUp", help = "Do not clean up working directory when done.", action = "store_true")
        parser.add_argument("-v", "--verbose", help = "Run in verbose mode (indicate progress, etc.)", action = 'store_true')
        parser.add_argument("-s", "--contextRequirement", help = "Specify context sequence requirements (multiples can be passed separated by commas)")
        parser.add_argument("-e", "--contextExclusion", help = "Specify context sequence exclusions (multiples can be passed separated by commas)")
        parser.add_argument("--sampleSize", help = "Specify how many lines from both the locus list and data set should be shown", default = 10, type = int)
        parser.add_argument("-r", "--minRepresentation", help = "Minimum percent of the time a specific locus needs to have been reported in samples", default = 75, type = int)
        parser.add_argument("-p", "--maxParallelJobs", help = "Maximum number of parallel array jobs allowed", default = 301, type = int)
        parser.add_argument("--scratchFolder", help = "Force use of a different folder for the temp dir.")
        parser.add_argument("--directToCount", help = "Go directly to the counting step, pass the temporary directory to be used")
        parser.add_argument("--directToFilter2", help = "Go directly to the second filter step, pass the temporary directory to be used")
        parser.add_argument("--directToFinalBuild", help = "Go directly to the final build process")
        parser.add_argument("-m", "--emptyCellMarker", help = "Marker for blank cells.", default = "")
        parser.add_argument("-o", "--outputFile", help = "Specify the output file name.", default = "output.txt")
        parser.add_argument("-k", "--pickleOut", help = "Output to a Pandas pickle instead of a delimited text file.", action = 'store_true')
        rawArgs = parser.parse_args()
        self.doFilter1 = True
        self.doCount = True
        self.doFilter2 = True
        self.doFinalBuild = True
        if rawArgs.verbose:
            try:
                import random
                import urllib
                crit = 20
                d20 = random.randint(1,20)
                if d20 == crit:
                    print(urllib.request.urlopen("https://raw.githubusercontent.com/michael-weinstein/randomBits/master/Ben.txt").read().decode('utf-8'))  #this works better if your terminal has a light background
            except:
                pass
        if rawArgs.directory:
            if not os.path.isdir(rawArgs.directory):
                raise RuntimeError("Specified directory does not exist: " + rawArgs.directory)
            self.directory = rawArgs.directory
        else:
            self.directory = False
        self.minCoverage = rawArgs.minCoverage
        if rawArgs.tempdir:
            if os.path.isdir:
                raise RuntimeError("Specified temporary directory already exists.  Please use one that that does not.  " + rawArgs.tempdir)
            self.tempdir = rawArgs.tempdir
        else:
            self.tempdir = False
        self.verbose = rawArgs.verbose
        if rawArgs.contextRequirement:
            self.contextRequirement = rawArgs.contextRequirement.split(",")
            self.contextRequirement = [item.upper() for item in self.contextRequirement]
        else:
            self.contextRequirement = []
        if rawArgs.contextExclusion:
            self.contextExclusion = rawArgs.contextExclusion.split(",")
            self.contextExclusion = [item.upper() for item in self.contextExclusion]
        else:
            self.contextExclusion = []
        if self.contextExclusion and self.contextRequirement:  #why someone would specify both an inclusion and exclusion list is beyond me, but...
            for item in self.contextRequirement:
                if item in self.contextExclusion:
                    raise RuntimeError("Error, " + item + " is included in both the requirement and exclusion lists.")
        self.sampleSize = rawArgs.sampleSize
        self.minRepresentation = rawArgs.minRepresentation
        self.maxParallelJobs = rawArgs.maxParallelJobs
        if rawArgs.scratchFolder:
            if os.path.isdir(rawArgs.scratchFolder):
                self.scratchFolder = rawArgs.scratchFolder
            else:
                raise RuntimeError("Specified scratch folder does not exist. " + rawArgs.scratchFolder)
        else:
            self.scratchFolder = ""
        if rawArgs.directToCount:
            self.doFilter1 = False
            if os.path.isdir(rawArgs.directToCount):
                self.tempdir = rawArgs.directToCount
            else:
                raise RuntimeError("Specified temporary directory for continuing does not exist: " + rawArgs.directToCount)
        if rawArgs.directToFilter2:
            self.doFilter1 = False
            self.doCount = False
            if os.path.isdir(rawArgs.directToFilter2):
                self.tempdir = rawArgs.directToFilter2
            else:
                raise RuntimeError("Specified temporary directory for continuing does not exist: " + rawArgs.directToFilter2)
        if rawArgs.directToFinalBuild:
            self.doFilter1 = False
            self.doCount = False
            self.doFilter2 = False
            if os.path.isdir(rawArgs.directToFinalBuild):
                self.tempdir = rawArgs.directToFinalBuild
            else:
                raise RuntimeError("Specified temporary directory for continuing does not exist: " + rawArgs.directToFilter2)
        self.noCleanUp = False
        if rawArgs.noCleanUp:
            self.noCleanUp = True
        emptyCellMarker = rawArgs.emptyCellMarker
        try:
            emptyCellMarker = int(emptyCellMarker)
            if emptyCellMarker in [0,1]:
                print("Warning: Empty cell marker value may potentially collide with valid data values.")
                if not yesAnswer("Proceed anyway?"):
                    quit("By your command.")
        except ValueError:
            try:
                emptyCellMarker = float(emptyCellMarker)
                if emptyCellMarker <= 1 and emptyCellMarker >= 0:
                    print("Warning: Empty cell marker value may potentially collide with valid data values.")
                    if not yesAnswer("Proceed anyway?"):
                        quit("By your command.")
            except ValueError:
                pass
        self.emptyCellMarker = emptyCellMarker
        outputFile = rawArgs.outputFile
        if rawArgs.pickleOut:
            self.pickleOut = True
            outputFile = outputFile.replace(".txt",".pandas.pkl")
        else:
            self.pickleOut = False
        if os.path.isfile(outputFile):
            print("Warning: Output file already exists.")
            if not yesAnswer("Overwrite existing file?"):
                quit("By your command")
        self.outputFile = outputFile

def displaySplash():
    print("   ___       _   _   _      __ _             ")
    print("  / __\ __ _| |_| |_| | ___/ _\ |_ __ _ _ __ ")
    print(" /__\/// _` | __| __| |/ _ \ \| __/ _` | '__|")
    print("/ \/  \ (_| | |_| |_| |  __/\ \ || (_| | |   ")
    print("\_____/\__,_|\__|\__|_|\___\__/\__\__,_|_|   ")
    print("BattleStar written by Michael Weinstein, 2016")
    print("University of California, Los Angeles, Daniel Cohn laboratory and Collaboratory")
    print("email: [myfirstname].[mylastname] AT ucla.edu")

def createTempDir():  #makes a temporary directory for this run.  Completions will clock out here and results will be reported back to it.
    if args.verbose:
        print ("Creating temporary directory")
    import re
    import os
    import datetime
    successful = False
    while not successful:
        currenttime = datetime.datetime.now()
        currenttime = str(currenttime)
        currenttime = re.sub(r'\W','',currenttime)
        tempdir = args.scratchFolder + '.battlestar' + currenttime
        if os.path.isdir(tempdir):
            continue
        try:
            os.mkdir(tempdir)
        except OSError:
            continue
        successful = True
    os.mkdir(tempdir + os.sep + "filter1")
    os.mkdir(tempdir + os.sep + "filter2")
    os.mkdir(tempdir + os.sep + "loci")
    os.mkdir(tempdir + os.sep + "lociGather")
    os.mkdir(tempdir + os.sep + "lociClockOut")
    os.mkdir(tempdir + os.sep + "filter1ClockOut")
    os.mkdir(tempdir + os.sep + "filter2ClockOut")
    os.mkdir(tempdir + os.sep + "bashFiles")
    os.mkdir(tempdir + os.sep + "finalParts")
    os.mkdir(tempdir + os.sep + "finalPartsClockOut")
    return tempdir
    
def getFilter1FileList():
    import os
    if args.directory:
        directory = args.directory
    else:
        directory = os.getcwd()
    if not directory.endswith(os.sep):
        directory += os.sep
    rawFileList = os.listdir(directory)
    filteredFileList = []
    for file in rawFileList:
        if file.endswith(".ratio"):
            filteredFileList.append(directory + file)
    if len(filteredFileList) < args.maxParallelJobs:
        parallelJobs = len(filteredFileList)
    fileJobList = []
    for i in range(0,parallelJobs):
        fileJobList.append([])
    for i in range(0,len(filteredFileList)):
        fileJobList[i % parallelJobs].append(filteredFileList[i])
    return fileJobList

def runFilter1(fileList, tempdir):
    import os
    if not os.path.isdir("schedulerOutput"):
        os.mkdir("schedulerOutput")
    wrapperRunnerName = tempdir + os.sep + "bashFiles" + os.sep + "wrapper.sh"
    wrapperRunner = open(wrapperRunnerName, 'w')
    wrapperRunner.write("#!/bin/bash\n")
    wrapperRunner.write(pythonInterpreterAbsolutePath + " raptor.py " + "--tempdir " + tempdir + " --clockOutDir " + tempdir + os.sep + "filter1ClockOut")
    wrapperRunner.close()
    clockOutFlush(tempdir + os.sep + "filter1ClockOut")
    for i in range(0,len(fileList)):
        arguments = {"--fileList" : ",".join(fileList[i]),
                     "--minCoverage" : str(args.minCoverage),
                     "--tempdir" : tempdir,
                     "--contextRequirement" : ",".join(args.contextRequirement),
                     "--contextExclusion" : ",".join(args.contextExclusion),
                     "--sampleSize" : str(args.sampleSize)}
        argumentList = []
        for key in list(arguments.keys()):
            if arguments[key]:
                argumentList.append(key + " " + arguments[key])
        argumentString = " ".join(argumentList)
        if i == 0:
            if args.verbose:
                argumentString += " -v"
            thisNodesJob = pythonInterpreterAbsolutePath + " viper.py " + argumentString
        else:
            bashFileName = tempdir + os.sep + "bashFiles" + os.sep + str(i) + ".sh"
            bashFile = open(bashFileName, 'w')
            bashFile.write("#!/bin/bash\n")
            bashFile.write(pythonInterpreterAbsolutePath + " viper.py " + argumentString)
            bashFile.close()
    jobRange = "1-" + str(len(fileList)) + " "
    command = "qsub -cwd -V -N Vipers -l h_data=4G,time=23:59:00 -e " + os.getcwd() +  "/schedulerOutput/ -o " + os.getcwd() + "/schedulerOutput/ " + "-t " + jobRange + wrapperRunnerName
    if args.verbose:
        print("BASH " + command)
    os.system(command)
    if args.verbose:
        print("Starting the job on this node.")
        print("BASH " + thisNodesJob)
    os.system(thisNodesJob)
    if args.verbose:
        print("Monitoring other node filtering and extraction jobs.")
    monitorJobs(tempdir + os.sep + "filter1ClockOut", len(fileList))
    
def monitorJobs(clockOutDirectory, jobCount):
    import os
    import time
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
                        break
        time.sleep(1)
    if args.verbose:
        print("Completed " + str(completed) + " of " + str(jobCount) + " jobs.       ")
        
def clockOutFlush(clockOutDir):
    import os
    import shutil
    shutil.rmtree(clockOutDir)
    os.mkdir(clockOutDir) 
        
def bashFileFlush(tempdir):
    import os
    import shutil
    bashFileDirectory = tempdir + os.sep + "bashFiles"
    shutil.rmtree(bashFileDirectory)
    os.mkdir(bashFileDirectory)    
        
def countRepresentation(tempdir):
    import os
    arguments = {"--minRepresentation" : str(args.minRepresentation),
                 "--tempdir" : tempdir}
    argumentList = []
    for key in list(arguments.keys()):
        if arguments[key]:
            argumentList.append(key + " " + arguments[key])
    argumentString = " ".join(argumentList)
    command = pythonInterpreterAbsolutePath + " blackbird.py " + argumentString
    if args.verbose:
        command += " -v"
        print("BASH " + command)
    os.system(command)
    
def getFilter2FileList(tempdir):
    import os
    directory = tempdir + os.sep + "filter1"
    rawFileList = os.listdir(directory)
    filteredFileList = []
    for file in rawFileList:
        if file.endswith(".data.pkl"):
            filteredFileList.append(directory + os.sep + file)
    if len(filteredFileList) < args.maxParallelJobs:
        args.parallelJobs = len(filteredFileList)
    fileJobList = []
    for i in range(0,args.parallelJobs):
        fileJobList.append([])
    for i in range(0,len(filteredFileList)):
        fileJobList[i % args.parallelJobs].append(filteredFileList[i])
    return fileJobList

def runFilter2(fileList, tempdir):
    import os
    if not os.path.isdir("schedulerOutput"):
        os.mkdir("schedulerOutput")
    wrapperRunnerName = tempdir + os.sep + "bashFiles" + os.sep + "wrapper.sh"
    wrapperRunner = open(wrapperRunnerName, 'w')
    wrapperRunner.write("#!/bin/bash\n")
    wrapperRunner.write(pythonInterpreterAbsolutePath + " raptor.py " + "--tempdir " + tempdir + " --clockOutDir " + tempdir + os.sep + "filter2ClockOut")
    wrapperRunner.close()
    clockOutFlush(tempdir + os.sep + "filter2ClockOut")
    for i in range(0,len(fileList)):
        arguments = {"--fileList" : ",".join(fileList[i]),
                     "--tempdir" : tempdir}
        argumentList = []
        for key in list(arguments.keys()):
            if arguments[key]:
                argumentList.append(key + " " + arguments[key])
        argumentString = " ".join(argumentList)
        if i == 0:
            if args.verbose:
                argumentString += " -v"
            thisNodesJob = pythonInterpreterAbsolutePath + " cylonRaider.py " + argumentString
        else:
            bashFileName = tempdir + os.sep + "bashFiles" + os.sep + str(i) + ".sh"
            bashFile = open(bashFileName, 'w')
            bashFile.write("#!/bin/bash\n")
            bashFile.write(pythonInterpreterAbsolutePath + " cylonRaider.py " + argumentString)
            bashFile.close()
    jobRange = "1-" + str(len(fileList)) + " "
    command = "qsub -cwd -V -N CylonRaiders -l h_data=4G,time=23:59:00 -e " + os.getcwd() +  "/schedulerOutput/ -o " + os.getcwd() + "/schedulerOutput/ " + "-t " + jobRange + wrapperRunnerName
    if args.verbose:
        print("BASH " + command)
    os.system(command)
    if args.verbose:
        print("Starting the job on this node.")
        print("BASH " + thisNodesJob)
    os.system(thisNodesJob)
    if args.verbose:
        print("Monitoring other nodes representation filtering jobs.")
    monitorJobs(tempdir + os.sep + "filter2ClockOut", len(fileList))

def cleanUp(tempdir):
    import shutil
    if args.verbose:
        print("Cleaning up temporary directory")
    shutil.rmtree(tempdir)
    
def yesAnswer(question):  #asks the question passed in and returns True if the answer is yes, False if the answer is no, and keeps the user in a loop until one of those is given.  Also useful for walking students through basic logical python functions
    answer = False  #initializes the answer variable to false.  Not absolutely necessary, since it should be undefined at this point and test to false, but explicit is always better than implicit
    while not answer:  #enters the loop and stays in it until answer is equal to True
        print (question + ' (Y/N)')  #Asks the question contained in the argument passed into this subroutine
        answer = input('>>') #sets answer equal to some value input by the user
        if str(answer) == 'y' or str(answer) == 'Y':  #checks if the answer is a valid yes answer
            return True  #sends back a value of True because of the yes answer
        elif str(answer) == 'n' or str(answer) == 'N': #checks to see if the answer is a valid form of no
            return False  #sends back a value of False because it was not a yes answer
        else: #if the answer is not a value indicating a yes or no
            print ('Invalid response.')
            answer = False #set answer to false so the loop will continue until a satisfactory answer is given
            
def getGroupData(tempdir):
    import os
    import pickle
    acceptedLociFileName = tempdir + os.sep + "loci" + os.sep + "acceptedLoci.pkl"
    if not os.path.isfile(acceptedLociFileName):
        raise RuntimeError("Group listing function was unable to find the list of accepted loci.  Was this process missed?")
    acceptedLociFile = open(acceptedLociFileName, 'rb')
    acceptedLociTree = pickle.load(acceptedLociFile)
    acceptedLociFile.close()
    unsortedContigs = list(acceptedLociTree.keys())
    numberedContigs = []
    namedContigs = []
    groupMatrix = {}
    chrM = False
    for contig in unsortedContigs:
        try:
            contig = int(contig)
            numberedContigs.append(contig)
        except ValueError:
            try:
                contig = float(contig)
                numberedContigs.append(contig)
            except ValueError:
                if contig.upper() == "M":
                    chrM = True
                    mContig = contig
                else:
                    namedContigs.append(contig)
    numberedContigs.sort()
    namedContigs.sort()
    contigs = numberedContigs + namedContigs
    if chrM:
        contigs += mContig
    contigs = [str(contig) for contig in contigs]
    if args.verbose:
        print("Contig order: " + ", ".join(contigs))
    for contig in contigs:
        keys = list(acceptedLociTree[contig].keys())
        keys.sort()
        groupMatrix[contig] = keys
    return (contigs, groupMatrix)

def makeJobsGroupList(contigs, groupMatrix):
    totalGroups = 0
    for contig in contigs:
        totalGroups += len(groupMatrix[contig])
    if totalGroups < args.maxParallelJobs:
        parallelJobs = totalGroups
    else:
        parallelJobs = args.maxParallelJobs
    jobGroupList = []
    for i in range(0,parallelJobs):
        jobGroupList.append([])
    jobNumber = 0
    added = 0
    groupsPerJob = totalGroups // parallelJobs 
    remainder = totalGroups % parallelJobs
    thisJobSize = groupsPerJob
    if jobNumber <= remainder -1:
        thisJobSize += 1
    for contig in contigs:
        for group in groupMatrix[contig]:
            jobGroupList[jobNumber].append(contig + "." + str(group))
            added += 1
            if added == thisJobSize:
                jobNumber += 1
                added = 0
                thisJobSize = groupsPerJob
                if jobNumber <= remainder - 1:
                    thisJobSize += 1
    return jobGroupList

def orderedCaseList(tempdir):
    import os
    import pickle
    filter2Dir = tempdir + os.sep + "filter2"
    filteredFiles = []
    rawFiles = os.listdir(filter2Dir)
    for file in rawFiles:
        if file.endswith(".data.pkl"):
            filteredFiles.append(file)
    file = open(filter2Dir + os.sep + "orderedCaseList.pkl", 'wb')
    pickle.dump(filteredFiles, file)
    file.close()
    return filteredFiles

def trimRepeatingEnds(nameList, trimStart = True, trimEnd = False):
    startTrim = 0
    endTrim = 0
    firstName = nameList[0]
    allIdentical = True
    for name in nameList:
        if name != firstName:
            allIdentical = False
            break
    if allIdentical:
        print("WARNING: All sample names appear to be identical.")
        return nameList
    shortestItemLength = len(firstName)
    for name in nameList:
        if len(name) < shortestItemLength:
            shortestItemLength = len(name)
    if trimEnd:
        allIdentical = True
        for i in range(-1,-shortestItemLength-1,-1):
            for name in nameList:
                if name[i] != firstName[i]:
                    allIdentical = False
                    break
            if allIdentical:
                endTrim += 1
            else:
                break
    if trimStart:
        allIdentical = True
        for i in range(0, shortestItemLength):
            for name in nameList:
                if name[i] != firstName[i]:
                    allIdentical = False
                    break
            if allIdentical:
                startTrim += 1
            else:
                break
    trimmedNames = [name[startTrim : len(name)-endTrim] for name in nameList]
    for name in trimmedNames:
        if not name:
            print("Warning: Name trim blanked at least one of the sample names.")
            if not yesAnswer("Use anyway?"):
                return nameList
            else:
                break
    return trimmedNames
        
def buildFinalDataFrame(tempdir, caseList, totalPieces):
    import pickle
    import pandas
    import os
    caseNameList = [name.split(".")[0] for name in caseList]
    caseNameList = trimRepeatingEnds(caseNameList, trimStart = False)
    finalDataFrame = pandas.DataFrame()
    for i in range(0,totalPieces):
        fileName = tempdir + os.sep + 'finalParts' + os.sep + str(i) + ".data.pkl"
        if not os.path.isfile(fileName):
            raise RuntimeError("Expecting final piece file " + fileName + " but it was not found.")
        file = open(fileName, 'rb')
        data = pickle.load(file)  #this should come in structured as {locus:[list,of,percentages,in,order,set,by,case,list]}
        file.close()
        finalDataFrame = finalDataFrame.append(data)
    finalDataFrame.columns = caseNameList
    return finalDataFrame        

def getAcceptedCoordinateList(tempdir):
    import os
    import pickle
    acceptedLociFileName = tempdir + os.sep + 'loci' + os.sep + 'acceptedLoci.pkl'
    if not os.path.isfile(acceptedLociFileName):
        raise RuntimeError("Group listing function was unable to find the list of accepted loci.  Was this process missed?")
    acceptedLociFile = open(acceptedLociFileName, 'rb')
    acceptedLociTree = pickle.load(acceptedLociFile)
    acceptedLociFile.close()
    unsortedContigs = list(acceptedLociTree.keys())
    numberedContigs = []
    namedContigs = []
    groupMatrix = {}
    chrM = False
    for contig in unsortedContigs:
        try:
            contig = int(contig)
            numberedContigs.append(contig)
        except ValueError:
            try:
                contig = float(contig)
                numberedContigs.append(contig)
            except ValueError:
                if contig.upper() == "M":
                    chrM = True
                    mContig = contig
                else:
                    namedContigs.append(contig)
    numberedContigs.sort()
    namedContigs.sort()
    contigs = numberedContigs + namedContigs
    if chrM:
        contigs += mContig
    contigs = [str(contig) for contig in contigs]
    if args.verbose:
        print("Contig order: " + ", ".join(contigs))
    coordinateList = []
    for contig in contigs:
        groups = list(acceptedLociTree[contig].keys())
        groups.sort()
        for group in groups:
            positions = list(acceptedLociTree[contig][group].keys())
            positions.sort()
            for position in positions:
                coordinateList.append(contig + ":" + str(position))            
    coordinateListFileName = tempdir + os.sep + 'loci' + os.sep + 'acceptedCoordinateList.pkl'
    coordinateListFile = open(coordinateListFileName,'wb')
    pickle.dump(coordinateList, coordinateListFile)
    coordinateListFile.close()
    coordinateIndex = {}
    index = 0
    for coordinates in coordinateList:
        coordinateIndex[coordinates] = index
        index += 1
    coordinateIndexFileName = tempdir + os.sep + 'loci' + os.sep + 'acceptedCoordinateIndex.pkl'
    coordinateIndexFile = open(coordinateIndexFileName,'wb')
    pickle.dump(coordinateIndex, coordinateIndexFile)
    coordinateIndexFile.close()
    return coordinateList

def runFinalBuild(tempdir):
    import os
    arguments = {"--outputFile" : args.outputFile,
                 "--tempdir" : tempdir}
    argumentList = []
    for key in list(arguments.keys()):
        if arguments[key]:
            argumentList.append(key + " " + arguments[key])
    argumentString = " ".join(argumentList)
    command = pythonInterpreterAbsolutePath + " resurrectionShip.py " + argumentString
    if args.pickleOut:
        command += " --pickleOut "
    if args.verbose:
        command += " -v "
        print("BASH " + command)
    os.system(command)

def main():
    import datetime
    import pandas
    global args
    args = CheckArgs()
    if args.verbose:
        displaySplash()
    if args.verbose:
        start = datetime.datetime.now()
    if args.tempdir:
        tempdir = args.tempdir
    else:
        tempdir = createTempDir()
    if args.doFilter1:
        jobFilesList = getFilter1FileList()
        if not jobFilesList:
            raise RuntimeError("No data files to process for filter 1.")
        runFilter1(jobFilesList, tempdir)
    bashFileFlush(tempdir)
    if args.doCount:
        countRepresentation(tempdir)
    acceptedCoordinateList = getAcceptedCoordinateList(tempdir)
    bashFileFlush(tempdir)
    if args.doFilter2:
        jobFilesList = getFilter2FileList(tempdir)
        if not jobFilesList:
            raise RuntimeError("No data files to process for filter 2.")
        runFilter2(jobFilesList, tempdir)
    bashFileFlush(tempdir)
    if args.doFinalBuild:
        if args.verbose:
            print("Starting final build script.")
            runFinalBuild(tempdir)
    if not args.noCleanUp:
        try:
            cleanUp(tempdir)
        except:
            try:
                cleanUp(tempdir)
            except:
                print("Unable to remove temporary directory: " + tempdir)
                print("Please remove it manually.")
    if args.verbose:
        runtime = datetime.datetime.now() - start
        print("Whole process complete in " + str(runtime))
        
main()
    