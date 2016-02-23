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
        parser.add_argument("-f", "--file", help = "Pass a filename for this job to work on directly.")
        parser.add_argument("-l", "--fileList", help = "Pass a pickle containing a list of files to operate on.")
        parser.add_argument("-c", "--minCoverage", help = "Minimum coverage", default = 10, type = int)
        parser.add_argument("-t", "--tempdir", help = "Holds the name of the temporary directory we are using.")
        parser.add_argument("-v", "--verbose", help = "Run in verbose mode (indicate progress, etc.)", action = 'store_true')
        parser.add_argument("-s", "--contextRequirement", help = "Specify context sequence requirements (multiples can be passed separated by commas)")
        parser.add_argument("-e", "--contextExclusion", help = "Specify context sequence exclusions (multiples can be passed separated by commas)")
        parser.add_argument("--sampleSize", help = "Specify how many lines from both the locus list and data set should be shown", default = 10, type = int)
        rawArgs = parser.parse_args()
        if rawArgs.file and rawArgs.fileList:
            raise RuntimeError("Error: A single file and a list of files cannot both be specified for a run.  Too confusing.")
        if rawArgs.file:
            if os.path.isfile(rawArgs.file):
                self.file = rawArgs.file
            else:
                raise RuntimeError("File not found: " + rawArgs.file)
        elif rawArgs.fileList:
            self.file = False
            fileList = rawArgs.fileList.split(",")
            for fileName in fileList:
                if not os.path.isfile(fileName):
                    raise RuntimeError("File list item not found: " + fileName)
            self.fileList = fileList
        else:
            raise RuntimeError("No file name or file list pickle argument passed.  Nothing for me to work on.  Bored now.")
        self.minCoverage = rawArgs.minCoverage
        if rawArgs.tempdir:
            if os.path.isdir:
                self.tempdir = rawArgs.tempdir
            else:
                raise RuntimeError("Temporary directory not found: " + rawArgs.tempdir)
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

class HeaderLine(object):
    
    def __init__(self, rawLine, delimiter = "\t"):
        self.line = rawLine.strip()
        self.line = self.line.split(delimiter)
        self.colNames = {}
        for i in range(0, len(self.line)):
            self.colNames[self.line[i].lower()] = i
            
    def indexOf(self, colName):
        colName = colName.lower()
        return self.colNames[colName]

def removeTrailingZeros(string):
    if "." in string:
        string = string.rstrip("0")
        string = string.rstrip(".")
    return string

class DataLine(object):
    
    def __init__(self, rawLine, header, delimiter = "\t"):
        self.isBadLine = False
        self.header = header
        self.rawLine = rawLine.strip()
        self.line = rawLine.strip()
        self.line = self.line.split(delimiter)
        try:
            self.line[self.header.indexOf("pos")] = int(removeTrailingZeros(self.line[self.header.indexOf("pos")]))
            self.line[self.header.indexOf("ratio")] = float(self.line[self.header.indexOf("ratio")])
            self.line[self.header.indexOf("eff_ct_count")] = int(removeTrailingZeros(self.line[self.header.indexOf("eff_ct_count")]))
            self.line[self.header.indexOf("c_count")] = int(removeTrailingZeros(self.line[self.header.indexOf("c_count")]))
            self.line[self.header.indexOf("ct_count")] = int(removeTrailingZeros(self.line[self.header.indexOf("ct_count")]))
            self.line[self.header.indexOf("rev_g_count")] = int(removeTrailingZeros(self.line[self.header.indexOf("rev_g_count")]))
            self.line[self.header.indexOf("rev_ga_count")] = int(removeTrailingZeros(self.line[self.header.indexOf("rev_ga_count")]))
        except (ValueError, IndexError):
            self.isBadLine = True
        else:
            self.ratioLine = (self.line[self.header.indexOf("chr")].replace("chr",""), self.line[self.header.indexOf("pos")], self.line[self.header.indexOf("ratio")])
            self.locusString = (self.line[self.header.indexOf("chr")].replace("chr",""), self.line[header.indexOf("pos")], self.line[header.indexOf("pos")]//1000000)
    
    def hasSufficientCoverage(self, minimum):
        if self.line[self.header.indexOf("eff_ct_count")] >= minimum:
            return True
        else:
            return False
    
    def hasCorrectContext(self, includedContext, excludedContext):
        context = self.line[self.header.indexOf("context")]
        if includedContext:
            if not context in includedContext:
                return False
        if excludedContext:
            if context in includedContext:
                return False
        return True
    
    def locusString(self):
        return 
        
    def __str__(self):
        return self.rawLine
        
def processFile(fileName):
    import os
    inputFile = open(fileName, 'r')
    outputFileName = args.tempdir + os.sep + "prefilter" + os.sep + fileName.split(os.sep)[-1] + ".prefilter"
    outputFile = open(outputFileName, 'w')
    if args.verbose:
        print("Started input and output files.")
    line = inputFile.readline()
    line = line.strip()
    if line:
        headerLine = HeaderLine(line)
        if args.verbose:
            print("Read header line.")
    else:
        raise RuntimeError("File appears to have an empty header line.  Unable to identify columns.")
    outputFile.write(line + "\n")
    progress = 1
    accepted = 0
    while line:
        if args.verbose and (progress % 10000 == 0 or progress == 1):
            print("Processed " + str(progress) + " raw lines.  Accepted " + str(accepted) + " lines.                    ", end = "\r")
        line = inputFile.readline()
        if not line:
            break
        line = line.strip()
        if not line:
            line = True
            continue
        dataLine = DataLine(line, headerLine)
        if dataLine.isBadLine:
            progress += 1
            continue
        if dataLine.hasSufficientCoverage(args.minCoverage) and dataLine.hasCorrectContext(args.contextRequirement, args.contextExclusion) :
            outputFile.write(line + "\n")
            accepted += 1
        progress += 1
    if args.verbose:
        print("Processed " + str(progress) + " raw lines.  Accepted " + str(accepted) + " lines.                    ")
        print("Closing working files.")
    inputFile.close()
    outputFile.close()
    os.rename(outputFileName, outputFileName + ".ratio")
    
def main():
    import pickle
    import datetime
    global args
    args = CheckArgs()
    if args.verbose:
        start = datetime.datetime.now()
    if args.file:
        processFile(args.file)
    else:
        for fileName in args.fileList:
            processFile(fileName)
    if args.verbose:
        runtime = datetime.datetime.now() - start
        print("Context and coverage filter on large file complete in " + str(runtime))
        
main()
                
