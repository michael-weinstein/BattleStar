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
        parser.add_argument("-t", "--tempdir", help = "Holds the name of the temporary directory we are using.")
        parser.add_argument("-c", "--clockOutDir", help = "Directory for clocking out when done.")
        rawArgs = parser.parse_args()
        if rawArgs.tempdir:
            if os.path.isdir(rawArgs.tempdir):
                self.tempdir = rawArgs.tempdir
            else:
                raise RuntimeError("Temporary directory not found: " + rawArgs.tempdir)
        else:
            raise RunTimeError("No temporary directory specified.")
        if rawArgs.clockOutDir:
            if os.path.isdir(rawArgs.clockOutDir):
                self.clockOutDir = rawArgs.clockOutDir
                if not self.clockOutDir.endswith(os.sep):
                    self.clockOutDir += os.sep
            else:
                raise RuntimeError("Clock out directory not found: " + rawArgs.clockOutDir)
        else:
            raise RuntimeError("No clockout directory given.")
                
def main():
    import os  #import the library for making os system calls
    global args  #declare args as a global
    args = CheckArgs()  #get an object containing validated arguments
    try:
        thisJob = int(os.environ["SGE_TASK_ID"])   #get the array job number from environmental variables
    except KeyError:  #if it cannot get that value
        raise RuntimeError("Unable to find a valid task ID in OS environment variables.")   #something is wrong so quit
    tempdir = args.tempdir  #get the tempdir from arguments
    if not tempdir.endswith(os.sep):  #if it does not end with a separator
        tempdir += os.sep  #add one
    bashFilePath = tempdir + "bashFiles" + os.sep + str(thisJob) + ".sh"  #initialize a string with our bash file name based upon our job number
    jobStatus = os.system("bash " + bashFilePath)  #run the bash file we just identified and set jobStatus to its exit status
    if jobStatus == 0:  #if the job finished successfully
        touchFilePath = args.clockOutDir + str(thisJob)  #define our clockout file
        touchFile = open(touchFilePath, 'w')  #create our clockout file
        touchFile.close()  #close it without writing anything
    
main()