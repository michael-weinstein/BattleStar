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
    import os
    global args
    args = CheckArgs()
    try:
        thisJob = int(os.environ["SGE_TASK_ID"])   #Yes, yes, this is a fertile job and we will thrive.  We will rule over all this job and we will call it... thisJob.
    except KeyError:   #I think we should call it your unhandled exception
        raise RuntimeError("Unable to find a valid task ID in OS environment variables.")  #Arghhh... curse your sudden but inevitable betrayal!
    tempdir = args.tempdir
    if not tempdir.endswith(os.sep):
        tempdir += os.sep
    bashFilePath = tempdir + "bashFiles" + os.sep + str(thisJob) + ".sh"
    jobStatus = os.system("bash " + bashFilePath)
    if jobStatus == 0:
        touchFilePath = args.clockOutDir + str(thisJob)
        touchFile = open(touchFilePath, 'w')
        touchFile.close()
    
main()