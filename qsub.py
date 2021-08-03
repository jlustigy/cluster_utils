import sys, os
import datetime
import shutil
import stat
import subprocess
import fire

def write_qsub_python_script(runfile, name="script", subname="submit.csh",
                      workdir = "", submit = True, rm_after_submit = True,
                      preamble = [], nslots = None, pe = "ocmp"):
    """
    Write and (optionally) submit a python file to run via Sun Grid Engine.

    runfile : str
        Python script file (*.py) that will be submitted
    name : str
        Name of code that will be visible in the queue
    subname : str
        Name of bash script that will be created to submit job
    workdir : str
        Relative directory for job
    submit : bool
        Set to directly submit job
    rm_after_submit : bool
        Set to remove bash script after submission
    preamble : list of str
        List of strings for extra commands to run before executing python code
    nslots : int
        Number of CPUs to announce that the code will use (note that you can use
        more in your python code, but you probably shounldn't do that!)
    pe : str
        Parallel environment flag for SGE

    Returns
    -------
    None

    """

    # Check that the given runfile exists
    if not os.path.exists(runfile):
        print("runfile does not exist.")
        return

    # Get absolute path of workdir
    abs_place = os.path.abspath(workdir)

    # Create name of script file
    newfile = os.path.join(abs_place, subname)

    # Open new file for writing
    f = open(newfile, 'w')

    # Write lines
    f.write('#!/bin/bash\n')
    f.write('#$ -N %s\n' %name)                 # Provide a name for the job
    f.write('#$ -cwd\n')                        # Run in current working directory
    f.write('#$ -V\n')                          # Pass all environmental variables
    f.write('#$ -o %s\n' %(name+".qsub.log"))   # Specifiy an output log
    f.write('#$ -j y\n')                        # Join error log with output log
    f.write('#$ -S /bin/bash\n')                # Use bash
    if nslots is not None:
        f.write('#$ -pe %s %i\n' %(pe, nslots))     # Specify number of slots/CPUs

    # Loop over preamble statements
    for i in range(len(preamble)):
        f.write('%s\n' %preamble[i])

    f.write('\n')

    # Standard python call
    f.write('python %s\n' %runfile)

    f.close()

    # Set permissions to allow execute
    st = os.stat(newfile)
    os.chmod(newfile, st.st_mode | stat.S_IEXEC)

    # Submit the run to the queue?
    if submit:
        subprocess.check_call(["qsub", newfile])

        # Delete the bash script after submitting
        if rm_after_submit:
            os.system('rm %s' %newfile)

    return


if __name__ == "__main__":

    fire.Fire(write_qsub_python_script)
