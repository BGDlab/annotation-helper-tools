import pandas as pd
import numpy as np
from IPython.display import clear_output


def dealWithNext(df, fn, nextStep, column, idx):
    end = False
    print(nextStep)

    if nextStep == 'n':
        idx = df[df[column].isnull()].index[0]
        print()
    elif nextStep == 'p': 
        # decrement index
        print("Revisiting the previous report...")
        idx -= 1
        print()
    elif nextStep == 'r':
        print("Repeating the current report...")
        print()
    elif nextStep == 's':
        print("Saving and continuing...")
        safelySaveDf(df, fn)
        idx = df[df[column].isnull()].index[0]
        print()
    elif nextStep == 'e':
        print("Saving and exiting...")
        safelySaveDf(df, fn)
        end = True
        
    return idx, end


def safelySaveDf(df, fn):
    try:
        df = df.astype(str)
        df.to_csv(fn, index=False)
        return True
    except PermissionError:
        print("Error: write access to "+fn+" denied. Please check that the file is not locked by Datalad.")
        return False


def markRedText(line, toMark):
    start = '\x1b[5;30;41m' # red background, bold black text
    end = '\x1b[0m'
    
    if type(toMark) == str:
        line = line.replace(toMark, start+toMark+end)
        
    elif type(toMark) == list:
        for phrase in toMark:
            line = line.replace(str(phrase), start+str(phrase)+end)
    
    else:
        print("Error: the second argument must be either a string or a list of strings")
        
    return line

        
def markGreenText(line, toMark):    
    start = '\x1b[5;30;42m' # green background, bold black text
    end = '\x1b[0m'
    
    if type(toMark) == str:
        line = line.replace(toMark, start+toMark+end)
        
    elif type(toMark) == list:
        for phrase in toMark:
            line = line.replace(str(phrase), start+str(phrase).upper()+end)
    
    else:
        print("Error: the second argument must be either a string or a list of strings")
        
    return line


def markYellowText(line, toMark):
    start = '\x1b[5;30;43m' # yellow background, bold black text
    end = '\x1b[0m'
    
    if type(toMark) == str:
        line = line.replace(toMark, start+toMark+end)
        
    elif type(toMark) == list:
        for phrase in toMark:
            line = line.replace(str(phrase), start+str(phrase)+end)
    
    else:
        print("Error: the second argument must be either a string or a list of strings")
        
    return line


def loadDataframe(fn):
    # Load the dataframe
    df = pd.read_csv(fn)
    # If these two columns are not in the dataframe, add them
    if 'scan_reason' not in list(df):
        df['scan_reason'] = np.nan
    if 'pat_history' not in list(df):
        df['pat_history'] = np.nan

    # If there are "Unnamed:" columns in the dataframe, remove them
    toDrop = [col for col in list(df) if "Unnamed:" in col]
    if len(toDrop) > 0:
        df = df.drop(columns=toDrop)

    return df


def checkForAnnotator(df, name):
    # Get the number of columns containing the word "annotator" in their name
    nameCols = [c for c in list(df) if "annotator" in c]

    # Get the individual's name from the columns
    names = [df[c].values[0] for c in nameCols]

    # If their name is not in the list of annotator names
    if name not in names:
        # Add a new column
        nameCol = "annotator_"+ str(len(nameCols)+1).zfill(2)
        # Put their name in the column
        df[nameCol] = name
        # Make a column for annotations_name
        annotationCol = "confirm_clip_" + str(len(nameCols)+1).zfill(2)
        df[annotationCol] = np.nan

    return df, nameCol, annotationCol


def markClipStatus(df, fn, name, clearScreen=False, toHighlight=[]):

    # Initialize variables
    count = 0
    indicators = ['CLINICAL', 'INDICATION', 'HISTORY', 'REASON'] + toHighlight
    end = False
    # Change this 
    idx = df[df['confirm_clip'].isnull()].index[0]
    print("Row", idx)
    print()

    # Using a while loop allows forward and backward iteration
    while idx >= 0 and idx < df.shape[0]:
        row = df.loc[idx, :]

        # Might need to change this if statement to enable backwards iteration
        # Get the text to print
        narr = row['narrative_text']
        if not type(row['impression_text']) is float:
            narr += "IMPRESSION:"+ row['impression_text']
    
        # Format the text - green, yellow, then red
        narr = markYellowText(narr, indicators)

        # Print the text
        print(narr)
        print()

        # Get input from the user - is the patient CLIP
        clip = ""

        while not (clip == "y" or clip == "n" or clip == "skip"):
            clip = input('Does the patient belong in the "Cohort with Limited Imaging Pathology"? (y/n/skip) ')
            print()

        # if the user doesn't skip the entry, do stuff
        if clip != "skip" and clip in ["y", "n"]:

            # Update the dataframe with the CLIP status
            if clip == "y":
                df.loc[idx, "confirm_clip"] = True 
                df.loc[idx, 'annotator'] = name
            elif clip == "n":
                df.loc[idx, "confirm_clip"] = False 
                df.loc[idx, 'annotator'] = name
       
            # Increment the counter
            count += 1


        elif clip == "skip":
            print("Skipping report")
            df.loc[idx, "confirm_clip"] = "skipped"
            df.loc[idx, 'annotator'] = name
 
        if clearScreen:
            clear_output()
    
        if count % 10 == 0:
            print("Total annotated scans this round:", count)
            print()

         
        nextStep = input("What would you like to do next? (n for next unannotated report/p for previous/r to redo current report/s for save/e for exit) ")
        idx, end = dealWithNext(df, fn, nextStep, 'confirm_clip', idx)

        if end: 
            return
        else:
            print("Row", idx)

    print("You have gone through all of the sessions!")
    safelySaveDf(df, fn)

def nadiaMarkClipStatus(df, fn, name, clearScreen=False, toHighlight=[]):

    # Initialize variables
    count = 0
    indicators = ['CLINICAL', 'INDICATION', 'HISTORY', 'REASON'] + toHighlight
    end = False
    # Change this 
    idx = df[df['confirm_clip'].isnull()].index[0]
    print("Row", idx)
    print()

    # Using a while loop allows forward and backward iteration
    while idx >= 0 and idx < df.shape[0]:
        row = df.loc[idx, :]

        # Might need to change this if statement to enable backwards iteration
        # Get the text to print
        narr = row['narrative_text']
        if not type(row['impression_text']) is float:
            narr += "IMPRESSION:"+ row['impression_text']
    
        # Format the text - green, yellow, then red
        narr = markYellowText(narr, indicators)

        # Print the text
        print(narr)
        print()

        # Get input from the user - is the patient CLIP
        clip = ""

        while not (clip == "y" or clip == "n" or clip == "skip"):
            clip = input('Does the patient belong in the "Cohort with Limited Imaging Pathology"? (y/n/skip) ')
            print()

        # if the user doesn't skip the entry, do stuff
        if clip != "skip" and clip in ["y", "n"]:

            # Update the dataframe with the CLIP status
            if clip == "y":
                df.loc[idx, "confirm_clip"] = True 
                df.loc[idx, 'annotator'] = name
            elif clip == "n":
                df.loc[idx, "confirm_clip"] = False 
                df.loc[idx, 'annotator'] = name
       
            # Increment the counter
            count += 1

        elif clip == "skip":
            print("Skipping report")
            df.loc[idx, "confirm_clip"] = "skipped"
            df.loc[idx, 'annotator'] = name
 
        # Automatic save
        safelySaveDf(df, fn)

        if clearScreen:
            clear_output()

        print("Save Successful!")              
   
        if count % 10 == 0:
            print("Total annotated scans this round:", count)
            print()

        nextStep = input("What would you like to do next? (n for next unannotated report/p for previous/r to redo current report/e for exit) ")
        idx, end = dealWithNext(df, fn, nextStep, 'confirm_clip', idx)

        if end: 
            return
        else:
            print("Row", idx)

    print("You have gone through all of the sessions!")
    safelySaveDf(df, fn)



def markReasonAndHistory(df, fn, name):

    # Initialize variables
    count = 0
    indicators = ['CLINICAL', 'INDICATION', 'HISTORY', 'REASON']
    end = False
    idx = df[(df['scan_reason'].isnull()) & (df['pat_history'].isnull())].index[0]
    print(idx)

    # Using a while loop allows forward and backward iteration
    while idx >= 0 and idx < df.shape[0]:
        row = df.loc[idx, :]

        # Might need to change this if statement to enable backwards iteration
        # Get the text to print
        narr = row['narrative_text']
        if not type(row['impression_text']) is float:
            narr += "IMPRESSION:"+ row['impression_text']
    
        # Format the text - green, yellow, then red
        narr = markYellowText(narr, indicators)

        # Print the text
        print("Narrative index (Excel-based rows):", idx+2)
        print(narr)
        print()

        # Get input from the user
        scan_reason = input("Why was this scan was performed? (type 'skip' to skip this narrative, 'missing' if no reason found) ")
        pat_history = input("What is the patient's clinical history? (type 'skip' to skip this narrative, 'missing' if no history found) ")
        print()

        if scan_reason != "skip" and pat_history != "skip" and scan_reason != "" and pat_history != "":

            # Add the input to the dataframe
            df.loc[idx, 'scan_reason'] = scan_reason
            df.loc[idx, 'pat_history'] = pat_history
            df.loc[idx, 'annotator'] = name
    
            # Increment the counter
            count += 1
    
            if count % 10 == 0:
                print("Total annotated scans this round:", count)
                print()

        nextStep = input("What would you like to do next? (n for next unannotated report/p for previous/r to redo current report/s for save/e for exit) ")
        idx, end = dealWithNext(df, fn, nextStep, 'scan_reason', idx)

        print(idx)
        print("end?", end)

        if end: 
            return
        else:
            print("Row", idx)

    print("You have gone through all of the sessions!")
    safelySaveDf(df, fn)


def markAllFields(df, fn, name):

    # Initialize variables
    count = 0
    indicators = ['CLINICAL', 'INDICATION', 'HISTORY', 'REASON']
    end = False
    # Change this 
    idx = df[df['confirm_clip'].isnull()].index[0]
    # idx = df[(df['scan_reason'].isnull()) & (df['pat_history'].isnull())].index[0]
    print("Row", idx)

    # Using a while loop allows forward and backward iteration
    while idx >= 0 and idx < df.shape[0]:
        row = df.loc[idx, :]

        # Might need to change this if statement to enable backwards iteration
        # Get the text to print
        narr = row['narrative_text']
        if not type(row['impression_text']) is float:
            narr += "IMPRESSION:"+ row['impression_text']
    
        # Format the text - green, yellow, then red
        narr = markYellowText(narr, indicators)

        # Print the text
        print(narr)
        print()

        # Get input from the user - is the patient CLIP
        clip = ""

        while not (clip == "y" or clip == "n" or clip == "skip"):
            clip = input('Does the patient belong in the "Cohort with Limited Imaging Pathology"? (y/n/skip) ')
            print()

        # if the user doesn't skip the entry, do stuff
        if clip != "skip":

            # Update the dataframe with the CLIP status
            if clip == "y":
                df.loc[idx, "confirm_clip"] = True 
                df.loc[idx, 'annotator'] = name
            elif clip == "n":
                df.loc[idx, "confirm_clip"] = False 
                df.loc[idx, 'annotator'] = name

            # Ask the user to identify the reason for the scan and any patient history information
            scan_reason = input("Why was this scan was performed? (type 'skip' to skip this narrative, 'missing' if no reason found) ")
            pat_history = input("What is the patient's clinical history? (type 'skip' to skip this narrative, 'missing' if no history found) ")
            print()

            if scan_reason != "skip" and pat_history != "skip" and scan_reason != "" and pat_history != "":
    
                # Add the input to the dataframe
                df.loc[idx, 'scan_reason'] = scan_reason
                df.loc[idx, 'pat_history'] = pat_history
        
                # Increment the counter
                count += 1
        
                if count % 10 == 0:
                    print("Total annotated scans this round:", count)
                    print()

        elif clip == "skip":
            print("Skipping report")
            df.loc[idx, "confirm_clip"] = "skipped"
            df.loc[idx, 'annotator'] = name
 

        nextStep = input("What would you like to do next? (n for next unannotated report/p for previous/r to redo current report/s for save/e for exit) ")
        idx, end = dealWithNext(df, fn, nextStep, 'confirm_clip', idx)

        if end: 
            return
        else:
            print("Row", idx)

    print("You have gone through all of the sessions!")
    safelySaveDf(df, fn)


# Main
if __name__ == "__main__":

    print("Radiology Report Annotation Helper Library v 0.2")
    print("Written and maintained by Jenna Young, PhD (@jmschabdach on Github)")
    print("Tested and used by:")
    print("- Caleb Schmitt")


