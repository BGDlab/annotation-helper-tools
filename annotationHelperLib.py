import pandas as pd
import numpy as np
from IPython.display import clear_output


def dealWithNext(df, fn, nextStep, column, idx):
    end = False
    print(nextStep)
    print(idx)

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
        saved = safelySaveDf(df, fn)
        print(saved)
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

    if line is np.nan:
        return '<No report available.>'

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
 
    if line is np.nan:
        return '<No report available.>'
   
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
    
    if line is np.nan:
        return '<No report available.>'

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
        annotationCol = "clip_status_" + str(len(nameCols)+1).zfill(2)
        df[annotationCol] = np.nan
    else:
        # The name is in names, get the columns of interest
        nameCol = nameCols[names.index(name)]
        annotationCol = nameCol.replace("annotator_", "clip_status_")


    return df, nameCol, annotationCol



# Main
if __name__ == "__main__":

    print("Radiology Report Annotation Helper Library v 0.2")
    print("Written and maintained by Jenna Young, PhD (@jmschabdach on Github)")
    print("Tested and used by:")
    print("- Caleb Schmitt, Summer 2021")
    print("- Nadia Ngom, Fall 2021 - Spring 2022")
    print("- Emma Yang, Summer 2022")
    print("- Aaron Alexander-Bloch")


