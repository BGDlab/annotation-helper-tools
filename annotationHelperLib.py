import pandas as pd
import numpy as np

def safelySaveDf(df, fn):
    try:
        df = df.astype(str)
        df.to_csv(fn)
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

    return df


def markReasonAndHistory(df, fn, name):

    # Initialize variables
    count = 0
    indicators = ['CLINICAL', 'INDICATION', 'HISTORY', 'REASON']
    end = False
    idx = df[(df['scan_reason'].isnull()) & (df['pat_history'].isnull())].index[0]
    print(idx)

    # Using a while loop allows forward and backward iteration
    while not end:
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

        if nextStep == 'n':
            idx = df[(df['scan_reason'].isnull()) & (df['pat_history'].isnull())].index[0]
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
            idx = df[(df['scan_reason'].isnull()) & (df['pat_history'].isnull())].index[0]
            print()
        elif nextStep == 'e':
            print("Saving and exiting...")
            safelySaveDf(df, fn)
            return


    print("You have gone through all of the sessions!")
    safelySaveDf(df, fn)


# Main
if __name__ == "__main__":

    print("Radiology Report Annotation Helper Library v 0.1")
    print("Written and maintained by Jenna Young, PhD (@jmschabdach on Github)")
    print("Tested and used by:")
    print("- Caleb Schmitt")


