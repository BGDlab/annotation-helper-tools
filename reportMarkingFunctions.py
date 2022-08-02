import pandas as pd
import numpy as np
from annotationHelperLib import *
from IPython.display import clear_output


def markClipStatusNewReports(df, fn, name, clearScreen=False, toHighlight=[]):

    # Initialize variables
    count = 0
    indicators = ['CLINICAL', 'INDICATION', 'HISTORY', 'REASON'] + toHighlight
    end = False

    df, annotatorNameCol, annotatorRatingCol = checkForAnnotator(df, name)

    # TODO dejankify this bit
    if 'num_subject_sessions' not in list(df):
        tmpDf = df[['pat_id', 'age_in_days']].drop_duplicates()
        for idx, row in df.iterrows():
            df.loc[idx, 'num_subject_sessions'] = len(tmpDf[tmpDf['pat_id'] == row['pat_id']])
#        df['num_subject_sessions'] = df.groupby('pat_id')['pat_id'].transform('count')

    df = df.sort_values(by=['confirm_clip', 'num_subject_sessions', 'pat_id', 'age_in_days'], ascending=True)

    # TODO add case where there are no reports that haven't been looked at but skips are present
    # 2022-07-26 added condition to select subjects with multiple reports
    idx = df[(df[annotatorRatingCol].isnull()) & (df['confirm_clip'].isnull()) & (df['num_subject_sessions'] > 1) & (len(set(df['age_in_days'].values)) > 1)].index[0]
    # print("Row", idx)
    print()

    # Using a while loop allows forward and backward iteration
    while idx >= 0 and idx < df.shape[0]:
        row = df.loc[idx, :]

        # Might need to change this if statement to enable backwards iteration
        # Get the text to print
        narr = row['narrative_text']
        if not type(row['impression_text']) is float and not type(row['impression_text']) is np.float64:
            narr += "IMPRESSION:"+ row['impression_text']
    
        # Format the text - green, yellow, then red
        narr = markYellowText(narr, indicators)

        # Print the text
        print(' Subj:', row['pat_id'])
        print('# ses:', row['num_subject_sessions'])
        print('  Age:', row['age_in_days'])
        print()
        print(narr)
        print()

        # Get input from the user - is the patient CLIP
        clip = ""

        while not (clip == "0" or clip == "1" or clip == "2"):
            clip = str(input('Assign a CLIP rating to this report (0/1/2): '))
            print()

        # if the user doesn't skip the entry, do stuff
        if clip in ["0", "1", "2"]:

            # Update the dataframe with the CLIP status
            df.loc[idx, annotatorRatingCol] = int(clip) 
            df.loc[idx, annotatorNameCol] = name
       
            # Increment the counter
            count += 1


#        elif clip == "skip":
#            print("Skipping report")
#            df.loc[idx, annotatorRatingCol] = "skipped"
#            df.loc[idx, annotatorNameCol] = name
 
        if clearScreen:
            clear_output()
    
        if count % 10 == 0:
            print("Total annotated scans this round:", count)
            print()

         
        #nextStep = input("What would you like to do next? (n for next unannotated report/p for previous/r to redo current report/s for save/e for exit) ")
        #print(idx)
        #idx, end = dealWithNext(df, fn, nextStep, annotatorRatingCol, idx)
        #print(idx, end)
        safelySaveDf(df, fn)
        print("Current index:", idx)
        idx = df[(df[annotatorRatingCol].isnull()) & (df['confirm_clip'].isnull()) & (df['num_subject_sessions'] > 1) & (len(set(df['age_in_days'].values)) > 1)].index[0]
        print("Next index:", idx)

        #if end: 
        #    return
        #else:
        #    print("Row", idx)

    print("You have gone through all of the sessions!")
    safelySaveDf(df, fn)


def markClipStatusSelfEval(df, fn, name, clearScreen=False, toHighlight=[]):

    # Initialize variables
    count = 0
    indicators = ['CLINICAL', 'INDICATION', 'HISTORY', 'REASON'] + toHighlight
    end = False

    df, annotatorNameCol, annotatorRatingCol = checkForAnnotator(df, name)

    # TODO add case where there are no reports that haven't been looked at but skips are present
    idx = df[df[annotatorRatingCol].isnull()].index[0]
    # print("Row", idx)
    print()

    # Using a while loop allows forward and backward iteration
    while idx >= 0 and idx < df.shape[0]:
        row = df.loc[idx, :]

        # Might need to change this if statement to enable backwards iteration
        # Get the text to print
        narr = row['narrative_text']
        if not type(row['impression_text']) is float and not type(row['impression_text']) is np.float64:
            narr += "IMPRESSION:"+ row['impression_text']
    
        # Format the text - green, yellow, then red
        narr = markYellowText(narr, indicators)

        # Print the text
        print(narr)
        print()

        # Get input from the user - is the patient CLIP
        clip = ""

        while not (clip == "0" or clip == "1" or clip == "2"):
            clip = str(input('Assign a CLIP rating to this report (0/1/2): '))
            print()

        # if the user doesn't skip the entry, do stuff
        if clip in ["0", "1", "2"]:

            # Update the dataframe with the CLIP status
            df.loc[idx, annotatorRatingCol] = int(clip) 
            df.loc[idx, annotatorNameCol] = name
       
            # Increment the counter
            count += 1


#        elif clip == "skip":
#            print("Skipping report")
#            df.loc[idx, annotatorRatingCol] = "skipped"
#            df.loc[idx, annotatorNameCol] = name
 
        if clearScreen:
            clear_output()
    
        if count % 10 == 0:
            print("Total annotated scans this round:", count)
            print()

         
        #nextStep = input("What would you like to do next? (n for next unannotated report/p for previous/r to redo current report/s for save/e for exit) ")
        #print(idx)
        #idx, end = dealWithNext(df, fn, nextStep, annotatorRatingCol, idx)
        #print(idx, end)
        safelySaveDf(df, fn)
        print("Current index:", idx)
        idx = df[df[annotatorRatingCol].isnull()].index[0]
        print("Next index:", idx)

        #if end: 
        #    return
        #else:
        #    print("Row", idx)

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
    print("- Caleb Schmitt, Summer 2021")
    print("- Nadia Ngom, Fall 2021 - Spring 2022")


