import pandas as pd
import numpy as np
from annotationHelperLib import *
from IPython.display import clear_output
from google.cloud import bigquery # SQL table interface on Arcus

##
# Mark CLIP status (0/1/2) for unmarked radiology reports
# @param df A pandas DataFrame object containing radiology reports (hyperspecific format)
# @param fn A str containing the full path to the file containing the radiology reports
# @param name A str containing the full name of the grader (to also be referenced in publications)
# @param clearScreen A boolean specifying whether or not to clear the screen after grading a report (default: False)
# @param toHighlight A list of str to highlight if they appear in the report text
def markClipStatusNewReports(df, fn, name, clearScreen=False, toHighlight=[]):

    # Initialize variables
    count = 0
    indicators = ['CLINICAL', 'INDICATION', 'HISTORY', 'REASON'] + toHighlight
    end = False

    # Check to see if the annotator already exists in the data frame
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

        # Only accept a grade if it is 0/1/2
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


##
# Mark CLIP status (0/1/2) for reports in the self-eval
# @param df A pandas DataFrame object containing radiology reports (hyperspecific format)
# @param fn A str containing the full path to the file containing the radiology reports
# @param name A str containing the full name of the grader (to also be referenced in publications)
# @param clearScreen A boolean specifying whether or not to clear the screen after grading a report (default: False)
# @param toHighlight A list of str to highlight if they appear in the report text
def markClipStatusSelfEval(df, fn, name, clearScreen=False, toHighlight={}):

    # Initialize variables
    count = 0
    indicators = ['CLINICAL', 'INDICATION', 'HISTORY', 'REASON']
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
        for key in sorted(toHighlight.keys()):
            narr = markTextColor(narr, toHighlight[key], key)

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

##
# Mark CLIP status (0/1/2) for unmarked radiology reports - Nadia specific (DEPRECATED)
# @param df A pandas DataFrame object containing radiology reports (hyperspecific format)
# @param fn A str containing the full path to the file containing the radiology reports
# @param name A str containing the full name of the grader (to also be referenced in publications)
# @param clearScreen A boolean specifying whether or not to clear the screen after grading a report (default: False)
# @param toHighlight A list of str to highlight if they appear in the report text
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


##
# Mark the reason for scan and any patient history
# @param df A pandas DataFrame object containing radiology reports (hyperspecific format)
# @param fn A str containing the full path to the file containing the radiology reports
# @param name A str containing the full name of the grader (to also be referenced in publications)
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

# Mark CLIP status (y/n) for unmarked radiology reports (DEPRECATED)
# @param df A pandas DataFrame object containing radiology reports (hyperspecific format)
# @param fn A str containing the full path to the file containing the radiology reports
# @param name A str containing the full name of the grader (to also be referenced in publications)
# @param clearScreen A boolean specifying whether or not to clear the screen after grading a report (default: False)
# @param toHighlight A list of str to highlight if they appear in the report text
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
    
    
##
# Pull the report associated with a proc_ord_id for which the specified grader has a grade of 999, and then grade the report. Modifies lab.grader_table
# @param name A str containing the full name of the grader (to also be referenced in publications)
# @param toHighlight A dictionary with str keys specifying a color to highlight the list of str text with
def markOneReportSQL(name, toHighlight = {}):

    # Initialize the client service
    client = bigquery.Client()
    
    # Get a row from the grader table for the specified rater that has not been graded yet
    getSingleRowQuery = 'SELECT * FROM lab.grader_table WHERE grader_name like "' + name + '" and grade = 999 LIMIT 1'

    df = client.query(getSingleRowQuery).to_dataframe()
    
    # Get the report for that proc_ord_id from the primary report table
    getReportRow = 'SELECT * FROM arcus.reports_annotations_master where proc_ord_id like "'+str(df['proc_ord_id'].values[0])+'"'
    reportDf = client.query(getReportRow).to_dataframe()


    # Combine the narrative and impression text
    reportText = reportDf['narrative_text'].values[0] 
    if reportDf['impression_text'].values[0] != 'nan':
        reportText += ' ' + reportDf['impression_text'].values[0]
    
    # If the user passed a dictionary of lists to highlight
    if len(toHighlight.keys()) > 0:
        for key in sorted(toHighlight.keys()):
            reportText = markTextColor(reportText, toHighlight[key], key)
            
    # Print the report and ask for a grade
    print(reportText)
    print()
    grade = str(input('Assign a CLIP rating to this report (0 do not use/1 maybe use/2 definitely use): '))
    print()
    
    # Update the grader table with the new grade
    updateQuery = 'UPDATE lab.grader_table set grade = '+str(grade)
    updateQuery += ' WHERE proc_ord_id = '+str(df['proc_ord_id'].values[0])
    updateQuery += ' and grader_name like "' + name + '"'

    updateJob = client.query(updateQuery)
    updateJob.result()
    print("Grade saved. Run the cell again to grade another report.")
    
##
# Get more proc_ord_id for which no reports have been rated for the specified user to grade
# @param name A str containing the full name of the grader (to also be referenced in publications)
def getMoreReportsToGrade(name):
    # Initialize the client service
    client = bigquery.Client()
    
    # Set up the query to get more reports for the specified person to annotate
    addReportsQuery = "insert into lab.grader_table select " +
    addReportsQuery += " distinct cast(source.proc_ord_id as int64), '"
    addReportsQuery += name
    addReportsQuery += "' as grader_name, 'Unique' as grade_category, "
    addReportsQuery += "999 as grade, "
    addReportsQuery += "from arcus.reports_annotations_master source "
    addReportsQuery += "left outer join lab.grader_table filter "
    addReportsQuery += "on cast(source.proc_ord_id as int64) = filter.proc_ord_id "
    addReportsQuery += "where filter.proc_ord_id is null limit 100 ;"
    
    # Submit the query
    supplementRaterReports = client.query(addReportsQuery)
    supplementRaterReports.result()
    
    # Check: how many reports were added for the user?
    getUserUnratedCount = 'SELECT * FROM lab.grader_table WHERE grader_name like "' + name + '" and grade = 999'

    df = client.query(getSingleRowQuery).to_dataframe()
    
    # Inform the user
    print(len(df), "reports were added for grader", name)
    


# Main
if __name__ == "__main__":

    print("Radiology Report Annotation Helper Library v 0.2")
    print("Written and maintained by Jenna Young, PhD (@jmschabdach on Github)")
    print("Tested and used by:")
    print("- Caleb Schmitt, Summer 2021")
    print("- Nadia Ngom, Fall 2021 - Spring 2022")


