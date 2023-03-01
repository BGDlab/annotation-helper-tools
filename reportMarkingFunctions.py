import pandas as pd
import numpy as np
from annotationHelperLib import *
from IPython.display import clear_output
from google.cloud import bigquery # SQL table interface on Arcus


    
##
# Pull the report associated with a proc_ord_id for which the specified grader has a grade of 999, and then grade the report. Modifies lab.grader_table
# @param name A str containing the full name of the grader (to also be referenced in publications)
# @param toHighlight A dictionary with str keys specifying a color to highlight the list of str text with
def jennaRegradeOneTrainingReportSQL(name, toHighlight = {}):

    # Initialize the client service
    client = bigquery.Client()
    
    # Get a row from the grader table for the specified rater that has not been graded yet
    getSingleRowQuery = 'SELECT * FROM lab.training_examples WHERE grade = 999 LIMIT 1'

    df = client.query(getSingleRowQuery).to_dataframe()
    
    if len(df) == 0:
        print("There are currently no ungraded training reports.")
        return
    
    # Combine the narrative and impression text
    reportText = df['narrative_text'].values[0] 
    if df['impression_text'].values[0] != 'nan':
        reportText += ' ' + str(df['impression_text'].values[0])
    
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
    updateQuery = 'UPDATE lab.training_examples set grade = '+str(grade)
    updateQuery += ' WHERE report_id like "'+str(df['report_id'].values[0]) + '"'

    updateJob = client.query(updateQuery)
    updateJob.result()

    # Ask for a reason the report was given the grade it was
    reason = str(input('Why does this report get that grade? '))
    print()

    # Update the grader table with the new grade
    updateQuery = 'UPDATE lab.training_examples set reason="'+ reason + '"'
    updateQuery += ' WHERE report_id like "'+str(df['report_id'].values[0]) + '"'

    updateJob = client.query(updateQuery)
    updateJob.result()
    print("Grade saved. Run the cell again to grade another report.")
    
    
##
# Pull the report associated with a proc_ord_id for which the specified grader has a grade of 999, and then grade the report. Modifies lab.grader_table
# @param name A str containing the full name of the grader (to also be referenced in publications)
# @param toHighlight A dictionary with str keys specifying a color to highlight the list of str text with
def markSelfEvalReportSQL(name, toHighlight = {}):

    # Initialize the client service
    client = bigquery.Client()
    
    # Get a row from the grader table for the specified rater that has not been graded yet
    getSingleRowQuery = 'SELECT * FROM lab.training_selfeval WHERE grader_name like "' + name + '" and grade = 999 LIMIT 1'

    df = client.query(getSingleRowQuery).to_dataframe()
    
    if len(df) == 0:
        print("There are currently no reports to grade for", name, " in the table. You have completed the self-evaluation.")
        return
    
    # Get the report for that proc_ord_id from the primary report table
    getReportRow = 'SELECT * FROM arcus.training_examples_selfeval where combo_id like "'+str(df['report_id'].values[0])+'"'
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
    updateQuery = 'UPDATE lab.training_selfeval set grade = '+str(grade)
    updateQuery += ' WHERE report_id like "'+str(df['report_id'].values[0])+'"'
    updateQuery += ' and grader_name like "' + name + '"'

    updateJob = client.query(updateQuery)
    updateJob.result()
    
    # Ask for a reason the report was given the grade it was
    reason = str(input('Why does this report get that grade? '))
    print()

    # Update the grader table with the new grade
    updateQuery = 'UPDATE lab.training_selfeval set reason="'+ reason + '"'
    updateQuery += ' WHERE report_id like "'+str(df['report_id'].values[0]) + '"'
    updateQuery += ' and grader_name like "' + name + '"'

    updateJob = client.query(updateQuery)
    updateJob.result()
    print("Grade saved. Run the cell again to grade another report.")
    
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
    
    if len(df) == 0:
        print("There are currently no reports to grade for", name, " in the table. Please add more to continue.")
        return
    
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
    addReportsQuery = "insert into lab.grader_table "
    # addReportsQuery += " distinct cast(source.proc_ord_id as int64), '"
    # addReportsQuery += name
    # addReportsQuery += "' as grader_name, 'Unique' as grade_category, "
    # addReportsQuery += "999 as grade, "
    # addReportsQuery += "from arcus.reports_annotations_master source "
    # addReportsQuery += "left outer join lab.grader_table filter "
    # addReportsQuery += "on cast(source.proc_ord_id as int64) = filter.proc_ord_id "
    # addReportsQuery += "where filter.proc_ord_id is null limit 100 ;"
    
    addReportsQuery += "with CTE as (select distinct cast(source.proc_ord_id as int64) as proc_ord_id, '" + name + "' as grader_name, 'Unique' as grade_category, 999 as grade, proc_ord_year, age_in_days,"
    addReportsQuery += """from 
                          arcus.reports_annotations_master source 
                            left outer join
                          lab.grader_table filter
                          on cast(source.proc_ord_id as int64) = filter.proc_ord_id
                          where filter.proc_ord_id is null
                          order by source.proc_ord_year desc, source.age_in_days asc
                          limit 100)
                        select proc_ord_id, grader_name, grade_category, grade from CTE"""

    # Submit the query
    supplementRaterReports = client.query(addReportsQuery)
    supplementRaterReports.result()
    
    # Check: how many reports were added for the user?
    getUserUnratedCount = 'SELECT * FROM lab.grader_table WHERE grader_name like "' + name + '" and grade = 999'

    df = client.query(getUserUnratedCount).to_dataframe()
    
    # Inform the user
    print(len(df), "reports were added for grader", name)
    
def welcomeUser(name):
    client = bigquery.Client()
    
    query = 'select * from lab.grader_table where grader_name like "' + name + '"'
    
    df = client.query(query).to_dataframe()
        
    if len(df) == 0:
        print("Welcome,", name)
        print("...")
        
        insertReliabilityQuery = 'insert into lab.grader_table select '
        insertReliabilityQuery += 'distinct cast(proc_ord_id as int64) as proc_ord_id, "'
        insertReliabilityQuery += name + '" as grader_name, "Reliability" as grade_category, '
        insertReliabilityQuery += 'cast(clip_status_03 as int64) as grade, '
        insertReliabilityQuery += 'from arcus.reliability_ratings;'
        
        updateJob = client.query(insertReliabilityQuery)
        updateJob.result()
        
        print("Entries for your reliability ratings have been added to your list of reports to grade.")
    
    else:
        print("Welcome back,", name)
        print("...")
        
        getToRateCount = 'select * from lab.grader_table where grader_name like "'
        getToRateCount += name + '" and grade = 999'
        
        raterUnratedDf = client.query(getToRateCount).to_dataframe()
              
        if len(raterUnratedDf) == 0:
              print("You are caught up on your report ratings")
              # TODO add function here to get more reports for the user
        else:
              print("You currently have", len(raterUnratedDf), "ungraded reports to work on.")

# Main
if __name__ == "__main__":

    print("Radiology Report Annotation Helper Library v 0.2")
    print("Written and maintained by Jenna Young, PhD (@jmschabdach on Github)")
    print("Tested and used by:")
    print("- Caleb Schmitt, Summer 2021")
    print("- Nadia Ngom, Fall 2021 - Spring 2022")


