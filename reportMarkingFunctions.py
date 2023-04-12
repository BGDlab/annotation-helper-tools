import pandas as pd
import numpy as np
import random
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
        reportText += ' IMPRESSION:' + str(df['impression_text'].values[0])
    
    # If the user passed a dictionary of lists to highlight
    if len(toHighlight.keys()) > 0:
        for key in toHighlight.keys():
            reportText = markTextColor(reportText, toHighlight[key], key)
            
    # Print the report and ask for a grade
    print(reportText)
    print()
    grade = str(input('Assign a SLIP rating to this report (0 do not use/1 maybe use/2 definitely use): '))
    while grade != "0" and grade != "1" and grade != "2":
        grade = str(input('Invalid input. Assign a SLIP rating to this report (0 do not use/1 maybe use/2 definitely use): '))
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
# Iteratively show the user all of the SLIP/non SLIP example reports in a random order (training step 1)
# @param toHighlight A dictionary with str keys specifying a color to highlight the list of str text with
def readSampleReports(toHighlight = {}):

    # Initialize the client service
    client = bigquery.Client()
    
    # Get the SLIP and non-SLIP example reports
    getSlipExamples = 'SELECT * FROM lab.training_examples;'

    slipDf = client.query(getSlipExamples).to_dataframe()

    slipReportsList = [ row['narrative_text'] + '\n\nIMPRESSION: ' + str(row['impression_text']) + '\n\nReport given grade of ' + str(row['grade']) for i, row in slipDf.iterrows()]
        
    # Shuffle the list of all reports
    random.shuffle(slipReportsList)
    
    # Iteratively print each report
    for report in slipReportsList:
        # If the user passed a dictionary of lists to highlight
        if len(toHighlight.keys()) > 0:
            reportText = report
            for key in toHighlight.keys():
                reportText = markTextColor(reportText, toHighlight[key], key)
            
            # Print the report and ask for a grade
            print(reportText)
        else:
            print(report)
    
        print()
    
        confirm = str(input('After you read the report and understand its grade, press ENTER to continue to the next report.'))
        clear_output()
        
    print('You have finished reading the example reports. Rerun this cell to read them again or proceed to the next section.')
    
    
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
        reportText += ' IMPRESSION:' + reportDf['impression_text'].values[0]
    
    # If the user passed a dictionary of lists to highlight
    if len(toHighlight.keys()) > 0:
        for key in toHighlight.keys():
            reportText = markTextColor(reportText, toHighlight[key], key)
            
    # Print the report and ask for a grade
    print(reportText)
    print()
    grade = str(input('Assign a SLIP rating to this report (0 do not use/1 maybe use/2 definitely use): '))
    while grade != "0" and grade != "1" and grade != "2":
        grade = str(input('Invalid input. Assign a SLIP rating to this report (0 do not use/1 maybe use/2 definitely use): '))
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
    
    # Print out the grade and reason Jenna gave the report
    print()
    truthQuery = 'SELECT grade, reason from lab.training_selfeval WHERE report_id like "'+str(df['report_id'].values[0])
    truthQuery += '" and grader_name not like "'+name+'"'
    
    truthDf = client.query(truthQuery).to_dataframe()
    print("For reference, other graders have given this report the following grades for the specified reasons:")
    print()
    for idx, row in truthDf.iterrows():
        if int(row['grade']) != 999:
            print("Grade:", row['grade'], "For reason:", row['reason'])
    
    print()
    confirmContinue = str(input('Press enter to continue'))
          
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
        reportText += '\n\nIMPRESSION: ' + reportDf['impression_text'].values[0]
    
    # If the user passed a dictionary of lists to highlight
    if len(toHighlight.keys()) > 0:
        for key in toHighlight.keys():
            reportText = markTextColor(reportText, toHighlight[key], key)
            
    # Print the report and ask for a grade
    print(reportText)
    print()
    grade = str(input('Assign a SLIP rating to this report (0 do not use/1 maybe use/2 definitely use): '))
    while grade != "0" and grade != "1" and grade != "2":
        grade = str(input('Invalid input. Assign a SLIP rating to this report (0 do not use/1 maybe use/2 definitely use): '))
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
    
    query = 'select * from lab.training_selfeval where grader_name like"'+name+'"'
    df = client.query(query).to_dataframe()
    
    if len(df) == 0: # the person has not done the training self-eval
        print("Welcome,", name)
        print("It appears you have yet to complete the self-evaluation. The self-evaluation is intended for you to practice grading radiology reports youself.")
        print("After you enter a grade and a reason for that grade for the given report, the grades and reasons for that report by other users will be displayed.")
        print("Your grade and reason will be added to that list.")
        
        insertSelfEvalQuery = 'insert into lab.training_selfeval select '
        insertSelfEvalQuery += 'distinct report_id, 999 as grade, "'
        insertSelfEvalQuery += name + '" as grader_name, "missing" as reason '
        insertSelfEvalQuery += 'from lab.training_selfeval;'
        
        updateJob = client.query(insertSelfEvalQuery)
        updateJob.result()
        
        return False
        
    elif len(df[df['grade'] == 999]) > 0: # the person has not finished the training self-eval
        print("Welcome back,", name)
        getToRateSelfEvalQuery = 'select * from lab.training_selfeval where grader_name like "' +name+'" and grade = 999'
        selfEvalUnratedDf = client.query(getToRateSelfEvalQuery).to_dataframe()
        print("You currently have", len(selfEvalUnratedDf), "ungraded self-evaluation reports to work on.")
        
        return False
    
    query = 'select * from lab.grader_table where grader_name like "' + name + '"'
    
    df = client.query(query).to_dataframe()
        
    if len(df) == 0:
        print("Welcome,", name)
        print("...")
        
        for grade in range(3):
            print(grade)
            insertReliabilityQuery = "insert into lab.grader_table "
            insertReliabilityQuery += "select "
            insertReliabilityQuery += "distinct cast(proc_ord_id as int64), "
            insertReliabilityQuery += "'"+name+"' as grader_name, "
            insertReliabilityQuery += "'Reliability' as grade_category, "
            insertReliabilityQuery += "999 as grade "
            insertReliabilityQuery += "from lab.grader_table where "
            insertReliabilityQuery += "grader_name = 'Jenna Schabdach' and "
            insertReliabilityQuery += "grade_category = 'Reliability' and "
            insertReliabilityQuery += "grade = "+str(grade) + " "
            insertReliabilityQuery += "limit 50 ; "
        
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
                
    return True


##
# Add reports to grade for a user from a list of ids
# @param procIds A list of proc_ord_ids specified by the user
# @param name A string containing the identifier for the user
# @param maxToAdd An int specifying the maximum number of reports to add
def addReportsFromListForUser(procIds, name, maxToAdd=100):
    # Set up variables
    client = bigquery.Client()
    reportsInTableStatus = {}
    invalidIds = []
    addedReports = 0
    inTableReports = 0
    queryValidIdSql = "SELECT * from arcus.reports_annotations_master WHERE cast(proc_ord_id as int64) = "
    queryReportInTable = "SELECT * from lab.grader_table WHERE cast(proc_ord_id as int64) = "
    
    for procId in procIds:
        queryInsertReport = "INSERT into lab.grader_table (proc_ord_id, grader_name, grade_category, grade)"
        if addedReports >= maxToAdd:
            break
        
        # First check: is the id valid?
        dfValidId = client.query(queryValidIdSql+str(int(procId))+";").to_dataframe()
        
        if len(dfValidId) == 0:
            print("Error")
            invalidIds.append(procId)
            continue
            
        # Second check: is the id already in the report table?
        dfReportInTable = client.query(queryReportInTable+str(procId)+";").to_dataframe()
        print(queryReportInTable+str(procId)+";")
        if len(dfReportInTable) == 0:
            # add the report to the table for the u
            queryInsertReport += " VALUES (cast('"+str(procId)+"' as int64), '"+name+"', 'Unique', 999);"
            addReportJob = client.query(queryInsertReport)
            addReportJob.result()
            addedReports += 1
        elif len(dfReportInTable) == 1:
            inTableReports += 1
            grade = int(dfReportInTable['grade'].values[0])
            grader = dfReportInTable['grader_name'].values[0]
            print(dfReportInTable['grader_name'].values)
            # look at the grade
            if grade == 999:
                print(grader)
                if grader in list(reportsInTableStatus.keys()):
                    reportsInTableStatus[grader] += 1
                else:
                    reportsInTableStatus[grader] = 1
            else:
                if str(grade) in list(reportsInTableStatus.keys()):
                    reportsInTableStatus[str(grade)] += 1
                else:
                    reportsInTableStatus[str(grade)] = 1
        else:
            print("There is more than one entry for the specified id in the reports table")
            
            
    # Finished adding reports
    print(addedReports, "were added for", name, "from the provided list")
    print(len(invalidIds), "were invalid procedure order ids")
    print(inTableReports, "are already in the grader table:")
    for key in reportsInTableStatus.keys():
        if len(key) > 1:
            print(key, "has", reportsInTableStatus[key], "of these reports assigned to them")
        else:
            print(reportsInTableStatus[key], "reports have been rated", key)
            
            
# Main
if __name__ == "__main__":

    print("Radiology Report Annotation Helper Library v 0.2")
    print("Written and maintained by Jenna Young, PhD (@jmschabdach on Github)")
    print("Tested and used by:")
    print("- Caleb Schmitt, Summer 2021")
    print("- Nadia Ngom, Fall 2021 - Spring 2022")


