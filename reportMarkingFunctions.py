import pandas as pd
import numpy as np
import random
from annotationHelperLib import *
from IPython.display import clear_output
from google.cloud import bigquery # SQL table interface on Arcus
from collections import Counter 

numUsersForValidation = 2
 
    
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
def markOneReportSQL(name, project, toHighlight = {}):

    # Initialize the client service
    client = bigquery.Client()
    
    # Get a row from the grader table for the specified rater that has not been graded yet - start with Reliability
    getSingleRowQuery = 'SELECT * FROM lab.grader_table_with_metadata WHERE grader_name = "' + name + '" and grade = 999 and grade_category = "Reliability" LIMIT 1'
    df = client.query(getSingleRowQuery).to_dataframe()
    
    if len(df) == 0:
        # Get a row from the grader table for the specified rater that has not been graded yet - if no Reliability, then Unique
        getSingleRowQuery = 'SELECT * FROM lab.grader_table_with_metadata WHERE grader_name = "' + name + '" and grade = 999 and grade_category = "Unique" LIMIT 1'
        df = client.query(getSingleRowQuery).to_dataframe()
        
    
    if len(df) == 0:
        print("There are currently no reports to grade for", name, " in the table. Please add more to continue.")
        return
    
    # Get the report for that proc_ord_id from the primary report table
    getReportRow = 'SELECT * FROM arcus.procedure_order where proc_ord_id = "'+str(df['proc_ord_id'].values[0])+'"'
    reportDf = client.query(getReportRow).to_dataframe()
    
    # If the id was in the new table:
    if len(reportDf) == 1:
        originTable = "arcus.procedure_order"
        
        getReportRow = 'SELECT * FROM arcus.procedure_order_narrative where proc_ord_id = "'+str(df['proc_ord_id'].values[0])+'"'
        reportText = client.query(getReportRow).to_dataframe()['narrative_text'].values[0]
        
        getReportRow = 'SELECT * FROM arcus.procedure_order_impression where proc_ord_id = "'+str(df['proc_ord_id'].values[0])+'"'
        reportDf = client.query(getReportRow).to_dataframe()
        
        if len(reportDf) == 1:
            reportText += "\n\nIMPRESSION: " + reportDf['impression_text'].values[0]
            
    elif len(reportDf) == 0:
        getReportRow = 'SELECT * FROM arcus.reports_annotations_master where proc_ord_id = "'+str(df['proc_ord_id'].values[0])+'"'
        reportDf = client.query(getReportRow).to_dataframe()
        
        if len(reportDf) > 0: 
            originTable = "arcus.reports_annotations_master"
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
    
    # Ask the user to confirm the grade
    confirmGrade = "999"
    while confirmGrade != grade :
        while confirmGrade != "0" and confirmGrade != "1" and confirmGrade != "2":
            confirmGrade = str(input("Please confirm your grade by reentering it OR enter a revised value to change the grade: "))
        if confirmGrade != grade:
            grade = confirmGrade
            confirmGrade = "999"
            
    print("Saving your grade of", grade, "for this report.")
    
    # LOH - do more changes need to be made here to change the metadata in the table? I think no but ...
    # Update the grader table with the new grade
    updateQuery = 'UPDATE lab.grader_table_with_metadata set grade = '+str(grade)
    updateQuery += ' WHERE proc_ord_id = "'+str(df['proc_ord_id'].values[0])+'"' 
    updateQuery += ' and grader_name like "' + name + '"'

    updateJob = client.query(updateQuery)
    updateJob.result()
    print("Grade saved. Run the cell again to grade another report.")
    
##
# Get more proc_ord_id for which no reports have been rated for the specified user to grade
# @param name A str containing the full name of the grader (to also be referenced in publications)
def getMoreReportsToGrade(name, project="SLIP", queryFn="./queries/slip_base.txt", numberToAdd=50):
    # Global var declaration
    global numUsersForValidation
    print("It is expected for this function to take several minutes to run. Your patience is appreciated.")
    
    # Initialize the client service
    client = bigquery.Client()
    
    # Open the specified query file
    with open(queryFn, 'r') as f:
        qProject = f.read()
    # Run the query from the specified file
    dfProject = client.query(qProject).to_dataframe()
    # Now we have the ids of the reports we want to grade for Project project
    projectProcIds = dfProject['proc_ord_id'].values 
    
    # Get the proc_ord_ids from the grader table
    qGradeTable = "SELECT proc_ord_id, grader_name from lab.grader_table_with_metadata where grade_category='Unique'; "
    dfGradeTable = client.query(qGradeTable).to_dataframe()
    gradeTableProcIds = dfGradeTable['proc_ord_id'].values
    userProcIds = dfGradeTable[dfGradeTable['grader_name'] == name]['proc_ord_id'].values
    
    # Validation: are there any reports for the project that need to be validated that name hasn't graded?
    toAddValidation = []
    for procId in projectProcIds: # for each proc_id in the project
        if procId in dfGradeTable['proc_ord_id'].values: # if the proc_id report was already graded
            graders = dfGradeTable.loc[dfGradeTable['proc_ord_id'] == procId, "grader_name"].values
            gradersStr = ", ".join(graders)
            # if the report was not graded by Coarse Text Search or the user and has not been graded N times
            if "Coarse Text Search" not in gradersStr and name not in gradersStr and len(graders) < numUsersForValidation:
                toAddValidation.append(procId)   
            
    # projectReportsInTable = [procId for procId in projectProcIds if procId in dfGradeTable['proc_ord_id'].values and not dfGradeTable.loc[dfGradeTable['proc_ord_id'] == procId, "grader_name"].str.contains("Coarse Text Search").any() ]
    # Ignore procIds rated by User name
    # toAddValidation = [procId for procId in procIdsNeedValidation if procId not in userProcIds][:numberToAdd]
    
    # Add validation reports - procIds already in the table
    if len(toAddValidation) > 0:
        addReportsQuery = 'insert into lab.grader_table_with_metadata (proc_ord_id, grader_name, grade, grade_category, pat_id, age_in_days, proc_ord_year, proc_name, report_origin_table, project) VALUES '
        for procId in toAddValidation:
            row = dfProject[dfProject['proc_ord_id'] == procId]
            addReportsQuery += '("'+str(procId)+'", "'+name+'", 999, "Unique", "'
            addReportsQuery += row['pat_id'].values[0]+'", '+str(row['proc_ord_age'].values[0])
            addReportsQuery += ', '+str(row['proc_ord_year'].values[0])+', "'+str(row['proc_ord_desc'].values[0].replace("'", "\'"))
            addReportsQuery += '", "arcus.procedure_order", "'+project+'"), '
        addReportsQuery = addReportsQuery[:-2]+";"
        addingReports = client.query(addReportsQuery)
        addingReports.result()

    
    # New reports
    toAddNew = [procId for procId in projectProcIds if procId not in dfGradeTable['proc_ord_id'].values][:(numberToAdd - len(toAddValidation))]
    
    # Add new reports
    if len(toAddNew) > 0:
        addReportsQuery = 'insert into lab.grader_table_with_metadata (proc_ord_id, grader_name, grade, grade_category, pat_id, age_in_days, proc_ord_year, proc_name, report_origin_table, project) VALUES '
        for procId in toAddNew:
            row = dfProject[dfProject['proc_ord_id'] == procId]
            addReportsQuery += '("'+str(procId)+'", "'+name+'", 999, "Unique", "'
            addReportsQuery += row['pat_id'].values[0]+'", '+str(row['proc_ord_age'].values[0])
            addReportsQuery += ', '+str(row['proc_ord_year'].values[0])+', "'+str(row['proc_ord_desc'].values[0].replace("'", "\'"))
            addReportsQuery += '", "arcus.procedure_order", "'+project+'"), '
        addReportsQuery = addReportsQuery[:-2]+";"
        addingReports = client.query(addReportsQuery)
        addingReports.result()
    
    # Check: how many reports were added for the user?
    getUserUnratedCount = 'SELECT * FROM lab.grader_table_with_metadata WHERE grader_name like "' + name + '" and grade = 999'

    df = client.query(getUserUnratedCount).to_dataframe()
    
    # Inform the user
    print(len(df), "reports were added for grader", name)
    
    
def welcomeUser(name):
    print("Welcome,", name)
   
    client = bigquery.Client()
    
    # Possibly pull this bit into its own function - make it user proof
    qCheckSelfEval = 'select * from lab.training_selfeval where grader_name like"'+name+'"'
    selfEvalDf = client.query(qCheckSelfEval).to_dataframe()
    
    if len(selfEvalDf) == 0:
        print("It appears you have yet to do the self-evaluation. Please grade those reports before continuing.")
        # break
        
    elif 999 in selfEvalDf['grade'].values:
        print("It appears you have started the self-evaluation but have not finished it. Please grade those reports before continuing.")
        # break
    
    qReliability = 'select * from lab.grader_table_with_metadata where grade_category = "Reliability" and grader_name like"'+name+'"'
    reliabilityDf = client.query(qReliability).to_dataframe()
           
    if not checkReliabilityRatings(reliabilityDf):       
        print("It appears you have yet to grade the reliability reports.")
        addReliabilityReports(name)
            
    elif 999 in reliabilityDf['grade'].values:
        reliabilityCount = len(reliabilityDf[reliabilityDf['grade'] == 999])
        print("You have", reliabilityCount, "reliability reports to grade.")
    
    else:
        getToRateCount = 'select * from lab.grader_table_with_metadata where grader_name like "'
        getToRateCount += name + '" and grade = 999'
        
        raterUnratedDf = client.query(getToRateCount).to_dataframe()
              
        if len(raterUnratedDf) == 0:
              print("You are caught up on your report ratings")
              # TODO add function here to get more reports for the user
        else:
              print("You currently have", len(raterUnratedDf), "ungraded reports to work on.")
                
    return True

            
def addReliabilityReports(name):
    client = bigquery.Client()
    
    # Get the grader table
    queryGetGraderTable = "SELECT * from lab.grader_table_with_metadata where grader_name = '"+name+"' and grade_category = 'Reliability';"
    graderDf = client.query(queryGetGraderTable).to_dataframe()
    
    reliabilityDf = pd.read_csv("~/arcus/shared/reliability_report_info.csv")
    addReports = False

    queryInsertReport = "INSERT into lab.grader_table_with_metadata (proc_ord_id, grader_name, grade, grade_category, pat_id, age_in_days, proc_ord_year, proc_name, report_origin_table, project) VALUES"
    
    # print(graderDf['proc_ord_id'].values)

    for idx, row in reliabilityDf.iterrows():
        # print(row['proc_ord_id'])
        if str(row['proc_ord_id']) not in graderDf['proc_ord_id'].values:
            # Add the report
            queryInsertReport += " ('"+str(int(row['proc_ord_id']))+"', '"+name+"', 999, 'Reliability', '"
            queryInsertReport += row['pat_id']+"', "+str(row['age_in_days'])+", "+str(row['proc_ord_year'])+", '"
            queryInsertReport += row['proc_name']+"', '"+row['report_origin_table']+"', '"+row['project']+"'),"
            addReports = True

    if addReports:
        print("Adding reliability reports to grade")
        queryInsertReport = queryInsertReport[:-1] + ";"
        # print(queryInsertReport)
        addReportJob = client.query(queryInsertReport)
        addReportJob.result()
        
            
def checkReliabilityRatings(graderDf):
        
    if len(graderDf) == 0:
        return False
    
    name = graderDf['grader_name'].values[0]
    reliabilityDf = pd.read_csv("~/arcus/shared/reliability_report_info.csv")
    reliabilityIds = reliabilityDf['proc_ord_id'].values
    graderReliabilityDf = graderDf[graderDf['grade_category'] == 'Reliability']
    graderIds = graderReliabilityDf['proc_ord_id'].values
    numReliability = len([i for i in reliabilityIds if str(i) in graderIds])
    # numGradedReliability = len(graderReliabilityDf[graderReliabilityDf['grade'] != 999]['proc_ord_id'].values)
    numGradedReliability = len([i for i in reliabilityIds if str(i) in graderIds and max(graderReliabilityDf[graderReliabilityDf['proc_ord_id'] == str(i)]['grade'].values) != 999]) 
    
    print(numReliability)
    print(len(reliabilityIds))
    assert numReliability == len(reliabilityIds)
    print(name, "has graded", numGradedReliability, "of", numReliability, "reliability reports")
    
    if numGradedReliability == numReliability:
        return True
    elif numGradedReliability < numReliability:
        return False
    else:
        print("Error (code surplus): Grader has graded more reliability reports than exist")
        

def getGraderStatusReport(name):
    client = bigquery.Client()
    
    query = "select * from lab.grader_table_with_metadata where "
    query += "grader_name = '"+ name +"';"
    
    df = client.query(query).to_dataframe()
    
    # Case: user not in table
    if len(df) == 0:
        print("User is not in the table yet.")
        return
    
    # Reliability ratings
    checkReliabilityRatings(df)
    
    # Unique
    uniqueReportsDf = df[df['grade_category'] == 'Unique']
    gradedUniqueReportsDf = df[(df['grade_category'] == 'Unique') & (df['grade'] != 999)]
    print(name, "has graded", gradedUniqueReportsDf.shape[0], "unique reports of", uniqueReportsDf.shape[0], "assigned where")
    for grade in range(3):
        numGraded = gradedUniqueReportsDf[gradedUniqueReportsDf['grade'] == grade].shape[0]
        print(numGraded, "have been given a grade of", grade)
        
            
# Main
if __name__ == "__main__":

    print("Radiology Report Annotation Helper Library v 0.2")
    print("Written and maintained by Jenna Young, PhD (@jmschabdach on Github)")
    print("Tested and used by:")
    print("- Caleb Schmitt, Summer 2021")
    print("- Nadia Ngom, Fall 2021 - Spring 2022")
    print("- Alesandra Gorgone, Spring 2023")

