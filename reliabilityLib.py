import pandas as pd
import numpy as np
import random
from annotationHelperLib import *
from sklearn.metrics import cohen_kappa_score
from IPython.display import clear_output
from google.cloud import bigquery # SQL table interface on Arcus

##
# Get the proc_ord_id values for the reliability reports
# @return reliabilityReportProcIds A list of the 151 proc_ord_ids for the reliability reports
def getReliabilityProcOrdIds():
    tmp = pd.read_csv("~/arcus/shared/reliability_report_info.csv")
    procedureIds = tmp['proc_ord_id']
    return procedureIds    


def getReliabilityRatingsDf():
    # Initialize the client service
    client = bigquery.Client()

    reliabilityRatingsQuery = "select * from lab.grader_table_with_metadata where grade_category = 'Reliability';"
    reliabilityDf = client.query(reliabilityRatingsQuery).to_dataframe()
    reliabilityDf[['grade', 'proc_ord_id']] = reliabilityDf[['grade', 'proc_ord_id']].astype("int64")
    print(reliabilityDf.shape)


    # Pivot
    ratingsDf = pd.pivot_table(reliabilityDf, 
                               values="grade", 
                               index="proc_ord_id", 
                               columns="grader_name")

    colsToDrop = ['Jisoo Kang', 'Sreya Subramanian']
    ratingsDf = ratingsDf.drop(columns=colsToDrop)
    ratingsDf.reset_index(inplace=True)
    print(ratingsDf.shape)

    query = """
    with cte as (
      select
        distinct(proc_ord_id) as proc_ord_id,
        grader_name
      from
        lab.grader_table_with_metadata
      where
        grade_category = 'Reliability'
        and grader_name = 'Megan M. Himes'
    )
    select
      main.proc_ord_id
    from
      lab.grader_table_with_metadata main
      inner join cte on main.proc_ord_id = cte.proc_ord_id
    where
      main.grader_name = "Alesandra Gorgone"
    """

    reliabilityDf = client.query(query).to_dataframe()
    reliabilityIds = sorted(list(reliabilityDf['proc_ord_id'].values))[:-1]

    ratingsDf = ratingsDf[ratingsDf['proc_ord_id'].isin(reliabilityIds)]

    return ratingsDf

def identifyDisagreementReports(grades1, grades2):
    # Double check that the incoming dataframes are sorted the same way
    grades1 = grades1.sort_values("proc_ord_id", ignore_index=True)
    grades2 = grades2.sort_values("proc_ord_id", ignore_index=True) 
    
    # Triple check the order
    assert list(grades1['proc_ord_id'].values) == list(grades2['proc_ord_id'].values)
    
    shared = 0
    disagreementProcIds = [] 
    
    for idx, row in grades1.iterrows():
        procId = row['proc_ord_id']
        grades = [grades1.iloc[idx]['grade'],
                  grades2.iloc[idx]['grade']]
        if 999.0 in grades:
            continue
        else:
            shared += 1
            if (max(grades) - min(grades)) == 2:
                disagreementProcIds.append(procId)
                
    return disagreementProcIds

##
# For a pair of users with their grades in separate dataframes, compare them and calculate kappa
# @param userName1 A string with the user's name (taken from the db table)
# @param userGrades1 A dataframe with the proc_ord_id and corresponding grades for that graders reliability reports
# @param userName2 A string with the user's name (taken from the db table)
# @param userGrades2 A dataframe with the proc_ord_id and corresponding grades for that graders reliability reports
# @return kappa A float variable, Cohen's kappa
def compareUserPairGrades(userName1, userGrades1, userName2, userGrades2):
    # Double check that the incoming dataframes are sorted the same way
    userGrades1 = userGrades1.sort_values("proc_ord_id")
    userGrades2 = userGrades2.sort_values("proc_ord_id") 
    
    # Triple check the order
    assert list(userGrades1['proc_ord_id'].values) == list(userGrades2['proc_ord_id'].values)

    for grade in range(3):
        # Get the df of reports from the userReliabilityGrades table that are in the list
        user1GradeProcs = userGrades1[userGrades1['grade'] == grade]['proc_ord_id']
        print("Of the", len(user1GradeProcs), "reports", userName1, "gave a", grade, ",", userName2, "gave")
        # for each grade level
        for baseGrade in range(3):
            # Get the number of reports whose grades
            user2ProcGrades = userGrades2[userGrades2['proc_ord_id'].isin(user1GradeProcs)]
            count = user2ProcGrades[user2ProcGrades['grade'] == baseGrade].shape[0]
            print(count, "a grade of", baseGrade)
    print(" ")
            
    # Calculate Cohen's kappa
    kappa = cohen_kappa_score(list(userGrades1["grade"].values), list(userGrades2["grade"].values))
    print("With a Cohen's kappa of", kappa)
    return kappa


def calculateKappa2vAll(userGrades1, userGrades2):
    grades1 = userGrades1['grade'].values.astype(int)
    grades2 = userGrades2['grade'].values.astype(int)
    
    condenser = lambda x : 2 if x == 2 else 0
    
    grades1 = [condenser(i) for i in grades1]
    grades2 = [condenser(i) for i in grades2]
    
    kappa = cohen_kappa_score(grades1, grades2)
    
    print(" 2 vs. 0+1 kappa:", kappa)
    return kappa
    
def calculateKappa0vAll(userGrades1, userGrades2):
    grades1 = userGrades1['grade'].values.astype(int)
    grades2 = userGrades2['grade'].values.astype(int)
    
    condenser = lambda x : 0 if x == 0 else 2
    
    grades1 = [condenser(i) for i in grades1]
    grades2 = [condenser(i) for i in grades2]
    
    kappa = cohen_kappa_score(grades1, grades2)
    
    print(" 0 vs. 1+2 kappa:", kappa)
    return kappa
    
    
def getReportsForUser(user, procIds):
    client = bigquery.Client()
    
    getUserReports = "select cast(proc_ord_id as int64) as proc_ord_id, grade from lab.grader_table_with_metadata "
    getUserReports += "where grader_name = '"+user
    getUserReports += "' and grade_category = 'Reliability';"
    
    userReliabilityReports = client.query(getUserReports).to_dataframe()
    print(user)
    print(userReliabilityReports.shape)
    userReliabilityReports = userReliabilityReports[userReliabilityReports['proc_ord_id'].astype(int).isin(procIds)]
    print(userReliabilityReports.shape)
    
    return userReliabilityReports

def printReportFromProcOrdId(procOrdId):
    client = bigquery.Client()
    
    # Get the report for that proc_ord_id from the primary report table
    getReportRow = 'SELECT * FROM arcus.reports_annotations_master where proc_ord_id like "'+str(procOrdId)+'"'
    reportDf = client.query(getReportRow).to_dataframe()
    
    # If the id was in the original table:
    if len(reportDf) == 1:
        # Combine the narrative and impression text
        reportText = reportDf['narrative_text'].values[0] 
        if reportDf['impression_text'].values[0] != 'nan':
            reportText += '\n\nIMPRESSION: ' + reportDf['impression_text'].values[0]
            
    elif len(reportDf) == 0:
        getReportRow = 'SELECT * FROM arcus.procedure_order_narrative where proc_ord_id like "'+str(df['proc_ord_id'].values[0])+'"'
        reportText = client.query(getReportRow).to_dataframe()['narrative_text'].values[0]
        
        getReportRow = 'SELECT * FROM arcus.procedure_order_impression where proc_ord_id like "'+str(df['proc_ord_id'].values[0])+'"'
        reportDf = client.query(getReportRow).to_dataframe()
        
        if len(reportDf) == 1:
            reportText += "\n\nIMPRESSION: " + reportDf['impression_text'].values[0]
            
    print(reportText)
    
    
def printDisagreementReports(disagreementIds, grades1, grades2):
    for procId in disagreementIds:
        reportGrade1 = grades1[grades1['proc_ord_id'].astype(str) == str(procId)]['grade'].values[0]
        reportGrade2 = grades2[grades2['proc_ord_id'].astype(str) == str(procId)]['grade'].values[0]
        
        printReportFromProcOrdId(procId)
        print("\nThis report was given the grades of", reportGrade1, "by user 1 and", reportGrade2, "by user 2")
        
        confirmContinue = str(input('Press enter to continue'))
        clear_output()
    