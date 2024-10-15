import pandas as pd
import numpy as np
import random
import os
from IPython.display import clear_output
from google.cloud import bigquery # SQL table interface on Arcus
from dxFilterLibraryPreGrading import *
from reportMarkingFunctions import *
import json


def add_reports_to_project(cohort):
    # Set up the client
    client = bigquery.Client()
    # Load the query for the cohort
    q_cohort_to_add = load_cohort_config(cohort)
    # Get all the reports in the cohort
    df_cohort_to_add = client.query(q_cohort_to_add).to_dataframe()
    
    # Get all reports labelled as belonging to the cohort
    q_existing_cohort = 'select * from lab.proc_ord_projects where project = "'+cohort+'";'
    df_existing_cohort = client.query(q_existing_cohort).to_dataframe()
    
    # Get any reports for the cohort not currently labeled
    df_joint = df_cohort_to_add.merge(df_existing_cohort.drop_duplicates(),
                                      on=['pat_id', 'proc_ord_id'],
                                      how='left', indicator=True)
    df_cohort_new = df_joint[df_joint['_merge'] == 'left_only']

    # If there are reports to add to the cohort project table
    if df_cohort_new.shape[0] > 0:
        # Create the query to add reports to the project table
        q_insert_projects = 'insert into lab.proc_ord_projects (proc_ord_id, pat_id, project) VALUES '
        for idx, row in df_cohort_new.iterrows():
            q_insert_projects += '("'+row['proc_ord_id']+'", "'+row['pat_id']+'", "'+cohort+'"), '
        
        q_insert_projects = q_insert_projects[:-2]+";"
        
        # Run the insertion query
        j_insert_projects = client.query(q_insert_projects)
        j_insert_projects.result()


def get_project_report_stats(cohort):
    # Set up the client
    client = bigquery.Client()
    # Get the number of reports with the project label
    q_project_reports = 'select * from lab.proc_ord_projects where project = "'+cohort+'"'
    df_project_reports = client.query(q_project_reports).to_dataframe()
    # Get the number of reports with the project label in the grader table
    q_graded_reports = 'select distinct reports.* from lab.test_grader_table_with_metadata reports join lab.proc_ord_projects projects on (reports.proc_ord_id = projects.proc_ord_id and reports.pat_id = projects.pat_id) where projects.project = "'+cohort+'"'
    df_graded_reports = client.query(q_graded_reports).to_dataframe()
    # Print info
    print("Project:", cohort)
    print("Total reports:", len(df_project_reports), "(note each report must be graded by 2 graders)")
    print("Graded 0:", len(df_graded_reports[df_graded_reports['grade'] == 0]))
    print("Graded 1:", len(df_graded_reports[df_graded_reports['grade'] == 1]))
    print("Graded 2:", len(df_graded_reports[df_graded_reports['grade'] == 2]))
    print("Queued:", len(df_graded_reports[df_graded_reports['grade'] == 999]))
    print("Skipped:", len(df_graded_reports[df_graded_reports['grade'] < 0]))