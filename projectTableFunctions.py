import pandas as pd
import numpy as np
import random
import os
from IPython.display import clear_output
from google.cloud import bigquery # SQL table interface on Arcus
from dxFilterLibraryPreGrading import *
from reportMarkingFunctions import *
import json
import matplotlib.pyplot as plt


req_table = "lab.requested_sessions_main_with_metadata_project_independent"
grader_table = "lab.grader_table_with_metadata_project_independent"


##
#

def load_project(project_name, name):
    if project_name != "AUTO":
        project_id = project
    else:
        try:
            with open(os.path.expanduser("~/arcus/shared/annotation-helper-tools/.auto_control.json"), 'r') as file:
                project_assign = json.load(file)
                project_id = project_assign[name]
        except KeyError:
            raise KeyError(f"Grader {name} not found in .auto_control.json. Please manually specify your project instead of using AUTO.")
    print(f"Project: {project_id}")
    return(project_id)

def load_cohort_config(project_id, field):
    fn = "./queries/config.json" 
    with open(fn, "r") as f:
        project_lookup = json.load(f)

    # Get the info for the specified project
    project_info = project_lookup[project_id]
    if field == "query":
        query_fn = project_info["query"]
        q_dx_filter = ""
        if "dx_filter" in project_info:
            # Get the name of the dx filter file
            fn_dx_filter = project_info["dx_filter"]
            # Expand the tilda for each user
            fn_dx_filter_full = os.path.expanduser(fn_dx_filter)
            # Convert the contents of the dx filter file to a sql query
            q_dx_filter = convert_exclude_dx_csv_to_sql(fn_dx_filter_full)
    
        ## --- I think this was put into a function?
        # Open the specified query file
        with open(query_fn, "r") as f:
            q_project = f.read()
    
        # If there is a dx filter, incorporate it into the loaded query
        if q_dx_filter != "":
            q_tmp = q_dx_filter + q_project.split("where")[0]
            q_tmp += "left join exclude_table on proc_ord.pat_id = exclude_table.pat_id where exclude_table.pat_id is null and"
            q_tmp += q_project.split("where")[1]
            q_project = q_tmp
    
        return q_project

    elif field == "grade_criteria":
        return project_info['grade_criteria']


def add_reports_to_project(cohort):
    # Set up the client
    client = bigquery.Client()
    # Load the query for the cohort
    q_cohort_to_add = load_cohort_config(cohort, 'query')
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

    # Load the config
    criteria = load_cohort_config(cohort, "grade_criteria")
    
    # Get the number of reports with the project label in the grader table
    q_graded_reports = 'select distinct reports.* from lab.grader_table_with_metadata_project_independent reports join lab.proc_ord_projects projects on (reports.proc_ord_id = projects.proc_ord_id and reports.pat_id = projects.pat_id) where projects.project = "'+cohort+'" and grade_criteria = "'+criteria+'";'
    df_graded_reports = client.query(q_graded_reports).to_dataframe()
    
    # Print info
    print("Project:", cohort)
    print("Total reports:", len(df_project_reports), "(note each report must be graded by 2 graders)")
    print("Graded 0:", len(df_graded_reports[df_graded_reports['grade'] == 0]))
    print("Graded 1:", len(df_graded_reports[df_graded_reports['grade'] == 1]))
    print("Graded 2:", len(df_graded_reports[df_graded_reports['grade'] == 2]))
    print("Queued:", len(df_graded_reports[df_graded_reports['grade'] == 999]))
    print("Skipped:", len(df_graded_reports[df_graded_reports['grade'] < 0]))


## Main function for plotting the age at scan for any cohort
# @param cohort str
# @param color_by list
# @param only_requested boolean
def plot_age_at_scan(cohort, color_by=[], only_requested=False):
    df_cohort = get_unique_report_data(cohort, only_requested)

    title = "Histogram of Age At Scan for Distinct "+cohort
    # If only requested
    if only_requested:
        title += " (Requested Only, N = "+str(len(df_cohort))+")"
    else:
        title += " (All Graded, N = "+str(len(df_cohort))+")"

    # Set up the figure
    fig, ax = plt.subplots(1, 1, tight_layout=True)
    
    # Check the color_by arg
    if "sex" in color_by and "grade" in color_by:
        # Add a column to the table with the combined sex/grade info
        print("Histograms for both sex and grade not yet available")
        return
    elif "grade" in color_by:
        grades = list(set(df_cohort['avg_grade_group'].values))
        _, bin_edges = np.histogram(df_cohort['age_in_years'], 50)
        for grade in sorted(grades):
            ax.hist(df_cohort[df_cohort['avg_grade_group'] == grade]['age_in_years'], 
                              bin_edges, histtype='bar', 
                              stacked=True, label=str(grade))
        plt.legend()

    elif "sex" in color_by:
        sexes = list(set(df_cohort['sex'].values))
        _, bin_edges = np.histogram(df_cohort['age_in_years'], 50)
        for sex in sorted(sexes):
            ax.hist(df_cohort[df_cohort['sex'] == sex]['age_in_years'], 
                              bin_edges, histtype='bar', 
                              stacked=True, label=str(sex))
        plt.legend()
        
    else:
        ax.hist(df_cohort['age_in_years'], bins=50)
        
    plt.title(title)
    plt.xlabel("Age at Scan (years)")
    plt.ylabel("Count")
    plt.grid(visible=True)
    plt.show()

# A helper function to get the subset of unique graded reports for a given cohort
def get_unique_report_data(cohort, only_requested):
    # Set up the client
    client = bigquery.Client()
    global grader_table
    global req_table
    
    # Create the query to get the cohort info
    q_cohort = 'select grader.*, pat.sex from '+grader_table+' grader'
    q_cohort += ' join lab.proc_ord_projects projects on grader.proc_ord_id = projects.proc_ord_id'
    q_cohort += ' join arcus.procedure_order proc on projects.proc_ord_id = proc.proc_ord_id'
    q_cohort += ' join arcus.patient pat on pat.pat_id = proc.pat_id'
    # If only requested
    if only_requested:
        q_cohort += ' join '+req_table+' req on req.proc_ord_id = grader.proc_ord_id'
    # Add the project condition
    q_cohort += ' where projects.project = "'+cohort+'"'
    q_cohort += ' and grade >= 0 and grade <= 2'
    q_cohort += ' and grader.grade_category = "Unique"'
    q_cohort += ' order by grader.proc_ord_id, grader.pat_id desc'

    # Execute the query
    df_cohort = client.query(q_cohort).to_dataframe()

    # Drop duplicates
    df_cohort = df_cohort.drop_duplicates()
    
    # Add an age in years column
    df_cohort['age_in_years'] = df_cohort['age_in_days']/365.25

    # Get the average grade for each report
    proc_ids = list(set(df_cohort['proc_ord_id'].values))
    df_cohort['avg_grade_group'] = 999.0
    df_cohort['avg_grade'] = 999.0
    for proc_id in proc_ids:
        df_cohort.loc[df_cohort['proc_ord_id'] == proc_id, 'avg_grade'] = np.mean(df_cohort[df_cohort['proc_ord_id'] == proc_id]['grade'].values)
        df_cohort.loc[df_cohort['proc_ord_id'] == proc_id, 'avg_grade_group'] = np.floor(np.mean(df_cohort[df_cohort['proc_ord_id'] == proc_id]['grade'].values))

    # Get only the subset of columns we care about
    cols = ['proc_ord_id', 'age_in_days', 'age_in_years', 'proc_ord_year', 'avg_grade', 'avg_grade_group', 'sex']
    df_cohort = df_cohort[cols].drop_duplicates()

    return df_cohort