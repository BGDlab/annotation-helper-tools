import pandas as pd
import numpy as np
import os
import random
import json
from annotationHelperLib import *
from dxFilterLibraryPreGrading import *
from IPython.display import clear_output
from google.cloud import bigquery  # SQL table interface on Arcus
from datetime import date

num_validation_graders = 2
grader_table_name = "lab.test_grader_table_with_metadata"
project_table_name = "lab.proc_ord_projects"


##
# Back up lab.grader_table_with_metadata. Can be run on its own
# or within another function
def backup_grader_table():
    client = bigquery.Client()
    # Step 1: save the grader_table_with_metadata to a .csv
    tmp_dir = os.path.expanduser("~/arcus/shared/.backups/")
    if not os.path.exists(tmp_dir):
        os.makedirs(tmp_dir)
    tmp_csv = os.path.join(tmp_dir, "lab_grader_table_with_metadata.csv")
    get_table_query = "select * from lab.grader_table_with_metadata"
    grader_table = client.query(get_table_query).to_dataframe()
    grader_table.to_csv(tmp_csv, index=False)

    # Step 2: drop table lab.bak_grader_table_with_metadata
    q_drop_table = "drop table lab.bak_grader_table_with_metadata"
    job = client.query(q_drop_table)
    job.result()

    # Step 3: create table lab.bak_grader_table_with_metadata
    q_create_backup_table = "create table lab.bak_grader_table_with_metadata as select * from lab.grader_table_with_metadata"
    job = client.query(q_create_backup_table)
    job.result()
    print("lab.grader_table_with_metadata backup successful")


##
# Regrade skipped reports
# @param client A bigquery client object
# @param project_name A string used to identify the project
# @param grader A string of the grader's name (leave blank to review all flagged reports)
# @param flag The level of "skip" to examine (-1 is group, -2 is clinician)
def regrade_skipped_reports(client, project_name="", grader="", flag=-1):
    # Get the flagged reports
    if grader == "":
        q = "select * from lab.grader_table_with_metadata where grade = " + str(flag)
    else:
        q = (
            "select * from lab.grader_table_with_metadata where grade = "
            + str(flag)
            + " and grader_name = '"
            + grader
            + "'"
        )

    if project_name != "":
        print("Examining the skipped reports for", project_name)
        q += " and project like '%" + project_name + "%'"

    q += ";"
    flagged_reports = client.query(q).to_dataframe()

    if flagged_reports.shape[0] == 0:
        print("There are currently no reports with the grade of", flag)

    # Shuffle the flagged reports
    flagged_reports = flagged_reports.sample(frac=1)

    # for each flagged report
    count = 0
    for idx, row in flagged_reports.iterrows():
        clear_output()
        count += 1
        print(str(count) + "/" + str(len(flagged_reports)))
        print()
        # Add a print to show why the report was previously flagged
        # Check if the report is in the lab.skipped_reports table
        check_skipped_query = (
            "select * from lab.skipped_reports where proc_ord_id = '"
            + str(row["proc_ord_id"])
        )
        check_skipped_query += "' and grader_name = '" + row["grader_name"] + "';"
        skipped_df = client.query(check_skipped_query).to_dataframe()

        print("Grader: ", row["grader_name"])

        is_skip_logged = False
        if len(skipped_df) == 1:
            is_skip_logged = True

        if is_skip_logged:
            print("Reason report was flagged:", skipped_df["skip_reason"].values[0])
        else:
            print("Skipped reason not available.")

        # Print the report
        proc_ord_id = row["proc_ord_id"]

        print("Projects:", row["project"])
        print("Year of scan:", row["proc_ord_year"])
        print("Age at scan:", np.round(row["age_in_days"] / 365.25, 2), "years")
        proc_ord_id = row["proc_ord_id"]
        with open("phrases_to_highlight.json", "r") as f:
            to_highlight = json.load(f)
        print_report(proc_ord_id, client, to_highlight)

        print()
        # ask for grade
        grade = get_grade(enable_md_flag=True)
        print(grade)

        if grade != -1 or grade != -2:
            regrade_reason = get_reason("regrade")

            # Update the grader table with the new grade
            q_update = "UPDATE lab.grader_table_with_metadata set grade = " + str(
                grade
            )
            q_update += ' WHERE proc_ord_id = "' + str(proc_ord_id) + '"'
            q_update += ' and grader_name = "' + row["grader_name"] + '"'

            j_update = client.query(q_update)
            j_update.result()

            if is_skip_logged:
                # Update the skipped reports table
                q_update_skipped = "update lab.skipped_reports set grade = " + str(
                    grade
                )
                q_update_skipped += ', regrade_reason = "' + regrade_reason + '" '
                q_update_skipped += (
                    'where proc_ord_id = "' + str(proc_ord_id) + '" and '
                )
                q_update_skipped += 'grader_name = "' + row["grader_name"] + '";'

                j_update_skipped = client.query(q_update)
                j_update_skipped.result()
            else:
                # Add the report to the skipped reports table.
                # ('proc_ord_id', 'grade', 'grader_name', 'skip_date', 'skip_reason', 'regrade_date', 'regrade_reason')
                q_skip_report = "insert into lab.skipped_reports values ("
                today = date.today().strftime("%Y-%m-%d")
                q_skip_report += (
                    "'"
                    + str(proc_ord_id)
                    + "',"
                    + str(grade)
                    + ", '"
                    + row["grader_name"]
                    + "', '', '', '"
                    + today
                    + "', '"
                    + regrade_reason
                    + "');"
                )

            print("New grade saved. Run the cell again to grade another report.")


##
# Print a count of the number of reports graded by each grader since date d
# @param d A string representation of the date in YYYY-MM-DD format
def get_grade_counts_since(d):
    client = bigquery.Client()

    # Query the table
    q = (
        'select * from lab.grader_table_with_metadata where grade_date != "0000-00-00" and cast(grade_date as date) >= cast("'
        + d
        + '" as date);'
    )
    df = client.query(q).to_dataframe()

    # Get the count of rows for each grader
    graders = list(set(df["grader_name"].values))

    # Print the table header
    print("# Reports \t Grader Name")

    # Print the rows for each grader
    for grader in graders:
        print(len(df[df["grader_name"] == grader]), "\t\t", grader)

    # Print a statement about who has not graded any reports
    print()
    print(
        "Any graders not in the displayed table have not graded any reports since before "
        + d
    )


##
# Iteratively show the user all of the SLIP/non SLIP example reports in a random order (training step 1)
# @param to_highlight A dictionary with str keys specifying a color to highlight the list of str text with
def read_sample_reports(to_highlight={}):
    # Initialize the client service
    client = bigquery.Client()

    # Get the SLIP and non-SLIP example reports
    get_slip_examples = "SELECT * FROM lab.training_examples;"

    df_slip = client.query(get_slip_examples).to_dataframe()

    slip_reports = [
        row["narrative_text"]
        + "\n\nIMPRESSION: "
        + str(row["impression_text"])
        + "\n\nReport given grade of "
        + str(row["grade"])
        for i, row in df_slip.iterrows()
    ]

    # Shuffle the list of all reports
    random.shuffle(slip_reports)

    # Iteratively print each report
    for report in slip_reports:
        # If the user passed a dictionary of lists to highlight
        if len(to_highlight.keys()) > 0:
            report_text = report
            for key in to_highlight.keys():
                report_text = mark_text_color(report_text, to_highlight[key], key)

            # Print the report and ask for a grade
            print(report_text)
        else:
            print(report)

        print()

        confirm = str(
            input(
                "After you read the report and understand its grade, press ENTER to continue to the next report."
            )
        )
        clear_output()

    print(
        "You have finished reading the example reports. Rerun this cell to read them again or proceed to the next section."
    )


##
# Add reports for the user to grade for the self-eval
# @param name A str containing the full name of the grader (to also be referenced in publications)
def add_self_eval_reports(name):
    client = bigquery.Client()

    q_get_selfeval = "select distinct report_id from lab.training_selfeval;"
    df_self_eval = client.query(q_get_selfeval).to_dataframe()
    report_ids = df_self_eval["report_id"].values

    q_insert_report = "INSERT into lab.training_selfeval (report_id, grade, grader_name, reason) VALUES"

    for report in report_ids:
        q_insert_report += " ('" + str(report) + "', 999, '" + name + "', ' '),"

    q_insert_report = q_insert_report[:-1] + ";"
    print(
        "Adding "
        + str(len(report_ids))
        + " self-evaluation reports for "
        + name
        + " to grade."
    )
    j_add_report = client.query(q_insert_report)
    j_add_report.result()


##
# Pull the report associated with a proc_ord_id for which the specified grader has a grade of 999, and then grade the report. Modifies lab.grader_table
# @param name A str containing the full name of the grader (to also be referenced in publications)
# @param to_highlight A dictionary with str keys specifying a color to highlight the list of str text with
def mark_selfeval_report_sql(name, to_highlight={}):
    # Initialize the client service
    client = bigquery.Client()

    # Get a row from the grader table for the specified rater that has not been graded yet
    q_get_single_row = (
        'SELECT * FROM lab.training_selfeval WHERE grader_name like "'
        + name
        + '" and grade = 999 LIMIT 1'
    )

    df = client.query(q_get_single_row).to_dataframe()

    if len(df) == 0:
        print(
            "There are currently no reports to grade for",
            name,
            " in the table. You have completed the self-evaluation.",
        )
        return

    # Get the report for that proc_ord_id from the primary report table
    q_get_report_row = (
        'SELECT * FROM arcus_2023_05_02.reports_annotations_master where combo_id = "'
        + str(df["report_id"].values[0])
        + '"'
    )
    df_report = client.query(q_get_report_row).to_dataframe()
    print(df_report.shape)
    print(list(df_report))

    # Combine the narrative and impression text
    report_text = df_report["narrative_text"].values[0]
    if df_report["impression_text"].values[0] != "nan":
        report_text += " IMPRESSION:" + df_report["impression_text"].values[0]

    # If the user passed a dictionary of lists to highlight
    if len(to_highlight.keys()) > 0:
        for key in to_highlight.keys():
            report_text = mark_text_color(report_text, to_highlight[key], key)

    # Print the report and ask for a grade
    print(report_text)
    print()
    grade = str(
        input(
            "Assign a SLIP rating to this report (0 do not use/1 maybe use/2 definitely use): "
        )
    )
    while grade != "0" and grade != "1" and grade != "2":
        grade = str(
            input(
                "Invalid input. Assign a SLIP rating to this report (0 do not use/1 maybe use/2 definitely use): "
            )
        )
    print()

    # Update the grader table with the new grade
    q_update = "UPDATE lab.training_selfeval set grade = " + str(grade)
    q_update += ' WHERE report_id like "' + str(df["report_id"].values[0]) + '"'
    q_update += ' and grader_name like "' + name + '"'

    j_update = client.query(q_update)
    j_update.result()

    # Ask for a reason the report was given the grade it was
    reason = str(input("Why does this report get that grade? "))
    print()

    # Update the grader table with the new grade
    q_update = 'UPDATE lab.training_selfeval set reason="' + reason + '"'
    q_update += ' WHERE report_id like "' + str(df["report_id"].values[0]) + '"'
    q_update += ' and grader_name like "' + name + '"'

    j_update = client.query(q_update)
    j_update.result()

    # Print out the grade and reason others gave the report
    print()
    q_others = (
        'SELECT grade, reason from lab.training_selfeval WHERE report_id like "'
        + str(df["report_id"].values[0])
    )
    q_others += '" and grader_name not like "' + name + '"'

    df_truth = client.query(q_others).to_dataframe()
    print(
        "For reference, other graders have given this report the following grades for the specified reasons:"
    )
    print()
    for idx, row in df_truth.iterrows():
        if int(row["grade"]) != 999:
            print("Grade:", row["grade"], "For reason:", row["reason"])

    print()
    confirm_continue = str(input("Press enter to continue"))

    print("Grade saved. Run the cell again to grade another report.")


##
# Pull the report associated with a proc_ord_id for which the specified grader has a grade of 999, and then grade the report. Modifies lab.grader_table
# @param name A str containing the full name of the grader (to also be referenced in publications)
# @param to_highlight A dictionary with str keys specifying a color to highlight the list of str text with
def mark_one_report_sql(name, project, to_highlight={}):
    # Initialize the client service
    client = bigquery.Client()

    # Get a row from the grader table for the specified rater that has not been graded yet - start with Reliability
    q_get_single_row = (
        'SELECT * FROM lab.grader_table_with_metadata grader inner join arcus_2023_04_05.procedure_order_narrative narr on narr.proc_ord_id = grader.proc_ord_id WHERE grader_name = "'
        + name
        + '" and grade = 999 and grade_category = "Reliability" LIMIT 1'
    )
    df = client.query(q_get_single_row).to_dataframe()
    source_table = "arcus_2023_04_05.procedure_order_narrative"

    if len(df) == 0:
        # Get a row from the grader table for the specified rater that has not been graded yet - if no Reliability, then Unique
        q_get_single_row = (
            'SELECT * FROM lab.grader_table_with_metadata WHERE grader_name = "'
            + name
            + '" and grade = 999 and grade_category = "Unique" LIMIT 1'
        )
        df = client.query(q_get_single_row).to_dataframe()
        source_table = "arcus.procedure_order_narrative"

    if len(df) == 0:
        print(
            "There are currently no reports to grade for",
            name,
            " in the table. Please add more to continue.",
        )
        return

    print("Year of scan:", df["proc_ord_year"].values[0])
    print("Age at scan:", np.round(df["age_in_days"].values[0] / 365.25, 2), "years")
    print("Project:", project)
    proc_ord_id = df["proc_ord_id"].values[0]
    print_report(proc_ord_id, client, to_highlight, source_table)  # -- LOH
    grade = get_grade(enable_md_flag=False)

    # write the case to handle the skipped reports #TODO - make sure that a regular user can't mark -2 on an original report
    if grade == -1:
        # Ask the user for a reason
        skip_reason = get_reason("skip")
        # Write a query to add the report to the skipped reports table.
        # ('proc_ord_id', 'grade', 'grader_name', 'skip_date', 'skip_reason', 'regrade_date', 'regrade_reason')
        q_skip_report = "insert into lab.skipped_reports values ("
        today = date.today().strftime("%Y-%m-%d")
        q_skip_report += (
            "'"
            + str(proc_ord_id)
            + "', "
            + str(grade)
            + ", '"
            + name
            + "', '"
            + str(today)
            + "', '"
            + skip_reason
            + "', '', '');"
        )

        # Execute the query
        # print(q_skip_report)
        j_skip_report = client.query(q_skip_report)
        j_skip_report.result()

    # LOH - do more changes need to be made here to change the metadata in the table? I think no but ...
    # Update the grader table with the new grade
    q_update = "UPDATE lab.grader_table_with_metadata set grade = " + str(grade)
    today = date.today().strftime("%Y-%m-%d")
    q_update += ', grade_date = "' + today + '"'
    q_update += ' WHERE proc_ord_id = "' + str(df["proc_ord_id"].values[0]) + '"'
    q_update += ' and grader_name like "' + name + '"'

    j_update = client.query(q_update)
    j_update.result()
    print("Grade saved. Run the cell again to grade another report.")


##
#
def load_cohort_config(project_id):
    fn = "./queries/config.json"  ## write this file
    with open(fn, "r") as f:
        project_lookup = json.load(f)

    # Get the info for the specified project
    project_info = project_lookup[project_id]
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


##
# Get more proc_ord_id for which no reports have been rated for the specified user to grade
# @param name A str containing the full name of the grader (to also be referenced in publications)
def get_more_reports_to_grade(name, project_id="SLIP Adolescents", num_to_add=100):
    if project_id == "SLIP":
        print(
            "SLIP is too broad of a cohort definition. Please modify your project_id to include the appropriate age group descriptor and then rerun this function."
        )
        return -1

    # Global var declaration
    global num_validation_graders
    print(
        "It is expected for this function to take several minutes to run. Your patience is appreciated."
    )

    # Initialize the client service
    client = bigquery.Client()

    # Load the config file
    q_project = load_cohort_config(project_id)

    # Run the query from the specified file -- should the query itself be passed to a dx filtering option?
    df_project = client.query(q_project).to_dataframe()
    # Now we have the ids of the reports we want to grade for Project project
    project_proc_ids = df_project["proc_ord_id"].values
    print("Number of ids for project", project_id, len(project_proc_ids))

    # Get the proc_ord_ids from the grader table
    q_grade_table = (
        "SELECT proc_ord_id, grader_name, project from lab.grader_table_with_metadata where grade_category='Unique' and project like '%"
        + project_id
        + "%' ; "
    )
    df_grade_table = client.query(q_grade_table).to_dataframe()
    table_proc_ids = df_grade_table["proc_ord_id"].values
    user_proc_ids = df_grade_table[df_grade_table["grader_name"] == name][
        "proc_ord_id"
    ].values

    # Validation: are there any reports for the project that need to be validated that name hasn't graded?
    to_add_validation = {}
    for proc_id in project_proc_ids:  # for each proc_id in the project
        if (
            proc_id in df_grade_table["proc_ord_id"].values
        ):  # if the proc_id report was already graded
            graders = df_grade_table.loc[
                df_grade_table["proc_ord_id"] == proc_id, "grader_name"
            ].values
            graders_str = ", ".join(graders)
            # if the report was not graded by Coarse Text Search or the user and has not been graded N times
            if (
                "Coarse Text Search" not in graders_str
                and name not in graders_str
                and len(graders) < num_validation_graders
            ):
                to_add_validation[proc_id] = df_grade_table.loc[
                    df_grade_table["proc_ord_id"] == proc_id, "project"
                ].values[0]

    # Ignore proc_ids rated by User name
    print(
        "Number of reports that need to be validated for " + project_id + ":",
        len(to_add_validation),
    )

    # Add validation reports - proc_ids already in the table
    count_added = 0
    if len(to_add_validation) > 0:
        q_add_reports = "insert into lab.grader_table_with_metadata (proc_ord_id, grader_name, grade, grade_category, pat_id, age_in_days, proc_ord_year, proc_name, report_origin_table, project, grade_date) VALUES "
        for proc_id in to_add_validation:
            if count_added < num_to_add and proc_id not in user_proc_ids:
                row = df_project[df_project["proc_ord_id"] == proc_id]
                q_add_reports += (
                    '("' + str(proc_id) + '", "' + name + '", 999, "Unique", "'
                )
                q_add_reports += (
                    row["pat_id"].values[0] + '", ' + str(row["proc_ord_age"].values[0])
                )
                q_add_reports += (
                    ", "
                    + str(row["proc_ord_year"].values[0])
                    + ', "'
                    + str(row["proc_ord_desc"].values[0].replace("'", "'"))
                )
                q_add_reports += (
                    '", "arcus.procedure_order", "'
                    + to_add_validation[proc_id]
                    + '", "0000-00-00"), '
                )
                count_added += 1
        q_add_reports = q_add_reports[:-2] + ";"
        j_add_reports = client.query(q_add_reports)
        j_add_reports.result()

    # New reports
    print("Number of validation reports added:", count_added)
    to_add_new = [
        proc_id
        for proc_id in project_proc_ids
        if proc_id not in df_grade_table["proc_ord_id"].values
    ][: (num_to_add - count_added)]

    # Add new reports
    print("Number of new reports to grade:", len(to_add_new))
    if len(to_add_new) > 0:
        q_add_reports = "insert into lab.grader_table_with_metadata (proc_ord_id, grader_name, grade, grade_category, pat_id, age_in_days, proc_ord_year, proc_name, report_origin_table, project, grade_date) VALUES "
        for proc_id in to_add_new:
            row = df_project[df_project["proc_ord_id"] == proc_id]
            q_add_reports += (
                '("' + str(proc_id) + '", "' + name + '", 999, "Unique", "'
            )
            q_add_reports += (
                row["pat_id"].values[0] + '", ' + str(row["proc_ord_age"].values[0])
            )
            q_add_reports += (
                ", "
                + str(row["proc_ord_year"].values[0])
                + ', "'
                + str(row["proc_ord_desc"].values[0].replace("'", "'"))
            )
            q_add_reports += (
                '", "arcus.procedure_order", "' + project_id + '", "0000-00-00"), '
            )
        q_add_reports = q_add_reports[:-2] + ";"
        j_add_reports = client.query(q_add_reports)
        j_add_reports.result()

    # Check: how many reports were added for the user?
    if (len(to_add_validation) + len(to_add_new)) == 0:
        print(
            "There are no reports returned by the specified query that have yet to be either graded or validated."
        )
    else:
        get_user_unrated_count = (
            'SELECT * FROM lab.grader_table_with_metadata WHERE grader_name like "'
            + name
            + '" and grade = 999'
        )

        df = client.query(get_user_unrated_count).to_dataframe()

        # Inform the user
        print(len(df), "reports are in the queue for grader", name)


##
# Get more proc_ord_id for which no reports have been rated for the specified user to grade
# @param name A str containing the full name of the grader (to also be referenced in publications)
def get_second_look_reports_to_grade(name, num_to_add=100):
    # Global var declaration
    global num_validation_graders
    print(
        "It is expected for this function to take several minutes to run. Your patience is appreciated."
    )

    # Initialize the client service
    client = bigquery.Client()

    # Get the proc_ord_ids from the grader table
    q_grade_table = '''
                    with CTE as (
                      select
                        distinct proc_ord_id,
                        count(proc_ord_id) as graded_by_count
                      from
                        lab.grader_table_with_metadata
                      group by
                        proc_ord_id
                    )
                    select
                      meta.pat_id,
                      meta.proc_ord_id,
                      meta.proc_name,
                      meta.age_in_days,
                      meta.proc_ord_year,
                      meta.project,
                      meta.grader_name,
                      CTE.graded_by_count
                    from
                      CTE
                      inner join lab.grader_table_with_metadata meta on CTE.proc_ord_id = meta.proc_ord_id
                    where
                      CTE.graded_by_count < 2
                      and meta.grader_name not like "Coarse Text Search%"
                      and meta.grader_name not like "'''
    q_grade_table += name + '" ;'
    df_grade_table = client.query(q_grade_table).to_dataframe()
    table_proc_ids = df_grade_table["proc_ord_id"].values
    user_proc_ids = df_grade_table[df_grade_table["grader_name"] == name][
        "proc_ord_id"
    ].values
    print(df_grade_table.shape)

    # How many reports need validation
    to_add_validation = {}
    for proc_id in df_grade_table[
        "proc_ord_id"
    ].values:  # if the proc_id report was already graded
        graders = df_grade_table.loc[
            df_grade_table["proc_ord_id"] == proc_id, "grader_name"
        ].values
        graders_str = ", ".join(graders)
        # if the report was not graded by Coarse Text Search or the user and has not been graded N times
        if (
            "Coarse Text Search" not in graders_str
            and name not in graders_str
            and len(graders) < num_validation_graders
        ):
            to_add_validation[proc_id] = df_grade_table.loc[
                df_grade_table["proc_ord_id"] == proc_id, "project"
            ].values[0]

    print("Number of reports that need to be validated:", len(to_add_validation))

    # Add validation reports - proc_ids already in the table
    count_added = 0
    if len(to_add_validation) > 0:
        q_add_reports = "insert into lab.grader_table_with_metadata (proc_ord_id, grader_name, grade, grade_category, pat_id, age_in_days, proc_ord_year, proc_name, report_origin_table, project, grade_date) VALUES "
        for proc_id in to_add_validation:
            if count_added < num_to_add and proc_id not in user_proc_ids:
                row = df_grade_table[df_grade_table["proc_ord_id"] == proc_id]
                q_add_reports += (
                    '("' + str(proc_id) + '", "' + name + '", 999, "Unique", "'
                )
                q_add_reports += (
                    row["pat_id"].values[0] + '", ' + str(row["age_in_days"].values[0])
                )
                q_add_reports += (
                    ", "
                    + str(row["proc_ord_year"].values[0])
                    + ', "'
                    + row["proc_name"].values[0]
                )
                q_add_reports += (
                    '", "arcus.procedure_order", "'
                    + to_add_validation[proc_id]
                    + '", "0000-00-00"), '
                )
                count_added += 1
        q_add_reports = q_add_reports[:-2] + ";"
        j_add_reports = client.query(q_add_reports)
        j_add_reports.result()

    # New reports
    print("Number of validation reports added:", count_added)
    q_get_user_unrated_count = (
        'select * from lab.grader_table_with_metadata where grade = 999 and grader_name like "%'
        + name
        + '%";'
    )
    df = client.query(q_get_user_unrated_count).to_dataframe()

    # Inform the user
    print(len(df), "reports are in the queue for grader", name)


def welcome_user(name):
    print("Welcome,", name)

    client = bigquery.Client()

    # Possibly pull this bit into its own function - make it user proof
    q_check_self_eval = (
        'select * from lab.training_selfeval where grader_name like"' + name + '"'
    )
    df_self_eval = client.query(q_check_self_eval).to_dataframe()

    if len(df_self_eval) == 0:
        print(
            "It appears you have yet to do the self-evaluation. Please grade those reports before continuing."
        )
        add_self_eval_reports(name)
        return

    elif 999 in df_self_eval["grade"].values:
        print(
            "It appears you have started the self-evaluation but have not finished it. Please grade those reports before continuing."
        )
        return

    q_reliability = (
        'select * from lab.grader_table_with_metadata where grade_category = "Reliability" and grader_name like"'
        + name
        + '"'
    )
    df_reliability = client.query(q_reliability).to_dataframe()

    if not check_reliability_ratings(df_reliability):
        print("It appears you have yet to grade the reliability reports.")
        add_reliability_reports(name)

    elif 999 in df_reliability["grade"].values:
        reliability_count = len(df_reliability[df_reliability["grade"] == 999])
        print("You have", reliability_count, "reliability reports to grade.")

    else:
        q_get_queued_count = (
            'select * from lab.grader_table_with_metadata where grader_name like "'
        )
        q_get_queued_count += name + '" and grade = 999'

        df_grader_unrated = client.query(q_get_queued_count).to_dataframe()

        if len(df_grader_unrated) == 0:
            print("You are caught up on your report ratings")
            # TODO add function here to get more reports for the user
        else:
            print(
                "You currently have",
                len(df_grader_unrated),
                "ungraded reports to work on.",
            )

    return True


def add_reliability_reports(name):
    client = bigquery.Client()

    # Get the grader table
    q_get_grader_table = (
        "SELECT * from lab.grader_table_with_metadata where grader_name = '"
        + name
        + "' and grade_category = 'Reliability';"
    )
    df_grader = client.query(q_get_grader_table).to_dataframe()

    df_reliability = pd.read_csv("~/arcus/shared/reliability_report_info.csv")
    add_reports = False

    q_insert_report = "INSERT into lab.grader_table_with_metadata (proc_ord_id, grader_name, grade, grade_category, pat_id, age_in_days, proc_ord_year, proc_name, report_origin_table, project, grade_date) VALUES"

    # print(df_grader['proc_ord_id'].values)

    for idx, row in df_reliability.iterrows():
        # print(row['proc_ord_id'])
        if str(row["proc_ord_id"]) not in df_grader["proc_ord_id"].values:
            # Add the report
            q_insert_report += (
                " ('"
                + str(int(row["proc_ord_id"]))
                + "', '"
                + name
                + "', 999, 'Reliability', '"
            )
            q_insert_report += (
                row["pat_id"]
                + "', "
                + str(row["age_in_days"])
                + ", "
                + str(row["proc_ord_year"])
                + ", '"
            )
            q_insert_report += (
                row["proc_name"]
                + "', '"
                + row["report_origin_table"]
                + "', '"
                + row["project"]
            )
            q_insert_report += "', '0000-00-00'),"
            add_reports = True

    if add_reports:
        print("Adding reliability reports to grade")
        q_insert_report = q_insert_report[:-1] + ";"
        # print(q_insert_report)
        j_add_report = client.query(q_insert_report)
        j_add_report.result()


def check_reliability_ratings(df_grader):
    if len(df_grader) == 0:
        return False

    name = df_grader["grader_name"].values[0]
    df_reliability = pd.read_csv("~/arcus/shared/reliability_report_info.csv")
    reliability_ids = df_reliability["proc_ord_id"].values
    df_grader_reliability = df_grader[df_grader["grade_category"] == "Reliability"]
    grader_ids = df_grader_reliability["proc_ord_id"].values
    num_reliability = len([i for i in reliability_ids if str(i) in grader_ids])
    num_graded_reliability = len(
        [
            i
            for i in reliability_ids
            if str(i) in grader_ids
            and max(
                df_grader_reliability[df_grader_reliability["proc_ord_id"] == str(i)][
                    "grade"
                ].values
            )
            != 999
        ]
    )

    print(num_reliability)
    print(len(reliability_ids))
    assert num_reliability == len(reliability_ids)
    print(
        name,
        "has graded",
        num_graded_reliability,
        "of",
        num_reliability,
        "reliability reports",
    )

    if num_graded_reliability == num_reliability:
        return True
    elif num_graded_reliability < num_reliability:
        return False
    else:
        print(
            "Error (code surplus): Grader has graded more reliability reports than exist"
        )


##
# For a specified list of reports, change their grades to 999 to put them back
# in a user's queue. ASSUMES THE USER HAS VERIFIED THE REPORTS TO RELEASE
# @param grader_name A string specifying the grader
# @param reports_list A list of proc_ord_id elements to reset the grades for
def release_reports(grader_name, reports_list):
    # Initialize the client
    client = bigquery.Client()

    # For each report
    for proc_id in reports_list:
        # Update the grader table with the new grade
        q_update = "UPDATE lab.grader_table_with_metadata set grade = 999,"
        q_update += ' grade_date="0000-00-00"'
        q_update += ' WHERE proc_ord_id = "' + str(proc_id) + '"'
        q_update += ' and grader_name = "' + grader_name + '"'

        j_update = client.query(q_update)
        j_update.result()

    print(len(reports_list), "were released back into the queue for", grader_name)


# LOH How do I test this?
# Create a user Bob Belcher
# Add the reliability reports
# Grade some of his reliability reports
# Back up his grades
# Check
# Grade more
# Back up his grades again
# Check
def backup_reliability_grades(user):
    client = bigquery.Client()

    q = "select * from lab.grader_table_with_metadata where grader_name = '" + user
    q += "' and grade_category = 'Reliability'"
    df_primary = client.query(q).to_dataframe()

    for proc_id in df_primary["proc_ord_id"].values:
        # If the proc id is not in the df for the user
        q = "select * from lab.reliability_grades_original where grader_name = '" + user
        q += (
            "' and grade_category = 'Reliability' and proc_ord_id = "
            + str(proc_id)
            + ";"
        )
        df_backup = client.query(q).to_dataframe()

        # if the query returned an empty dataframe
        if len(df_backup) == 0:
            # Then add the row to the table
            q_add = "insert into lab.reliability_grades_original (proc_ord_id, grader_name, "
            q_add += (
                "grade, grade_category, pat_id, age_in_days, proc_ord_year, proc_name, "
            )
            q_add += (
                "report_origin_table, project) values ('"
                + str(proc_id)
                + "', '"
                + df_primary["grader_name"].values[0]
            )
            q_add += (
                "', "
                + str(df_primary["grade"].values[0])
                + ", 'Reliability', '"
                + str(df_primary["pat_id"].values[0])
            )
            q_add += (
                "', "
                + str(df_primary["age_in_days"].values[0])
                + ", "
                + str(df_primary["proc_ord_year"].values[0])
            )
            q_add += (
                ", '"
                + str(df_primary["proc_name"].values[0])
                + "', '"
                + str(df_primary["report_origin_table"].values[0])
            )
            q_add += (
                "', '"
                + str(df_primary["project"].values[0])
                + "', '"
                + str(df_primary["grade_date"].values[0])
                + "' ) ;"
            )

        elif len(df_backup) == 1:
            if df_backup["grade"].values == 999:
                q_update = (
                    "UPDATE lab.reliability_grades_original set grade = "
                    + df_primary["grade"].values[0]
                )
                q_update += (
                    ' WHERE proc_ord_id = "'
                    + str(df_primary["proc_ord_id"].values[0])
                    + '"'
                )
                q_update += (
                    ' and grader_name = "'
                    + str(df_primary["grader_name"].values[0])
                    + '"'
                )

                j_update = client.query(q_update)
                j_update.result()


def print_report(
    proc_id, client, to_highlight={}, source_table="arcus.procedure_order_narrative"
):
    try:
        # Get the report for that proc_ord_id from the primary report table
        q_get_report_row = (
            "SELECT * FROM "
            + source_table
            + ' where proc_ord_id like "'
            + str(proc_id)
            + '"'
        )
        df_report = client.query(q_get_report_row).to_dataframe()
    except:
        print(
            "AN ERROR HAS OCCURRED: REPORT", proc_id, "CANNOT BE FOUND IN", source_table
        )

    # If the id was in the new table:
    if len(df_report) == 1:
        origin_table = source_table
        domain = source_table.split(".")[0]

        q_get_report_row = (
            "SELECT * FROM "
            + domain
            + '.procedure_order_narrative where proc_ord_id = "'
            + str(proc_id)
            + '"'
        )
        report_text = (
            client.query(q_get_report_row).to_dataframe()["narrative_text"].values[0]
        )

        q_get_report_row = (
            "SELECT * FROM "
            + domain
            + '.procedure_order_impression where proc_ord_id = "'
            + str(proc_id)
            + '"'
        )
        df_report = client.query(q_get_report_row).to_dataframe()

        if len(df_report) == 1:
            report_text += "\n\nIMPRESSION: " + df_report["impression_text"].values[0]

    elif len(df_report) == 0:
        print("proc_ord_id not in", source_table, ":", proc_id)

    report_text = " ".join(report_text.split())
    report_text = report_text.replace("CLINICAL INDICATION", "\n\nCLINICAL INDICATION")
    report_text = report_text.replace("TECHNIQUE", "\n\nTECHNIQUE")
    report_text = report_text.replace("HISTORY", "\n\nHISTORY")
    report_text = report_text.replace("IMPRESSION", "\n\nIMPRESSION")
    report_text = report_text.replace("FINDINGS", "\n\nFINDINGS")
    report_text = report_text.replace("COMPARISON", "\n\nCOMPARISON")

    # If the user passed a dictionary of lists to highlight
    if len(to_highlight.keys()) > 0:
        for key in to_highlight.keys():
            report_text = mark_text_color(report_text, to_highlight[key], key)

    # Print the report and ask for a grade
    print(report_text)
    print()
    # Print the proc_ord_id
    print("Report id:", str(proc_id))
    print()


def get_reason(usage):
    if usage == "skip":
        message = "This report was skipped. Please include the part(s) of the report that were confusing:"
    elif usage == "regrade":
        message = "This report was previously skipped. Please include an explanation why it received its updated grade:"

    reason = str(input(message))
    while type(reason) != str and len(reason) <= 5:  # arbitrary minimum string length
        reason = str(input(message))

    return reason


def get_grade(enable_md_flag=False):
    if enable_md_flag:
        potential_grades = ["0", "1", "2", "-1", "-2"]
        grade = str(
            input(
                "Assign a SLIP rating to this report (0 do not use/1 maybe use/2 definitely use/-1 skip/-2 escalate to clinician): "
            )
        )

    else:
        potential_grades = ["0", "1", "2", "-1"]
        grade = str(
            input(
                "Assign a SLIP rating to this report (0 do not use/1 maybe use/2 definitely use/-1 skip): "
            )
        )

    while grade not in potential_grades:
        if not enable_md_flag:
            if grade == "-2":
                print(
                    "Reports cannot be marked for clinician review without undergoing peer review first. Please flag using a grade of -1 instead."
                )
                message = "Please enter a grade value from the acceptable grade list (0/1/2/-1): "
                grade = str(input(message))
            else:
                grade = str(
                    input(
                        "Invalid input. Assign a SLIP rating to this report (0 do not use/1 maybe use/2 definitely use/-1 skip): "
                    )
                )
        else:
            grade = str(
                input(
                    "Invalid input. Assign a SLIP rating to this report (0 do not use/1 maybe use/2 definitely use/-1 skip/-2 escalate to MD): "
                )
            )

    print()

    # Ask the user to confirm the grade
    confirm_grade = "999"
    while confirm_grade != grade:
        while confirm_grade not in potential_grades:
            if not enable_md_flag and confirm_grade == "-2":
                print(
                    "Reports cannot be marked for clinician review without undergoing peer review first. Please flag using a grade of -1 instead."
                )
                message = "Please enter a grade value from the acceptable grade list (0/1/2/-1): "
            else:
                message = "Please confirm your grade by reentering it OR enter a revised value to change the grade: "
            confirm_grade = str(input(message))
        if confirm_grade != grade:
            if not enable_md_flag and confirm_grade == "-2":
                print(
                    "Reports cannot be marked for clinician review without undergoing peer review first. Please flag using a grade of -1 instead."
                )
                message = "Please enter a grade value from the acceptable grade list (0/1/2/-1): "
                confirm_grade = str(input(message))
            else:
                grade = confirm_grade
                confirm_grade = "999"

    if confirm_grade == "-1":
        print("This report is being marked as SKIPPED (-1) for you.")
        return -1
    elif confirm_grade == "-2":
        print(
            "WARNING: this report is being marked as SKIPPED for you AND is being escalated to a clinician for further review."
        )
        return -2
    else:
        print("Saving your grade of", grade, "for this report.")
        return grade


def get_grader_status_report(name):
    client = bigquery.Client()

    query = "select * from lab.grader_table_with_metadata where "
    query += "grader_name = '" + name + "';"
    print(query)
    df = client.query(query).to_dataframe()

    # Case: user not in table
    if len(df) == 0:
        print("User is not in the table yet.")
        return

    # Reliability ratings
    check_reliability_ratings(df)

    # Unique
    df_unique_reports = df[df["grade_category"] == "Unique"]
    df_graded_unique_reports = df[
        (df["grade_category"] == "Unique") & (df["grade"] != 999)
    ]
    print(
        name,
        "has graded",
        df_graded_unique_reports.shape[0],
        "unique reports of",
        df_unique_reports.shape[0],
        "assigned where",
    )
    for grade in range(3):
        num_graded = df_graded_unique_reports[
            df_graded_unique_reports["grade"] == grade
        ].shape[0]
        print(num_graded, "have been given a grade of", grade)


# Main
if __name__ == "__main__":
    print("Radiology Report Annotation Helper Library v 0.2")
    print("Written and maintained by Jenna Young, PhD (@jmschabdach on Github)")
    print("Tested and used by:")
    print("- Caleb Schmitt, Summer 2021")
    print("- Nadia Ngom, Fall 2021 - Spring 2022")
    print("- Alesandra Gorgone, Spring 2023")
