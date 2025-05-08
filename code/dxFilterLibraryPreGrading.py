import pandas as pd
import os
from google.cloud import bigquery  # SQL table interface on Arcus


##
# Given a table of requested proc_ord_ids and metadata,
# get all the diagnoses for those patients in phecode space
# @param table_name A string representing the name of the SQL table containing patient ids
# @returns df_req_dx_phecodes A dataframe containing the patient ids, their icd-10 dx, and the phecode mappings
def map_proc_req_to_phecodes(table_name, verbose=False):
    # Set up the client
    client = bigquery.Client()

    # Get the diagnosis codes from the problem_list, encounter_diagnosis, and procedure_order_diagnosis tables
    q = f"""with joint_dx as (
      select
        req.pat_id, 
        dx.encounter_id,
        'arcus.problem_list' as dx_source,
        case 
          when dx.dx_id is null then "2475657929"
          else dx.dx_id end as dx_id
      from {table_name} req 
        left join arcus.problem_list dx on dx.pat_id = req.pat_id
      union all
      select
        req.pat_id, 
        dx.encounter_id,
        'arcus.encounter_diagnosis' as dx_source,
        case 
          when dx.dx_id is null then "2475657929"
          else dx.dx_id end as dx_id
      from {table_name} req 
        left join arcus.encounter_diagnosis dx on dx.pat_id = req.pat_id
    )
    select DISTINCT
      joint_dx.pat_id,
      joint_dx.encounter_id,
      joint_dx.dx_id as dx_id,
      icd.code as icd10_list,
      joint_dx.dx_source
    from
      arcus.diagnosis_icd10 icd
      right join joint_dx on joint_dx.dx_id = icd.dx_id
    order by
      joint_dx.pat_id;
      """
    # Drop duplicates
    df_req_dx = client.query(q).to_dataframe()
    df_req_dx = df_req_dx.dropna().drop_duplicates()

    # Deal with R51 icd codes missing from Phecode_map_v1_2_icd10cm_beta.csv
    df_req_dx.loc[df_req_dx["icd10_list"].str.contains("R51"), "icd10_list"] = "R51"

    # Get the phecodes - currently loading from a .csv
    q = f"""
        SELECT * 
        FROM lab.icd10_to_phecode;
      """

    # Drop duplicates
    df_icd_phecodes = client.query(q).to_dataframe()
    if verbose:
        print("loaded phecodes:", df_icd_phecodes.shape)  # correct number

    # Join the dx of requested patients to the phecodes
    if verbose:
        print(list(df_req_dx))
        print(list(df_icd_phecodes))
    df_req_dx_phecodes = pd.merge(
        df_req_dx, df_icd_phecodes, how="inner", left_on="icd10_list", right_on="icd10cm"
    )
    # print(df_req_dx_phecodes[df_req_dx_phecodes['pat_id'].str.contains('HM47OMXOT')].shape)
    # Remove columns that are part of the phecode file but aren't used in our analysis
    cols_to_drop = ["exclude_range", "exclude_name", "leaf", "rollup"]
    df_req_dx_phecodes = df_req_dx_phecodes.drop(columns=cols_to_drop).dropna()
    # print(df_req_dx_phecodes[df_req_dx_phecodes['pat_id'].str.contains('HM47OMXOT')].shape)

    if verbose:
        print(df_req_dx_phecodes.shape)
        print(list(df_req_dx_phecodes))
        print("Dropping dx_id and icd10_list columns, then dropping duplicates")

    df_req_dx_phecodes = (
        df_req_dx_phecodes.drop(columns=["dx_id", "icd10_list"]).drop_duplicates().dropna()
    )

    return df_req_dx_phecodes


##
# Identify a cohort of subjects who have specific phecode dx
# @param df_pat_dx A dataframe with the columns...
# @param df_dx A dataframe with the diagnoses of interest as phecodes
# @param is_filter_exclude A boolean flag indicating to use the df_dx to exclude patients from the study (default: True/exclude)
# @returns df_pat_dx A modified dataframe containing a subset of the patients as filtered by dx
def filter_subs_by_dx(df_pat_dx, df_dx, is_filter_exclude=True):
    # Inner join the two data frames on phecode
    df_joint = pd.merge(df_pat_dx, df_dx, how="inner", on="phecode")

    if "exclude_or_include_AAB_TS" in list(df_joint):
        if is_filter_exclude:
            print("Excluding patients with any dx labelled 'exclude'")
            # Remove patients with excluded dxs
            pat_ids_to_drop = list(
                set(
                    df_joint[df_joint["exclude_or_include_AAB_TS"] == "exclude"][
                        "pat_id"
                    ].values
                )
            )
            df_pat_dx = df_pat_dx[~df_pat_dx["pat_id"].isin(pat_ids_to_drop)]
        else:
            pat_ids_to_keep = list(
                set(
                    df_joint[df_joint["exclude_or_include_AAB_TS"] == "include"][
                        "pat_id"
                    ].values
                )
            )
            df_pat_dx = df_pat_dx[df_pat_dx["pat_id"].isin(pat_ids_to_keep)]

    else:
        # Get the pat_id for all subjects who have those Dx's
        df_joint = df_joint.dropna()
        pats_with_dx = list(set(df_joint["pat_id"].values))

        if is_filter_exclude:  # if the specified diagnoses need to be exluded
            # Remove the patients who have the specified diagnoses
            df_pat_dx = df_pat_dx[~df_pat_dx["pat_id"].isin(pats_with_dx)]
        else:  # if the specified diagnoses need to be included
            # Remove everyone from the original dataframe who doesn't have the dx(s) of interest
            df_pat_dx = df_pat_dx[df_pat_dx["pat_id"].isin(pats_with_dx)]

    df_pat_dx = df_pat_dx.drop_duplicates()
    # Return the desired data frame
    return df_pat_dx


##
# Use the filtered list of patients to create a new table with that subset's metadata
# @param df_filtered_dx A dataframe of desired patients and their dxs
# @param orig_table_name A string representing the name of the SQL table containing patient ids
# @param new_table_name A string representing the name of the new SQL table to create for the subset of patients (default: "")
# @returns df_filtered_meta A dataframe with metadata for the patients filtered by dx
def get_pats_of_interest(df_filtered_dx, orig_table_name, new_table_name=""):
    # set up client
    client = bigquery.Client()

    # Get the original table
    q = "select * from " + orig_table_name
    # Adding in age/scan year filter
    q += " where proc_ord_year = 2023 "
    q += " and proc_ord_age > 12*365 "
    q += " and proc_ord_age < 21*365 "
    df_orig = client.query(q).to_dataframe()

    # Join the two tables, inner on pat_id
    df_filtered_meta = df_orig[df_orig["pat_id"].isin(df_filtered_dx["pat_id"])]

    if new_table_name != "":
        # Create the new table
        my_schema = []
        for c in list(df_filtered_meta):
            # print(c, df_filtered_meta[c].dtypes)
            if "weight" in c or "length" in c or "avg_" in c:
                my_schema.append(bigquery.SchemaField(c, "FLOAT"))
                df_filtered_meta = df_filtered_meta.astype({c: float})
            elif "year" in c or "proc_ord_age" in c or "age_in_days" in c:
                my_schema.append(bigquery.SchemaField(c, "INTEGER"))
                df_filtered_meta = df_filtered_meta.astype({c: "Int64"})
            else:
                my_schema.append(bigquery.SchemaField(c, "STRING"))

        # Since string columns use the "object" dtype, pass in a (partial) schema
        # to ensure the correct BigQuery data type.
        job_config = bigquery.LoadJobConfig(schema=my_schema)
        job = client.load_table_from_dataframe(
            df_filtered_meta, new_table_name, job_config=job_config
        )

        # Wait for the load job to complete.
        job.result()
        print(new_table_name, "created")

    # Create a new table new_table_name with just the desired individuals
    return df_filtered_meta


def add_dx_filter_to_query(fn_query, q_dx_filter):
    with open(fn_query, "r") as f:
        q_project = f.read()

    # If there is a dx filter, incorporate it into the loaded query
    if q_dx_filter != "":
        q_tmp = q_dx_filter + q_project.split("where")[0]
        q_tmp += "left join exclude_table on proc_ord.pat_id = exclude_table.pat_id where exclude_table.pat_id is null and"
        q_tmp += q_project.split("where")[1]

    return q_tmp


def convert_exclude_dx_csv_to_sql(fn):
    # Load the dx filter file
    df = pd.read_csv(fn)
    # Check that the filter file has the columns we expect it to have, namely include/exclude (with specific types) and phecode
    assert "exclude_or_include_AAB_TS" in list(df)
    assert "phecode" in list(df)
    # Get only the codes we want to exclude
    dx_exclude = list(set(df[df["exclude_or_include_AAB_TS"] == "exclude"]["phecode"]))
    # Start the query
    q = "with exclude_table as (select pat_id, phecode from lab.patient_phecode_dx where "
    # For each exclude row,
    for dx in dx_exclude:
        q += "phecode = " + str(dx) + " or "

    # After iterating through the rows, remove the last "or "
    q = q[:-3] + ") "

    # Return the filter query
    return q
