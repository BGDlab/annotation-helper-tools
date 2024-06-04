import pandas as pd
import numpy as np
import random
import os
import json
import matplotlib.pyplot as plt
from IPython.display import clear_output
from google.cloud import bigquery # SQL table interface on Arcus


##
# Given a table of requested proc_ord_ids and metadata, 
# get all the diagnoses for those patients in phecode space
# @param tableName A string representing the name of the SQL table containing patient ids
# @returns dfReqDxPheCodes A dataframe containing the patient ids, their icd-10 dx, and the phecode mappings
def mapProcReqToPheCodes(tableName, verbose=False):
    # Set up the client
    client = bigquery.Client()
    
    # Get the diagnosis codes from the problem_list, encounter_diagnosis, and procedure_order_diagnosis tables
    q = """with joint_dx as (
      select
        req.pat_id, 
        'arcus.problem_list' as dx_source,
        dx.dx_id,
        
        case 
          when dx.dx_id is null then "2475657929"
          else dx.dx_id end as dx_id
      from """
    q += tableName 
    q += """ req 
        left join arcus.problem_list dx on dx.pat_id = req.pat_id
      union all
      select
        req.pat_id, 
        'arcus.encounter_diagnosis' as dx_source,
        case 
          when dx.dx_id is null then "2475657929"
          else dx.dx_id end as dx_id
      from """
    q += tableName
    q += """ req 
        left join arcus.encounter_diagnosis dx on dx.pat_id = req.pat_id
      union all
      select
        req.pat_id, 
        'arcus.procedure_order_diagnosis' as dx_source,
        case 
          when dx.dx_id is null then "2475657929"
          else dx.dx_id end as dx_id
      from """
    q += tableName
    q += """ req 
        left join arcus.procedure_order_diagnosis dx on dx.pat_id = req.pat_id
    )
    select
      joint_dx.pat_id,
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
    dfReqDx = client.query(q).to_dataframe()
    dfReqDx = dfReqDx.dropna().drop_duplicates()

    # Deal with R51 icd codes missing from Phecode_map_v1_2_icd10cm_beta.csv
    dfReqDx.loc[dfReqDx['icd10_list'].str.contains("R51"), 'icd10_list'] = "R51"
       
    # Get the phecodes - currently loading from a .csv
    basePath = "/home/youngjm/private_transfer/ICDexclusion_code/"
    fnICDPheCodes = os.path.join(basePath, "Phecode_map_v1_2_icd10cm_beta.csv")
    dfICDPheCodes = pd.read_csv(fnICDPheCodes, encoding = "unicode_escape") 
    if verbose:
        print("loaded phecodes:",dfICDPheCodes.shape) # correct number
    
    # Join the dx of requested patients to the phecodes
    if verbose:
        print(list(dfReqDx))
        print(list(dfICDPheCodes))
    dfReqDxPheCodes = pd.merge(dfReqDx, dfICDPheCodes, how="inner", left_on="icd10_list", right_on="icd10cm")
    # print(dfReqDxPheCodes[dfReqDxPheCodes['pat_id'].str.contains('HM47OMXOT')].shape)
    # Remove columns that are part of the phecode file but aren't used in our analysis
    colsToDrop = ['exclude_range', 'exclude_name', 'leaf', 'rollup'] 
    dfReqDxPheCodes = dfReqDxPheCodes.drop(columns=colsToDrop).dropna()
    # print(dfReqDxPheCodes[dfReqDxPheCodes['pat_id'].str.contains('HM47OMXOT')].shape)

    
    if verbose:
        print(dfReqDxPheCodes.shape)
        print(list(dfReqDxPheCodes))
        print("Dropping dx_id and icd10_list columns, then dropping duplicates")
        
    dfReqDxPheCodes = dfReqDxPheCodes.drop(columns=['dx_id', 'icd10_list']).drop_duplicates().dropna()
    
    return dfReqDxPheCodes 


##
# Identify a cohort of subjects who have specific phecode dx
# @param dfPatDx A dataframe with the columns...
# @param dfDx A dataframe with the diagnoses of interest as phecodes
# @param isFilterExclude A boolean flag indicating to use the dfDx to exclude patients from the study (default: True/exclude)
# @returns dfPatDx A modified dataframe containing a subset of the patients as filtered by dx
def filterSubsByDx(dfPatDx, dfDx, isFilterExclude = True):
    # Inner join the two data frames on phecode
    dfJoint = pd.merge(dfPatDx, dfDx, how="inner", on='phecode')
    
    if "exclude_or_include_AAB_TS" in list(dfJoint):
        if isFilterExclude:
            print("Excluding patients with any dx labelled 'exclude'")
            # Remove patients with excluded dxs
            patIdsToDrop = list(set(dfJoint[dfJoint['exclude_or_include_AAB_TS'] == "exclude"]['pat_id'].values))
            dfPatDx = dfPatDx[~dfPatDx['pat_id'].isin(patIdsToDrop)]
        else:
            patIdsToKeep = list(set(dfJoint[dfJoint['exclude_or_include_AAB_TS'] == "include"]['pat_id'].values))
            dfPatDx = dfPatDx[dfPatDx['pat_id'].isin(patIdsToKeep)]
        
    else:
        # Get the pat_id for all subjects who have those Dx's
        dfJoint = dfJoint.dropna()
        listPatWDx = list(set(dfJoint['pat_id'].values))

        if isFilterExclude: # if the specified diagnoses need to be exluded
            # Remove the patients who have the specified diagnoses
            dfPatDx = dfPatDx[~dfPatDx['pat_id'].isin(listPatWDx)]
        else: # if the specified diagnoses need to be included 
            # Remove everyone from the original dataframe who doesn't have the dx(s) of interest
            dfPatDx = dfPatDx[dfPatDx['pat_id'].isin(listPatWDx)]
    
    dfPatDx = dfPatDx.drop_duplicates()
    # Return the desired data frame  
    return dfPatDx


##
# Use the filtered list of patients to create a new table with that subset's metadata
# @param dfFilteredDx A dataframe of desired patients and their dxs
# @param origTableName A string representing the name of the SQL table containing patient ids
# @param newTableName A string representing the name of the new SQL table to create for the subset of patients (default: "")
# @returns dfFilteredMeta A dataframe with metadata for the patients filtered by dx
def getPatsOfInterest(dfFilteredDx, origTableName, newTableName=""):
    # set up client
    client = bigquery.Client()
    
    # Get the original table
    q = "select * from "+origTableName
    # Adding in age/scan year filter
    q += " where proc_ord_year = 2023 "
    q += " and proc_ord_age > 12*365 "
    q += " and proc_ord_age < 21*365 "
    dfOrig = client.query(q).to_dataframe()
    
    # Join the two tables, inner on pat_id
    dfFilteredMeta = dfOrig[dfOrig['pat_id'].isin(dfFilteredDx['pat_id'])] 
    # dfFilteredMeta = dfFilteredMeta.dropna()
    # dfFilteredMeta = dfFilteredMeta.replace(np.nan, None)
    
    if newTableName != "":
        # Create the new table
        mySchema = []
        for c in list(dfFilteredMeta):
            # print(c, dfFilteredMeta[c].dtypes)
            if "weight" in c or "length" in c or "avg_" in c:
                mySchema.append(bigquery.SchemaField(c, "FLOAT"))
                dfFilteredMeta = dfFilteredMeta.astype({c: float})
            elif "year" in c or "proc_ord_age" in c or "age_in_days" in c:
                mySchema.append(bigquery.SchemaField(c, "INTEGER"))
                dfFilteredMeta = dfFilteredMeta.astype({c: "Int64"})
            else:
                mySchema.append(bigquery.SchemaField(c, "STRING"))

        # Since string columns use the "object" dtype, pass in a (partial) schema
        # to ensure the correct BigQuery data type.
        job_config = bigquery.LoadJobConfig(schema=mySchema)
        job = client.load_table_from_dataframe(
            dfFilteredMeta, newTableName, job_config=job_config
        )

        # Wait for the load job to complete. 
        job.result()
        print(newTableName, "created")
    
    # Create a new table newTableName with just the desired individuals
    return dfFilteredMeta



def addDxFilterToQuery(fn_query, q_dx_filter):
    with open(fn_query, 'r') as f:
        q_project = f.read()

    # If there is a dx filter, incorporate it into the loaded query
    if q_dx_filter != "":
        q_tmp = q_dx_filter + q_project.split("where")[0] 
        q_tmp += "left join exclude_table on proc_ord.pat_id = exclude_table.pat_id where exclude_table.pat_id is null and"
        q_tmp += q_project.split("where")[1]

    return q_tmp


def convertExcludeDxCsvToSql(fn):
    # Load the dx filter file
    df = pd.read_csv(fn)
    # Check that the filter file has the columns we expect it to have, namely include/exclude (with specific types) and phecode
    assert "exclude_or_include_AAB_TS" in list(df)
    assert "phecode" in list(df)
    # Get only the codes we want to exclude
    dx_exclude = list(set(df[df['exclude_or_include_AAB_TS'] == "exclude"]['phecode']))
    # Start the query
    q = "with exclude_table as (select pat_id, phecode from lab.patient_phecode_dx where "
    # For each exclude row, 
    for dx in dx_exclude:
        q += "phecode = "+str(dx)+" or "
        
    # After iterating through the rows, remove the last "or "
    q = q[:-3] +")"
    
    # Return the filter query
    return q