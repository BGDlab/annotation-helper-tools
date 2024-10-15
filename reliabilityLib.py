import pandas as pd
from annotationHelperLib import *
from sklearn.metrics import cohen_kappa_score
from IPython.display import clear_output
from google.cloud import bigquery  # SQL table interface on Arcus


##
# Get the proc_ord_id values for the reliability reports
# @return proc_ord_ids A list of the 151 proc_ord_ids for the reliability reports
def get_reliability_proc_ord_ids():
    tmp = pd.read_csv("~/arcus/shared/reliability_report_info.csv")
    proc_ord_ids = tmp["proc_ord_id"]
    return proc_ord_ids


def get_reliability_ratings_df():
    # Initialize the client service
    client = bigquery.Client()

    reliability_ratings_query = "select * from lab.grader_table_with_metadata where grade_category = 'Reliability';"
    df_reliability = client.query(reliability_ratings_query).to_dataframe()
    df_reliability[["grade", "proc_ord_id"]] = df_reliability[
        ["grade", "proc_ord_id"]
    ].astype("int64")
    print(df_reliability.shape)

    # Pivot
    df_ratings = pd.pivot_table(
        df_reliability, values="grade", index="proc_ord_id", columns="grader_name"
    )

    cols_to_drop = ["Jisoo Kang", "Sreya Subramanian"]
    df_ratings = df_ratings.drop(columns=cols_to_drop)
    df_ratings.reset_index(inplace=True)
    print(df_ratings.shape)

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

    df_reliability = client.query(query).to_dataframe()
    reliability_ids = sorted(list(df_reliability["proc_ord_id"].values))[:-1]

    df_ratings = df_ratings[df_ratings["proc_ord_id"].isin(reliability_ids)]

    return df_ratings


##
# Identifies reports with different grades for 2 graders
# @param df_grades1 A dataframe object with columns proc_ord_id and grade for user 1
# @param df_grades2 A dataframe object with columns proc_ord_id and grade for user 2
# @return disagreement_proc_ord_ids A list of proc_ord_id strings specifying reports with 2 different grades
def identify_disagreement_reports(df_grades1, df_grades2):
    # Double check that the incoming dataframes are sorted the same way
    df_grades1 = df_grades1.sort_values("proc_ord_id", ignore_index=True)
    df_grades2 = df_grades2.sort_values("proc_ord_id", ignore_index=True)

    # Triple check the order
    assert list(df_grades1["proc_ord_id"].values) == list(
        df_grades2["proc_ord_id"].values
    )

    shared = 0
    disagreement_proc_ord_ids = []

    for idx, row in df_grades1.iterrows():
        proc_ord_id = row["proc_ord_id"]
        grades = [df_grades1.iloc[idx]["grade"], df_grades2.iloc[idx]["grade"]]
        if 999.0 in grades:
            continue
        else:
            shared += 1
            if grades[0] != grades[1]:
                disagreement_proc_ord_ids.append(proc_ord_id)

    return disagreement_proc_ord_ids


##
# For a pair of users with their grades in separate dataframes, compare them and calculate kappa
# @param user1_grades A dataframe with the proc_ord_id and corresponding grades for that graders reliability reports
# @param user2_grades A dataframe with the proc_ord_id and corresponding grades for that graders reliability reports
# @return kappa A float variable, Cohen's kappa
def calc_kappa(user1_grades, user2_grades):
    # Double check that the incoming dataframes are sorted the same way
    user1_grades = user1_grades.sort_values("proc_ord_id")
    user2_grades = user2_grades.sort_values("proc_ord_id")

    # Triple check the order
    assert list(user1_grades["proc_ord_id"].values) == list(
        user2_grades["proc_ord_id"].values
    )

    # Calculate Cohen's kappa
    kappa = cohen_kappa_score(
        list(user1_grades["grade"].values), list(user2_grades["grade"].values)
    )
    # print("Cohen's kappa:", kappa)
    return kappa


def calc_kappa_2_v_all(user1_grades, user2_grades):
    grades1 = user1_grades["grade"].values.astype(int)
    grades2 = user2_grades["grade"].values.astype(int)

    condenser = lambda x: 2 if x == 2 else 0

    grades1 = [condenser(i) for i in grades1]
    grades2 = [condenser(i) for i in grades2]

    kappa = cohen_kappa_score(grades1, grades2)

    print(" 2 vs. 0+1 kappa:", kappa)
    return kappa


def calc_kappa_0_v_all(user1_grades, user2_grades):
    grades1 = user1_grades["grade"].values.astype(int)
    grades2 = user2_grades["grade"].values.astype(int)

    condenser = lambda x: 0 if x == 0 else 2

    grades1 = [condenser(i) for i in grades1]
    grades2 = [condenser(i) for i in grades2]

    kappa = cohen_kappa_score(grades1, grades2)

    print(" 0 vs. 1+2 kappa:", kappa)
    return kappa


def get_reports_for_user(user, proc_ord_ids):
    client = bigquery.Client()

    get_user_reports = "select cast(proc_ord_id as int64) as proc_ord_id, grade from lab.grader_table_with_metadata "
    get_user_reports += "where grader_name = '" + user
    get_user_reports += "' and grade_category = 'Reliability';"

    user_reliablity_reports = client.query(get_user_reports).to_dataframe()
    user_reliablity_reports = user_reliablity_reports[
        user_reliablity_reports["proc_ord_id"].astype(int).isin(proc_ord_ids)
    ]

    return user_reliablity_reports


def print_report_from_proc_ord_id(proc_ord_id):
    client = bigquery.Client()

    print("Proc ord id:", proc_ord_id)
    print()

    # Get the report for that proc_ord_id from the primary report table
    get_report_row = (
        'SELECT * FROM arcus_2023_05_02.reports_annotations_master where proc_ord_id like "'
        + str(proc_ord_id)
        + '"'
    )
    df_report = client.query(get_report_row).to_dataframe()

    # If the id was in the original table:
    if len(df_report) == 1:
        # Combine the narrative and impression text
        report_text = df_report["narrative_text"].values[0]
        if df_report["impression_text"].values[0] != "nan":
            report_text += "\n\nIMPRESSION: " + df_report["impression_text"].values[0]

    elif len(df_report) == 0:
        get_report_row = (
            'SELECT * FROM arcus.procedure_order_narrative where proc_ord_id like "'
            + str(proc_ord_id)
            + '"'
        )
        report_text = (
            client.query(get_report_row).to_dataframe()["narrative_text"].values[0]
        )

        get_report_row = (
            'SELECT * FROM arcus.procedure_order_impression where proc_ord_id like "'
            + str(proc_ord_id)
            + '"'
        )
        df_report = client.query(get_report_row).to_dataframe()

        if len(df_report) == 1:
            report_text += "\n\nIMPRESSION: " + df_report["impression_text"].values[0]

    print(report_text)


def print_disagreement_reports(disagreement_ids, grades1, grades2):
    for proc_ord_id in disagreement_ids:
        report_grade1 = grades1[grades1["proc_ord_id"].astype(str) == str(proc_ord_id)][
            "grade"
        ].values[0]
        report_grade2 = grades2[grades2["proc_ord_id"].astype(str) == str(proc_ord_id)][
            "grade"
        ].values[0]

        print_report_from_proc_ord_id(proc_ord_id)
        print(
            "\nThis report was given the grades of",
            report_grade1,
            "by user 1 and",
            report_grade2,
            "by user 2",
        )

        confirm_continue = str(input("Press enter to continue"))
        clear_output()


def calculate_metric_for_graders(graders, metric):
    proc_ord_ids = get_reliability_proc_ord_ids()
    metric_table = pd.DataFrame(0, columns=graders[1:], index=graders[:-1])

    for idx1 in range(len(graders) - 1):
        grades1 = get_reports_for_user(graders[idx1], proc_ord_ids)
        grades1 = grades1.sort_values("proc_ord_id", ignore_index=True)

        for idx2 in range(idx1 + 1, len(graders)):
            # Get the grades for the graders
            grades2 = get_reports_for_user(graders[idx2], proc_ord_ids)
            grades2 = grades2.sort_values("proc_ord_id", ignore_index=True)

            if metric == "disagreement":
                tmp = identify_disagreement_reports(grades1, grades2)
                metric_table.loc[graders[idx1], graders[idx2]] = len(tmp)

            elif metric == "kappa":
                k = calc_kappa(grades1, grades2)
                metric_table.loc[graders[idx1], graders[idx2]] = k

            elif metric == "kappa2vAll":
                k = calc_kappa_2_v_all(grades1, grades2)
                metric_table.loc[graders[idx1], graders[idx2]] = k
            elif metric == "kappa0vAll":
                k = calc_kappa_0_v_all(grades1, grades2)
                metric_table.loc[graders[idx1], graders[idx2]] = k

    return metric_table
