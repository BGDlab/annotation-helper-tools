import pandas as pd
import numpy as np


def deal_with_next(df, fn, next_step, column, idx):
    end = False
    print(next_step)
    print(idx)

    if next_step == "n":
        idx = df[df[column].isnull()].index[0]
        print()
    elif next_step == "p":
        # decrement index
        print("Revisiting the previous report...")
        idx -= 1
        print()
    elif next_step == "r":
        print("Repeating the current report...")
        print()
    elif next_step == "s":
        print("Saving and continuing...")
        saved = safely_save_df(df, fn)
        print(saved)
        idx = df[df[column].isnull()].index[0]
        print()
    elif next_step == "e":
        print("Saving and exiting...")
        safely_save_df(df, fn)
        end = True

    return idx, end


def safely_save_df(df, fn):
    try:
        df = df.astype(str)
        df.to_csv(fn, index=False)
        return True
    except PermissionError:
        print(
            "Error: write access to "
            + fn
            + " denied. Please check that the file is not locked by Datalad."
        )
        return False


def mark_text_color(line, to_mark, color):
    # Sort the list of strings to highlight by length (longest to shortest)
    to_mark = sorted(to_mark, key=len)[::-1]

    if color == "green":
        start = "\x1b[5;30;42m"  # green background, bold black text
    elif color == "yellow":
        start = "\x1b[5;30;43m"  # yellow background, bold black text
    elif color == "red":
        start = "\x1b[5;30;41m"  # red background, bold black text
    elif color == "gray" or color == "grey":
        start = "\x1b[5;30;47m"  # gray background, bold black text

    end = "\x1b[0m"

    if line is np.nan:
        return "<No report available.>"

    if type(to_mark) == str:
        line = line.replace(to_mark, start + to_mark + end)

    elif type(to_mark) == list:
        for phrase in to_mark:
            line = line.replace(str(phrase), start + str(phrase).upper() + end)

    else:
        print("Error: the second argument must be either a string or a list of strings")

    return line


def load_df(fn):
    # Load the dataframe
    df = pd.read_csv(fn)
    # If these two columns are not in the dataframe, add them
    if "scan_reason" not in list(df):
        df["scan_reason"] = np.nan
    if "pat_history" not in list(df):
        df["pat_history"] = np.nan

    # If there are "Unnamed:" columns in the dataframe, remove them
    to_drop = [col for col in list(df) if "Unnamed:" in col]
    if len(to_drop) > 0:
        df = df.drop(columns=to_drop)

    return df


def check_for_annotator(df, name):
    # Get the number of columns containing the word "annotator" in their name
    name_cols = [c for c in list(df) if "annotator" in c]

    # Get the individual's name from the columns
    names = [df[c].values[0] for c in name_cols]

    # If their name is not in the list of annotator names
    if name not in names:
        # Add a new column
        name_col = "annotator_" + str(len(name_cols) + 1).zfill(2)
        # Put their name in the column
        df[name_col] = name
        # Make a column for annotations_name
        annotation_col = "clip_status_" + str(len(name_cols) + 1).zfill(2)
        df[annotation_col] = np.nan
    else:
        # The name is in names, get the columns of interest
        name_col = name_cols[names.index(name)]
        annotation_col = name_col.replace("annotator_", "clip_status_")

    return df, name_col, annotation_col


# Main
if __name__ == "__main__":
    print("Radiology Report Annotation Helper Library v 0.2")
    print("Written and maintained by Jenna Young, PhD (@jmschabdach on Github)")
    print("Tested and used by:")
    print("- Caleb Schmitt, Summer 2021")
    print("- Nadia Ngom, Fall 2021 - Spring 2022")
    print("- Emma Yang, Summer 2022")
    print("- Aaron Alexander-Bloch")
