from annotationHelperLib import *
import argparse


if __name__ == "__main__":

    # Set up the argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--filename", help="")
    parser.add_argument("-n", "--annotator-name", help='First and last name of the annotator in the format "First Last"')

    # Parse the arguments
    args = parser.parse_args()
    fn = args.filename
    name = args.annotator_name

    # Load the dataframe
    df = loadDataframe(fn)

    # Check the user's name
    df, nameCol, annotationCol = checkForAnnotator(df, name)

    # Interactively mark the reports
    markReliabilityReports(df, fn, annotationCol)

