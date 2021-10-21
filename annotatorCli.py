from annotationHelperLib import *
import argparse

# Main
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
    print(list(df))

    # Annotate
    #markAllFields(df, fn, name)
    

