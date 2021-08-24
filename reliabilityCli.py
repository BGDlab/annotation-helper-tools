from annotationHelperLib import *

# How to add a new annotator?
def checkForAnnotator(df, name):
    # Get the number of columns containing the word "annotator" in their name
    nameCols = [c for c in list(df) if "annotator" in c]
    
    # Get the individual's name from the columns
    names = [df[c].values[0] for c in nameCols]
    
    # If their name is not in the list of annotator names
    if name not in names:
        # Add a new column
        newNameCol = "annotator_"+ str(len(nameCols)+1).zfill(2)
        # Put their name in the column
        df[newNameCol] = name
        # Make a column for annotations_name
        newAnnotationCol = "confirm_clip_" + str(len(nameCols)+1).zfill(2) 
        df[newAnnotationCol] = np.nan

    return df


if __name__ == "__main__":

    fn = "/Users/schabdachj/Data/clip/tables/annotator_onboarding/shared_reports_for_reliability.csv"
    name = "Jisoo Kang"

    # load the dataframe
    df = loadDataframe(fn)

    # Check the user's name
    df = checkForAnnotator(df, name)

    print(list(df))
    print(df.head)

