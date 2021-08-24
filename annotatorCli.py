import pandas as pd
import numpy as np
from annotationHelperLib import *

# Main
if __name__ == "__main__":

    fn = "/Users/schabdachj/Data/clip/tables/rawdata/caleb_start.csv"
    df = loadDataframe(fn)
    markReasonAndHistory(df, fn)

