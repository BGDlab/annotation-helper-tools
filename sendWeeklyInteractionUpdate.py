from reportMarkingFunctions import *
from datetime import date, timedelta

def main():
    # Get today's date
    today = date.today()
    lastWeek = (today - timedelta(days=7)).isoformat()
    last4Weeks = (today - timedelta(days=28)).isoformat()

    # Print a nice header
    print("The interaction report for SCIT-605 is below:\n")

    # Print the interaction report for the last week
    print("Since", lastWeek)
    getGradeCountsSinceDate(lastWeek)

    # Print the interaction report for the last 4 weeks
    print("\nSince", last4Weeks)
    getGradeCountsSinceDate(last4Weeks)

if __name__ == "__main__":
    main()
