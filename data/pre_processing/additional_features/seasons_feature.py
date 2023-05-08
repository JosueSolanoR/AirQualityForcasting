import pandas as pd

def add_seasons(df):
  """
  Args:
    df - pandas dataframe
    
  Output:
    df - Updated pandas dataframe with season features. The updated df adds
         four more columns ('Winter', 'Summer', 'Monsoon', and 'Post-Monsoon'),
         where each row has a 1 on one of the four seasons, according to the
         'From' column.
  """
  columns = df.columns
  fromIdx = 1
  # We could split this further down to day persicion.
  # This conversion is absed on the informatrion on "Seasons and Normalization"
  monthToSeason = {"Winter":["Dec", "Jan", "Feb", "Mar", "Apr"], 
                   "Summer":["May", "Jun", "Jul"],
                   "Monsoon":["Aug", "Sep"],
                   "Post-Monsoon":["Oct", "Nov"]}
  seasons = []
  for index, row in df.iterrows():
    day, month, year = row[fromIdx].split('-')
    if month in monthToSeason["Winter"]:
      seasons.append("Winter")
    elif month in monthToSeason["Summer"]:
      seasons.append("Summer")
    elif month in monthToSeason["Monsoon"]:
      seasons.append("Monsoon")
    elif month in monthToSeason["Post-Monsoon"]:
      seasons.append("Post-Monsoon")
  # Creates a column for each label in seasons and assigns a 0 or 1
  dummies = pd.get_dummies(seasons)
  # Concatinate the four season columns to df
  df = pd.concat([df, dummies], axis='columns')
  return df