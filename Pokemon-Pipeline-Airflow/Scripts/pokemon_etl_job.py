#===================================================================================================================
# Extract Transform Load (ETL) STEP
# Author: Gustavo Barreto
# Language: Python
# Date 26/12/2023
#===================================================================================================================

import pandas as pd
import re
import ast

# Reading the data 
location = '/opt/airflow/datasets/raw/raw_pokemon_dataset.csv'
df = pd.read_csv(location)

# If a Pokemon has more than one type we will create a column for each type
df['type'] = df['type'].apply(lambda x: re.sub(r"\[|\]|'| ", '', x).split(','))
df_expanded_types = df['type'].apply(lambda x: pd.Series(x))

# Renaming the new type columns
new_column_names = [f'type_{i}' for i in range(1, df_expanded_types.shape[1] +1)]
df_expanded_types.columns = new_column_names

# Now we will do the same thing for the stats column, but in this case we will need to convert it back to a dict after saving it in a csv it got converted to a string
df['stats'] = df['stats'].apply(lambda x: ast.literal_eval(x)) # Converting a string to a dict
df_expanded_stats = df['stats'].apply(lambda x: pd.Series(x))

# Joining/combinig the new fields with the original Data frame
df = pd.concat([df, df_expanded_types, df_expanded_stats], axis=1).drop(['type', 'stats', 'moves'], axis=1)

# Filling out the NaN as we know that a pokemon have at least one type, we will ignore the type_1 and be filling out the type_2 
df['type_2'] = df['type_2'].fillna('single type')

# Now lets title every string col and convert hidden_ability to a string since we cannot title/capitalize a dict 
df['hidden_ability'] = df['hidden_ability'].apply(str)
for c, t in zip(df.columns, df.dtypes):
    if t == 'object':
        df[c] = df[c].apply(str.title)

# Selecting the columns order
df = df[[
'name',
'type_1',
'type_2',
'heigth_cm',
'weight_kg',
'hp', 'attack',
'defense',
'speed',
'special-attack',
'special-defense',
'total_moves',
'hidden_ability'
]] 

# Saving the new dataframe
path = '/opt/airflow/datasets/refined/'
df.to_csv(f'{path}pokemon_dataset.csv', index=False, encoding='utf-8')