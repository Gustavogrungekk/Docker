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

# Some data cleaning on type
df['type'] = df['type'].apply(lambda x: re.sub(r"\[|\]|'| ", '', x).split(','))

# If a Pokemon has more than one type we will create a column for each type
df_expanded_types = df['type'].apply(lambda x: pd.Series(x))

# Renaming the new columns
new_column_names = [f'type_{i}' for i in range(1, df_expanded_types.shape[1] +1)]
df_expanded_types.columns = new_column_names

# Now we will do the same thing for the stats column
df['stats'] = df['stats'].apply(lambda x: ast.literal_eval(x)) # Converting a string to a dict

df_expanded_stats = df['stats'].apply(lambda x: pd.Series(x))

# Joining/combinig the new fields with the original Data frame
df = pd.concat([df, df_expanded_types, df_expanded_stats], axis=1).drop(['type', 'stats', 'moves'], axis=1)

# Filling out the NaN as we know that a pokemon have at least one type, we will ignore the type_0 and be filling out the type_2 
df['type_2'] = df['type_2'].fillna('single type')

# Now lets title every string col
for c, t in zip(df.columns, df.dtypes):
    if t == 'object':
        df[c] = df[c].apply(str.title)


path = '/opt/airflow/datasets/refined/'
df.to_csv(f'{path}pokemon_dataset.csv', index=False, encoding='utf-8')