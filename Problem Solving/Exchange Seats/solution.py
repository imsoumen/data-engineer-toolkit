
import pandas as pd

data = [
    (1, 'Amit'),
    (2, 'Deepa'),
    (3, 'Rohit'),
    (4, 'Anjali'),
    (5, 'Neha'),
    (6, 'Sanjay'),
    (7, 'Priya')
]


df = pd.DataFrame(data, columns=['id', 'student'])

df['rn'] = df.index + 1

df['new_id'] = df.apply(
    lambda row: df.iloc[row['rn'] - 2]['id'] if row['rn'] % 2 == 0 else (df.iloc[row['rn']]['id'] if row['rn'] < len(df) else row['id']),
    axis=1
)

# df['check'] = df.iloc[1]['id']
df = df[['id','new_id', 'student']]

print(df)


# Output
# id  new_id student
# 1       2    Amit
# 2       1   Deepa
# 3       4   Rohit
# 4       3  Anjali
# 5       6    Neha
# 6       5  Sanjay
# 7       7   Priya

