import pandas as pd

#create DataFrame
df = pd.DataFrame({'team': ['A', 'A', 'A', 'A', 'B', 'B', 'B', 'B'],
                   'position': ['G', 'G', 'F', 'F', 'G', 'F', 'F', 'F'],
                   'all_star': ['Y', 'N', 'Y', 'Y', 'N', 'N', 'N', 'Y'],
                   'points': [4, 4, 6, 8, 9, 5, 5, 12]})


my_table = pd.pivot_table(df, values='points',
                              index=['team', 'all_star'],
                              columns='position',
                              aggfunc='sum')


test = pd.concat([
    y._append(y.sum().rename((x, 'Total')))
    for x, y in my_table.groupby(level=0)
])._append(my_table.sum().rename(('Grand', 'Total')))

print(test)
