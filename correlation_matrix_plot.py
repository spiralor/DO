from matplotlib import pyplot as plt
import pandas as pd
import numpy as np

data_path = 'C:/Users/26982/Desktop/DO Experiment/Indian/GRIDDING_2dbf5_t_seed1_after1970_Indian.csv'
df = pd.read_csv(data_path)
corr = df.corr().round(2).values

f = plt.figure(figsize=(20, 20))

corr_mtrix = plt.matshow(corr, fignum=f.number, cmap='coolwarm')
cb = plt.colorbar()
cb.ax.tick_params(labelsize=20)
xticks_ticks = range(df.columns.shape[0])
xticks_labels = df.columns.to_numpy()
yticks_ticks = xticks_ticks
yticks_labels = xticks_labels
plt.xticks(xticks_ticks, xticks_labels, fontsize=20, rotation=90)
plt.yticks(range(df.select_dtypes(['number']).shape[1]), df.select_dtypes(['number']).columns, fontsize=20)

for row in range(16):
    for col in range(16):
        plt.text(row, col, corr[row][col], horizontalalignment='center', verticalalignment='center',
                 fontsize=20)

plt.title('2dbf5_t_seed1_after1970_Indian_Ocean Correlation Matrix', fontsize=40)
plt.show()
