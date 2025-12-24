import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
pd.set_option('display.max_columns', None)
plt.rcParams['font.family'] ='Malgun Gothic'
plt.rcParams['axes.unicode_minus'] =False

df_rc = pd.read_csv('c:/Users/nrise/Result_11.csv')
df_rr = pd.read_csv('c:/Users/nrise/Result_20.csv')




df_hour_rc = df_rc[df_rc['min_rc'] == 2].copy()
df_hour_rr = df_rr[df_rr['min_rr'] == 2].copy()

df_rr_filtered = df_rr[df_rr['min_rr'] <= 2].copy()
df_hour_rr_latest = (df_rr_filtered
                     .sort_values(['user2_id', 'min_rr'], ascending=[True, False])
                     .drop_duplicates('user2_id'))

df_hour = df_hour_rc.merge(df_hour_rr_latest[['user2_id', 'cumul_rcmd_2way_response_users']], on='user2_id', how='left', suffixes=('', '_rr'))
df_hour.head()

df_hour.sort_values('cumul_rcmd_2way_response_users_rr', ascending=False).head(10)

df_hour['cumul_rcmd_2way_response_users_rr'] = df_hour['cumul_rcmd_2way_response_users_rr'].fillna(0)


plt.figure(figsize=(10,6))
sns.scatterplot(data=df_hour, x='cumul_rcmd_users', y='cumul_rcmd_response_users', hue='user2_relative_score', palette = 'turbo', alpha=0.5)
plt.title('가입 후 1시간 추천 나간 수 vs 반응 수')
plt.xlabel('추천 나간 수 (누적)')
plt.ylabel('반응 수 (누적)')
plt.grid(True)
plt.savefig('rcmd_response.png')


plt.figure(figsize=(10,6))
sns.scatterplot(data=df_hour, x='cumul_rcmd_users', y='cumul_rcmd_2way_response_users_rr', hue='user2_relative_score', palette = 'turbo', alpha=0.5)
plt.title('가입 후 1시간 추천 나간 수 vs 나괜생 반응 수')
plt.xlabel('추천 나간 수 (누적)')
plt.ylabel('나괜생 반응 수 (누적)')
plt.grid(True)
plt.savefig('rcmd_2way_response.png')

# fig = plt.figure(figsize=(10,6))
# ax = fig.add_subplot(111, projection='3d') # 3D 축 생성
# scatter = ax.scatter(df_hour['cumul_rcmd_response_users'], df_hour['cumul_rcmd_users'], df_hour['cumul_rcmd_2way_response_users_rr'], c=df_hour['user2_relative_score'], cmap='turbo', s=50, alpha=0.6)

# ax.set_xlabel('Cumul Rcmd Response Users')
# ax.set_ylabel('Cumul Rcmd Users')
# ax.set_zlabel('Cumul Rcmd 2way Response Users')
# plt.title('3D Scatter Plot of Rcmds and Responses')
# plt.show()


df_hour_nonzero = df_hour[df_hour['cumul_rcmd_2way_response_users_rr'] > 0].copy()
bins = list(range(0, 3101, 100))
df_hour_nonzero['rcmd_users_bin'] = pd.cut(df_hour_nonzero['cumul_rcmd_users'], bins=bins, right=False)

bin_medians = df_hour_nonzero.groupby('rcmd_users_bin')['cumul_rcmd_2way_response_users_rr'].median().reset_index()
x_indices=  np.arange(len(bin_medians))
plt.figure(figsize=(20,10))
sns.boxplot(data=df_hour_nonzero, x='rcmd_users_bin', y='cumul_rcmd_2way_response_users_rr')
plt.plot(x_indices, bin_medians['cumul_rcmd_2way_response_users_rr'], color='red', marker='o', label='Median', linestyle='--')
plt.legend()    
plt.xlabel('추천 나간 수 구간')
plt.ylabel('나괜생 반응 수 (누적)')
plt.xticks(rotation=45)
plt.title('추천 나간 수 구간별 나괜생 반응 수 분포')
plt.grid(axis='y',alpha=0.5)
# plt.show()
plt.savefig('rcmd_users_bin_boxplot.png')
