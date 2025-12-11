import pandas as pd
import numpy as np 
import seaborn as sns
import matplotlib.pyplot as plt

pd.set_option('display.max_columns', None)
plt.rcParams['font.family'] ='Malgun Gothic'
plt.rcParams['axes.unicode_minus'] =False

user_log = pd.read_csv('c:/Users/nrise/Downloads/Result_27.csv')
user_log.head()
user_log.describe(include='all')
print(user_log.columns)
user_col = 'ci_hash'

referral_users = user_log[user_log['get_referral_reward']==0]
nfu_log = user_log[user_log['get_referral_reward']==0].copy()

nfu_b = nfu_log[nfu_log['user_group']=='B'].copy()
nfu_a = nfu_log[nfu_log['user_group']=='A'].copy()


def preprocess_df(nfu_a):
    first_purchase_idx = nfu_a[nfu_a['purchase_num']==1][[user_col,'action_num']]
    purchased_a = nfu_a.merge(first_purchase_idx, on=user_col, how='inner')
    num_purchased_a = purchased_a[user_col].nunique()

    a_pre = purchased_a[
        (purchased_a['action_num_x'] < purchased_a['action_num_y']) & (purchased_a['quantity'] <= 0)
        ].copy()

    a_post = purchased_a[
        (purchased_a['action_num_x'] > purchased_a['action_num_y']) & (purchased_a['quantity'] <= 0)
    ].copy()

    a_pre['quantity'] = a_pre['quantity'].abs()
    a_post['quantity'] = a_post['quantity'].abs()

    a_pre['use_type'] = a_pre['use_type'].str.replace('타임어택','친구 신청')
    a_post['use_type'] = a_post['use_type'].str.replace('타임어택','친구 신청')

    a_pre['use_type'] = a_pre['use_type'].fillna('Other')
    a_post['use_type'] = a_post['use_type'].fillna('Other')
    return a_pre, a_post, num_purchased_a

def create_pivot(df,num_users):
    freq_table = df.pivot_table(
        index = 'use_type',
        columns = 'description',
        values = user_col,
        aggfunc = 'count',
        fill_value = 0
    ) / num_users
    vol_table = df.pivot_table(
        index = 'use_type',
        columns = 'description',
        values = 'quantity',
        aggfunc = 'sum',
        fill_value = 0
    ) / num_users
    return freq_table, vol_table
def sort_pivot(freq,vol,df):
    total_freq = freq.sum(axis=0).sort_values(ascending=False)
    total_vol = vol.sum(axis=0).sort_values(ascending=False)
    vol = vol.reindex(columns=total_vol.index)
    freq = freq.reindex(columns=total_freq.index)
    top_descriptions = df.groupby('description')['ci_hash'].count().sort_values(ascending=False).index
    freq = freq[top_descriptions]
    vol = vol[top_descriptions]
    return freq, vol

a_pre, a_post, num_purchased_a = preprocess_df(nfu_a)
a_pre_freq, a_pre_vol = create_pivot(a_pre,num_purchased_a)
a_post_freq, a_post_vol = create_pivot(a_post,num_purchased_a)
a_pre_freq, a_pre_vol = sort_pivot(a_pre_freq, a_pre_vol, a_pre)
a_post_freq, a_post_vol = sort_pivot(a_post_freq, a_post_vol, a_post)

b_pre, b_post, num_purchased_b = preprocess_df(nfu_b)
b_pre_freq, b_pre_vol = create_pivot(b_pre,num_purchased_b)
b_post_freq, b_post_vol = create_pivot(b_post,num_purchased_b)
b_pre_freq, b_pre_vol = sort_pivot(b_pre_freq, b_pre_vol, b_pre)
b_post_freq, b_post_vol = sort_pivot(b_post_freq, b_post_vol, b_post)


# Visualization
fig, axes = plt.subplots(2, 2, figsize=(20, 16))

a_pre_freq.plot(kind='barh', stacked=True, ax=axes[0,0], colormap='turbo')
axes[0,0].set_title(f'Pre-Purchase: Avg Jelly Use (Group A, N={num_purchased_a})')
axes[0,0].set_xlabel('Avg Jelly Use per User')
axes[0,0].set_ylabel('Use Type (Context)')
axes[0,0].legend(loc='lower right', title='Description', bbox_to_anchor=(1, 0), fontsize='small',ncol=2)

a_post_freq.plot(kind='barh', stacked=True, ax=axes[1,0], colormap='turbo')
axes[1,0].set_title(f'Post-Purchase: Avg Jelly Use (Group A, N={num_purchased_a})')
axes[1,0].set_xlabel('Avg Jelly Use per User')
axes[1,0].set_ylabel('')
axes[1,0].legend(loc='lower right', title='Description', bbox_to_anchor=(1, 0), fontsize='small',ncol=2)


b_pre_freq.plot(kind='barh', stacked=True, ax=axes[0,1], colormap='turbo')
axes[0,1].set_title(f'Pre-Purchase: Avg Jelly Use (Group B, N={num_purchased_b})')
axes[0,1].set_xlabel('Avg Jelly Use per User')
axes[0,1].set_ylabel('Use Type (Context)')
axes[0,1].legend(loc='lower right', title='Description', bbox_to_anchor=(1, 0), fontsize='small',ncol=2)

b_post_freq.plot(kind='barh', stacked=True, ax=axes[1,1], colormap='turbo')
axes[1,1].set_title(f'Post-Purchase: Avg Jelly Use (Group B, N={num_purchased_b})')
axes[1,1].set_xlabel('Avg Jelly Use per User')
axes[1,1].set_ylabel('')
axes[1,1].legend(loc='lower right', title='Description', bbox_to_anchor=(1, 0), fontsize='small',ncol=2)

plt.tight_layout()
# plt.savefig('non-referral_pre_post_analysis_vol.png')
plt.show()