import pandas as pd
import numpy as np 
import seaborn as sns
import matplotlib.pyplot as plt

pd.set_option('display.max_columns', None)
plt.rcParams['font.family'] ='Malgun Gothic'
plt.rcParams['axes.unicode_minus'] =False

# Load Data
user_log = pd.read_csv('C:/Users/nrise/Result_10.csv')
user_log.head()
user_log.describe(include='all')
print(user_log.columns)
user_col = 'ci_hash'

referral_users = user_log[user_log['get_referral_reward']==0]
nfu_log = user_log[user_log['get_referral_reward']==0].copy()
nfu_log['rs_grade'] = nfu_log['relative_score'] // 10 +1

nfu_b = nfu_log[nfu_log['user_group']=='B'].copy()
nfu_a = nfu_log[nfu_log['user_group']=='A'].copy()

# Data Preprocessing Functions
def preprocess_df(nfu_a):
    first_purchase_idx = nfu_a[nfu_a['purchase_num']==1][[user_col,'action_num']]
    purchaser_ids = first_purchase_idx[user_col].unique()
    purchased_a = nfu_a.merge(first_purchase_idx, on=user_col, how='inner')
    num_purchased_a = purchased_a[user_col].nunique()

    a_non_pay = nfu_a[ (~nfu_a[user_col].isin(purchaser_ids)) & (nfu_a['quantity'] <= 0)].copy()

    a_pre = purchased_a[
        (purchased_a['action_num_x'] < purchased_a['action_num_y']) & (purchased_a['quantity'] <= 0)
        ].copy()

    a_post_10 = purchased_a[
        (purchased_a['action_num_x'] <= purchased_a['action_num_y']+10) & (purchased_a['action_num_x'] > purchased_a['action_num_y']) & (purchased_a['quantity'] <= 0)
    ].copy()

    a_post_20 = purchased_a[
        (purchased_a['action_num_x'] <= purchased_a['action_num_y']+20) & (purchased_a['action_num_x'] > purchased_a['action_num_y']+10) & (purchased_a['quantity'] <= 0)
    ].copy()

    pre_dfs = [a_pre, a_post_10, a_post_20, a_non_pay] # 리스트로 묶어서 처리
        
    for df in pre_dfs:
        df['quantity'] = df['quantity'].abs()
        if 'use_type' in df.columns:
            df['use_type'] = df['use_type'].str.replace('타임어택','친구 신청')
            df['use_type'] = df['use_type'].fillna('Other')

    return a_non_pay,a_pre, a_post_10,a_post_20, num_purchased_a

def create_pivot(df,num_users):
    freq = df.pivot_table(
        index = 'ci_hash',
        columns = ['use_type','description'],
        values = 'quantity',
        aggfunc = 'count',
        fill_value = 0
    ) 
    # / num_users
    vol = df.pivot_table(
        index = 'ci_hash',
        columns = ['use_type','description'],
        values = 'quantity',
        aggfunc = 'sum',
        fill_value = 0
    ) 
    # / num_users

    # total_freq = freq.sum(axis=0).sort_values(ascending=False)
    # total_vol = vol.sum(axis=0).sort_values(ascending=False)
    # vol = vol.reindex(columns=total_vol.index)
    # freq = freq.reindex(columns=total_freq.index)
    # top_descriptions = df.groupby('description')['ci_hash'].count().sort_values(ascending=False).index
    # freq = freq[top_descriptions]
    # vol = vol[top_descriptions]

    return freq, vol

# Data Preprocessing
a_non_pay,a_pre, a_post, a_post_20, num_purchased_a = preprocess_df(nfu_a)
a_non_freq, a_non_vol = create_pivot(a_non_pay,num_purchased_a)
a_pre_freq, a_pre_vol = create_pivot(a_pre,num_purchased_a)
a_post_freq, a_post_vol = create_pivot(a_post,num_purchased_a)
a_post_20_freq, a_post_20_vol = create_pivot(a_post_20,num_purchased_a)

b_non_pay,b_pre, b_post, b_post_20, num_purchased_b = preprocess_df(nfu_b)
b_non_freq, b_non_vol = create_pivot(b_non_pay,num_purchased_b)
b_pre_freq, b_pre_vol = create_pivot(b_pre,num_purchased_b)
b_post_freq, b_post_vol = create_pivot(b_post,num_purchased_b)
b_post_20_freq, b_post_20_vol = create_pivot(b_post_20,num_purchased_b)

# combine pre and post data with indexing columns
a_all_freq = pd.concat([a_non_freq.reset_index().assign(period='non'),a_pre_freq.reset_index().assign(period='pre'), a_post_freq.reset_index().assign(period='post_10'), a_post_20_freq.reset_index().assign(period='post_20')], axis=0, ignore_index=True)
b_all_freq = pd.concat([b_non_freq.reset_index().assign(period='non'),b_pre_freq.reset_index().assign(period='pre'), b_post_freq.reset_index().assign(period='post_10'), b_post_20_freq.reset_index().assign(period='post_20')], axis=0, ignore_index=True)
a_all_vol = pd.concat([a_non_vol.reset_index().assign(period='non'),a_pre_vol.reset_index().assign(period='pre'), a_post_vol.reset_index().assign(period='post_10'), a_post_20_vol.reset_index().assign(period='post_20')], axis=0, ignore_index=True)
b_all_vol = pd.concat([b_non_vol.reset_index().assign(period='non'),b_pre_vol.reset_index().assign(period='pre'), b_post_vol.reset_index().assign(period='post_10'), b_post_20_vol.reset_index().assign(period='post_20')], axis=0, ignore_index=True)

a_all_freq.to_csv('a_all_freq_periods.csv')
b_all_freq.to_csv('b_all_freq_periods.csv')
a_all_vol.to_csv('a_all_vol_periods.csv')
b_all_vol.to_csv('b_all_vol_periods.csv')

user_grade = nfu_log[['ci_hash', 'rs_grade']].drop_duplicates(subset='ci_hash')
user_grade.to_csv('user_grade.csv')

# Visualization
clmap = 'tab20'
fig, axes = plt.subplots(2, 4, figsize=(40, 16))
a_pre_freq.plot(kind='barh', stacked=True, ax=axes[0,0], colormap=clmap)
axes[0,0].set_title(f'Pre-Purchase: Avg Action Count (Group A, N={num_purchased_a})')
axes[0,0].set_xlabel('Avg Action Count per User')
axes[0,0].set_ylabel('Use Type (Context)')
axes[0,0].legend(loc='lower right', title='Description', bbox_to_anchor=(1, 0), fontsize='small',ncol=2)

a_post_freq.plot(kind='barh', stacked=True, ax=axes[1,0], colormap=clmap)
axes[1,0].set_title(f'Post-Purchase: Avg Action Count (Group A, N={num_purchased_a})')
axes[1,0].set_xlabel('Avg Action Count per User')
axes[1,0].set_ylabel('')
axes[1,0].legend(loc='lower right', title='Description', bbox_to_anchor=(1, 0), fontsize='small',ncol=2)


b_pre_freq.plot(kind='barh', stacked=True, ax=axes[0,1], colormap=clmap)
axes[0,1].set_title(f'Pre-Purchase: Avg Action Count (Group B, N={num_purchased_b})')
axes[0,1].set_xlabel('Avg Action Count per User')
axes[0,1].set_ylabel('Use Type (Context)')
axes[0,1].legend(loc='lower right', title='Description', bbox_to_anchor=(1, 0), fontsize='small',ncol=2)

b_post_freq.plot(kind='barh', stacked=True, ax=axes[1,1], colormap=clmap)
axes[1,1].set_title(f'Post-Purchase: Avg Action Count (Group B, N={num_purchased_b})')
axes[1,1].set_xlabel('Avg Action Count per User')
axes[1,1].set_ylabel('')
axes[1,1].legend(loc='lower right', title='Description', bbox_to_anchor=(1, 0), fontsize='small',ncol=2)


a_pre_vol.plot(kind='barh', stacked=True, ax=axes[0,2], colormap=clmap)
axes[0,2].set_title(f'Pre-Purchase: Avg Jelly Use (Group A, N={num_purchased_a})')
axes[0,2].set_xlabel('Avg Jelly Use per User')
axes[0,2].set_ylabel('Use Type (Context)')
axes[0,2].legend(loc='lower right', title='Description', bbox_to_anchor=(1, 0), fontsize='small',ncol=2)

a_post_vol.plot(kind='barh', stacked=True, ax=axes[1,2], colormap=clmap)
axes[1,2].set_title(f'Post-Purchase: Avg Jelly Use (Group A, N={num_purchased_a})')
axes[1,2].set_xlabel('Avg Jelly Use per User')
axes[1,2].set_ylabel('')
axes[1,2].legend(loc='lower right', title='Description', bbox_to_anchor=(1, 0), fontsize='small',ncol=2)


b_pre_vol.plot(kind='barh', stacked=True, ax=axes[0,3], colormap=clmap)
axes[0,3].set_title(f'Pre-Purchase: Avg Jelly Use (Group B, N={num_purchased_b})')
axes[0,3].set_xlabel('Avg Jelly Use per User')
axes[0,3].set_ylabel('Use Type (Context)')
axes[0,3].legend(loc='lower right', title='Description', bbox_to_anchor=(1, 0), fontsize='small',ncol=2)

b_post_vol.plot(kind='barh', stacked=True, ax=axes[1,3], colormap=clmap)
axes[1,3].set_title(f'Post-Purchase: Avg Jelly Use (Group B, N={num_purchased_b})')
axes[1,3].set_xlabel('Avg Jelly Use per User')
axes[1,3].set_ylabel('')
axes[1,3].legend(loc='lower right', title='Description', bbox_to_anchor=(1, 0), fontsize='small',ncol=2)

plt.tight_layout()
plt.savefig('non-referral_pre_post_analysis_all.png')
plt.show()

a_pre_vol.to_csv('a_pre_vol.csv')
a_post_vol.to_csv('a_post_vol.csv')
b_pre_vol.to_csv('b_pre_vol.csv')
b_post_vol.to_csv('b_post_vol.csv')
a_pre_freq.to_csv('a_pre_freq.csv')
a_post_freq.to_csv('a_post_freq.csv')
