#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Import pandas matplotlib and seaborn
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import datetime as dt
import numpy as np


# In[2]:


import os
path = 'G:\Projects\Olist\RFM'
files = os.listdir(path)
for file in files:
    print(file)


# In[3]:


#loading data into notebook
df = pd.read_csv(r"G:\Projects\Olist\RFM\online.csv")


# In[4]:


df.shape


# In[5]:


df.head()


# In[6]:


# Define a function that will parse the date
def get_day(x):
    return dt.datetime.strptime(x, "%m/%d/%Y %H:%M")


# In[7]:


# Create InvoiceDay column
df['purchase_date'] = df['purchase_date'].apply(get_day) 


# In[8]:


#check the values in 'InvoiceDay' column
for date in df['purchase_date'][:5]:
    print(date)


# In[9]:


# Group by CustomerID and select the purchase_date value
grouped_df = df.groupby('customer_id')['purchase_date']

# Check grouped_df values
grouped_df.head()


# In[10]:


# Assign a minimum InvoiceDay value to the dataset as a new 'CohortDay' column
df['CohortDay']= grouped_df.transform('min')

#check values of 'CohortDay' column
df.head()


# In[11]:


#Create Function to Get the integers for date parts
def get_date_int(df, column):
    year = df[column].dt.year
    month = df[column].dt.month
    day = df[column].dt.day
    return year, month,day


# In[12]:


# Get the integers for date parts from the `purcahse_date` column
purchase_year, purchase_month, purchase_day = get_date_int(df,'purchase_date')

# Get the integers for date parts from the `CohortDay` column
Cohort_year, Cohort_month, Cohort_day = get_date_int(df,'CohortDay')


# In[13]:


# Calculate difference in years
years_diff = purchase_year - Cohort_year

# Calculate difference in months
months_diff = purchase_month - Cohort_month

# Calculate difference in days
days_diff = purchase_day - Cohort_day

# Extract the difference in days from all previous values
df['CohorIndex'] = years_diff*12 + months_diff +1

#check CohortIndex Column
df.head()


# In[14]:


#create function to get the month of a given date column
def get_month(column):
    return dt.datetime(column.year, column.month, 1)


# In[15]:


#create purchase_month column
df['purchase_month'] = df['purchase_date'].apply(get_month)

#check purchase_month column
df.head()


# In[16]:


#create cohort_month column
grouping = df.groupby('customer_id')['purchase_month']

df['cohort_month'] = grouping.transform('min')

#check purchase_month column
df.head()


# ## Count monthly active customers from each cohort

# In[17]:


#count monthly active customers from each cohort
grouping = df.groupby(['cohort_month','CohorIndex'])

cohort_data = grouping['customer_id'].apply(pd.Series.nunique)

#check cohort_data table
cohort_data.head()


# In[18]:


#reset index for cohort_data to be able to access columns that stored as indeces
cohort_data = cohort_data.reset_index()

#create cohort_counts table
cohort_counts = cohort_data.pivot_table(
                                  index = 'cohort_month',
                                  columns = 'CohorIndex',
                                  values = 'customer_id'
                                    )

#check cohort_counts table
cohort_counts.head(10)


# ## Calculate retention rate 

# In[19]:


# Select the first column and store it to cohort_sizes table
cohort_sizes = cohort_counts.iloc[:,0]

#check values of cohort_sizes table
cohort_sizes.head(10)


# In[20]:


# Divide the cohort count by cohort sizes along the rows to get Retention
retention = cohort_counts.divide(cohort_sizes,axis=0)

#check retention table
retention.head(10)


# In[21]:


#display retention as percentages
retention.round(3)*100


# ## Retention Rates Visualization in a Heatmap

# In[22]:


# Initialize an 8 by 6 inches plot figure
plt.figure(figsize=(8, 6))

# Add a title
plt.title('Retention Rates of Monthly Cohorts')

# Create the heatmap
sns.heatmap(data = retention, 
            annot=True, 
            fmt = '.0%',
            vmin = .0,
            vmax = .5,
            cmap='Blues')
plt.show()


# ## Calculate Average Price

# In[23]:


# Create a groupby object and pass the monthly cohort and cohort index as a list
grouping =  df.groupby(['cohort_month','CohorIndex'])

# Calculate the average of the unit price column
cohort_data = grouping['UnitPrice'].mean()/10

#check values in cohort_data table
cohort_data.head(20)


# In[24]:


#reset index
cohort_data = cohort_data.reset_index()

#check values of cohort_data table
cohort_data.head()


# In[25]:


# Create a pivot 
average_price = cohort_data.pivot_table(
                                        index = 'cohort_month',
                                        columns = 'CohorIndex',
                                        values = 'UnitPrice')
#check values of pivot table
average_price.head(10)


# In[26]:


#round values of averag_price table
average_price.round(1)


# In[27]:


# Initialize an 8 by 6 inches plot figure
plt.figure(figsize=(8, 6))

# Add a title
plt.title('Average Spend by Monthly Cohorts')

# Create the heatmap
sns.heatmap(data = average_price, 
            annot=True, 
            fmt = '.0%',
            vmin = .0,
            vmax = .5,
            cmap='BuGn')
plt.show()


# # RFM

# In[28]:


df['spend'] = df['UnitPrice'] * df['Quantity']

df.head()


# In[29]:


#create df of customers with aggregated spends for each customer
spend_df = df.groupby('customer_id')['spend'].sum().reset_index()

spend_df.head()


# In[30]:


# Create a spend quartile with 4 groups - a range between 1 and 5
spend_df['Spend_Quartile'] = pd.qcut(spend_df['spend'], q=4, labels=range(1,5))

#check spend_df table values
spend_df.head()


# In[31]:


#create a copy of dataframe with recency column in days
df_copy = df.copy()

df_copy['cohort_days'] = days_diff

df_copy.head()


# In[32]:


#create df of customers with aggregated spends for each customer
recency_df = df_copy.groupby('customer_id')['cohort_days'].sum().reset_index()

recency_df.head()


# In[33]:


#snapshot is the date of the day after the last transaction
snapshot_date = max(df['purchase_date']) + dt.timedelta(days=1)


# In[34]:


# Calculate Recency, Frequency and Monetary value for each customer 
RFM_datamart = df.groupby(['customer_id']).agg({
    'purchase_date': lambda x: (snapshot_date - x.max()).days, #Recency = days since last customer purchase 
    'order_id': 'count', #Frequency = count of orders within 12 months for customer
    'spend': 'sum'}) #MonetaryValue = sum of spends within 12 months for customer

# Rename the columns 
RFM_datamart.rename(columns={'purchase_date': 'Recency',
                         'order_id': 'Frequency',
                         'spend': 'MonetaryValue'}, inplace=True)

RFM_datamart.head()


# In[35]:


#create reversed recency labels > because low recency values are better
r_labels = list(range(4, 0, -1))
print(r_labels)


# In[36]:


#Create groups of 4 segmentations for Recency, Frequency, MonetaryValue
RFM_datamart['R'] = pd.qcut(RFM_datamart['Recency'], q=4, labels = r_labels)

RFM_datamart['F'] = pd.qcut(RFM_datamart['Frequency'], q=4, labels = range(1,5))

RFM_datamart['M'] = pd.qcut(RFM_datamart['MonetaryValue'], q=4, labels = range(1,5))

#check RFM_datamart table
RFM_datamart.head()


# In[37]:


#calculate RFM score for each customer
RFM_datamart['RFM_Score'] = RFM_datamart[['R','F','M']].sum(axis=1)

RFM_datamart.head()


# In[38]:


#create RFM segment for each customer

#concat values of R, F, M columns
def join_RFM (x):
        r_value = int(x['R'])
        f_value = int(x['F'])
        m_value = int(x['M'])
        return str(r_value) + str(f_value) + str(m_value)
               
#create 'RFM_Segment' column by concating values through join_RFM definition
RFM_datamart['RFM_Segment'] = RFM_datamart.apply(join_RFM,axis=1)

#check values
RFM_datamart.head()


# In[39]:


RFM_datamart.describe()


# In[40]:


#create function of segmentation level
def RFM_level(df):
    if df['RFM_Score']>= 10:
        return 'High'
    elif df['RFM_Score']>= 6 and df['RFM_Score']<10:
        return'Medium'
    else:
        return 'Low'


# In[41]:


# Create a new column RFM_Level
RFM_datamart['RFM_Level'] = RFM_datamart.apply(RFM_level, axis = 1)

#check values
RFM_datamart.head()


# In[42]:


# Calculate average values for each RFM_Level, and return a size of each segment 
rfm_level_agg = RFM_datamart.groupby('RFM_Level').agg({
    'Recency': 'mean',
    'Frequency': 'mean',
  
  	# Return the size of each segment
    'MonetaryValue': ['mean', 'count']
}).round(1)

# Print the aggregated dataset
print(rfm_level_agg)


# In[43]:


#creat RFM_df
RFM_df = RFM_datamart[['Recency','Frequency','MonetaryValue']]


# In[44]:


# diaplay summary statistics of Recency, Frequency, Monetary
RFM_df.describe()


# In[45]:


import warnings

# Suppress all warnings
warnings.filterwarnings("ignore")


# In[46]:


#display recency distribution
sns.distplot(RFM_df['Recency'])
plt.show()


# In[47]:


#display Frecuency distribution
sns.distplot(RFM_df['Frequency'])
plt.show()


# In[48]:


#display MonetaryValue distribution
sns.distplot(RFM_df['MonetaryValue'])
plt.show()


# In[49]:


#log transformation of  Recency to have normal distribution
RFM_df['Recency_Log'] = np.log(RFM_df['Recency'])

sns.distplot(RFM_df['Recency_Log'] )
plt.show()


# In[50]:


#log transformation of Frequency to have normal distribution
RFM_df['Frequency_Log'] = np.log(RFM_df['Frequency'])

sns.distplot(RFM_df['Frequency_Log'] )
plt.show()


# In[51]:


#log transformation of Monetary to have normal distribution
RFM_df['Monetary_Log'] = np.log(RFM_df['MonetaryValue'])

sns.distplot(RFM_df['Monetary_Log'] )
plt.show()


# In[52]:


#center the data by manually standarize average values for all varieables : Recency, Frequency, MonetaryValue
centered_datamart = RFM_df - RFM_df.mean()

#display summary statistic
centered_datamart.describe().round(2)


# In[53]:


#scale the data by manually standarize standard deviation values for all varieables : Recency, Frequency, MonetaryValue
scaled_datamart = RFM_df/ RFM_df.std()

#display summary statistic
scaled_datamart.describe().round(2)


# In[54]:


# Normalize the data by applying both centering and scaling
normalized_datamart = (RFM_df - RFM_df.mean()) / RFM_df.std()

#display summary statistics of normalized data
normalized_datamart.describe().round(2)


# In[56]:


from sklearn.preprocessing import StandardScaler

# Initialize a scaler
scaler = StandardScaler()

# Fit the scaler
scaler.fit(RFM_df)


# In[57]:


# Scale and center the data
normalized_datamart = scaler.transform(RFM_df)

# Create a pandas DataFrame >> because RFM_df was turned into array in the previous step
normalized_datamart = pd.DataFrame(normalized_datamart, index=RFM_df.index, columns=RFM_df.columns)

#display summary statistics of normalized data
normalized_datamart.describe().round(2)


# In[62]:


# Import KMeans 
from sklearn.cluster import KMeans

# Initialize KMeans
kmeans = KMeans(n_clusters=3, random_state=1) 

# Fit k-means clustering on the normalized data set
kmeans.fit(normalized_datamart)

# Extract cluster labels
cluster_labels = kmeans.labels_

#display cluster_labels values
print(cluster_labels)


# In[ ]:




