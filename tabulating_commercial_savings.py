#!/usr/bin/env python
# coding: utf-8

# This program reads in a file and processes it by chunk. It allows the user to specify two columns and it will track whether one of the columns is greater than the other. It will output the number of rows, the number of correct incidents and the percentage of correct. 

# In[1]:


#Use Jupyter magic to change directories 
#C:\Users\skell\Downloads
get_ipython().run_line_magic('cd', '"C:\\Users\\skell\\Downloads"')


# In[2]:


#Open and download packages 
import pandas as pd 
import zipfile as zfile


# In[3]:


#open the zipped file in its own directory
#first set directories 
path_to_zip_file = 'FY2019_All_Contracts_Full_20200713.zip'
directory_to_extract_to = 'FY2019_All_Contracts_Full_20200713'

#unzip and extract 
with zfile.ZipFile(path_to_zip_file, 'r') as zip_ref:
    zip_ref.extractall(directory_to_extract_to)


# In[4]:


#use jupyter magic function to change directories again 
#C:\Users\skell\Downloads\FY2019_All_Contracts_Full_20200713
get_ipython().run_line_magic('cd', '"C:\\Users\\skell\\Downloads\\FY2019_All_Contracts_Full_20200713"')


# In[5]:


#Double check what is in the folder 
get_ipython().run_line_magic('ls', '')


# In the above directory there are 7 files. Each is almost 2 gigs. We're going to focus on one file and if we need to expand to all we will do that in the next iteration. 

# In[12]:


#read in the dataframe for review of the columns 
df = pd.read_csv('FY2019_All_Contracts_Full_20200714_1.csv',sep = ',',nrows=10)
df.dtypes

# list all columns from the dataframe 
list(df.columns) 


# In this case there are MANY columns so we first look for all the numeric columns and then all of the non-numeric. In the end we will read in just what we need and do the comparisons in chunks. 

# In[14]:


#check for all numerical columns 
import numpy as np
list(df.select_dtypes(np.number))


# In[18]:


#now read in non-numeric 
#gapminder.select_dtypes(exlude='float')
non_numeric_columns = list(df.select_dtypes(exclude=np.number))
non_numeric_columns


# In[26]:


#Well use this block to find good columns 
#['award_id_piid', 'parent_award_id_piid']
#['modification_number', 'parent_award_modification_number']
#['foreign_funding_description', 'award_description', 'inherently_governmental_functions_description', 
#'product_or_service_code_description', '
#dod_claimant_program_description', 'naics_description', 'dod_acquisition_program_description']
import re
r = re.compile(".*description")
newlist = list(filter(r.match, non_numeric_columns)) # Read Note
print(newlist)


# In[61]:


#read in the dataframe as a test for the correct columns of interest 
#make columns into a list 
col_names = ['award_id_piid', 'parent_award_id_piid','modification_number', 
             'parent_award_modification_number','foreign_funding_description', 'award_description', 
             'total_dollars_obligated','base_and_exercised_options_value',
             'current_total_value_of_award','base_and_all_options_value','potential_total_value_of_award','action_date',
             'period_of_performance_start_date', 'period_of_performance_current_end_date',
             'period_of_performance_potential_end_date','ordering_period_end_date','solicitation_date',
             'funding_agency_name','recipient_name']

#identify columns that are dates 
date_columns = ['action_date','period_of_performance_start_date', 'period_of_performance_current_end_date',
                'period_of_performance_potential_end_date','ordering_period_end_date','solicitation_date']

#create the dateframe 
df = pd.read_csv('FY2019_All_Contracts_Full_20200714_1.csv',sep = ',', nrows=5,usecols=col_names,
                parse_dates=date_columns)

#check the dtypes
df.dtypes


# In[62]:


#examine the subset of data and summarize it before we begin tabulation 
df.head(5)


# In[63]:


#create test column 
df['Comparison'] = df['total_dollars_obligated']>df['base_and_exercised_options_value']
df.head(5)


# In[64]:


#tally up the column for true and false
df.Comparison.sum()


# First well define the process to be done in each iteration of the data and then well build the function to process each section. 

# In[69]:


#Define chunk size of the data and iterate over the full file.
#define variables of interest 
size = 0 
successes = 0

#define main loop for counting rows and successes 
chunksize = 10 ** 6
for chunk in pd.read_csv('FY2019_All_Contracts_Full_20200714_1.csv',sep = ',',usecols=col_names,
                parse_dates=date_columns,chunksize=chunksize):
    
    #create column for comparisons 
    chunk['Comparison'] = chunk['total_dollars_obligated']>chunk['base_and_exercised_options_value']
    
    #tally up the successes 
    successes_sub=chunk.Comparison.sum()
    successes=successes+successes_sub
    
    #count the number of rows 
    count_row = chunk.shape[0]
    
    #add back to the main variables 
    size = size + count_row
   
print(size," ",successes," ",successes/size)

