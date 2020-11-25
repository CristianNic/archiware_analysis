import os
import re as re
import datetime as dt
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

def create_paths(path):

    head, tail = os.path.split(path)
    path_save_results = head + '/' + 'Results' + '/'  
    run_time = dt.datetime.today().strftime('%Y-%m-%d %I:%M:%S %p')
    path_save_results_runs = path_save_results + run_time + '/'
    path_graphs = path_save_results_runs + 'Graphs' + '/'
        
    # Create paths if they do not exist
    try:
        os.stat(path_save_results)
    except:
        os.mkdir(path_save_results)
    
    try:
        os.stat(path_save_results_runs)
    except:
        os.mkdir(path_save_results_runs)

    try:
        os.stat(path_graphs)
    except:
        os.mkdir(path_graphs)
        
    return path_save_results_runs, path_graphs


def load_data(path):

    path = path 
    
    df = pd.read_csv(path, sep='\t', dtype={'Folders': 'string'}, \
                      engine="c", low_memory=True, usecols=[0,2,3]) # verbose=True
    df.columns = ['Folders', 'Bytes', 'Date']
    pd.set_option('display.max_colwidth', 550)
    
    return df


def extract_archives(df):
    '''Isolate volumes of interest: Vol1'''
    
    df_vol1  = df[df['Folders'].str.contains('Volume1')] 

    return df_vol1


def convert_time(df_vol1):
    '''Convert time from unix timestamp to human readable'''
    
    df_vol1 = df_vol1.copy().reset_index()  
    df_vol1['Date'] = pd.to_datetime(df_vol1['Date'], unit='s', origin='unix')
    df_vol1['Year'] = df_vol1['Date'].dt.year       
    df_vol1 = df_vol1[['Folders', 'Bytes', 'Year']]  
    
    return df_vol1


def convert_bytes(df_vol1):
    '''Convert bytes column to TB'''
    
    terabytes_vol1  = [byte * (1/1099511627776) for byte in df_vol1['Bytes']]

    df_vol1['TB'] = terabytes_vol1
    df_vol1['TB'] = df_vol1['TB']
    df_vol1 = df_vol1[['Folders', 'TB', 'Year']]
    
    return df_vol1


def vol1_calculations(df_vol1):
    '''Calculate the number and size of projects; when they were created and \
    when they were archived'''

    # Split folders column
    df_vol1_f = df_xsan['Folders'].str.split("/", n=6, expand=True)   
    df_vol1_f.fillna('', inplace=True)  
    df_vol1_f.columns = ['0', '1', 'Volume', 'Project Archive', 'Folder_Year_Label', 'Project', 'Remainder of Path']
        
   # Extract all projects from Volumes/Volume1/Projects/Year
    df_vol1_projects = df_vol1_f.loc[df_vol1_f.Folder_Year_Label.isin(['2014', '2015', '2016', '2017', '2018', \
                                                                       '2019', '2020', '2021', '2022', '2023'])]

    # Join table  w/ previous calculations TB and Year
    df_vol1_table = df_vol1_projects.join(df_vol1) 
    df_vol1_table = df_vol1_projects.join(df_vol1)
    df_vol1_table = df_vol1_table[['Volume', 'Project Archive', 'Folder_Year_Label', 'Project', 'TB', 'Year']]

    # Filters 
    # Remove all empty project folders 
    df_vol1_table_filtered = df_vol1_table.loc[df_xsan_table['Project'] != '']

    # Remove folders labeled as project instead of by year 
    df_vol1_table_filtered = df_vol1_table_filtered.loc[df_vol1_table_filtered['Project'] != 'Project']

    # Keep only folders containing 2014-2023 and not 000
    df_vol1_table_filtered = df_vol1_table_filtered[(df_vol1_table_filtered['Project'].str.contains\
                                                     ('2014|2015|2016|2017|2018|2019|2020|2021|2022|2023')) & \
                                                     (~df_vol1_table_filtered['Project'].str.contains('000'))] 

    # Number of projects creater per year
    # Group by project folder 
    df_vol1_year_created = df_vol1_table_filtered.groupby(['Volume','Project Archive', 'Folder_Year_Label', 'Project'])\
                           ['Project'].nunique().to_frame(name = 'Count').reset_index()

    df_vol1_year_created = df_vol1_year_created.groupby('Folder_Year_Label')\
                           ['Project'].count().to_frame(name='Projects').reset_index()

    df_vol1_year_created.rename(columns={'Folder_Year_Label': 'Year Created'}, inplace=True)

    # TB archived per year
    df_vol1_year_created_TB = df_vol1_table_filtered.groupby(['Folder_Year_Label'])['TB'].sum()\
                                                         .round(2).to_frame(name = 'TB').reset_index()

    df_vol1_year_created_TB.rename(columns={'Folder_Year_Label': 'Year Created'}, inplace=True)

    # Join tables, adding TB column
    df_vol1_year_created['TB'] = df_vol1_year_created_TB['TB'].round(2)
    df_vol1_year_created.rename(columns={'TB': 'Size (TB)'}, inplace=True)
    
    # print('Volume 1 Projects by Year Created')
    # df_vol1_year_created.head()
    
    
    # Number of projects archived per year: 
    # Group by project folder 
    df_vol1_year_archived = df_vol1_table_filtered.groupby(['Volume', 'Project Archive', 'Year', 'Project'])\
                                                      ['Project'].nunique().to_frame(name = 'Count').reset_index()

    df_vol1_year_archived = df_vol1_year_archived.groupby('Year')['Project'].count().to_frame(name='Projects').reset_index()

    df_vol1_year_archived.rename(columns={'Year': 'Year Archived'}, inplace=True)
    
    # TB 
    df_vol1_year_archived_TB = df_vol1_table_filtered.groupby(['Year'])['TB'].sum()\
                                                         .round(2).to_frame(name = 'TB').reset_index()

    df_vol1_year_archived_TB.rename(columns={'Year': 'Year Archived'}, inplace=True)

    df_vol1_year_archived_TB.head()

    # Join tables, adding TB column 
    df_vol1_year_archived['TB'] = df_vol1_year_archived_TB['TB'].round(2)
    df_vol1_year_archived.rename(columns={'TB': 'Size (TB)'}, inplace=True)
    
    # print('Volume 1 Projects by Year Archived')
    # df_vol1_year_archived.head()
    
    return df_vol1_year_created, df_vol1_year_archived

 
'''Function below is a template. If multiple volumes are analyzed, modify this 
function to add up totals across volumes. Then add results to the csv export function, 
and generate a graph by duplicating the graph function'''

# def totals(df_vol1_year_created, df_vol1_year_archived, df_vol2_year_created, df_vol2_year_archived):

#     # Append the tables together
#     df_all_projects_created = pd.concat([df_vol1_year_created, df_vol2_year_created])

#     # Group Years together while adding up Pojects and TB
#     df_all_projects_created = df_all_projects_created.groupby(['Year Created']).agg({'Projects':'sum','Size (TB)':'sum'}).round(2).reset_index()  # 19 entries

#     # Append the tables together
#     df_all_projects_archived = pd.concat([df_vol1_year_archived, df_vol2_year_archived]) 

#     # Group Years together while adding up Pojects and TB
#     df_all_projects_archived = df_all_projects_archived.groupby(['Year Archived']).agg({'Projects':'sum','Size (TB)':'sum'}).round(2).reset_index()

#     return df_all_projects_created, df_all_projects_archived      


def csv(df_vol1_year_created, df_vol1_year_archived, path_save_results_runs):
   
    vol1_projects_created  = 'Volume 1 Projects Created.csv'
    vol1_projects_archived = 'Volume 1 Projects Archived.csv'

    # all_projects_created   = 'Total Projects Created.csv'
    # all_projects_archived  = 'Total Projects Archived.csv'

    path_save_results_runs  = str(path_save_results_runs)
    
    with open(path_save_results_runs + vol1_projects_created, 'w') as f:
        df_vol1_year_created.to_csv(f, index=False)
        f.close
        
    with open(path_save_results_runs + vol1_projects_archived, 'w') as f:         
        df_vol1_year_archived.to_csv(f, index=False) 
        f.close
    
    # with open(path_save_results_runs + all_projects_archived, 'w') as f:               
    #     df_all_projects_archived.to_csv(f, index=False) 
    #     f.close

    # with open(path_save_results_runs + all_projects_created, 'w') as f:
    #     df_all_projects_created.to_csv(f, index=False)
    #     f.close   


def vol1_graph(df_vol1_year_created, df_vol1_year_archived, path_graphs):  

    plt.subplots(nrows=2, ncols=2, figsize=(15,12)) 

    # 1. When were Volume 1 Projects Created?
    ax1 = plt.subplot(2, 2, 1)
    plt.title('Projects Created', fontsize=18)
    plt.ylabel('Projects', fontsize=16)
    plt.xticks(fontsize=13)
    plt.yticks(fontsize=13)
    plt.bar(df_vol1_year_created['Year Created'], df_vol1_year_created['Projects'])  #  (x, y, colour)

    # 2. When were Volume 1 Projects Archived? 
    ax2 = plt.subplot(2, 2, 2, sharey=ax1)
    plt.title('Projects Archived', fontsize=18)
    plt.ylabel('Projects', fontsize=16)
    plt.xticks(fontsize=13)
    plt.yticks(fontsize=13)
    plt.bar(df_vol1_year_archived['Year Archived'], df_vol1_year_archived['Projects'])

    # 3. What was the size of Volume 1 Projects Created in TB?
    ax3 = plt.subplot(2, 2, 3, sharey=ax1)
    plt.title('TB Created', fontsize=18)
    plt.ylabel('TB', fontsize=16)
    plt.xticks(fontsize=13)
    plt.yticks(fontsize=13)
    plt.bar(df_vol1_year_created['Year Created'], df_vol1_year_created['Size (TB)'], color='green')

    # 4. What was the size of Volume 1 Projects Archived in TB?
    ax4 = plt.subplot(2, 2, 4, sharey=ax1)
    plt.title('TB Archived', fontsize=18)
    plt.ylabel('TB', fontsize=16)
    plt.xticks(fontsize=13)
    plt.yticks(fontsize=13)
    plt.bar(df_vol1_year_archived['Year Archived'], df_vol1_year_archived['Size (TB)'], color='green')

    plt.suptitle('Volume 1 Projects Archived and Created', fontsize=24, fontweight='bold').set_position([.5, 0.98])
    
    plt.tight_layout()

    # Save to jpeg
    plt.savefig(path_graphs + 'Volume 1 Projects Archived and Created.jpg')
  

def main():
 
    start_time = dt.datetime.now()
    
    print('\nWelcome to the Archiware Archive Data Analysis script.')
    
    print('\nAccepts archive inventory files generated with Archiware commands inv-vol-size-btime.')
    
    path = input('\nPlease enter the absolute file path (eg. /users/name/folder/filename.tsv): ')

    path_save_results_runs, path_graphs = create_paths(path)

    print('\nLaoding data file...')  
    df = load_data(path)
            
    print('Isolating volumes of interest: Volume1...')
    df_vol1 = extract_archives(df)

    print('Converting time to human readable...')
    df_vol1 = convert_time(df_vol1)

    print('Converting bytes to TB...')
    df_vol1 = convert_bytes(df_vol1)

    print('Calculating Volume 1 Projects...')
    df_vol1_year_created, df_vol1_year_archived = vol1_calculations(df_vol1)
         
    print('Writing to output location ' + path_save_results_runs + '...')    

    csv(df_vol1_year_created, df_vol1_year_archived, path_save_results_runs)        
        
    vol1_graph(df_vol1_year_created, df_vol1_year_archived, path_graphs): 
    
    print('\nFinished =)')

    end_time = dt.datetime.now()
    elapsed_time = end_time - start_time 
    print('\nProcessed in:', elapsed_time)

if __name__ == '__main__':
    main()   







