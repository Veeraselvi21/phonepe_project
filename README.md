![image](https://github.com/Veeraselvi21/phonepe_project/assets/133382909/30434d0f-1a8c-4984-bcfe-739eceea1f7d)
# Phonepe Pulse Data Visualization and Exploration: A User-Friendly Tool Using Streamlit and Plotly
# PhonePe?
#### PhonePe is an Indian digital payment and financial services company. sing PhonePe, users can send and receive money, recharge mobile, DTH, and data cards, make utility payments, pay at shops, invest in tax saving funds, liquid funds, buy insurance, mutual funds, and digital gold.
# PhonePe Pulse?
#### PhonePe Pulse is your window to the world of how India transacts with interesting trends, deep insights and in-depth analysis based on our data put together by the PhonePe team.This year, it crossed 2000 Cr. transactions and 30 Crore registered users,  India's largest digital payments platform with 46% UPI market share.The overall report on the Transaction can be freely available for download on [PhonePe Pulse website.](https://www.phonepe.com/pulse/about-us/)
### Technologies used in the project
#### GitHub Cloning, Python, Pandas, MySQL, mysql-connector-python, Streamlit, and Plotly.
## Workflow
### Step 1: 
#### Install the necessary libraries and import the required library.
     !pip install GitPython
    
   If the libraries are already installed then we have to import those into our script by mentioning the below codes.

        import pandas as pd
        import mysql.connector as sql
        import streamlit as st
        import plotly.express as px
        import os
        import json
        from streamlit_option_menu import option_menu
        from PIL import Image
    
 
### Step 2: Data Extraction 
#### Extract data from the Phonepe pulse Github repository through scripting and clone it.
        os.system("git clone https://github.com/PhonePe/pulse.git")
### Step 3: Data Transformation
#### Transform the data into a suitable format and perform any necessary cleaning and pre-processing steps.
        path = Path to the cloned file 

        aggregated_transaction_list = [item for item in os.listdir(path) if item != '.DS_Store']

        output = {'State': [], 'Year': [], 'Quarter': [], 'Transaction_type': [], 'Transaction_count': [],'Transaction_amount': []}
#### For loop to get to the Json file to extract the required data
        for state in aggregated_transaction_list:
            n_state = os.path.join(path, state)
            aggregated_year_list = [item for item in os.listdir(n_state) if item != '.DS_Store']
    
            for year in aggregated_year_list:
                n_year = os.path.join(n_state, year)
                aggregated_file_list = [item for item in os.listdir(n_year) if item != '.DS_Store']
                
                for file in aggregated_file_list:
                    n_file = os.path.join(n_year, file)
                    with open(n_file, 'r') as data:
                    dict = json.load(data)
                    
                    for value in dict['data']['transactionData']:
                        name = value['name']
                        count = value['paymentInstruments'][0]['count']
                        amount = value['paymentInstruments'][0]['amount']
                        output['Transaction_type'].append(name)
                        output['Transaction_count'].append(count)
                        output['Transaction_amount'].append(amount)
                        output['State'].append(state)
                        output['Year'].append(year)
                        output['Quarter'].append(int(file.strip('.json')))
#### Converting the Extracted data into DataFrame              
    df_aggregated_transaction = pd.DataFrame(output)
#### Converting the DataFrame into CSV file 

    df.to_csv('filename.csv',index=False)
### Step 4: Data Insertion
#### Insert the transformed data into a MySQL database for efficient storage and retrieval. To insert the data we need to create a connection using mysql.connector we have to import it.
#### To establish connection
    import mysql.connector
        mydb = mysql.connector.connect(
        host="localhost",
        user="Username",
        password="Password",
        database="database_name"
        )

    mycursor = mydb.cursor(buffered=True)

#### Create Table and Insert Data into Mysql
    mycursor.execute("create table 'Table name' (col1 varchar(100), col2 int, col3 int, col4 varchar(100), col5 int, col6 bigint)")

        for i,row in df.iterrows():
        
            #here %S means string values 
            sql = "INSERT INTO agg_trans VALUES (%s,%s,%s,%s,%s,%s)"
            mycursor.execute(sql, tuple(row))
            mydb.commit()
            
### Step 5: Creating Dashboard - Visualization
#### Create a live geo-visualization dashboard using Streamlit and Plotly in Python to display the data in an interactive and visually appealing manner.Fetch the data from the MySQL database to display in the dashboard.
