##Importing all the required modules
import requests
from bs4 import BeautifulSoup
import pandas as pd
import math
import mysql.connector
from time import sleep
#from sqlalchemy import types, create_engine

pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 1000)

#Taking the target url 
f_url = "https://shop.cannontools.co.uk/includes/ad_order_Print.php?fp=showorder&sid="
l_url = "&ord=no&fbclid=IwAR1zWwmTbLyVs3ICPQ4ge0XrsfBXbJFNpY6X7RBeVcNfUtkx-yYn3fjwG5w"

try:
    
    #Taking the highest and lowset limit of Sales id Data point user want to extract
    l = int(input("Enter your lower limit : ")) 
    h = int(input("Enter your highest limit : "))
    chunk = int(input("Data Chunk size : "))


except Exception as ex:   
    print(ex)


#taking required lists, variables and doing basic calculations
lowest = 0
highest = 0
left = []
right = []
date = []
time = []
val_value = []
total_heading = []
serial = []
low = l
index_id=[]
sid = []
aa = []
invoice = []
inv = []

#we'll loop through the limit given above by the user and split the file into 2022 data item per csv file while saving it
#This would help us saving memory and troubleshoot if any error occurs... I'm not using any exception handling option here

sub = h-l
if chunk > sub:
    ss = 1
    sss= ss+1
    
else: 
    ss = math.ceil((h-l)/chunk)
    sss = ss+1

for s in range(sss): 
    serial.append(low)
    low = low+chunk
    
high_end = serial[-1]


try:
    for d in range(ss):
        if d == len(serial)-1:   
            break
        elif d == len(serial)-2:
            lowest = serial[d]
            highest = h
            
        else:
            lowest = serial[d]
            highest = serial[d+1]
            
            #################### scraping starting here #####################
        for i in range(lowest, highest):
                
            a = []
            nval = []

            print(i, end=" ")
            url = f_url+str(i)+l_url
            response = requests.get(url)
            html_soup = BeautifulSoup(response.text, 'html.parser')
            table = html_soup.find_all('table')[0]
            
            
            ########################## table 4 and 5 Section #########################
            tablee1 = table.find_all('table')[-1]
            n_trrr = tablee1.find_all('tr')
            a_numm = len(n_trrr)
            tdr0 = []
            for ei in range(a_numm):

                tdrp = tablee1.find_all('tr')[ei]
                for kp in range(len(tdrp.find_all('td'))):
                    tdr0.append(tdrp.find_all('td')[kp].get_text())
            aa.append(tdr0)
            del tdr0

            ################################################## table 5 ######################
            table2 = table.find_all('table')[7]
            n = len(table2.find_all('tr'))
            inn = []
            
            for q in range(1, n):
                tdd0 = []
                intab2 = table2.find_all('tr')[q]
                for qq in range(len(intab2.find_all('td'))):
                    tdd0.append(intab2.find_all('td')[qq].get_text())
                inn.append(tdd0)
                del tdd0
            invoice.append([inn])
            del inn

            
            ##########################################################################
            tr1 = table.find_all('tr')[1]
            td1 = tr1.find_all('td')[0]
            ta1 = td1.find_all('table')[0]
            trr = ta1.find_all('tr')[0] #all two table are still combined

            #this is the splitting point
            tdd0 = trr.find_all('td')[0]
            td_right = trr.find_all('td')[1]

            ##################################################
            #split done and working on left section
            t2 = tdd0.find_all('table')[0]
            t3 = t2.find_all('table')[0]
            final = t3.find_all('tr')

            for j in range(len(final)):
                #start extracting information form the table td
                final_tr = t3.find_all('tr')[j]
                fd = final_tr.find_all('td')[1].get_text()
                a.append(fd)
            left.append(a)
            del a

            #################################################
            #right hand table work going on
            nval = []
            tr1 = table.find_all('tr')[1]
            td1 = tr1.find_all('table')[3]

            #all two table are still combined
            rown = td1.find_all('td')

            for i in range(len(rown)):
                nval.append(td1.find_all('td')[i].get_text())
            right.append(nval)
            del nval

            ################################################
            #time and date table row
            trr1 = table.find_all('tr')[1]
            tdf1 = trr1.find_all('table')[-1]
            taf10 = tdf1.find_all('tr')[0]
            taf11 = tdf1.find_all('tr')[1]

            dtval = taf10.find_all('td')[1].get_text()
            ttval = taf11.find_all('td')[1].get_text()
                
            #append the value in list
            date.append(dtval)
            time.append(ttval)
            #Sleep time for avoiding  detection service from the target webserver
            sleep(2)

            #End of for loop scrapping for data items
except Exception as excep:
    print(excep," Occurred")
    
    ################################################
    df_left = pd.DataFrame(left)
    df_right = pd.DataFrame(right)
    t = pd.DataFrame(time)
    da = pd.DataFrame(date)
    df123 = pd.concat([da,t,df_left, df_right], axis = 1)
            
    #################################################
    
    ########################## table 4 and 5 Section #########################
    df4 = pd.DataFrame(aa)
    df5 = pd.DataFrame(invoice)
    #df45 = pd.concat([dfaa, dfin], axis = 1)

    ########################
    for i in range(lowest,highest):
        sid.append(i)
    df123[2] = sid
    #df45[0] = sid
    print(d)
    save123 = r"Sales_first_table_"+str(d+1)+".csv"
    save4 = r"Sales_second_table_00"+str(d+1)+".csv"
    save5 = r"Sales_second_table_11"+str(d+1)+".csv"

    df123.to_csv(save123)
    df4.to_csv(save4)
    df5.to_csv(save5)           
print("Finished")



#Lets load the csv file here
df1 = pd.read_csv(r"Sales_first_table_"+str(ss)+".csv")
df4 = pd.read_csv(r"Sales_second_table_00"+str(ss)+".csv")
df5 = pd.read_csv(r"Sales_second_table_11"+str(ss)+".csv")

print(df1.head())
print(df4.head())
print(df5.head())

######

#####################
###if you've decided to export data to mysql then use this portion after uncommenting the mysql section above

####Use this portion of the code only if you need to export the data to mysql database
    MYSQL_USER = str(input("Enter MYSQL_USER : "))
    MYSQL_PASSWORD = str(input("Enter MYSQL_PASSWORD : "))
    MYSQL_HOST_IP = str(input("Enter MYSQL_HOST_IP : "))
    MYSQL_PORT = int(input("Enter MYSQL_PORT : "))
    MYSQL_DATABASE = str(input("Enter MYSQL_DATABASE Schema Name : "))
    tableName = str(input("Enter tableName : "))

sqlEngine = create_engine('mysql+mysqlconnector://'+MYSQL_USER+':'+MYSQL_PASSWORD+'@'+MYSQL_HOST_IP+':'+str(MYSQL_PORT)+'/'+MYSQL_DATABASE, echo=False)
dbConnection = sqlEngine.connect()

try:
    frame   = df.to_sql(tableName, dbConnection, if_exists='fail');
except ValueError as vx:
    print(vx)
except Exception as ex:   
    print(ex)
else:
    print("Table %s created successfully."%tableName);   
finally:
    dbConnection.close()


   
"""
serial = []
x = len(df5.columns)
e = 7
initial = 1
for i in range(x):
    if i == initial:
        serial.append(i) 
    elif i == e:
        serial.append(i)
    elif i == (e+1):
        serial.append(i)
        e+=7
df5 = df5.drop(df5.columns[serial], axis = 1)

df5 = df5.drop(df5.columns[0], axis = 1)


for k in range(lowest, highest):
    num.append(k)    
df1[0] = num


"""

""" #Indexing, concatenating, cleaning, merging of data before export to mysql

l = len(df5.columns)
s = []
for i in range(l):
    s.append(i)
df5.columns = s

######### df4 starts here ####################

#indexing 
l = len(df4.columns)
s = []
for i in range(l):
    s.append(i)
df4.columns = s

s_num = []
for j in range(lowest,highest):
    s_num.append(j)
df4[0] = s_num



#concat df4 and df5 and create df2 from them
ff = [df4, df5]
df2 = pd.concat(ff, axis = 1)

df2.insert(9,'9', '       ')
df2

#indexing df2 
l = len(df2.columns)
s = []
for i in range(l):
    s.append(i)
df2.columns = s

#########################
#indexing df1
l = len(df1.columns)
s = []
for i in range(l):
    s.append(i)
df1.columns = s

df1 = df1.drop(df1.columns[3],axis = 1)
num = []


#concat df4 and df5 and create df2 from them
fff = [df1, df2]
df = pd.concat(fff, axis = 1)

#indexing df1
l = len(df.columns)
s = []
for i in range(l):
    s.append(int(i))
df.columns = s

"""




'''
