import mysql.connector
import sys
import time
from tabulate import tabulate
from collections import OrderedDict
from datetime import datetime, timedelta
from docx import Document  # install python-docx
import requests
from urllib.request import urlopen
import json
from PyPDF2 import PdfFileWriter, PdfFileReader
import os
import io
from win32com import client as wc  # pip install pywin32
import pdfplumber
import docx2txt
#from datetime import datetime, timedelta
from dateutil.relativedelta import *
from elastic_app_search import Client


def open_database(hostname, user_name, mysql_pw, database_name):
    global conn
    conn = mysql.connector.connect(host=hostname,
                                   user=user_name,
                                   password=mysql_pw,
                                   database=database_name
                                   )
    global cursor
    cursor = conn.cursor()

def printFormat(result):
    header = []
    for cd in cursor.description:  # get headers
        header.append(cd[0])
    return (tabulate(result, headers=header))  # print results in table format

# select and display query
def executeSelect(query):
    cursor.execute(query)
    res = printFormat(cursor.fetchall())
    return res

def insert(table, values):
    query = "INSERT into " + table + " values (" + values + ")" + ';'
    cursor.execute(query)
    conn.commit()

def create_table(table_name, conditions):
    query = "CREATE TABLE " + table_name + " (" + conditions + ");"
    cursor.execute(query)
    conn.commit()

def copy_table(table_name, original_table):
    query = "CREATE TABLE " + table_name + " LIKE " + original_table + ";"
    cursor.execute(query)
    conn.commit()

def dump_table(table_name, original_table):
    query = "INSERT INTO " + table_name + " SELECT * FROM " + original_table + ";"
    cursor.execute(query)
    conn.commit()

def drop(table):
    query = "DROP TABLE " + table + ";"
    cursor.execute(query)
    conn.commit()

def update_basedID(table, column_name, value, id_key):
    query = "UPDATE " + table + " SET " + column_name + \
        " = '" + value + "' WHERE id= "+str(id_key)+";"
    cursor.execute(query)
    conn.commit()

def executeUpdate(query):  # use this function for delete and update
    cursor.execute(query)
    conn.commit()

def split2pop(selection):
    selection = selection.split('\n')
    selection.pop(1)
    selection.pop(0)
    result = selection
    return result
# return the sequence from the first to last element  with comma to insert into table


def list2string(selection):
    avail = ""
    for i in selection:
        avail += str(i) + ','
    avail = avail[:-1]  # return a sequence from the the first to last element
    result = avail
    return result


def work_country(work_auth):
    auth = executeSelect(
        "SELECT id FROM candidates_table WHERE work_auth = '" + work_auth + "';")
    auth = split2pop(auth)
    return auth


def jobs_avail(country):
    work = executeSelect(
        "SELECT id FROM jobs_chase WHERE country = '" + str(country) + "' AND active=1;")
    work = split2pop(work)
    avail = list2string(work)
    return avail


def jobs_avail_eu():
    work = executeSelect("SELECT id FROM jobs_chase WHERE active=1 AND country IN ('Austria', 'Belgium', 'Bulgaria','Croatia','Cyprus','Czech Republic','Denmark','Estonia','Finland','France','Germany','Greece','Hungary','Ireland','Italy','Latvia','Lithuania','Luxembourg','Malta','Netherlands','Poland','Portugal','Romania','Slovakia','Slovenia','Spain','Sweden');")
    work = split2pop(work)
    avail = list2string(work)
    return avail


def jobs_avail_latin():
    work = executeSelect(
        "SELECT id FROM jobs_chase WHERE active=1 AND country IN ('Antigua','Argentina','Bahamas','Barbados','Belize','Bolivia','Brazil','Chile','Colombia','Costa Rica','Cuba','Dominica','Dominican Rep','Ecuador','El Salvador','Grenada','Guatemala','Guyana','Haiti','Honduras','Jamaica','Mexico','Nicaragua','Panama','Paraguay','Peru','Saint Lucia','Suriname','Uruguay','Venezuela');")
    work = split2pop(work)
    avail = list2string(work)
    return avail

def jobs_avail_remote():
    work = executeSelect(
        "SELECT id FROM jobs_chase WHERE active=1 AND remote = '1';")
    work = split2pop(work)
    avail = list2string(work)
    return avail

def job_location(list_candidate, table_name):
    job_Australia_country = jobs_avail("Australia")
    job_China_country = jobs_avail("China")
    job_India_country = jobs_avail("India")
    job_UK_country = jobs_avail("United Kingdom")
    job_USA_country = jobs_avail("United States")
    job_EU_country = jobs_avail_eu()
    job_Latin_country = jobs_avail_latin()
    job_Remote_country = jobs_avail_remote()
    for cand_id in list_candidate:
        work_auth = executeSelect(
            "SELECT work_auth FROM candidates_table WHERE id = "+str(cand_id)+";")
        work_auth = split2pop(work_auth)
        work_auth = list2string(work_auth)
        list_work_auth = work_auth.split(',')
        # print(len(list_candidate))
        list_job = ""
        # This loop run iterate each element in the work_auth of a candidate
        # and find a list of job match with the work authorization
        for j in list_work_auth:
            if j == "Australia":
                list_job += job_Australia_country+","
            elif j == "China":
                list_job += job_China_country+","
            elif j == "India":
                list_job += job_India_country+","
            elif j == "United Kingdom":
                list_job += job_UK_country+","
            elif j == "USA":
                list_job += job_USA_country+","
            elif j == "European Union":
                list_job += job_EU_country+","
            elif j == "Latin America":
                list_job += job_Latin_country+","
            elif j == "Other" or j == "null" or j == "I would only work remotely":
                list_job += job_Remote_country+","
            elif j == "My current country of residence":
                cand_country = executeSelect(
                    "SELECT cand_country FROM candidates_table WHERE id ="+str(cand_id)+";")
                cand_country = split2pop(cand_country)
                cand_country = list2string(cand_country)
                job_in_country = jobs_avail(cand_country)
                list_job += job_in_country+","
            else:
                list_job += job_avail(j)+","
            list_job = list_job[:-1]
            # OrderedDict.fromkeys to remove the duplicate job_id in the list_job
            list_job = list_job.split(",")
            list_job = list(OrderedDict.fromkeys(list_job))
            list_job = list2string(list_job)
            # ----------------------end remove duplicate------------------------
        data_row = cand_id + ",'" + list_job + "'"
        insert(table_name, data_row)

def job_function(list_candidate, table_name):
    # This segment to get list of job for each function. Data is stored in the dictionary.
    # It cut down running SQL command  in general.
    functions = executeSelect(
        "SELECT job_function FROM jobs_table GROUP BY job_function;")
    functions = split2pop(functions)
    dict_functions = {}
    for x in functions:
        dict_functions[x] = list_job_eachfunction(x)
    # Start a loop for candidate
    for i in list_candidate:
        cand_desired_function = executeSelect(
            "SELECT desired_function FROM candidates_table WHERE id="+i+";")
        cand_desired_function = split2pop(cand_desired_function)
        cand_desired_function = list2string(cand_desired_function)
        cand_desired_function = cand_desired_function.split(",")
        job_list = ""
        for j in cand_desired_function:
            if (j == "null"):
                job_list = "null,"
            else:
                job_list += dict_functions[j]+","
        if (job_list != ""):
            job_list = job_list[:-1]
        data_row = i + ",'" + job_list + "'"
        insert(table_name, data_row)

def job_cause(list_candidate, table_name):
    # This segment to get list of job for each cause. Data is stored in the dictionary.
    # It cut down running SQL command  in general.
    cause_array = ["Black leader", "Blind or Low vision", "Neurodivergent",
                   "Refugee or Forcibly displaced", "Returning Citizen", "Single Mom / Parent"]
    dict_functions = {}
    for x in cause_array:
        if x == "Black leader":
            y = "Blackleaders"
        elif x == "Blind or Low vision":
            y = "Blind"
        elif x == "Neurodivergent":
            y = "Neurodivergent"
        elif x == "Refugee or Forcibly displaced":
            y = "Refugee"
        elif x == "Returning Citizen":
            y == "ReturningCitizen"
        elif x == "Single Mom / Parent":
            y == "Singlemom"
        dict_functions[x] = list_job_cause(y)
    # Start a loop for candidate
    for i in list_candidate:
        cand_cause = executeSelect(
            "SELECT jfh_cause FROM candidates_table WHERE id="+i+";")
        cand_cause = split2pop(cand_cause)
        cand_cause = list2string(cand_cause)
        cand_cause = cand_cause.split(",")
        cause_list = ""
        for j in cand_cause:
            if (j == "null"):
                cause_list = "null,"
            elif (j == "Other"):
                cause_list = "Other,"
            else:
                cause_list += dict_functions[j]+","
        cause_list = cause_list[:-1]
        data_row = i + ",'" + cause_list + "'"
        insert(table_name, data_row)

def common_member3(a, b, c):
    a_set = set(a)
    b_set = set(b)
    c_set = set(c)
    job_list = {}
    if (a_set & b_set & c_set):
        job_list = a_set & b_set & c_set
    return job_list


def common_member2(a, b):
    a_set = set(a)
    b_set = set(b)
    job_list = {}
    if (a_set & b_set):
        job_list = a_set & b_set
    return job_list

def execute_noTable(query):
    cursor.execute(query)
    res = cursor.fetchall()
    return res

def filter_3condition(list_candidate, table_name):
    # Local variable  list job for filter_location
    job_Australia_country = jobs_avail("Australia")
    job_China_country = jobs_avail("China")
    job_India_country = jobs_avail("India")
    job_UK_country = jobs_avail("United Kingdom")
    job_USA_country = jobs_avail("United States")
    job_EU_country = jobs_avail_eu()
    job_Latin_country = jobs_avail_latin()
    job_Remote_country = jobs_avail_remote()
    # Local variable  list job for filter_function
    functions = executeSelect(
        "SELECT job_function FROM jobs_table GROUP BY job_function;")
    functions = split2pop(functions)
    dict_function_job = {}
    for x in functions:
        dict_function_job[x] = list_job_eachfunction(x)
    # Local variable list job for filter_cause
    cause_array = ["Black leader", "Blind or Low vision", "Neurodivergent",
                   "Refugee or Forcibly displaced", "Returning Citizen", "Single Mom / Parent"]
    dict_cause_job = {}
    for x in cause_array:
        if x == "Black leader":
            y = "Blackleaders"
        elif x == "Blind or Low vision":
            y = "Blind"
        elif x == "Neurodivergent":
            y = "Neurodivergent"
        elif x == "Refugee or Forcibly displaced":
            y = "Refugee"
        elif x == "Returning Citizen":
            y == "ReturningCitizen"
        elif x == "Single Mom / Parent":
            y == "Singlemom"
        dict_cause_job[x] = list_job_cause(y)
    for cand_id in list_candidate:
        work_auth = executeSelect(
            "SELECT work_auth FROM candidates_table WHERE id = "+str(cand_id)+";")
        work_auth = split2pop(work_auth)
        work_auth = list2string(work_auth)
        list_work_auth = work_auth.split(',')
        # print(len(list_candidate))
        list_job = ""
        # This loop run iterate each element in the work_auth of a candidate
        # and find a list of job match with the work authorization
        for j in list_work_auth:
            if j == "Australia":
                list_job += job_Australia_country+","
            elif j == "China":
                list_job += job_China_country+","
            elif j == "India":
                list_job += job_India_country+","
            elif j == "United Kingdom":
                list_job += job_UK_country+","
            elif j == "USA":
                list_job += job_USA_country+","
            elif j == "European Union":
                list_job += job_EU_country+","
            elif j == "Latin America":
                list_job += job_Latin_country+","
            elif j == "null" or j == "I would only work remotely":
                list_job += job_Remote_country+","
            elif j == "My current country of residence" or j == "Other":
                cand_country = executeSelect(
                    "SELECT cand_country FROM candidates_table WHERE id ="+str(cand_id)+";")
                cand_country = split2pop(cand_country)
                cand_country = list2string(cand_country)
                job_in_country = jobs_avail(cand_country)
                list_job += job_in_country+","
            else:
                list_job += jobs_avail(j)+","
        list_job = list_job[:-1]
        job_by_filter_location = list_job.split(",")
    # Start a loop for filter_function
        cand_desired_function = executeSelect(
            "SELECT desired_function FROM candidates_table WHERE id="+cand_id+";")
        cand_desired_function = split2pop(cand_desired_function)
        cand_desired_function = list2string(cand_desired_function)
        cand_desired_function = cand_desired_function.split(",")
        job_list = ""
        for j in cand_desired_function:
            if (j == "null"):
                job_list = "null,"
            else:
                job_list += dict_function_job[j]+","
        job_list += dict_function_job['Other']+","
        if (job_list != ""):
            job_list = job_list[:-1]
        job_by_filter_function = job_list.split(",")
    # Start a loop for filter_function
        cand_cause = executeSelect(
            "SELECT jfh_cause FROM candidates_table WHERE id="+cand_id+";")
        cand_cause = split2pop(cand_cause)
        cand_cause = list2string(cand_cause)
        cand_cause = cand_cause.split(",")
        cause_list = ""
        for j in cand_cause:
            if (j == "null"):
                cause_list = "null,"
            elif (j == "Other"):
                cause_list = "Other,"
            else:
                cause_list += dict_cause_job[j]+","
        cause_list = cause_list[:-1]
        job_by_filter_cause = cause_list.split(",")
    # Find job in three job_list by conditions
        jobs = ""
        if (job_by_filter_location[0] != ''):
            #print("location != empty")
            # print(job_by_filter_location)
            if (job_by_filter_function != "null"):
                #print("function !=null")
                # print(job_by_filter_function)
                if (job_by_filter_cause[0] != "null" and job_by_filter_cause[0] != "Other"):
                    #print("cause != null and other ")
                    # print(job_by_filter_cause)
                    jobs = common_member3(
                        job_by_filter_location, job_by_filter_function, job_by_filter_cause)
                    # print(jobs)
                else:
                    #print("cause == null or other")
                    # print(job_by_filter_cause)
                    jobs = common_member2(
                        job_by_filter_location, job_by_filter_function)
                    # print(jobs)
            else:
                #print("function !=null")
                # print(job_by_filter_function)
                if (job_by_filter_cause[0] != "null" and job_by_filter_cause[0] != "Other"):
                    #print("cause != null and other "+job_by_filter_cause)
                    jobs = common_member2(
                        job_by_filter_location, job_by_filter_cause)
                    # print(jobs)
                else:
                    #print("cause == null or other")
                    # print(job_by_filter_cause)
                    jobs = set(job_by_filter_location)
                    # print(jobs)
        # else : print("location is empty")
        jobs = ', '.join(jobs)
        data_row = cand_id + ",'" + jobs + "'"
        insert(table_name, data_row)

def cand_experience(cand, text,table):
        #HR FLOW API
        url = "https://api.hrflow.ai/v1/text/parsing"
        ##Just replace the text in the payload object
        payload = {"text": text}
        headers = {
            "accept": "application/json",
            "content-type": "application/json",
            "X-API-KEY": "ask_e8eda44531dd2f882441e266f17a6dc8",
            "X-USER-EMAIL": "hasanezzidine@gmail.com"
            }
        try:
            response = requests.post(url, json=payload, headers=headers)
            #print(response.text)
            parsedData = json.loads(response.text)
        except:
            print("No ")
        #Getting years of experience
        dates = []
        for p in parsedData["data"]["parsing"]["dates"]:
            p = p.replace('\'', '')
            #Get date with format yyyy-mm or yyyy-mm-dd
            if('-' in p):
                try:
                    dated = datetime.strptime(p, "%Y-%m").date()
                    dates.append(dated)
                except:
                    print("Wrong format yyyy-mm")
                    try:
                        dated = datetime.strptime(p, "%Y-%m-%d").date()
                        dates.append(dated)
                    except:
                        print("Also wrong format yyyy-mm-dd")
            #Get date with format yyyy
            elif(len(str(p)) == 4):
                try:
                    dated = datetime.strptime(p, "%Y").date()
                    dates.append(dated)
                except:
                    print("Wrong format yyyy")
            #Get date with format 'monthname yyyy'
            months = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]
            m_int = ""
            year =""
            count = 0
            for i in months:
                if(i in p):
                    words = p.split()
                    for i in words:
                        if(i in months):
                            try:
                                d = datetime.strptime(i, '%B').month
                                if(int(math.log10(d))+1 < 2):
                                    m_int = "0" + str(d)
                                else:
                                    m_int = str(d)
                                count = 1
                            except:
                                print("Wrong format")
                        else:
                            year = i
            #If month name format was found then format as date yyyy-mm
            if(count > 0):
                date_str = str(year) + "-" + str(m_int)
                try:
                    date_str = datetime.strptime(date_str, "%Y-%m").date()
                    dates.append(date_str)
                    print("Date with month name added: " + str(date_str))
                except:
                    print("Wrong format: no year")
        if (len(dates) > 0): 
            #print(dates)
            oldest = min(dates)
            latest = max(dates)
            #print("min date is: " + str(oldest))
            #print("max date is: " + str(latest))
            rdelta = relativedelta(latest, oldest)
            diff = rdelta.years
            #print("Difference is: " + str(diff))
            exp_query = "INSERT INTO "+table+" (cand_id, experience) VALUES (%s, %s) "
            record  = (cand, diff)
            cursor.execute(exp_query, record)
            conn.commit()
        else:
            print("No dates found")
            
def cand_experience_select(table):
    candidate_pool = executeSelect("SELECT cand_id FROM candidates_table LIMIT 10")
    candidate_pool = split2pop(candidate_pool)
    for cand in candidate_pool:
        cand_id = cand
        url = f"https://api.smartrecruiters.com/candidates/{cand_id}"
        headers = {
            "accept": "application/json",
            "x-smarttoken": "DCRA1-98401b1cf74a43a1959b72edba619256"
        }   
        response = requests.get(url, headers=headers)
        print(cand_id)
        try:
            text = response.json()["experience"]
            text = " ".join(str(x) for x in text)
            cand_experience(cand, text,table)
        except:
            print("No smart recruiter data")

def update_cand(table):
    yesterday = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d %H:%M:%S')
    cand_list = executeSelect(
        "SELECT id FROM candidates_table WHERE create_date >='"+yesterday + "';")
    cand_list = split2pop(cand_list)
    print(cand_list)
    filter_3condition(cand_list, table) 

def list_job_eachfunction(function):
    job_list = executeSelect(
        "SELECT id FROM jobs_table WHERE active=1 AND job_function LIKE '%"+function+"%';")
    job_list = split2pop(job_list)
    job_list = list2string(job_list)
    return job_list

def list_job_cause(cause_column):
    job_list = executeSelect(
        "SELECT id FROM jobs_table WHERE active=1 AND "+cause_column + "=1;")
    job_list = split2pop(job_list)
    job_list = list2string(job_list)
    return job_list

def parsingskill(table):
    candidate_pool = executeSelect(
        "SELECT id,cand_id FROM candidates_table LIMIT 10")
    candidate_pool = split2pop(candidate_pool)
    for cand in candidate_pool:
        candlist = cand.split("  ")
        cand_id = candlist[1]
        print(candlist[0])
        url = f"https://api.smartrecruiters.com/candidates/{cand_id}"
        headers = {
            "accept": "application/json",
            "x-smarttoken": "DCRA1-98401b1cf74a43a1959b72edba619256"
        }
        response = requests.get(url, headers=headers)
        # print(cand_id)
        #text = json.loads(response.text);
        try:
            text = response.json()["experience"]
            text = " ".join(str(x) for x in text)
            # print(text)
            # HR FLOW API
            url = "https://api.hrflow.ai/v1/text/parsing"
            # Just replace the text in the payload object
            payload = {"text": text}
            headers = {
                "accept": "application/json",
                "content-type": "application/json",
                "X-API-KEY": "ask_e8eda44531dd2f882441e266f17a6dc8",
                "X-USER-EMAIL": "hasanezzidine@gmail.com"
            }
            response = requests.post(url, json=payload, headers=headers)
            parsedData = json.loads(response.text)
            hard_skills = []
            soft_skills = []
            for i in parsedData["data"]["parsing"]["skills_hard"]:
                if i.casefold() not in hard_skills:
                    hard_skills.append(i.casefold())
            for i in parsedData["data"]["parsing"]["skills_soft"]:
                if i.casefold() not in soft_skills:
                    soft_skills.append(i.casefold())
            # Turn array into string format for insert
            hard_string = ""
            for i in hard_skills:
                if (len(hard_skills) > 1):
                    hard_string += i + ','
                else:
                    hard_string += i
            if (len(hard_skills) > 1):
                hard_string = hard_string[:-1]
            # Turn array into string format for insert
            soft_string = ""
            for i in soft_skills:
                if (len(soft_skills) > 1):
                    soft_string += i + ','
                else:
                    soft_string += i
            if (len(soft_skills) > 1):
                soft_string = soft_string[:-1]
        except:
            hard_string = ""
            soft_string = ""
        #print("Parsing for candidate: " + candidate_id)
        # insert information into candidate_skills table
        myquery = "INSERT INTO "+table+" (cand_id, hard_skills, soft_skills) VALUES (%s, %s, %s) "
        record = (cand_id, hard_string, soft_string)
        cursor.execute(myquery, record)
        conn.commit()

def relevanceTuning(table):
    client = Client(
        base_endpoint='jobforhumanity.ent.us-central1.gcp.cloud.es.io:9243/api/as/v1',
        api_key='private-v1bvt26wsdua7yooom2s1x63',
        use_https=True
    )

    list_candidate = execute_noTable(
        "SELECT id, work_auth,desired_function,jfh_cause,hard_skills,soft_skills,cand_country FROM candidates_3S_table LIMIT 10")
    for cand in list_candidate:
        #print(cand)
        remote_bool = 0
        list_function = ""
        cause_column=""
        skills =""
        list_country = ""
        print("id : ",cand[0])
        # Work_auth
        list_country = cand[1].replace(",", " ")
        list_country =list_country.replace("My current country of residence", cand[6])
        list_country =list_country.replace("Other", cand[6])
        # Job_function
        list_function = cand[2].replace(",", " ")
        #Skilss
        hardskill = cand[4].replace(",", " ")
        softskill = cand[5].replace(",", " ")
        skills = hardskill[:10]
        key_value = list_country + " "+ list_function + " "+ skills
        #print(key_value)
        print(list_function)
        engine_name = 'search-engine'
        data = client.search(engine_name, list_function[:120])
        #print(data)
        #print(data['results'][0]['_meta']['score'])
        x=data['results']
        list_job = []
        for i in range(len(x)):
            # print(x[i]['id']['raw'])
            # print(x[i]['_meta']['score'])
            dict_score = {x[i]['id']['raw']:x[i]['_meta']['score']}
            list_job.append(dict_score)
        #print(list_job)
        record = (cand[0], str(list_job) )
        query = "INSERT INTO "+table+" (cand_id, job_ranking) VALUES (%s, %s) "
        cursor.execute(query, record)
        conn.commit()
def close_db():  # use this function to close db
    cursor.close()
    conn.close()


def quit_program():
    print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
    print("Program Finished")
    close_db()
    quit()

def menu():
    print("\nEditing the JFH Database")
    print("\t 1: Filter Jobs Based on Location")
    print("\t 2: Filter Jobs Based on Desired Function")
    print("\t 3: Filter Jobs Based on Community/Cause")    
    print("\t 4: Run All 3 Filters")
    print("\t 5: Get experience year for each candidate")
    print("\t 6: Update Candidate From Hassan's Tables")
    print("\t 7: Parsing skills")
    print("\t 8: Ranking skills")
    print("\t 9: Quit Program")
    choice = input("\t Select an option from 1 to 9: ")
    if choice == '1':
        drop("filter_location_demo")
        create_table("filter_location_demo", "cand_id varchar(10), job_id longtext")
        list_candidate = executeSelect(
            "SELECT id FROM  candidates_table LIMIT 10;")
        list_candidate = split2pop(list_candidate)
        job_location(list_candidate, "filter_location_demo")
    elif choice == '2':
        drop("filter_desire_demo")
        create_table("filter_desire_demo", "cand_id varchar(10), job_id longtext")
        list_candidate = executeSelect(
            "SELECT id FROM  candidates_table LIMIT 10;")
        list_candidate = split2pop(list_candidate)
        job_function(list_candidate, "filter_desire_demo")
    elif choice == '3':
        drop("filter_cause_demo")
        create_table("filter_cause_demo", "cand_id varchar(10), job_id longtext")
        list_candidate = executeSelect(
            "SELECT id FROM  candidates_table LIMIT 10 ;")
        list_candidate = split2pop(list_candidate)
        job_cause(list_candidate, "filter_cause_demo")
    elif choice == '4':
        drop("filter_3conditions_demo")
        create_table("filter_3conditions_demo",
                     "cand_id varchar(100), job_id longtext")
        list_candidate = executeSelect("SELECT id FROM  candidates_table LIMIT 20;")
        list_candidate = split2pop(list_candidate)
        filter_3condition(list_candidate, "filter_3conditions_demo")
    elif choice == '5':
        drop("candidates_exp_demo")
        create_table("candidates_exp_demo", "cand_id text, experience text") 
        cand_experience_select("candidates_exp_demo")
    elif choice == '6':
        update_cand("filter_3conditions_demo")
    elif choice == '7':
        drop("candidates_skills_demo")
        create_table("candidates_skills_demo","cand_id text, hard_skills text, soft_skills text")
        parsingskill("candidates_skills_demo")
    elif choice == '8':
        drop("candidates_ranking_demo")
        create_table("candidates_ranking_demo", "cand_id text, job_ranking text")
        relevanceTuning("candidates_ranking_demo")
    elif choice == '9':
        quit_program()
        program_finish = True
    else:
        print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
        print("\t Invalid input. You should know better.")
        menu()


mysql_username = 'Matching_score_user'
mysql_password = 'Jfh2022_MS'
mysql_host = 'database-3.cln6ulf5jgzz.us-east-2.rds.amazonaws.com'
mysql_db = 'candidates'
open_database(mysql_host, mysql_username,
              mysql_password, mysql_db)  # open database

program_finish = False

while not program_finish:
     menu()


close_db()  # close database
