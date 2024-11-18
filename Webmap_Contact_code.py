import mysql.connector
import tkinter as tk
import tkinter.ttk as ttk
from tkinter import filedialog, messagebox
import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.robotparser import RobotFileParser
from requests.exceptions import RequestException, Timeout
import datetime
import traceback
import inspect
from mysql.connector import Error
import threading
import time
import os
import json
import webbrowser
from tkinter import messagebox
from collections import Counter
import re
from tkcalendar import Calendar
import datetime
from tkinter import messagebox, filedialog, StringVar
import mysql.connector
import pandas as pd
import ttkbootstrap as ttk
from ttkbootstrap.constants import *
import ttkbootstrap as tb
from bs4 import BeautifulSoup
import stanza
import re
from stanza.pipeline.core import DownloadMethod
from tkcalendar import Calendar

checkbox_state = False
check_box_checked = False
current_page_id = None
webmap_paused = False
webmap_thread = None
paused_page_id = None
paused_site_id = None
paused_url = None
current_page_id = 0
current_timeout = 2
timeout_var = None
webmap_pause_event = threading.Event()
webmap_paused_event = threading.Event()

root = tb.Window(themename="superhero")


class MySQLConnectionWindow:
    def __init__(self, parent, main_window_callback):
        self.parent = parent
        self.main_window_callback = main_window_callback
        self.parent.title("MySQL Connection")
        self.parent.geometry("400x300")

        self.label_username = tk.Label(parent, text="Username:")
        self.label_username.pack(pady=10)
        self.entry_username = tk.Entry(parent)
        self.entry_username.pack(pady=5)

        self.label_password = tk.Label(parent, text="Password:")
        self.label_password.pack(pady=10)
        self.entry_password = tk.Entry(parent, show="*")
        self.entry_password.pack(pady=5)

        self.label_database = tk.Label(parent, text="Database Name")
        self.label_database.pack(pady=10)
        self.entry_database = tk.Entry(parent)
        self.entry_database.pack(pady=5)

        try:
            with open("credentials1.json", "r") as json_file:
                data = json.load(json_file)
                saved_username = data.get("Credentials", {}).get("Username", "")
                saved_password = data.get("Credentials", {}).get("Password", "")
                saved_database = data.get("Credentials", {}).get("Database", "")
        except (FileNotFoundError, json.JSONDecodeError):
            saved_username = ""
            saved_password = ""
            saved_database = ""

        if saved_username:
            self.entry_username.insert(0, saved_username)
        if saved_password:
            self.entry_password.insert(0, saved_password)
        if saved_database:
            self.entry_database.insert(0, saved_database)

        self.button_save = tk.Button(parent, text="Save", command=self.save_credentials)
        self.button_save.pack(pady=15)
        self.button_save.config(state=tk.NORMAL, bg='lightgray', fg='black')

    def save_credentials(self):
        username = self.entry_username.get()
        password = self.entry_password.get()
        database = self.entry_database.get()
        try:
            start_time = time.time()
            db = mysql.connector.connect(
                host="localhost",
                user=username,
                password=password,
            )
            cursor = db.cursor()
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
            cursor.close()
            db.close()
            data = {"Credentials": {"Username": username, "Password": password, 'Database': database}}
            with open("credentials1.json", "w") as json_file:
                json.dump(data, json_file)
            self.parent.destroy()
            end_time = time.time()
            execution_time = end_time - start_time
            print(' fetch_and_process_sitemaps Execution time:', execution_time)
        except mysql.connector.Error as e:
            messagebox.showerror("Error", f"Failed to connect to MySQL: {e}")

def update_error_log(cursor, db, site_id, page_url, error, error_line):
    try:
        time_date = datetime.datetime.now().date()
        error_data = (site_id, page_url, str(error), error_line, time_date)
        cursor.execute(
            "INSERT INTO error_log (site_id, page_url, error, error_line, time_date) VALUES (%s, %s, %s, %s, %s)",
            error_data)
        db.commit()
    except mysql.connector.errors.OperationalError as e:
        if 'MySQL Connection not available.' in str(e):
            cursor.reconnect()
            time_date = datetime.datetime.now().date()
            error_data = (site_id, page_url, str(error), error_line, time_date)
            cursor.execute(
                "INSERT INTO error_log (site_id, page_url, error, error_line, time_date) VALUES (%s, %s, %s, %s, %s)",
                error_data)
            db.commit()
        else:
            print(f"Error updating error log: {e}")
    except Exception as e:
        print(f"Error updating error log: {e}")


def fetch_and_process_sitemap(sitemap_url, site_id, cursor, db):
    start_time = time.time()
    try:
        print(f"Processing sitemap: {sitemap_url}")
        response = requests.get(sitemap_url, timeout=current_timeout)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'xml')

        for tag in soup.find_all(['lastmod', 'image', 'changefreq', 'priority']):
            tag.decompose()

        text_content = soup.get_text(separator=' ', strip=True)
        print("Text content from", sitemap_url, ":\n", text_content)
        urls = [url.text for url in soup.find_all('loc')]

        cursor.execute("SELECT DISTINCT Url_pattern FROM url_pattern;")
        patterns = [pattern[0] for pattern in cursor.fetchall()]

        matched_urls = set()
        for url in urls:
            cursor.execute("SELECT COUNT(*) FROM url_pattern WHERE %s LIKE CONCAT('%', Url_pattern, '%')", (url,))
            count = cursor.fetchone()[0]
            if count > 0:
                matched_urls.add(url)

        if matched_urls:
            data_to_insert = [(site_id, url) for url in matched_urls]
        else:
            data_to_insert = [(site_id, url) for url in urls]

        cursor.executemany("INSERT INTO page_site (site_id, page_url) VALUES (%s, %s)", data_to_insert)
        db.commit()
        end_time = time.time()
        print(f"fetch_and_process_sitemap Execution time for {sitemap_url}: {end_time - start_time} seconds")
    except (RequestException, Timeout) as e:
        print(f"Error while fetching or processing sitemap {sitemap_url}: {e}")
        update_error_log(cursor, db, site_id, sitemap_url, e, inspect.currentframe().f_lineno)

def process_sitemap_index(sitemap_index_url, site_id, cursor, db):
    start_time = time.time()
    try:
        response = requests.get(sitemap_index_url, timeout=current_timeout)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'xml')
        sitemaps = soup.find_all('sitemap')
        for sitemap in sitemaps:
            if webmap_paused:
                return
            sitemap_url = sitemap.find('loc').text
            fetch_and_process_sitemap(sitemap_url, site_id, cursor, db)
            end_time = time.time()
            print(f"process_sitemap_index Execution time for {sitemap_url}: {end_time - start_time} seconds")

    except (RequestException, Timeout) as e:
        print(f"Error while fetching or processing sitemap index {sitemap_index_url}: {e}")


def fetch_and_process_sitemaps(content, site_id, cursor, db):
    start_time = time.time()
    global webmap_paused, url
    try:
        robots_url = f"{content}/robots.txt"
        rp = RobotFileParser(robots_url)
        rp.read()

        sitemap_urls = rp.site_maps()

        if sitemap_urls is None:
            sitemap_url = f"{content}/sitemap.xml"
            process_sitemap_index(sitemap_url, site_id, cursor, db)
            fetch_and_process_sitemap(sitemap_url, site_id, cursor, db)
        else:
            print("Sitemap URLs from robots.txt:", sitemap_urls)
            for sitemap_url in sitemap_urls:
                if 'sitemap_index.xml' in sitemap_url or 'post-sitemap.xml' in sitemap_url or \
                        'sitemap.xml' in sitemap_url or 'sitemap-index.xml' in sitemap_url or \
                        'page-sitemap.xml' in sitemap_url or 'practices-sitemap.xml' in sitemap_url or \
                        'attorneys-sitemap.xml' in sitemap_url or 'wp-sitemap.xml' in sitemap_url or \
                        'wp_sitemap-index.xml' in sitemap_url or 'pages-sitemap.xml' in sitemap_url or \
                        'page_sitemap.xml' in sitemap_url or 'sitemap-misc.xml' in sitemap_url:

                    if webmap_paused:
                        return
                    process_sitemap_index(sitemap_url, site_id, cursor, db)
                    fetch_and_process_sitemap(sitemap_url, site_id, cursor, db)

                    end_time = time.time()
                    execution_time = end_time - start_time
                    print(' fetch_and_process_sitemaps Execution time:', execution_time)
                else:
                    if webmap_paused:
                        return
                    process_sitemap_index(sitemap_url, site_id, cursor, db)
                    fetch_and_process_sitemap(sitemap_url, site_id, cursor, db)

    except Exception as e:
        print(f"Error while processing {content}: {e}")
        update_error_log(cursor, db, site_id, content, e, None)

        end_time = datetime.datetime.now()
        print(end_time)


def create_tables(cursor, db):
    try:
        start_time = time.time()
        cursor.execute("""
                CREATE TABLE IF NOT EXISTS sites_table (
                    sfid text,
                    url VARCHAR(4000),
                    acc_name text,
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    start_date_time DATETIME,
                    end_date_time DATETIME,
                    progress decimal(5,2),
                    status varchar(255) default 'open',
                    download_status varchar(600) default 'NO')
                """)
        cursor.execute("""
                CREATE TABLE IF NOT EXISTS error_log (
                    site_id VARCHAR(4000),
                    page_url VARCHAR(4000),
                    error VARCHAR(5000),
                    error_line INT,
                    time_date DATETIME,
                    download_status Varchar(400) default "NO"
                )
                """)
        cursor.execute("""
                        CREATE TABLE IF NOT EXISTS url_pattern (
                        Url_pattern VARCHAR(5000)) """)
        cursor.execute("""
                CREATE TABLE IF NOT EXISTS page_site (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    site_id INT,
                    page_url VARCHAR(9000),
                    status VARCHAR(20) default 'open',
                    progress DECIMAL(5,2),
                    FOREIGN KEY (site_id) REFERENCES sites_table(id))
                """)
        cursor.execute("""
                        CREATE TABLE IF NOT EXISTS attorney_info (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        site_id INT,
                        page_url VARCHAR(8000),
                        attorney_names TEXT,
                        contact_numbers VARCHAR(4000),
                        email_id  VARCHAR(4000),
                        download_status varchar(300) DEFAULT 'NO',
                        Date datetime,
                        FOREIGN KEY (site_id) REFERENCES sites_table(id)
                        )
                        """)
        try:
           cursor.execute("ALTER TABLE attorney_info ADD COLUMN Date datetime")
        except Error as e:
           print(f"table:")
    except Error as e:
        print(f"Error creating tables: {e}")
    end_time = time.time()
    execution_time = end_time - start_time
    print('create table Execution time', execution_time)


def update_credentials(username, password, database):
    data = {"Credentials": {"Username": username, "Password": password, "Database": database}}
    with open("credentials1.json", "w") as json_file:
        json.dump(data, json_file)
    Attorneys_info()


id = 0
site_id = 0
url = ""

def Attorneys_info():
    try:
        start_time = time.time()
        with open("credentials1.json", "r") as json_file:
            data = json.load(json_file)
            username = data["Credentials"]["Username"]
            password = data["Credentials"]["Password"]
            database = data["Credentials"]["Database"]
    except (FileNotFoundError, json.JSONDecodeError, KeyError):
        username = ""
        password = ""
        database = ""
        root_mysql = tk.Tk()
        connection_window = MySQLConnectionWindow(root_mysql, update_credentials)
        root_mysql.mainloop()
        Attorneys_info()
        return
    db = mysql.connector.connect(
        host="localhost",
        user=username,
        password=password,
        database=database
    )
    cursor = db.cursor()
    create_tables(cursor, db)
    #root = tk.Tk()
    root.title("WebMap Tool")
    root.geometry("1700x790")
    end_time = time.time()
    execution_time = end_time - start_time
    print('Credentials execution_time', execution_time)

    def upload_site_csv():
        start_time1 = time.time()
        global site_df, progress_bar, Totalurl
        site_csv_path = filedialog.askopenfilename(title="Select Site CSV File", filetypes=[("CSV files", "*.csv")])
        if site_csv_path:
            db = mysql.connector.connect(
                host="localhost",
                user=username,
                password=password,
                database=database
            )
            cursor = db.cursor()
            site_df = pd.read_csv(site_csv_path)
            account_list = site_df['account_id'].tolist()
            url_list = site_df['url'].tolist()
            acc_name_list = site_df['acc_name'].tolist()
            cursor.execute("INSERT INTO url_pattern values ('%/professionals%'),('%/people%'),('%/peoples%'),('%/our_team%'),('%/lawyers%'),('%/team%'),('%/attorneys%'),('%/about-us%'),('%/our-attorneys%') ,('%/legal_team%')")
            try:
                insert_values = []
                for account_id, url, acc_name in zip(account_list, url_list, acc_name_list):
                    if url.startswith(('https:')):
                        url = url
                    else:
                        url = 'https://www.' + url
                    start_time = datetime.datetime.now()
                    insert_values.append((account_id, url, acc_name, start_time, start_time, 'open'))

                insert_query = "INSERT INTO sites_table (sfid,url, acc_name, start_date_time, end_date_time,status) VALUES (%s, %s, %s, %s, %s,%s)"
                cursor.executemany(insert_query, insert_values)
                cursor.execute("UPDATE page_site SET progress = 0 WHERE status = 'completed'")
                db.commit()
                cursor.execute("UPDATE page_site SET progress = 0 WHERE status = 'url not found'")
                cursor.execute("UPDATE sites_table SET download_status = 'YES' WHERE status = 'open'")
                cursor.execute("UPDATE sites_table SET download_status = 'YES' WHERE status = 'completed'")
                cursor.execute("UPDATE sites_table SET download_status = 'YES' WHERE status = 'url not found'")
                cursor.execute("UPDATE page_site SET status = 'completed' WHERE status = 'open'")
                db.commit()
                #cursor.execute(
                    #"UPDATE attorney_info SET download_status = 'YES' WHERE download_status = 'NO'")
                #cursor.execute(
                    #"UPDATE error_log SET download_status = 'YES' WHERE download_status = 'NO'")
                #cursor.execute("UPDATE sites_table SET download_status = 'YES' WHERE status = 'completed'")
                #cursor.execute("UPDATE sites_table SET download_status = 'YES' WHERE status = 'url not found'")
                db.commit()
                cursor.close()
                db.close()
                url_count_label.config(text=f"Total Number of Site URLs to scan: {len(site_df)}")
                upload_site_button.config(state=tk.NORMAL,bg='lightgray', fg='black')
                run_webmap_button.config(state=tk.NORMAL,bg='lightgray', fg='black')
                sites_csv = tk.Label(root, text='Sites CSV file uploaded successfully! We are ready to process URLs!')
                sites_csv.pack()
                end_time2 = time.time()
                execution_time1 = end_time2 - start_time1
                print("upload site csv execution time", execution_time1)
            except mysql.connector.Error as e:
                messagebox.showerror("Error", f"Failed to connect to MySQL: {e}")

    def open_mysql_connection_window():
        root_mysql = tk.Toplevel()
        connection_window = MySQLConnectionWindow(root_mysql, return_to_main_window)
        root_mysql.mainloop()

    def return_to_main_window():
        root.destroy()
        Attorneys_info()

    def update_progress(progress_bar, progress_value):
        progress_bar["value"] = 0
        progress_label.config(text="0.00%")
        progress_bar["value"] = progress_value
        progress_label.config(text=f"{progress_value:.2f}%")
        root.update()

    def update_progress_in_page_site(cursor, db, progress_value):
        try:
            cursor.execute("UPDATE page_site SET  progress = %s ", (progress_value,))
            db.commit()
            update_progress(progress_bar, progress_value)
        except Exception as e:
            print(f"Error updating progress in page_site table: {e}")

    def fetch_text_from_url(url):
        start_timen = time.time()
        try:
            response = requests.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            text = soup.get_text(separator=",")
            end_time = time.time()
            execution_time = end_time - start_timen
            print('fetch_text_from_url Execution_time', execution_time)
            return text

        except requests.RequestException as e:
            print(f"Error fetching URL {url}: {e}")
            return None

    def extract_information_from_text(url, text, cursor, db, site_id, url_soup):
        start_time = time.time()
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b'
        phone_pattern = r'(\(?\d{3}\)?[\s.-]?\d{3,4}[\s.-]?\d{4})'

        nlp = stanza.Pipeline('en', processors='tokenize,mwt,ner', download_method=DownloadMethod.REUSE_RESOURCES)
        doc = nlp(text)
        email_dict = {}
        phone_dict = {}
        names = set()

        for sentence in doc.sentences:
            for ent in sentence.ents:
                if ent.type == 'PERSON':
                    name = ent.text
                    if name not in names:
                        names.add(name)

                        associated_text = text[max(ent.start_char - 40, 0):ent.end_char + 150]
                        emails_match = re.findall(email_pattern, associated_text)
                        phones_match = re.findall(phone_pattern, associated_text)
                        if emails_match:
                            email_dict[name] = emails_match[0]
                        if phones_match:
                            phone_dict[name] = phones_match[0]

        for name in names:
            print("Name:", name)
            email = email_dict.get(name, "Not available")
            phone = phone_dict.get(name, "Not available")
            print("Email:", email)
            print("Phone:", phone)
            print()

            store_attorney_info(cursor, db, site_id, url, name, phone, email)

        end_time = time.time()
        execution_time = end_time - start_time
        print('extract_information_from_text execution_time', execution_time)

    def extract_info_from_page(cursor, db, site_id, url, paragraph, url_soup):
        try:
            start_time = time.time()
            human_names = extract_human_names(paragraph)
            if human_names:
                text = fetch_text_from_url(url)
                if text:
                    extract_information_from_text(url, text, cursor, db, site_id, url_soup)
                else:
                    print("Failed to fetch text from URL")
            else:
                print("No human names found in the paragraph")
            end_time = time.time()
            execution_time = end_time - start_time
            print('extract_info_from_page execution time', execution_time)

        except mysql.connector.errors.DataError as e:
            print(f"Ignoring DataError for URL: {url}. Error: {e}")
            db.rollback()

    def extract_human_names(paragraph):
        start_time = time.time()
        human_names = []
        nlp = stanza.Pipeline('en', processors='tokenize,mwt,ner', download_method=DownloadMethod.REUSE_RESOURCES)
        doc = nlp(paragraph)
        for sentence in doc.sentences:
            for entity in sentence.ents:
                if entity.type == 'PERSON':
                    human_names.append(entity.text)
        end_time = time.time()
        execution_time = end_time - start_time
        print('extract_human_names execution time', execution_time)
        return human_names

    def store_attorney_info(cursor, db, site_id, url, human_name, contact_number, email_id):
        start_time = time.time()
        try:
            date = datetime.datetime.now().date()
            cursor.execute(
                "INSERT INTO attorney_info (site_id, page_url, attorney_names, contact_numbers, email_id, date) VALUES (%s, %s, %s, %s, %s, %s)",
                (site_id, url, human_name, contact_number, email_id, date))
            db.commit()
            end_time = time.time()
            execution_time = end_time - start_time
            print('store_attorney_info execution time', execution_time)
        except mysql.connector.errors.DataError as e:
            print(f"Ignoring DataError for URL: {url}. Error: {e}")
            db.rollback()

    def process_page_site_urls(site_id, cursor, db, webmap_paused, url, username, password, database, current_timeout,
                               webmap_pause_event, Totalurl, update_progress_in_page_site, store_attorney_info,
                               update_error_log):
        total_start_time = time.time()
        try:
            db = mysql.connector.connect(
                host="localhost",
                user=username,
                password=password,
                database=database
            )
            cursor = db.cursor()
            cursor.execute("SELECT COUNT(*) FROM page_site WHERE status='open'")
            total_sites = cursor.fetchone()[0]
            cursor.execute("SELECT site_id, page_url, status FROM page_site WHERE status='open'")
            page_urls_and_statuses = cursor.fetchall()

            for current_page_id, (site_id, url, status) in enumerate(page_urls_and_statuses, start=1):
                if isinstance(webmap_paused, threading.Event) and webmap_paused.is_set():
                    return
                if status == 'open':
                    try:
                        url_start_time = time.time()
                        url_response = requests.get(url, timeout=current_timeout)
                        url_response.raise_for_status()
                        url_soup = BeautifulSoup(url_response.content, 'html.parser')
                        for tag in url_soup(['script', 'style']):
                            tag.decompose()
                        paragraph = url_soup.get_text(separator=' ', strip=True)
                        print(f"Content from {current_page_id}", url, ":\n", paragraph)

                        human_names = []
                        nlp = stanza.Pipeline('en', processors='tokenize,mwt,ner',
                                              download_method=stanza.pipeline.core.DownloadMethod.REUSE_RESOURCES)
                        doc = nlp(paragraph)
                        for sentence in doc.sentences:
                            for entity in sentence.ents:
                                if entity.type == 'PERSON':
                                    human_names.append(entity.text)

                        if human_names:
                            try:
                                response = requests.get(url)
                                response.raise_for_status()
                                soup = BeautifulSoup(response.content, 'html.parser')
                                text = soup.get_text(separator=",")
                            except requests.RequestException as e:
                                print(f"Error fetching URL {url}: {e}")
                                text = None

                            if text:
                                email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b'
                                phone_pattern = r'(\(?\d{3}\)?[\s.-]?\d{3,4}[\s.-]?\d{4})'
                                email_dict = {}
                                phone_dict = {}
                                names = set()

                                doc = nlp(text)
                                for sentence in doc.sentences:
                                    for ent in sentence.ents:
                                        if ent.type == 'PERSON':
                                            name = ent.text
                                            if name not in names:
                                                names.add(name)
                                                associated_text = text[max(ent.start_char - 40, 0):ent.end_char + 150]
                                                emails_match = re.findall(email_pattern, associated_text)
                                                phones_match = re.findall(phone_pattern, associated_text)
                                                if emails_match:
                                                    email_dict[name] = emails_match[0]
                                                if phones_match:
                                                    phone_dict[name] = phones_match[0]

                                for name in names:
                                    print("Name:", name)
                                    email = email_dict.get(name, "Not available")
                                    phone = phone_dict.get(name, "Not available")
                                    print("Email:", email)
                                    print("Phone:", phone)
                                    print()
                                    store_attorney_info(cursor, db, site_id, url, name, phone, email)
                            else:
                                print("Failed to fetch text from URL")
                        else:
                            print("No human names found in the paragraph")

                        cursor.execute("UPDATE page_site SET status = 'completed' WHERE page_url = %s AND site_id=%s",
                                       (url, site_id))
                        db.commit()

                        url_end_time = time.time()
                        url_execution_time = url_end_time - url_start_time
                        print(f'Processing time for URL {current_page_id}: {url_execution_time} seconds')

                        progress_value = current_page_id / total_sites * 100
                        update_progress_in_page_site(cursor, db, progress_value)
                        Totalurl.config(text=f"Total Number of Page URLs to scan: {total_sites}")

                    except (RequestException, Timeout) as e:
                        print(f"Error while fetching or processing URL {url}: {e}")
                        update_error_log(cursor, db, site_id, url, e, inspect.currentframe().f_lineno)
                        cursor.execute("UPDATE page_site SET status = 'url not found' WHERE page_url = %s", (url,))
                        db.commit()
                        progress_value = current_page_id / total_sites * 100
                        update_progress_in_page_site(cursor, db, progress_value)
                if webmap_pause_event.is_set():
                    return
        except (RequestException, Timeout) as e:
            print(f"Error while fetching or processing URLs: {e}")
            update_error_log(cursor, db, site_id, url, e, inspect.currentframe().f_lineno)
            update_progress_in_page_site(cursor, db, 0)
        total_end_time = time.time()
        total_execution_time = total_end_time - total_start_time
        print(f'Total execution time for process_page_site_urls: {total_execution_time} seconds')

    def resume_process_open_urls(site_id, cursor, db, webmap_paused, url, username, password, database, current_timeout,
                               webmap_pause_event, Totalurl, update_progress_in_page_site, store_attorney_info,
                               update_error_log):
        total_start_time = time.time()
        try:
            db = mysql.connector.connect(
                host="localhost",
                user=username,
                password=password,
                database=database
            )
            cursor = db.cursor()
            cursor.execute("SELECT COUNT(*) FROM page_site WHERE status='open'")
            total_sites = cursor.fetchone()[0]
            cursor.execute("SELECT site_id, page_url, status FROM page_site WHERE status='open'")
            page_urls_and_statuses = cursor.fetchall()

            for current_page_id, (site_id, url, status) in enumerate(page_urls_and_statuses, start=1):
                if isinstance(webmap_paused, threading.Event) and webmap_paused.is_set():
                    return
                if status == 'open':
                    try:
                        url_start_time = time.time()
                        url_response = requests.get(url, timeout=current_timeout)
                        url_response.raise_for_status()
                        url_soup = BeautifulSoup(url_response.content, 'html.parser')
                        for tag in url_soup(['script', 'style']):
                            tag.decompose()
                        paragraph = url_soup.get_text(separator=' ', strip=True)
                        print(f"Content from {current_page_id}", url, ":\n", paragraph)

                        human_names = []
                        nlp = stanza.Pipeline('en', processors='tokenize,mwt,ner',
                                              download_method=stanza.pipeline.core.DownloadMethod.REUSE_RESOURCES)
                        doc = nlp(paragraph)
                        for sentence in doc.sentences:
                            for entity in sentence.ents:
                                if entity.type == 'PERSON':
                                    human_names.append(entity.text)

                        if human_names:
                            try:
                                response = requests.get(url)
                                response.raise_for_status()
                                soup = BeautifulSoup(response.content, 'html.parser')
                                text = soup.get_text(separator=",")
                            except requests.RequestException as e:
                                print(f"Error fetching URL {url}: {e}")
                                text = None

                            if text:
                                email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b'
                                phone_pattern = r'(\(?\d{3}\)?[\s.-]?\d{3,4}[\s.-]?\d{4})'
                                email_dict = {}
                                phone_dict = {}
                                names = set()

                                doc = nlp(text)
                                for sentence in doc.sentences:
                                    for ent in sentence.ents:
                                        if ent.type == 'PERSON':
                                            name = ent.text
                                            if name not in names:
                                                names.add(name)
                                                associated_text = text[max(ent.start_char - 40, 0):ent.end_char + 150]
                                                emails_match = re.findall(email_pattern, associated_text)
                                                phones_match = re.findall(phone_pattern, associated_text)
                                                if emails_match:
                                                    email_dict[name] = emails_match[0]
                                                if phones_match:
                                                    phone_dict[name] = phones_match[0]

                                for name in names:
                                    print("Name:", name)
                                    email = email_dict.get(name, "Not available")
                                    phone = phone_dict.get(name, "Not available")
                                    print("Email:", email)
                                    print("Phone:", phone)
                                    print()
                                    store_attorney_info(cursor, db, site_id, url, name, phone, email)
                            else:
                                print("Failed to fetch text from URL")
                        else:
                            print("No human names found in the paragraph")

                        cursor.execute("UPDATE page_site SET status = 'completed' WHERE page_url = %s AND site_id=%s",
                                       (url, site_id))
                        db.commit()

                        url_end_time = time.time()
                        url_execution_time = url_end_time - url_start_time
                        print(f'Processing time for URL {current_page_id}: {url_execution_time} seconds')

                        progress_value = current_page_id / total_sites * 100
                        update_progress_in_page_site(cursor, db, progress_value)
                        Totalurl.config(text=f"Total Number of Page URLs to scan: {total_sites}")

                    except (RequestException, Timeout) as e:
                        print(f"Error while fetching or processing URL {url}: {e}")
                        update_error_log(cursor, db, site_id, url, e, inspect.currentframe().f_lineno)
                        cursor.execute("UPDATE page_site SET status = 'url not found' WHERE page_url = %s", (url,))
                        db.commit()
                        progress_value = current_page_id / total_sites * 100
                        update_progress_in_page_site(cursor, db, progress_value)
                if webmap_pause_event.is_set():
                    return
        except (RequestException, Timeout) as e:
            print(f"Error while fetching or processing URLs: {e}")
            update_error_log(cursor, db, site_id, url, e, inspect.currentframe().f_lineno)
            update_progress_in_page_site(cursor, db, 0)
        total_end_time = time.time()
        total_execution_time = total_end_time - total_start_time
        print(f'Total execution time for process_page_site_urls: {total_execution_time} seconds')

    def run_webmap_process():
        start_time = time.time()
        global webmap_paused, site_id, url, paused_site_id, paused_url, webmap_pause_event
        webmap_paused = False
        pause_button.config(state=tk.NORMAL,bg='lightgray', fg='black')
        resume_button.config(state=tk.DISABLED)
        run_webmap_button.config(state=tk.NORMAL,bg='lightgray', fg='black')
        Download_button.config(state=tk.NORMAL,bg='lightgray', fg='black')
        upload_site_button.config(state=tk.DISABLED)

        def run_webmap_thread():
            global webmap_paused, cursor, db
            db = mysql.connector.connect(
                host="localhost",
                user=username,
                password=password,
                database=database
            )
            cursor = db.cursor()
            try:
                cursor.execute("SELECT id, url, status FROM sites_table")
                sites = cursor.fetchall()
                for site in sites:
                    site_id, url, status = site
                    start_time = datetime.datetime.now()
                    if status != 'open':
                        print(f"Skipping Site {site_id} - {url} as it is already marked as complete.")
                        continue
                    try:
                        fetch_and_process_sitemaps(url, site_id, cursor, db)
                    except Exception as e:
                        update_error_log(cursor, db, site_id, url, e, None)
                        cursor.execute("UPDATE sites_table SET status = 'not visited' WHERE id = %s", (site_id,))
                        db.commit()
                    end_date_time = datetime.datetime.now()
                    cursor.execute("UPDATE sites_table SET status = 'completed' WHERE id = %s", (site_id,))
                    db.commit()
                    cursor.execute("UPDATE sites_table SET end_date_time = %s WHERE id = %s",
                                   (end_date_time, site_id))
                    db.commit()
                while webmap_paused:
                    time.sleep(1)
                process_page_site_urls(site_id, cursor, db, webmap_paused, url, username, password, database, current_timeout,
                               webmap_pause_event, Totalurl, update_progress_in_page_site, store_attorney_info,
                               update_error_log)
            except Exception as e:
                db.rollback()
                messagebox.showerror("Error", f"An error occurred: {e}")
                traceback.print_exc()
            finally:
                cursor.close()
                db.close()
                sites_csv = tk.Label(root, text='Sites CSV file uploaded successfully! We are ready to process URLs!')
                sites_csv.pack()

        webmap_thread = threading.Thread(target=run_webmap_thread)
        webmap_thread.start()

        end_time = time.time()
        execution_time = end_time - start_time
        print('run_webmap_process execution time', execution_time)

    def on_pause_button_click():
        global webmap_paused, webmap_pause_event, webmap_paused_event
        webmap_paused = True
        webmap_pause_event.set()
        webmap_paused_event.set()
        pause_csv = tk.Label(root, text='Wepmap process paused successfully!')
        pause_csv.pack()
        pause_button.config(state=tk.DISABLED)
        resume_button.config(state=tk.NORMAL,bg='lightgray', fg='black')

    def new_resume_button_click():
        global webmap_paused, webmap_pause_event, webmap_paused_event
        resume_csv = tk.Label(root,
                              text='Wepmap process resumed successfully!')
        Download_button.config(state=tk.NORMAL,bg='lightgray', fg='black')
        resume_csv.pack()
        db = mysql.connector.connect(
            host="localhost",
            user=username,
            password=password,
            database=database
        )
        cursor = db.cursor()
        webmap_pause_event.clear()
        if run_webmap_button.winfo_exists() and run_webmap_button.winfo_ismapped():
            pause_button.config(state=tk.DISABLED)
            resume_button.config(state=tk.DISABLED)
            run_webmap_button.config(state=tk.NORMAL,bg='lightgray', fg='black')
            Download_button.config(state=tk.NORMAL,bg='lightgray', fg='black')
            pause_button.config(state=tk.NORMAL,bg='lightgray', fg='black')
            resume_process_open_urls(site_id, cursor, db, webmap_paused, url, username, password, database, current_timeout,
                               webmap_pause_event, Totalurl, update_progress_in_page_site, store_attorney_info,
                               update_error_log)
        else:
            pass

    def resume_on_halt():
        upload_site_button.config(state=tk.DISABLED)
        pause_button.config(state=tk.DISABLED)
        resume_button.config(state=tk.DISABLED)
        run_webmap_button.config(state=tk.NORMAL,bg='lightgray', fg='black')
        Download_button.config(state=tk.NORMAL,bg='lightgray', fg='black')

    def download_with_date_range():
        def download_csv_with_dates(from_date, to_date):
            try:
                download_path = filedialog.askdirectory()
                with mysql.connector.connect(
                        host="localhost",
                        user=username,
                        password=password,
                        database=database,
                ) as db:
                    cursor = db.cursor()

                    if from_date == to_date:
                        cursor.execute(
                            f"SELECT site_id,page_url,error,error_line,time_date FROM error_log WHERE Time_Date = '{from_date}'"
                        )
                    else:
                        cursor.execute(
                            f"SELECT site_id,page_url,error,error_line,time_date FROM error_log WHERE Time_Date BETWEEN '{from_date}' AND '{to_date}'"
                        )
                    error_log_data = cursor.fetchall()
                    error_log_df = pd.DataFrame(
                        error_log_data,
                        columns=["Site_id", "Page_url", "Error", "Error_line", "Time_Date"],
                    )
                    date_today = datetime.datetime.now().strftime("%m.%d.%Y")
                    error_log_filename = f'{download_path}/Contact_Crawler_Error_log{date_today}.csv'
                    error_log_df.to_csv(error_log_filename, index=False)

                    if from_date == to_date:
                        cursor.execute(
                            f"SELECT s.url,a.page_url,a.attorney_names, a.contact_numbers, a.email_id,a.Date FROM sites_table s JOIN attorney_info a ON s.id = a.site_id where a.Date ='{from_date}'"
                        )
                    else:
                        cursor.execute(
                            f"SELECT s.url,a.page_url,a.attorney_names, a.contact_numbers, a.email_id ,a.Date FROM sites_table s JOIN attorney_info a ON s.id = a.site_id where a.Date BETWEEN '{from_date}' AND '{to_date}';"
                        )
                    result = cursor.fetchall()
                    result_df = pd.DataFrame(result,
                                             columns=['Website', 'Page_url', 'Attorney_name', 'Contact_number', 'Email_id', "Date"])
                    date_today1 = datetime.datetime.now().strftime("%m.%d.%Y")
                    result_filename = f'{download_path}/Contact_Crawler_result{date_today1}.csv'
                    result_df.to_csv(result_filename, index=False)

                    messagebox.showinfo("Success", "Data downloaded successfully!")
            except Exception as e:
                messagebox.showerror("Error", f"An error occurred: {e}")

        def open_date_window():
            date_window = ttk.Toplevel(root)
            date_window.title("Enter Date Range")
            date_window.geometry("400x230")

            def download_button_click():
                from_date = from_date_entry.entry.get()
                to_date = to_date_entry.entry.get()
                try:
                    from_date = datetime.datetime.strptime(from_date, "%m/%d/%Y")
                    to_date = datetime.datetime.strptime(to_date, "%m/%d/%Y")
                    if from_date > to_date:
                        messagebox.showerror("Error", "From date cannot be after To date.")
                        return
                    download_csv_with_dates(from_date.strftime("%Y-%m-%d"), to_date.strftime("%Y-%m-%d"))
                    date_window.destroy()
                except ValueError:
                    messagebox.showerror("Error", "Invalid date format. Please use MM/DD/YYYY.")

            from_date_label = ttk.Label(date_window, text="From Date :")
            from_date_label.grid(row=0, column=0, padx=10, pady=5)

            from_date_entry = ttk.DateEntry(date_window, bootstyle="primary", dateformat="%m/%d/%Y")
            from_date_entry.grid(row=0, column=1, padx=10, pady=5)

            to_date_label = ttk.Label(date_window, text="To Date :")
            to_date_label.grid(row=1, column=0, padx=10, pady=5)

            to_date_entry = ttk.DateEntry(date_window, bootstyle="primary", dateformat="%m/%d/%Y")
            to_date_entry.grid(row=1, column=1, padx=10, pady=5)

            download_button = ttk.Button(date_window, text="Download", command=download_button_click)
            download_button.grid(row=2, column=0, columnspan=2, padx=10, pady=10)

        open_date_window()


    def check_and_rename_file(filename, max_size_mb=25):
        try:
            file_size_bytes = os.path.getsize(filename)
            file_size_mb = file_size_bytes / (1024 * 1024)
            if file_size_mb > max_size_mb:
                file_name, file_extension = os.path.splitext(filename)
                count = 1
                new_filename = f"{file_name}_{count}{file_extension}"
                while os.path.exists(new_filename):
                    count += 1
                    new_filename = f"{file_name}_{count}{file_extension}"
                os.rename(filename, new_filename)
                print(f"File '{filename}' exceeded size limit. Renamed to '{new_filename}'.")
        except Exception as e:
            print(f"Error checking and renaming file: {e}")
        Download_button.config(state=tk.NORMAL,bg='lightgray', fg='black')

    button_frame = tk.Frame(root)
    button_frame.pack(pady=10)
    settings_options = ["Database Setting", "Timeout Setting", "Add url Patterns"]
    selected_setting = tk.StringVar()
    settings_dropdown = ttk.Combobox(button_frame, textvariable=selected_setting, values=settings_options,
                                     state="readonly")
    settings_dropdown.grid(row=20, column=6, padx=16)
    settings_dropdown.set("Select setting option")

    def open_selected_settings_window(event):
        selected_option = selected_setting.get()
        if selected_option == "Database Setting":
            open_mysql_connection_window()
        elif selected_option == "Timeout Setting":
            open_settings_window()
        elif selected_option == "Add url Patterns":
            open_url_pattern_window()

    def open_url_pattern_window():
        url_pattern_window = tk.Toplevel(root)
        url_pattern_window.title("Add URL Pattern")
        url_pattern_window.geometry('300x150')

        pattern_label = tk.Label(url_pattern_window, text="Enter URL Pattern:")
        pattern_label.pack(pady=5)

        pattern_entry = tk.Entry(url_pattern_window)
        pattern_entry.pack(pady=5)

        save_button = tk.Button(url_pattern_window, text="Save Pattern",
                                command=lambda: save_url_pattern(pattern_entry.get(), url_pattern_window))
        save_button.pack(pady=5)

    def save_url_pattern(pattern, window):
        try:
            db = mysql.connector.connect(
                host="localhost",
                user=username,
                password=password,
                database=database
            )
            cursor = db.cursor()
            sql = "INSERT INTO url_pattern VALUES (%s)"
            cursor.execute(sql, (pattern,))
            db.commit()
            messagebox.showinfo("Success", "Pattern saved successfully.")
            window.destroy()
        except Exception as e:
            messagebox.showerror("Error", f"An error occurred: {e}")

    def open_settings_window():
        global timeout_var
        settings_window = tk.Toplevel(root)
        settings_window.title("Settings Window")
        settings_window.geometry('230x150')
        timeout_label = tk.Label(settings_window, text="Enter new timeout value:")
        timeout_label.pack(pady=10)
        timeout_var = tk.StringVar()
        timeout_entry = tk.Entry(settings_window, textvariable=timeout_var)
        timeout_entry.pack(pady=10)
        update_timeout_button = tk.Button(settings_window, text="Update Timeout",
                                          command=lambda: update_timeout(int(timeout_var.get()), settings_window))
        update_timeout_button.pack(pady=10)

    def update_timeout(new_timeout, settings_window):
        global current_timeout, timeout_var
        try:
            if new_timeout > 0:
                current_timeout = new_timeout
                timeout_var.set(new_timeout)
                settings_window.destroy()
                messagebox.showinfo("Timeout Updated", f"Timeout value updated to {new_timeout} seconds.")
            else:
                messagebox.showwarning("Invalid Timeout", "Please enter a positive integer for the timeout.")
        except ValueError:
            messagebox.showwarning("Invalid Timeout", "Please enter a valid integer for the timeout.")

    settings_dropdown.bind("<<ComboboxSelected>>", open_selected_settings_window)

    upload_site_button = tk.Button(button_frame, text="   Upload Websites   ", state=tk.DISABLED, command=upload_site_csv,width=22)
    upload_site_button.grid(row=20, column=1, padx=12)
    label_keyword = tk.Label(button_frame, text="1")
    label_keyword.grid(row=21, column=1, pady=5)
    run_webmap_button = tk.Button(button_frame, text="   Run Contact WebCrawler   ", state=tk.DISABLED, command=run_webmap_process,width=22)
    run_webmap_button.grid(row=20, column=3, padx=17)
    label_keyword = tk.Label(button_frame, text="2")
    label_keyword.grid(row=21, column=3, pady=5)
    Download_button = tk.Button(button_frame, text="   Download Result   ", state=tk.DISABLED, command=download_with_date_range,width=22)
    Download_button.grid(row=20, column=4, padx=12)
    label_keyword = tk.Label(button_frame, text="3")
    label_keyword.grid(row=21, column=4, pady=5)
    url_count_label = tk.Label(root, text="Total Number of Site URLs to scan: 0")
    url_count_label.place(relx=0.5, rely=0.120, anchor="center")
    Totalurl = tk.Label(root, text="Total Number of Page URLs to scan: 0")
    Totalurl.place(relx=0.5, rely=0.180, anchor="center")
    progress_bar = ttk.Progressbar(root, orient="horizontal", length=370, mode="determinate")
    progress_bar.place(relx=0.5, rely=0.23, anchor="center")
    progress_label = tk.Label(root, text="0%")
    progress_label.place(relx=0.5, rely=0.280, anchor="center")
    pause_button = tk.Button(button_frame, text="Pause", state=tk.DISABLED, command=on_pause_button_click,width=22)
    pause_button.grid(row=23, column=10, padx=20, pady=13)
    resume_button = tk.Button(button_frame, text="Resume", state=tk.DISABLED, command=new_resume_button_click,width=22)
    resume_button.grid(row=26, column=10, padx=20, pady=13)
    restart_button = tk.Button(button_frame, text="Resume on Halt", state=tk.DISABLED, command=new_resume_button_click,width=22)
    restart_button.grid(row=29, column=10, padx=20, pady=13)
    upload_site_button.config(state=tk.NORMAL,bg='lightgray', fg='black')
    restart_button.config(state=tk.NORMAL,bg='lightgray', fg='black')
    Download_button.config(state=tk.NORMAL,bg='lightgray', fg='black')
    root.mainloop()
    cursor.close()
    db.close()

if __name__ == "__main__":
    Attorneys_info()