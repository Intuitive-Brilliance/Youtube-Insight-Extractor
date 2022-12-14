#Importing the required libraries
from flask import Flask, render_template, request,jsonify
from flask_cors import CORS,cross_origin
import requests


from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait,Select
from selenium.webdriver.support import expected_conditions as EC
import pytube as pt
import time
import re
import datetime


import os
import pandas as pd
from pandas_profiling import ProfileReport

import pymysql as conn
import pymongo

import boto3

def log(file_object, log_message):
    now = datetime.datetime.now()
    date = now.date()
    current_time = now.strftime("%H:%M:%S")
    file_object.write(str(date) + "/" + str(current_time) + "\t\t" + log_message +"\n")


app = Flask(__name__)
# Route to display the home page
@app.route('/',methods=['GET'])
@cross_origin()
def homePage():
    file_object = open("Homepage_log/homepage_log.txt", 'a+')
    log_message = 'Render template - index.html called'
    log(file_object, log_message)
    return render_template("index.html")

# Route to show the compiled data in a web UI
@app.route('/overview',methods=['POST','GET'])
@cross_origin()
def index():
    if request.method == 'POST':
        try:
            #Opens Chrome
            def openchrome(url):
                file_object = open("Index_log/openchrome_log.txt", 'a+')
                log_message = 'Openchrome called'
                log(file_object, log_message)
                driver = webdriver.Chrome(executable_path='chromedriver.exe')
                driver.get(url)

                SCROLL_PAUSE_TIME = 4
                count = 0
                first_height = driver.execute_script("return document.documentElement.scrollHeight")
                last_height = first_height
                while count < 3:
                    if count == 2:
                        # Scroll down to top
                        driver.execute_script("window.scrollTo(0, arguments[0]);", first_height)
                        # Wait to load page
                        time.sleep(SCROLL_PAUSE_TIME)
                    else:
                        # Scroll down to bottom
                        driver.execute_script("window.scrollTo(0, arguments[0]);", last_height)
                        # Wait to load page
                        time.sleep(SCROLL_PAUSE_TIME)

                    # Calculate new scroll height and compare with last scroll height
                    new_height = driver.execute_script("return document.documentElement.scrollHeight")
                    last_height = new_height
                    count += 1
                log_message = 'Openchrome successfully executed'
                log(file_object, log_message)
                return driver

            # Scrapes channel
            def scrape_channel(driver, url):
                file_object = open("Index_log/scrapechannel_log.txt", 'a+')
                log_message = 'Scrapechannel called'
                log(file_object, log_message)
                titles_se = driver.find_elements(By.ID, "video-title")
                titles = [titles_se[i].text for i in range(50)]
                vids = pt.Channel(url)[0:50]
                views_se = driver.find_elements(By.XPATH, '//*[@id="metadata-line"]/span[1]')
                views = [views_se[i].text for i in range(50)]
                images_se = driver.find_elements(By.XPATH, '//*[@id="dismissible"]/ytd-thumbnail/a/yt-img-shadow/img')
                images = [images_se[i].get_attribute('src') for i in range(50)]
                channel_name = driver.find_elements(By.XPATH, '//*[@id="text"]')[2].text
                links = [titles_se[i].get_attribute("href") for i in range(50)]
                driver.quit()
                log_message = 'Channel:{} Scrapechannel successfully executed'.format(url)
                log(file_object, log_message)
                return (titles, views, images, links, channel_name)

            # Scrapes Videos
            def scrape_vids(driver, url):
                file_object = open("Index_log/scrapevids_log.txt", 'a+')
                log_message = 'Video:<{}> Scrapevids called'.format(url)
                log(file_object, log_message)
                try:
                    likes = driver.find_elements(By.XPATH, '//*[@id="text"]')[2].text
                except:
                    likes = 'NA'
                    log_message = 'Video:<{}> Likes not found'.format(url)
                    log(file_object, log_message)
                try:
                    duration_se = driver.find_elements(By.XPATH, "//span[@class='ytp-time-duration']")[0].text
                    temp = time.strptime(duration_se, '%M:%S')
                    duration = datetime.timedelta(minutes=temp.tm_min, seconds=temp.tm_sec).total_seconds()
                except:
                    try:
                        duration = pt.YouTube(url).length
                    except:
                        duration = 'NA'
                        log_message = 'Video:<{}> Duration not found'.format(url)
                        log(file_object, log_message)
                try:
                    comments_no = driver.find_elements(By.XPATH, '//*[@id="count"]/yt-formatted-string')[0].text.split(' ')[0]
                except:
                    comments_no = 'NA'
                    log_message = 'Video:<{}> Comment number not found'.format(url)
                    log(file_object, log_message)
                try:
                    comments = driver.find_elements(By.XPATH, '//*[@id="contents"]')
                except:
                    comments = 'NA'
                    log_message = 'Video:<{}> Comments not found'.format(url)
                    log(file_object, log_message)
                cmnts = dict()
                try:
                    cmnts_cluster = comments[0].text.split('\n')
                    for i in range(len(cmnts_cluster)):
                        if 'ago' in cmnts_cluster[i]:
                            if cmnts_cluster[i - 1] not in cmnts:
                                cmnts[cmnts_cluster[i - 1]] = cmnts_cluster[i + 1]
                            else:
                                cmnts[cmnts_cluster[i - 1] + '<' + str(i) + '>'] = cmnts_cluster[i + 1]
                    if len(cmnts) == 0:
                        cmnts = {'NA': 'NA'}
                        log_message = 'Video:<{}> Comment not found'.format(url)
                        log(file_object, log_message)
                except:
                    cmnts = {'NA': 'NA'}
                    log_message = 'Video:<{}> Comment not found'.format(url)
                    log(file_object, log_message)
                driver.quit()
                return (likes, comments_no, cmnts, duration)

            #Input
            searchString = request.form['content']
            url = searchString+'/videos'

            global channel_name, comments
            driver = openchrome(url)
            titles, views, images, links, channel_name = scrape_channel(driver, url)

            likes = list()
            comments_no = list()
            comments = list()
            duration = list()

            # Limiting the data to save time
            titles = titles[:1]
            views = views[:1]
            images = images[:1]
            links = links[:1]
            duration = duration[:1]

            count = 0
            for i in links:
                count += 1
                print('Video {}...'.format(count))
                driver = openchrome(i)
                likes_vid, comments_no_vid, cmnts_vid, duration_vid = scrape_vids(driver, i)
                likes.append(likes_vid)
                comments_no.append(comments_no_vid)
                comments.append(cmnts_vid)
                duration.append(duration_vid)

            # Compiling top comment
            top_comment = list()
            for vid_coms in comments:
                for each_comment in vid_coms:
                    top_comment.append(str(each_comment) + ': ' + vid_coms[each_comment])
                    break
            # Creating a list of dictionaries
            reviews = list()
            for i in range(len(titles)):
                reviews.append({'links':links[i],'titles':titles[i],'views':views[i],'images':images[i],'likes':likes[i],'comments_no':comments_no[i],'top_comment':top_comment[i],'duration':duration[i]})

            # Conversion of data into dataframe
            global df
            df = pd.DataFrame(zip(links, titles, views, images, likes, comments_no, top_comment, duration),
                              columns=['links', 'titles', 'views', 'images', 'likes', 'comments_no', 'top_comment',
                                       'duration'])
            df['channel_name'] = channel_name
            df = df[['links', 'titles', 'views', 'images', 'likes', 'comments_no', 'top_comment', 'duration','channel_name']]
            df.to_csv(channel_name + '.csv')

            return render_template('results.html', reviews=reviews[0:(len(reviews))],check_channel=channel_name)
        except Exception as e:
            print('The Exception message is: ',e)
            return 'something is wrong'

    else:
        return render_template('index.html')

# Route to generate profile report and display it
@app.route('/insights',methods=['POST','GET'])
@cross_origin()
def insights():
    if request.method == 'POST':
        try:
            file_object = open("Insights_log/insights_log.txt", 'a+')
            log_message = 'Insights function called'
            log(file_object, log_message)
            df_in = df[['views', 'likes', 'comments_no', 'duration']]
            df_in['length_of_title'] = [len(i) for i in df.titles]

            likes = df_in.likes.to_list()
            count=0

            log_message = 'Data cleaning started'
            log(file_object, log_message)
            # Data cleaning
            for lks in likes:
                try:
                    temp = int(lks)
                except:
                    try:
                        lks = lks.strip('K')
                        act_lks = int(float(lks) * 1000)
                        likes[count] = str(act_lks)
                    except:
                        likes[count] = 'NA'

                count += 1
            df_in['likes']=likes

            views = df_in.views.to_list()
            count = 0
            for vws in views:
                try:
                    temp = int(vws)
                except:
                    try:
                        vws = vws.strip('K views')
                        act_vws = int(float(vws) * 1000)
                        views[count] = str(act_vws)
                    except:
                        views[count] = 'NA'
                count += 1
            df_in['views']=views

            ProfileReport(df_in).to_file(os.getcwd()+'\\templates\\report.html')
            log_message = 'Profile report successfully generated'
            log(file_object, log_message)
            return render_template('report.html')
        except Exception as e:
            file_object = open("Insights_log/insights_log.txt", 'a+')
            log_message = 'Insights function called but something went wrong. Error:{}'.format(str(e))
            log(file_object, log_message)
            return 'Seomething went wrong '+str(e)


# Route to save data to cloud
@app.route('/savetocloud',methods=['POST','GET'])
@cross_origin()
def savetocloud():
    if request.method == 'POST':
        try:
            file_object = open("Savetocloud_log/savetocloud_log.txt", 'a+')
            log_message = 'Save to cloud function called'
            log(file_object, log_message)
            df_cloud = df[['links', 'titles', 'views', 'images', 'likes', 'comments_no', 'top_comment', 'duration']]

            lst_of_tuples = list(df_cloud.itertuples(index=False, name=None))
            try:
                file_object = open("Savetocloud_log/mysql.txt", 'a+')
                log_message = 'Trying to insert data into Azure Mysql'
                log(file_object, log_message)
                # Connecting to mysql and creating new table for channel and inserting data
                mydb = conn.connect(host='', user='', passwd='',port=3306, ssl_ca="")
                cursor = mydb.cursor()

                query = 'create table if not exists utuber_analytics.' + channel_name + '(utube_link varchar(300),title varchar(300),views varchar(80),thumnail_url varchar(300), likes varchar(40),comments varchar(8),top_comment varchar(600), duration varchar(100))'
                cursor.execute(query)
                mydb.commit()

            # Creating list of tuples to easily insert data
                for data in lst_of_tuples:
                    mydb = mydb = conn.connect(host='', user='',passwd='', port=3306, ssl_ca="")
                    cursor = mydb.cursor()
                    query = 'insert into utuber_analytics.' + channel_name + ' values' + str(data)
                    cursor.execute(query)
                    mydb.commit()
                mydb.close()

                outcome1 = 'Successfully uploaded to Azure MySQL Database!!!'


                log(file_object, 'Successfully uploaded to Azure MySQL Database')

            except Exception as e:
                outcome1 = str(e)
                log_message = 'Trying to insert data into Azure Mysql but something went wrong. Error:{}'.format(str(outcome1))
                log(file_object, log_message)

            try:
                file_object = open("Savetocloud_log/mongodb.txt", 'a+')
                log_message = 'Trying to insert data into MongoDB Atlas'
                log(file_object, log_message)
                # Connecting to MongoDB and storing data
                client = pymongo.MongoClient("")
                db = client.test

                titles = df.titles.to_list()
                images = df.images.to_list()

                cluster = client[channel_name]

                collec = cluster['thumbnail']
                for ele in range(len(titles)):
                    collec.insert_one({titles[ele]: images[ele]})

                collec = cluster['comments']
                for ele in range(len(titles)):
                    collec.insert_one({titles[ele]: comments[ele]})

                client.close()
                outcome2= 'Successfully uploaded to MongoDB Atlas!!!'


                log(file_object, 'Successfully uploaded to MongoDB Atlas')

            except Exception as e:
                outcome2 = str(e)
                log_message = 'Trying to insert data into MongoDB Atlas but something went wrong. Error:{}'.format(str(outcome2))
                log(file_object, log_message)

            outcome = 'Outcome:'+'\n' + str(outcome1) + '\n' + str(outcome2)

            return render_template('outcome.html',outcome =outcome )
        except Exception as e:
            file_object = open("Savetocloud_log/savetocloud_log.txt", 'a+')
            log_message = 'Save to cloud function called but something went wrong. Error:{}'.format(str(e))
            log(file_object, log_message)
            return 'Something went wrong: '+str(e)

# Route to download videos to cloud
@app.route('/downloadvid',methods=['POST','GET']) # route to show the review comments in a web UI
@cross_origin()
def downlaodvid():
    if request.method == 'POST':
        try:
            file_object = open("Downloadtocloud_log/downloadtocloud_log.txt", 'a+')
            log_message = 'Download to cloud function called'
            log(file_object, log_message)
            links = df.links.to_list()
            titles = df.titles.to_list()

            '''links = ['https://www.youtube.com/watch?v=1O0yazhqaxs','https://www.youtube.com/watch?v=FtdpDjV0dMY']
            titles = ['3 Second Video','2 second video']'''
            try:
                #channel_name = 'Telusko'
                # Connecting to S3 Bucket
                file_object = open("Downloadtocloud_log/s3bucket_log.txt", 'a+')
                log_message = 'Connecting to AWS S3 Bucket'
                log(file_object, log_message)
                s3 = boto3.resource(service_name='s3', region_name='', aws_access_key_id='',aws_secret_access_key='')
                bucket_name = channel_name.lower().replace(' ', '-')
                s3.create_bucket(ACL='private', Bucket=bucket_name)
                for ele in range(len(links)):
                    try:

                        log_message = 'Video:<{}> Downloading...'.format(titles[ele])
                        log(file_object, log_message)

                        # Downloading video
                        vid = pt.YouTube(links[ele])
                        path_vid = vid.streams.filter(file_extension='mp4', res="240p").first().download()


                        log_message = 'Video:<{}> Download sucessful'.format(titles[ele])
                        log(file_object, log_message)
                        # path_vid = os.getcwd()+'\\'+titles[ele]+'.mp4'
                    except:

                        log_message = 'Video:<{}> Download Failed. Tryng next video'.format(titles[ele])
                        log(file_object, log_message)
                        continue
                    try:

                        log_message = 'Video:<{}> Uploading...'.format(titles[ele])
                        log(file_object, log_message)

                        # Uploading video
                        s3.Bucket(bucket_name).upload_file(Filename=path_vid, Key=titles[ele] + '.mp4')


                        log_message = 'Video:<{}> Upload successful'.format(titles[ele])
                        log(file_object, log_message)
                    except:

                        log_message = 'Video:<{}> Upload failed'.format(titles[ele])
                        log(file_object, log_message)
                        continue

                # Uploading csv
                s3.Bucket(bucket_name).upload_file(Filename=channel_name + '.csv', Key=channel_name + '.csv')
    
                outcome = 'Videos successfully downloaded to AWS S3 Bucket!!!'

            except Exception as e:
                outcome = str(e)
                file_object = open("Downloadtocloud_log/s3bucket_log.txt", 'a+')
                log_message = 'Connection to AWS S3 Bucket failed. Error{}'.format(str(outcome))
                log(file_object, log_message)

            return render_template('outcome.html', outcome = 'Outcome: ' + str(outcome))
        except Exception as e:
            file_object = open("Downloadtocloud_log/downloadtocloud_log.txt", 'a+')
            log_message = 'Download to cloud function called but something went wrong. Error:{}'.format(str(e))
            log(file_object, log_message)
            return 'Something went wrong: ' + str(e)


if __name__ == "__main__":
    #app.run(host='127.0.0.1', port=8071, debug=True)
	app.run(debug=True)
