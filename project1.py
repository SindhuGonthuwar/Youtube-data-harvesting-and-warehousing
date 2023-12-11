from googleapiclient.discovery import build
import pymongo
import mysql.connector
import pandas as pd
import streamlit as st

def api_connect():   
   api_key='*****'
   youtube = build( 'youtube' , 'v3' , developerKey= api_key)
   return youtube
youtube=api_connect()

def get_channel_details(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()
    for i in range(len(response['items'])):
        data={
            "channel_title":response['items'][i]['snippet']['title'],
            "channel_Id":response['items'][i]['id'],
            "channel_description":response['items'][i]['snippet']['description'],
            'channel_published':response['items'][i]['snippet']['publishedAt'],
            'channel_playlistsid': response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
            'channel_views':response['items'][i]['statistics']['viewCount'],
            'channel_susbcribers':response['items'][i]['statistics']['subscriberCount'],
            'channel_videos':response['items'][i]['statistics']['videoCount']
        }
        
    
    return data

def get_video_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id,
                                     part="contentDetails").execute()
    
    Playlists_Id= response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token= None

    while True:
        response1=youtube.playlistItems().list(
            part="snippet",
            playlistId=Playlists_Id,
            maxResults=50,
            pageToken=next_page_token).execute()
        
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get("nextPageToken")

        if next_page_token is None:
            break

    return video_ids    

def get_video_details(video_ids):
    video_data=[]
    for video_id in video_ids:

        request=youtube.videos().list(
        part="snippet,contentDetails,statistics",
        id=video_id)

        response=request.execute()

        for i in response['items']:
            data= dict(channel_name=i['snippet']['channelTitle'],
                       channel_id=i['snippet']['channelId'],
                       video_id=i['id'],
                       title=i['snippet']['title'],
                       tags=i['snippet'].get('tags'),
                       description=i['snippet'].get('description'),
                       published_date=i['snippet']['publishedAt'],
                       Duration=i['contentDetails']['duration'],
                       views=i['statistics'].get('viewCount'),
                       comments=i['statistics'].get("commentCount"),
                       likecount=i['statistics'].get('likeCount'),
                       favorite_count=i['statistics'].get('favoriteCount'),
                       definition=i['contentDetails']['definition'],
                       caption_status=i['contentDetails']['caption'],
                       Thumbnai=i['snippet']['thumbnails'],
                       )
            video_data.append(data)
    return video_data

def get_comment_details(video_ids):
    comment_data=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response=request.execute()

            for i in response['items']:
                data= dict(
                    comment_id=i['snippet']['topLevelComment']['id'],
                    video_id=i['id'],
                    comment_text=i['snippet']['topLevelComment']['snippet']['textDisplay'],
                    comment_author=i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    comment_published=i['snippet']['topLevelComment']['snippet']['publishedAt']
                )
                comment_data.append(data)

    except:
       pass
    return comment_data

def get_playlists_details(channel_id):
    next_page_token= None
    all_data=[]
    while True:
        request=youtube.playlists().list(
            part='snippet,contentDetails',
            channelId= channel_id,
            maxResults=50,
            pageToken=next_page_token

        )
        response=request.execute()

        for i in response['items']:
            data=dict(playlists_id=i['id'],
                      title=i['snippet']['title'],
                      channel_id=i['snippet']['channelId'],
                      channel_name=i['snippet']['channelTitle'],
                      published_at=i['snippet']['publishedAt'],
                      video_count=i['contentDetails']['itemCount'])
            all_data.append(data)
        next_page_token=response.get('nextPageToken')
        if next_page_token is None:
            break
    return all_data

#mongodb connection
con=pymongo.MongoClient("mongodb://localhost:27017")
db=con['youtube']

def insert_channel_mdb(channel_id):
    db=con['youtube']
    col1=db['channel_details']
    try:
      if channel_id:
        existing_channel = col1.find_one({"channel_information.channel_Id": channel_id})   
        if existing_channel:
          print("channel already exists ")
        else:  
            c ={"channel_information":get_channel_details(channel_id)}
            col1.insert_one(c)
            col2=db['video_details']
            video_ids=get_video_ids(channel_id)
            v={"video_information":get_video_details(video_ids)}
            col2.insert_one(v)
            col3=db['comment_details']
            video_ids=get_video_ids(channel_id)
            g={"comment_information":get_comment_details(video_ids)}
            col3.insert_one(g)
            col4=db['playlists_details']
            col4.insert_one({"playlists_information":get_playlists_details(channel_id)})
            print("channel uploaded to mongodb successfully")
      else:
        print("Channel ID is null or empty. Cannot insert.")
    except Exception as e:
        print(f"Error occurred while inserting channel data: {e}")

 
def channels_table(channel_id):
    mydb= mysql.connector.connect(host='localhost',
                              user='root',
                              password='@Sindhu29',
                              database='youtube'
                             )
    mycursor=mydb.cursor()
    
    try:
        sql='''create table if not exists channels(channel_title varchar(100),
                                                   channel_Id varchar(80) primary key,
                                                   channel_description text,
                                                   channel_published varchar(200),
                                                   channel_playlistsid varchar(80),
                                                   channel_views int,
                                                   channel_susbcribers int,
                                                   channel_videos int)'''
    
        mycursor.execute(sql)
        mydb.commit()
    except:
        print("channels table already created")

    
    channels_list=[]
    db=con['youtube']
    col1=db['channel_details']
    b= col1.find_one({"channel_information.channel_Id":channel_id},{'_id':0})
    df=pd.DataFrame(b["channel_information"],index=[0])

    mydb= mysql.connector.connect(host='localhost',
                              user='root',
                              password='@Sindhu29',
                              database='youtube'
                             )
    mycursor=mydb.cursor()

    for index,row in df.iterrows():
        sql='''insert into channels(channel_title,
                                    channel_Id,
                                    channel_description,
                                    channel_published,
                                    channel_playlistsid,
                                    channel_views,
                                    channel_susbcribers,
                                    channel_videos)
                                    values(%s,%s,%s,%s,%s,%s,%s,%s)
                                    '''
        

        
        values = (row['channel_title'],
                  row['channel_Id'],
                  row['channel_description'],
                  row['channel_published'],
                  row['channel_playlistsid'],
                  row['channel_views'],
                  row['channel_susbcribers'],
                  row['channel_videos']
                   )
    
    try:
        mycursor.execute(sql,values)
        mydb.commit()
    except:
        print("channel details already inserted in sql")
    
import mysql.connector
import pandas as pd
import json
from datetime import datetime, timedelta
import re

def convert_duration(Duration):
    # (\d+) - one or more digits.
    # ? - makes the preceding element or group in the pattern optional
    # PT,M,S are alphabets as mentioned in the expression inbetween which digits are present
    # PT(hrs)H(mins)M(secs)S
    duration_regex = r'PT((\d+)H)?((\d+)M)?((\d+)S)?'
    matches = re.match(duration_regex, Duration)
    # Period of Time timestamp(string) to seconds(int)
    if matches:
        # In this pattern, the first group starts with "(", so it's the number 1.
        # The second group starts with "((", so it's number 2
        # The third group starts after ? & so, it goes on
        hours = int(matches.group(2) or 0)
        minutes = int(matches.group(4) or 0)
        seconds = int(matches.group(6) or 0)
        total_seconds = hours * 3600 + minutes * 60 + seconds
        return total_seconds

    return 0

def convert_published_date(published_date_str):
    # Assuming published_date_str is in a specific format, adjust as needed
    return datetime.strptime(published_date_str, "%Y-%m-%dT%H:%M:%SZ")

def videos_table(channel_id):
        mydb = mysql.connector.connect(host='localhost',
                                       user='root',
                                       password='@Sindhu29',
                                       database='youtube')
        mycursor=mydb.cursor()

        try:
            sql = '''create table if not exists videos(
                         channel_name varchar(100),
                         channel_id varchar(80),
                         video_id varchar(30) primary key,
                         title varchar(150),
                         tags text,
                         description text,
                         published_date datetime,
                         Duration int,
                         views int,
                         comments int,
                         likecount int,
                         favorite_count int,
                         definition varchar(10),
                         caption_status varchar(50),
                         Thumbnai text
                     )'''
            mycursor.execute(sql)
            mydb.commit()

            videos_list = []
            db = con['youtube']
            col2 = db['video_details']

            for video_data in col2.find({"video_information.channel_id":channel_id}, {"_id": 0}):
                for i in range(len(video_data['video_information'])):
                    videos_list.append(video_data['video_information'][i])

            df1 = pd.DataFrame(videos_list)

            for index, row in df1.iterrows():
                # Convert Duration to seconds
                duration_seconds = convert_duration(row['Duration'])

                # Convert published_date to datetime
                published_date = convert_published_date(row['published_date'])

                sql = '''insert into videos(
                             channel_name,
                             channel_id,
                             video_id,
                             title,
                             tags,
                             description,
                             published_date,
                             Duration,
                             views,
                             comments,
                             likecount,
                             favorite_count,
                             definition,
                             caption_status,
                             Thumbnai
                         )
                         values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''

                values = (
                    row['channel_name'],
                    row['channel_id'],
                    row['video_id'],
                    row['title'],
                    json.dumps(row['tags']),
                    row['description'],
                    published_date,
                    duration_seconds,
                    row['views'],
                    row['comments'],
                    row['likecount'],
                    row['favorite_count'],
                    row['definition'],
                    row['caption_status'],
                    json.dumps(row['Thumbnai']),
                )

                mycursor.execute(sql, values)
                mydb.commit()

        except Exception as e:
            print(f"Error: {e}")

    
#creating comments table
def comments_table():
    mydb= mysql.connector.connect(host='localhost',
                                  user='root',
                                  password='@Sindhu29',
                                  database='youtube')
    mycursor=mydb.cursor()


    drop_query='''drop table if exists comments'''
    mycursor.execute(drop_query)
    mydb.commit()

    sql = '''create table if not exists comments(comment_id varchar(100) primary key,
                                                 video_id varchar(50),
                                                 comment_text text, 
                                                 comment_author varchar(150),
                                                 comment_published varchar(200))'''
    mycursor.execute(sql)
    mydb.commit()


    comments_list=[]
    db=con['youtube']
    col3=db['comment_details']
    for comment_data in col3.find({},{"_id":0}):
        for i in range(len(comment_data['comment_information'])):
            comments_list.append(comment_data['comment_information'][i])

    df3=pd.DataFrame(comments_list)


    for index,row in df3.iterrows():
        sql = '''insert into comments(comment_id ,
                                      video_id ,
                                      comment_text, 
                                      comment_author ,
                                      comment_published)
                                      
                                      values(%s,%s,%s,%s,%s)'''
        values=(row['comment_id'] ,
               row['video_id'] ,
               row['comment_text'], 
               row['comment_author'] ,
               row['comment_published'])
        
        mycursor.execute(sql,values)
        mydb.commit()

    

def show_channels_table():
    channels_list=[]
    db=con['youtube']
    col1=db['channel_details']
    for channel in col1.find({},{'_id':0}):
        channels_list.append(channel['channel_information'])
    df=st.dataframe(channels_list)   
    return df

def show_videos_table():
    videos_list=[]
    db=con['youtube']
    col2=db['video_details']
    for video_data in col2.find({},{"_id":0}):
        for i in range(len(video_data['video_information'])):
            videos_list.append(video_data['video_information'][i])
    df1=st.dataframe(videos_list)
    return df1

def show_comments_table():
    comments_list=[]
    db=con['youtube']
    col3=db['comment_details']
    for comment_data in col3.find({},{"_id":0}):
        for i in range(len(comment_data['comment_information'])):
            comments_list.append(comment_data['comment_information'][i])
    df3=st.dataframe(comments_list)
    return df3


with st.sidebar:
    st.title(":rainbow[Youtube data harvesting and warehousing]")
    st.header(":red[*KEY TAKEAWAYS*]",divider='rainbow')
    st.caption(":blue[Python Scripting]")
    st.caption(":orange[Data collection]")
    st.caption(":orange[API integration]")
    st.caption(":blue[Data management using MongoDB and SQL]")

channel_id = st.text_input("Enter channel ID")

if st.button('collect and store data'):
    ch_ids=[]
    db=con["youtube"]
    col1=db["channel_details"]
    for channel_data in col1.find({},{"_id":0}):
        ch_ids.append(channel_data['channel_information']['channel_Id'])
    
    if channel_id in ch_ids:
        st.success("Channel Details of given channel id already exists")

    else:
        insert=insert_channel_mdb(channel_id)
        st.success(insert)
        st.success("channel details inserted into mongodb succesfully")

    
if st.button("Migrate to SQL "):
        channels_table(channel_id)
        videos_table(channel_id)
        comments_table()
        st.success("tables created successfully")
show_table=st.radio('Select the table to view:',('Channels','Videos','Comments'))

if show_table=='Channels':
    show_channels_table()
elif show_table=="Videos":
    show_videos_table()
elif show_table=="Comments":
    show_comments_table()
    
#sql connection
mydb= mysql.connector.connect(host='localhost',
                                  user='root',
                                  password='@Sindhu29',
                                  database='youtube')
mycursor=mydb.cursor()
question=st.selectbox("Select your question",("1.All the videos and the channel names",
                                              "2. Channels with the most number of videos",
                                              "3. 10 most viewed videos",
                                              "4. Videos with highest likes",
                                              "5. comments in each videos",
                                              "6. likes of all videos",
                                              '7. views of each channel',
                                              "8. videos published in the year of 2022",
                                              "9. average duration of all videos in each channel",
                                              "10. videos with highest number of comments"))
if question=="1.All the videos and the channel names":
    query1='''select title as videotitle,channel_name as channelname from videos'''
    mycursor.execute(query1)
    t1=mycursor.fetchall()
    df1=pd.DataFrame(t1,columns=['video title','channel name'])
    st.write(df1)
elif question=="2. Channels with the most number of videos":     
    query2='''select channel_title as channelname,channel_videos as no_of_videos from channels order by channel_videos desc'''
    mycursor.execute(query2)
    t2=mycursor.fetchall()
    df2=pd.DataFrame(t2,columns=['channel name','no of videos'])
    st.write(df2)
elif question=="3. 10 most viewed videos":
    query3='''select views as views,channel_name as channelname,title as videotitle from videos 
               where views is not null order by views desc limit 10'''
    mycursor.execute(query3)
    t3=mycursor.fetchall()
    df3=pd.DataFrame(t3,columns=['views','channel name','videotitle'])
    st.write(df3)
elif question=="4. Videos with highest likes":
    query4='''select  title as videotitle,channel_name as channelname,likecount as likecount
                   from videos where likecount is not null order by likecount desc'''
    mycursor.execute(query4)
    t4=mycursor.fetchall()
    df4=pd.DataFrame(t4,columns=['videotitle','channel name',"likecount"])
    st.write(df4)
elif question=="5. comments in each videos":
    query5='''select comments as no_comments,title as videotitle from videos where comments is not null'''
    mycursor.execute(query5)
    t5=mycursor.fetchall()
    df5=pd.DataFrame(t5,columns=['no of comments','videotitle'])
    st.write(df5)

elif question=="6. likes of all videos": 
    query6='''select likecount as likecount,title as videotitle from videos'''
    mycursor.execute(query6)
    t6=mycursor.fetchall()
    df6=pd.DataFrame(t6,columns=["likecount",'videotitle'])
    st.write(df6)
elif question=='7. views of each channel':
    query7='''select channel_title as channelname,channel_views as totalviews from channels''' 
    mycursor.execute(query7)
    t7=mycursor.fetchall()
    df7=pd.DataFrame(t7,columns=["channel name",'totalviews'])
    st.write(df7)
elif question=="8. videos published in the year of 2022":
    query8='''select title as video_title,published_date as videorelease,channel_name as channelname from videos
              where extract(year from published_date)=2022'''
    mycursor.execute(query8)
    t8=mycursor.fetchall()
    df8=pd.DataFrame(t8,columns=["video title","video release",'channelname'])
    st.write(df8)
elif question=="9. average duration of all videos in each channel":
    query9='''select channel_name as channelname,SEC_TO_TIME(AVG(Duration)) as average_duration from videos group by channel_name'''
    mycursor.execute(query9)
    t9=mycursor.fetchall()
    df9=pd.DataFrame(t9,columns=["channelname",'averageduration'])
        
    T9=[]
    for index,row in df9.iterrows():
        channel_title=row['channelname']
        average_duration=row['averageduration']
        average_duration_str=str(average_duration)
        T9.append(dict(channeltitle=channel_title,average_duration=average_duration_str))
    df91=pd.DataFrame(T9)

    st.write(df91)

elif question=="10. videos with highest number of comments":
    query10='''select title as videotitle,channel_name as channelname,comments as comments from videos where
              comments is not null order by comments desc'''
    mycursor.execute(query10)
    t10=mycursor.fetchall()
    df10=pd.DataFrame(t10,columns=["video title","channelname",'comments'])
    st.write(df10)
