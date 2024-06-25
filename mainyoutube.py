# ==================================================       /     IMPORT LIBRARY    /      =================================================== #
import mysql.connector
import googleapiclient.discovery
import pandas as pd
import datetime
import streamlit as st


#API Access:

api_service_name = "youtube"
api_version = "v3"
api_key="AIzaSyAERiC8c4NMIiT5MlUcrGXuPbOR3_Nim9w"
youtube = googleapiclient.discovery.build(api_service_name, api_version,developerKey=api_key)


#Streamlit page creation:

st.set_page_config(layout="wide")
st.title(":green[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
channel_id=st.text_input("***Enter the channel ID***")
ins=st.button("Insert")
#if channel_id and ins:
#     st.write(channel_data(channel_id))
     


# Define a function to retrieve channel Data from channel List

def channel_data(ch_id):
    request = youtube.channels().list(
    part="snippet,contentDetails,statistics",
    id=ch_id)
    response = request.execute()
       
    data = {
        'channel_id':ch_id,
        'channel_name':response['items'][0]['snippet']['title'],
        'channel_des':response['items'][0]['snippet']['description'],
        'channel_pid':response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
        'channel_pat':response['items'][0]['snippet']['publishedAt'][:-1],
        'channel_videoc':response['items'][0]['statistics']['videoCount'],
        'channel_viewc':response['items'][0]['statistics']['viewCount'],
        'channel_subs':response['items'][0]['statistics']['subscriberCount']
    }
    return data
    #ch_dt = channel_data(channel_id)
    #df = pd.DataFrame(ch_dt)
    #df = df.drop_duplicates(subset=['channel_id'])
    #st.write("This channel name already exists:")
    #st.write(df)
    
if channel_id and ins:
     st.write(channel_data(channel_id))


#if channel_id:
#    st.write("dataframe after removing duplicates:")
#    st.write(df)
#    st.write(channel_data(channel_id))


# Define a function to retrieve video IDs from channel playlist

def get_video_ids(ch_id):
    request = youtube.channels().list(part="contentDetails",id=ch_id)
    response = request.execute()
    if 'items' not in response or not response['items']:
        print("No items found in response.")
        return []
    
    try:
        chan_pid = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    except KeyError as e:
        print(f"KeyError: {e}")
        return []
    video_id=[]
    next_page_token = None
    while True:
        responsev=youtube.playlistItems().list(part="snippet",playlistId=chan_pid,maxResults=50,pageToken=next_page_token).execute()
        for v in range(len(responsev['items'])):
            video_id.append(responsev['items'][v]['snippet']['resourceId']['videoId'])
            next_page_token = responsev.get('nextpageToken') 
        if  next_page_token is None:
            break
    return video_id


#storing video_ids in a variable
A = get_video_ids(channel_id)


# Define a function to convert duration

import re
def convert_duration(Duration):
            regex = r'PT(\d+H)?(\d+M)?(\d+S)?'
            match = re.match(regex, Duration)
            if not match:
                return '00:00:00'
            hours, minutes, seconds = match.groups()
            hours = int(hours[:-1]) if hours else 0
            minutes = int(minutes[:-1]) if minutes else 0
            seconds = int(seconds[:-1]) if seconds else 0
            total_seconds = hours * 3600 + minutes * 60 + seconds
            return '{:02d}:{:02d}:{:02d}'.format(int(total_seconds / 3600), int((total_seconds % 3600) / 60),
                                                 int(total_seconds % 60))


# Get video information

def get_video_info(A):
    video_data=[]
    #for video_id in video_ids:
    for  video_id in A:
        request=youtube.videos().list(
            part='snippet,contentDetails,statistics',
            id=video_id
         )
        response=request.execute()
        #print(response)

        for item in response["items"]:
            data=dict(
                    video_id=item['id'],
                    channel_id=item['snippet']['channelId'],                
                    video_name=item['snippet']['title'],
                    video_description=item['snippet'].get('description'),
                    published_date=item['snippet']['publishedAt'][:-1],
                    view_count=item['statistics'].get('viewCount'),
                    like_count=item['statistics'].get('likecount'),
                    comment_count=item['statistics'].get('commentCount'),
                    favourite_count=item['statistics']['favoriteCount'],
                    duration=convert_duration(item['contentDetails']['duration']),
                    caption_status=item['contentDetails']['caption'],
                    )
                               
            video_data.append(data)
    return video_data



# Get comment information
def get_comment_info(A):
    comment_data = []
   
    for video_id in A:
        try:
            request = youtube.commentThreads().list(
                part = "snippet",
                videoId = video_id,
                maxResults = 50
            )
            response = request.execute()

            for item in response['items']:
                data = dict(Comment_Id = item['snippet']['topLevelComment']['id'],
                            Video_Id = item ['snippet']['topLevelComment']['snippet']['videoId'],
                            Comment_Text = item ['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_Author = item ['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_Published = item ['snippet']['topLevelComment']['snippet']['publishedAt'][:-1])
                comment_data.append(data)
        except:
            pass
    return comment_data

data1 = get_comment_info(A)


# Mysql Connection code:
import mysql.connector

mydb = mysql.connector.connect(
    host='localhost',
    user='root',
    password='12345678',
    port='3307',
    database='youtube'
)
mycursor = mydb.cursor()
query = """
            use youtube
        """
mycursor.execute(query)
for data in mycursor:
    print(data)
    #print(mydb)
mycursor = mydb.cursor(buffered=True)


#channel table creation and insertion
def create_table1():
     try:
        query = """create table if not exists channel(channel_id varchar(255),channel_name varchar(255),channel_des TEXT,channel_pid varchar(255),
                                channel_pat DATETIME,channel_videoc INT,channel_viewc INT,
                                channel_subs INT, PRIMARY KEY(channel_id))"""
        mycursor.execute(query)
        mydb.commit()
     
     except:
           print("channel table already created")   


def channel_insert(B):   
    try:
         
        sql = '''INSERT INTO channel(channel_id,
                                channel_name,
                                channel_des,
                                channel_pid,
                                channel_pat,
                                channel_videoc,
                                channel_viewc,
                                channel_subs
                                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)'''
            
        mycursor.execute(sql, tuple(B.values()))

        mydb.commit()

        st.success('Data inserted successfully')
        #print(mycursor.rowcount, "record inserted.")
    except:
         st.warning('channel_id already exist in SQL')

# video table creation and insertion 

def create_table2():
     try:
        query = """create table if not exists videos(video_id varchar(255),channel_id varchar(255),video_name varchar(255),video_description TEXT,published_date DATETIME,
                              view_count INT,like_count INT,comment_count INT,favourite_count INT,duration TIME,
                              caption_status varchar(255))"""
                              
        mycursor.execute(query)
        mydb.commit()
     
     except:
           print("videos table already created")  

data = get_video_info(A)
vi_ins = pd.DataFrame(data) 

def video_insert(vi_ins):
        try:    
            for index,row in vi_ins.iterrows():
                sql = '''INSERT INTO videos(video_id,
                                    channel_id,
                                    video_name,
                                    video_description,
                                    published_date,  
                                    view_count, 
                                    like_count, 
                                    comment_count, 
                                    favourite_count,
                                    duration,                                   
                                    caption_status                          
                                     ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
            val = (row["video_id"],
               row["channel_id"],
                row["video_name"],
                row["video_description"],
                row["published_date"],
                row["view_count"],
                row["like_count"],
                row["comment_count"],
                row["favourite_count"],
                row["duration"],               
                row["caption_status"])
        #print(val)
                           
            mycursor.execute(sql, val)
            mydb.commit()
            #st.success('Video information inserted successfully')
            print(mycursor.rowcount, "record inserted.")
        except:
            st.warning('video_id already exist in SQL')

        
# comment table creation and insertion

def create_table3():
    try:
        query ="""create table if not exists comments(comment_id varchar(255),video_id varchar(255),comment_text TEXT,comment_author varchar(255),
                                comment_published_date DATETIME, PRIMARY KEY(comment_id))"""
        mycursor.execute(query)
        mydb.commit()
    except:
        print("comment table already created")    



def comment_insert(cm_ins):
        try:
            for index,row in cm_ins.iterrows():
                sql = '''INSERT INTO comments (comment_id,
                                    video_id,
                                    comment_text, 
                                    comment_author,  
                                    comment_published_date                                
                                     ) VALUES (%s,%s,%s,%s,%s)'''
                val = (row["Comment_Id"],
                row["Video_Id"],
                row["Comment_Text"],
                row["Comment_Author"],
                row["Comment_Published"])
                           
            mycursor.execute(sql, val)
            mydb.commit()
            #st.success('Comment information inserted successfully')
            print(mycursor.rowcount, "record inserted.")
        except:
            st.warning('comment_id already exist in SQL')



#To convert into streamlit:

def view_channel_table():
    mycursor.execute("SELECT * FROM channel")

    result=mycursor.fetchall()

    channel_st=st.write(pd.DataFrame(result,columns=["channel_id",
                               "channel_name",
                               "channel_des",
                               "channel_pid",
                               "channel_pat",
                               "channel_videoc",
                               "channel_viewc",
                               "channel_subs"
                               ]))

    return channel_st


def view_videos_table():
    mycursor.execute("SELECT * FROM videos")

    result=mycursor.fetchall()

    videos_st=st.write(pd.DataFrame(result,columns=[ "video_id",
                            "channel_id",
                            "video_name",
                            "video_description",
                            "published_date",
                            "view_count",
                            "like_count",
                            "comment_count",
                            "favourite_count",
                            "duration",               
                            "caption_status"]))
    return videos_st


def view_comment_table():
    mycursor.execute("SELECT * FROM comment")

    result=mycursor.fetchall()

    comment_st=st.write(pd.DataFrame(result,columns=["comment_id",
                                  "video_id",
                                  "comment_text",
                                  "comment_author",
                                  "comment_published_date"]))

    return comment_st


 #To show data in streamlit:

if channel_id and ins:
    #B=channel_data(channel_id)
    #st.success("Data Inserted Successfully")
#elif(channel_id == channel_data(channel_id) and ins):
#     st.warning("channel_Id already exist")
# else:
    B=channel_data(channel_id)
    all_Video_Id=get_video_info(channel_id)
    data = get_video_info(A)
    vi_ins = pd.DataFrame(data)
    Comment_Details=get_comment_info(all_Video_Id)
    data = get_comment_info(A)
    cm_ins = pd.DataFrame(data1)
    video_Details=get_video_info(all_Video_Id)
    create_table1()
    channel_insert(B)
    create_table2()
    video_insert(vi_ins)
    create_table3()
    comment_insert(cm_ins)


# To covert into streamlit
def view_channels_table():
    mycursor.execute("SELECT * FROM channel")

    result=mycursor.fetchall()

    channel_st=st.write(pd.DataFrame(result,columns=["channel_id",
                                "channel_name",
                               "channel_des",
                               "channel_pid",
                               "channel_pat",
                               "channel_videoc",
                               "Channel_viewc",
                               "channel_subs"]))

    return channel_st

def view_videos_table():
    mycursor.execute("SELECT * FROM videos")

    result=mycursor.fetchall()

    videos_st=st.write(pd.DataFrame(result,columns=["video_id",
                                                "channel_iD",
                                                "Video_name",
                                                "Video_description",
                                                "published_date",
                                                "View_count",
                                                "like_count",
                                                "comment_count",
                                                "favourite_count",
                                                "duration",
                                                "caption_status"]))

    return videos_st

def view_comments_table():
    mycursor.execute("SELECT * FROM comments")

    result=mycursor.fetchall()

    comments_st=st.write(pd.DataFrame(result,columns=["Comment_Id",
                                  "video_id",
                                  "Comment_Text",
                                  "Comment_Author",
                                  "Comment_Published"]))

    return comments_st


#To choose the Tables in streamlit

with st.sidebar:
    show_table=st.radio("***SELECT TABLE FOR VIEW***",(":rainbow[CHANNELS]",":rainbow[VIDEOS:movie_camera:]",":rainbow[COMMENTS]"))
        
    if show_table==":rainbow[CHANNELS]":
        view_channels_table()

    elif show_table==":rainbow[VIDEOS:movie_camera:]":
        view_videos_table()

    elif show_table==":rainbow[COMMENTS]":
        view_comments_table()


# Selectbox creation

question_tosql = st.selectbox('*Select your Question*',
                                  ('1. What are the names of all the videos and their corresponding channels?',
                                   '2. Which channels have the most number of videos, and how many videos do they have?',
                                   '3. What are the top 10 most viewed videos and their respective channels?',
                                   '4. How many comments were made on each video, and what are their corresponding video names?',
                                   '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
                                   '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                                   '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                                   '8. What are the names of all the channels that have published videos in the year 2022?',
                                   '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                                   '10. Which videos have the highest number of comments, and what are their corresponding channel names?'),
                                  key='collection_question')




 # Create a connection to SQL

connect_for_question = mysql.connector.connect(host='localhost', user='root',port=3307 , password='12345678', db='youtube')
cursor = connect_for_question.cursor()
# Q1
if question_tosql == '1. What are the names of all the videos and their corresponding channels?':
        cursor.execute(
            "SELECT distinct channel_name, video_name FROM channel,videos where channel.channel_id = videos.channel_id")
        df1 = pd.DataFrame(cursor.fetchall(), columns=['Channel Name', 'Video Name'])
        st.dataframe(df1)
  
# Q2
elif question_tosql == '2. Which channels have the most number of videos, and how many videos do they have?':
        cursor.execute("SELECT distinct channel_name, channel_videoc FROM channel ORDER BY channel_videoc DESC;")
        df2 = pd.DataFrame(cursor.fetchall(), columns=['Channel Name', 'Video Count'])
        st.dataframe(df2)

# Q3
elif question_tosql == '3. What are the top 10 most viewed videos and their respective channels?':
        cursor.execute(
            "SELECT distinct channel_name, video_name, view_count FROM channel, videos where channel.channel_id  = videos.channel_id ORDER BY view_count DESC LIMIT 10;")
        df3 = pd.DataFrame(cursor.fetchall(), columns=['Channel Name', 'Video Name', 'View count'])
        st.dataframe(df3)
    
# Q4
elif question_tosql == '4. How many comments were made on each video, and what are their corresponding video names?':
        cursor.execute("SELECT distinct video_name, comment_count FROM videos;")
        df4 = pd.DataFrame(cursor.fetchall(), columns=['Video Name', 'Comment count'])
        st.dataframe(df4)
    
# Q5
elif question_tosql == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        cursor.execute(
            "SELECT distinct channel_name, video_name, like_count FROM channel, videos where channel.channel_id = videos.channel_id ORDER BY like_count DESC;")
        df5 = pd.DataFrame(cursor.fetchall(), columns=['Channel Name', 'Video Name', 'Like count'])
        st.dataframe(df5)
 
# Q6
elif question_tosql == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        st.write('*Note:- In November 2021, YouTube removed the public dislike count from all of its videos.*')
        cursor.execute(
            "SELECT distinct channel_name, video_name, like_count FROM channel , videos where channel.channel_id = videos.channel_id ORDER BY like_count DESC;")
        df6 = pd.DataFrame(cursor.fetchall(), columns=['Channel Name', 'Video Name', 'Like count', 'Dislike count'])
        st.dataframe(df6)
    
# Q7
elif question_tosql == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        cursor.execute("SELECT distinct channel_name, channel_viewc FROM channel ORDER BY channel_viewc DESC;")
        df7 = pd.DataFrame(cursor.fetchall(), columns=['Channel Name', 'Total number of views'])
        st.dataframe(df7)
    
    
# Q8
elif question_tosql == '8. What are the names of all the channels that have published videos in the year 2022?':
        cursor.execute(
            "SELECT distinct channel_name, video_name, published_date FROM channel, videos where channel.channel_id = videos.channel_id  AND EXTRACT(YEAR FROM published_date) = 2022;")
        result_8 = cursor.fetchall()
        df8 = pd.DataFrame(result_8, columns=['Channel Name', 'Video Name', 'Year 2022 only']).reset_index(drop=True)
        df8.index += 1
        st.dataframe(df8)
    
# Q9
elif question_tosql == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        cursor.execute(
            "SELECT distinct channel_name,TIME_FORMAT(SEC_TO_TIME(AVG(TIME_TO_SEC(TIME(Duration)))), '%H:%i:%s') AS duration  FROM channel, videos where channel.channel_id = videos.channel_id GROUP by channel_name ORDER BY duration DESC;")
        df9 = pd.DataFrame(cursor.fetchall(), columns=['Channel Name', 'Average duration of videos (HH:MM:SS)'])
        st.dataframe(df9)
 
# Q10
elif question_tosql == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        cursor.execute(
            "SELECT distinct channel_name, video_name, comment_count FROM channel, videos where channel.channel_id = videos.channel_id ORDER BY comment_count DESC;")
        df10 = pd.DataFrame(cursor.fetchall(), columns=['Channel Name', 'Video Name', 'Number of comments'])
        st.dataframe(df10)

# SQL DB connection close
connect_for_question.close()
