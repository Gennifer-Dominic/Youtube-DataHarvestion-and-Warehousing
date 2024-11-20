from googleapiclient.discovery import build
from mysql import connector
import streamlit as st
import pandas as pd

#api connection
def connect_api():
    api_id="AIzaSyBwp2vMzSp_ikDF_yfby3TmxwXSRj0APPM"
    api_servicename="youtube"
    api_version="v3"
    youtube=build(api_servicename,api_version,developerKey=api_id)
    return youtube

youtube=connect_api()


#getchannelinfo
def getchannelInfo(channel_id):
    request=youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response=request.execute()
    if (response['pageInfo']['totalResults']==0):
        data={}
        st.success("No valid id")
    else:
        for i in response['items']:
            data=dict(channel_name=i['snippet']['title'],
                      channel_id=i['id'],
                      channel_subcriber=i['statistics']['subscriberCount'],
                      channel_totalviews=i['statistics']['viewCount'],
                      channel_totalvideos=i['statistics']['videoCount'],
                      channel_description=i['snippet']['description'],
                      channel_playlistId=i['contentDetails']['relatedPlaylists']['uploads']
                     )
    return data



# get videos id
def getvideosid(channel_id):
    playlistids = []
    response = youtube.channels().list(id=channel_id,
                                       part='contentDetails').execute()
    playlist = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    nextpagetoken = None

    while True:
        playlistresponse = youtube.playlistItems().list(part='snippet',
                                                        playlistId=playlist,
                                                        maxResults=50, pageToken=nextpagetoken).execute()
        for i in range(len(playlistresponse['items'])):
            playlistids.append(playlistresponse['items'][i]['snippet']['resourceId']['videoId'])

        nextpagetoken = playlistresponse.get('nextPageToken')
        if nextpagetoken is None:
            break

    return playlistids


#get video info

def getvideodetails(videoidslist):
    videoDetails=[]
    for ids in videoidslist:
        request=youtube.videos().list(part='snippet,contentDetails,statistics',
                                      id=ids
                                      )
        response=request.execute()
        for i in response['items']:
            data=dict(channelname=i['snippet']['channelTitle'],
                      channelId=i['snippet']['channelId'],
                      videoid=i['id'],
                      title=i['snippet']['title'],
                      description=i['snippet']['description'],
                      publisheddate=i['snippet']['publishedAt'],
                      duration=i['contentDetails']['duration'],
                      viewscount=i['statistics']['viewCount'],
                      commentcount=i['statistics'].get('commentCount'),
                      favoriteCount=i['statistics']['likeCount'],
                      definition=i['contentDetails']['definition'],
                      captionstatus=i['contentDetails']['caption'],
                     )
            videoDetails.append(data)
    return videoDetails


# get comment info
def comment_info(videoidslist):
    commentdata_list = []
    try:
        for video_id in videoidslist:
            try:
                response = youtube.commentThreads().list(
                    part='snippet',
                    videoId=video_id,
                    textFormat='plainText',
                    maxResults=100
                ).execute()

                if 'items' not in response or not response['items']:
                    # print(f"No comments available or comments are disabled for video ID: {video_id}")
                    continue  # Skip to the next video_id

                while response:
                    for item in response['items']:
                        comment_data = {
                            'comment_id': item['snippet']['topLevelComment']['id'],
                            'video_id': item['snippet']['topLevelComment']['snippet']['videoId'],
                            'video_comment': item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            'comment_author': item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            'comment_published_date': item['snippet']['topLevelComment']['snippet']['publishedAt']
                        }
                        commentdata_list.append(comment_data)

                    # Check for the next page of comments
                    if 'nextPageToken' in response:
                        response = youtube.commentThreads().list(
                            part='snippet',
                            videoId=video_id,
                            textFormat='plainText',
                            maxResults=100,
                            pageToken=response['nextPageToken']
                        ).execute()
                    else:
                        break

            except Exception as e:
                # print(f"Failed to retrieve comments for video ID {video_id}. Reason: {e}")
                continue

    except Exception as e:
        print(f"An unexpected error occurred: {e}")

    return commentdata_list



# get playlist details

def get_playlist_details(channel_id):
    playlist_details = []
    nextpage_token = None
    while True:
        response = youtube.playlists().list(channelId=channel_id,
                                            part='contentDetails,snippet',
                                            maxResults=50,
                                            pageToken=nextpage_token
                                            ).execute()
        for playlist in response['items']:
            playlist_data = dict(playlist_id=playlist['id'],
                                 playlist_title=playlist['snippet']['title'],
                                 playlist_channelId=playlist['snippet']['channelId'],
                                 channel_name=playlist['snippet']['channelTitle'],
                                 published_date=playlist['snippet']['publishedAt'],
                                 playlist_video_count=playlist['contentDetails']['itemCount']
                                 )
            playlist_details.append(playlist_data)

        nextpage_token = response.get('nextPageToken')
        if nextpage_token is None:
            break
    return playlist_details



connection=connector.connect(host="localhost",
                             user="root",
                             password="root@123"
                             )
cursor=connection.cursor()
query= "create database if not exists You_tube"
cursor.execute(query)
query="use You_tube"
cursor.execute(query)

def channeltable():
    query="""create table if not exists channeldetails(channel_name varchar(100),
    channel_id varchar(100) primary key,
    channel_subcriber int,
    channel_totalviews int,
    channel_totalvideos int,
    channel_description text,
    channel_playlistId varchar(100))
    """
    cursor.execute(query)

def insertchannel(ch):
    ch['channel_subcriber'] = int(ch['channel_subcriber'] or 0)
    ch['channel_totalviews'] = int(ch['channel_totalviews'] or 0)
    ch['channel_totalvideos'] = int(ch['channel_totalvideos'] or 0)

    # alter the insert query
    table_name = "channeldetails"
    columns = ", ".join(ch.keys())
    placeholders = ", ".join(["%s"] * len(ch))
    sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

    # Execute the insert query
    cursor.execute(sql, tuple(ch.values()))

    # Commit the transaction
    connection.commit()

def playlisttable():
    query = """create table if not exists playlistsdetails(playlist_id varchar(100) primary key,
    playlist_title varchar(100),
    playlist_channelId varchar(100),
    channel_name varchar(100),
    published_date varchar(100),
    playlist_video_count int)
    """
    cursor.execute(query)

def insertplaylist(playlist_details):
    for i in range(len(playlist_details)):
        # playlist_details[i]['published_date'] = datetime.fromtimestamp(playlist_details[i]['published_date']).strftime('%Y-%m-%d %H:%M:%S')
        playlist_details[i]['playlist_video_count'] = int(playlist_details[i]['playlist_video_count'] or 0)
        table_name = "playlistsdetails"
        columns = ", ".join(playlist_details[i].keys())
        placeholders = ", ".join(["%s"] * len(playlist_details[i]))
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        # Execute the insert query
        cursor.execute(sql, tuple(playlist_details[i].values()))

        # Commit the transaction
        connection.commit()

def videotable():
    query = """create table if not exists videolist(channelname varchar(100),
    channelId varchar(100),
    videoid varchar(100) primary key,
    title varchar(100),
    description text,
    publisheddate varchar(100),
    duration varchar(100),
    viewscount int,
    commentcount int,
    favoriteCount int,
    definition varchar(100),
    captionstatus varchar(100)
    )
    """
    cursor.execute(query)

def videoinsert(videoinfo):
    # alter the insert query
    for i in range(len(videoinfo)):

        videoinfo[i]['viewscount'] = int(videoinfo[i]['viewscount'] or 0)
        videoinfo[i]['commentcount'] = int(videoinfo[i]['commentcount'] or 0)
        videoinfo[i]['favoriteCount'] = int(videoinfo[i]['favoriteCount'] or 0)
        table_name = "videolist"
        columns = ", ".join(videoinfo[i].keys())
        placeholders = ", ".join(["%s"] * len(videoinfo[i]))
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        # Execute the insert query
        cursor.execute(sql, tuple(videoinfo[i].values()))

        # Commit the transaction
        connection.commit()

def commenttable():
    query = """create table if not exists commentlist(comment_id varchar(100) primary key,
    video_id varchar(100),
    video_comment text,
    comment_author varchar(100),
    comment_published_date varchar(100))
    """
    cursor.execute(query)

def insertcomment(commentinfo):
    for i in range(len(commentinfo)):
        table_name = "commentlist"
        columns = ", ".join(commentinfo[i].keys())
        placeholders = ", ".join(["%s"] * len(commentinfo[i]))
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        # Execute the insert query
        cursor.execute(sql, tuple(commentinfo[i].values()))

        # Commit the transaction
        connection.commit()

#streamLit

st.markdown(f'<h1 style="text-align: center;">YouTube Data Harvesting and Warehousing</h1>',
                unsafe_allow_html=True)


def createtables():
    channeltable()
    playlisttable()
    videotable()
    commenttable()

channelId=st.text_input("Enter the Channel Id")

if(st.button("Store to Database")):
    Channeldetails = getchannelInfo(channelId)
    if(Channeldetails=={}):
        st.success("No Data to Insert")
    else:
        createtables()
        videoidslist = getvideosid(channelId)
        videoinfo = getvideodetails(videoidslist)
        commentinfo = comment_info(videoidslist)
        playlist_details = get_playlist_details(channelId)
        insertchannel(Channeldetails)
        insertplaylist(playlist_details)
        videoinsert(videoinfo)
        insertcomment(commentinfo)
        st.success("Data Collected and Stored Successfully")

st.header("Channel Details")
selectquery = ("""SELECT channel_name AS ChannelName,channel_subcriber AS Subcriber,
               channel_totalviews AS TotalViews,channel_totalvideos AS TotalVideos,
               channel_description AS Discription FROM channeldetails""")
cursor.execute(selectquery)
channeltable = cursor.fetchall()
df=pd.DataFrame(channeltable,columns=cursor.column_names)
st.dataframe(df)


question=st.selectbox("Select your Question.",("1. What are the names of all the videos and their corresponding channels?",
                                               "2.Which channels have the most number of videos, and how many videos do they have?",
                                               "3.What are the top 10 most viewed videos and their respective channels?",
                                               "4.How many comments were made on each video, and what are their corresponding video names?",
                                               "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                                               "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                               "7.What is the total number of views for each channel, and what are their corresponding channel names?",
                                               "8.What are the names of all the channels that have published videos in the year 2022",
                                               "9.What is the average duration of all videos in each channel, and what are their corresponding channel names",
                                               "10.Which videos have the highest number of comments, and what are their corresponding channel names"))

if(question=="1. What are the names of all the videos and their corresponding channels?"):
    query="select title,channelname from videolist"
    cursor.execute(query)
    q1 = cursor.fetchall()
    df = pd.DataFrame(q1, columns=cursor.column_names)
    st.dataframe(df)

elif (question == "2.Which channels have the most number of videos, and how many videos do they have?"):
    query = "select channel_name,channel_totalvideos from channeldetails order by channel_totalvideos desc"
    cursor.execute(query)
    q1 = cursor.fetchall()
    df = pd.DataFrame(q1, columns=cursor.column_names)
    st.dataframe(df)

elif (question == "3.What are the top 10 most viewed videos and their respective channels"):
    query = "select channelname,title,viewscount from videolist order by viewscount desc limit 10"
    cursor.execute(query)
    q1 = cursor.fetchall()
    df = pd.DataFrame(q1, columns=cursor.column_names)
    st.dataframe(df)

elif (question == "4.How many comments were made on each video, and what are their corresponding video names"):
    query = "select channelname,commentcount from videolist "
    cursor.execute(query)
    q1 = cursor.fetchall()
    df = pd.DataFrame(q1, columns=cursor.column_names)
    st.dataframe(df)

elif (question == "5.Which videos have the highest number of likes, and what are their corresponding channel names?"):
    query = "select channelname,favoriteCount from videolist order by favoriteCount desc"
    cursor.execute(query)
    q1 = cursor.fetchall()
    df = pd.DataFrame(q1, columns=cursor.column_names)
    st.dataframe(df)

elif (question == "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?"):
    query = "select channelname,favoriteCount from videolist"
    cursor.execute(query)
    q1 = cursor.fetchall()
    df = pd.DataFrame(q1, columns=cursor.column_names)
    st.dataframe(df)

elif (question == "7.What is the total number of views for each channel, and what are their corresponding channel names?"):
    query = "select channel_name,channel_totalviews from channeldetails order by channel_totalviews desc"
    cursor.execute(query)
    q1 = cursor.fetchall()
    df = pd.DataFrame(q1, columns=cursor.column_names)
    st.dataframe(df)

elif (question == "8.What are the names of all the channels that have published videos in the year 2022"):
    query = "select channelname,title,publisheddate from videolist where extract(year from publisheddate)=2022"
    cursor.execute(query)
    q1 = cursor.fetchall()
    df = pd.DataFrame(q1, columns=cursor.column_names)
    st.dataframe(df)

elif (question == "9.What is the average duration of all videos in each channel, and what are their corresponding channel names"):
    query = "select channelname,AVG(duration) from videolist group by channelname"
    cursor.execute(query)
    q1 = cursor.fetchall()
    df = pd.DataFrame(q1, columns=cursor.column_names)
    st.dataframe(df)

elif (question == "10.Which videos have the highest number of comments, and what are their corresponding channel names"):
    query = "select channelname,title,commentcount from videolist where commentcount is not null order by commentcount desc"
    cursor.execute(query)
    q1 = cursor.fetchall()
    df = pd.DataFrame(q1, columns=cursor.column_names)
    st.dataframe(df)