import streamlit as st
from mysql import connector
import pandas as pd

#database connection
connection=connector.connect(host="localhost",
                             user="root",
                             password="root@123",
                             database="Youtube"
                             )

cursor=connection.cursor()


st.title('Connect Mysql')

if(st.button("Channel table")):
    selectquery = ("select * from channeldetails")
    cursor.execute(selectquery)
    channeltable = cursor.fetchall()
    df=pd.DataFrame(channeltable,columns=cursor.column_names)
    st.dataframe(df)

if(st.button("Playlist table")):
    selectquery = ("select * from playlistsdetails")
    cursor.execute(selectquery)
    playlist = cursor.fetchall()
    df=pd.DataFrame(playlist,columns=cursor.column_names)
    st.dataframe(df)

if(st.button("Video List table")):
    selectquery = ("select * from videolist")
    cursor.execute(selectquery)
    videolist = cursor.fetchall()
    df=pd.DataFrame(videolist,columns=cursor.column_names)
    st.dataframe(df)

if(st.button("Comment List table")):
    selectquery = ("select * from commentlist")
    cursor.execute(selectquery)
    commentlist = cursor.fetchall()
    df=pd.DataFrame(commentlist,columns=cursor.column_names)
    st.dataframe(df)

question=st.selectbox("Select your Question.",("1. All the videos and the channel names.",
                                               "2.Channels with most number of videos.",
                                               "3.10 Most viewed videos.",
                                               "4.Comments in each videos.",
                                               "5.Videos with highest likes.",
                                               "6.Likes of all videos.",
                                               "7.Views of each channels.",
                                               "8.Videos published in the year of 2022.",
                                               "9.average duration of all videos in each channel.",
                                               "10.Videos with highest number of comments."))

if(question=="1. All the videos and the channel names."):
    query="select title,channelname from video_list"
    cursor.execute(query)
    q1 = cursor.fetchall()
    df = pd.DataFrame(q1, columns=cursor.column_names)
    st.dataframe(df)

elif (question == "2.Channels with most number of videos."):
    query = "select channel_name,channel_totalvideos from channeldetails order by channel_totalvideos desc"
    cursor.execute(query)
    q1 = cursor.fetchall()
    df = pd.DataFrame(q1, columns=cursor.column_names)
    st.dataframe(df)

elif (question == "3.10 Most viewed videos."):
    query = "select channelname,viewscount from videolist order by viewscount desc"
    cursor.execute(query)
    q1 = cursor.fetchall()
    df = pd.DataFrame(q1, columns=cursor.column_names)
    st.dataframe(df)

elif (question == "3.10 Most viewed videos."):
    query = "select channelname,title,viewscount from videolist order by viewscount desc limit 10"
    cursor.execute(query)
    q1 = cursor.fetchall()
    df = pd.DataFrame(q1, columns=cursor.column_names)
    st.dataframe(df)

elif (question == "4.Comments in each videos."):
    query = "select channelname,commentcount from videolist "
    cursor.execute(query)
    q1 = cursor.fetchall()
    df = pd.DataFrame(q1, columns=cursor.column_names)
    st.dataframe(df)

elif (question == "5.Videos with highest likes."):
    query = "select channelname,favoriteCount from videolist order by favoriteCount desc"
    cursor.execute(query)
    q1 = cursor.fetchall()
    df = pd.DataFrame(q1, columns=cursor.column_names)
    st.dataframe(df)

elif (question == "6.Likes of all videos."):
    query = "select channelname,viewscount from videolist"
    cursor.execute(query)
    q1 = cursor.fetchall()
    df = pd.DataFrame(q1, columns=cursor.column_names)
    st.dataframe(df)

elif (question == "7.Views of each channels."):
    query = "select channelname,channel_totalviews from channeldetails order by channel_totalviews desc"
    cursor.execute(query)
    q1 = cursor.fetchall()
    df = pd.DataFrame(q1, columns=cursor.column_names)
    st.dataframe(df)

elif (question == "8.Videos published in the year of 2022."):
    query = "select channelname,viewscount from videolist order by viewscount desc"
    cursor.execute(query)
    q1 = cursor.fetchall()
    df = pd.DataFrame(q1, columns=cursor.column_names)
    st.dataframe(df)

elif (question == "9.average duration of all videos in each channel."):
    query = "select channelname,viewscount from videolist order by viewscount desc"
    cursor.execute(query)
    q1 = cursor.fetchall()
    df = pd.DataFrame(q1, columns=cursor.column_names)
    st.dataframe(df)

elif (question == "10.Videos with highest number of comments."):
    query = "select channelname,viewscount from videolist order by viewscount desc"
    cursor.execute(query)
    q1 = cursor.fetchall()
    df = pd.DataFrame(q1, columns=cursor.column_names)
    st.dataframe(df)