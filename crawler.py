# -*- coding: utf-8 -*-
from __future__ import print_function
import twitter
from twitter.stream import TwitterStream
import codecs
import csv
import datetime
import shutil
import mysqlconnector
#自分のツイッターから下の四つのキーを取ってきて入力してください
CONSUMER_KEY="***"
CONSUMER_SECRET="*************"
ACCESS_TOKEN_KEY="************"
ACCESS_TOKEN_SECRET="*************************"
AUTH=twitter.OAuth(ACCESS_TOKEN_KEY,ACCESS_TOKEN_SECRET,CONSUMER_KEY,CONSUMER_SECRET)
api = twitter.Twitter(auth=AUTH,retry=True)


#global変数のみなさん
userlist=set()
arunokacontrol=[]


def WashContext(context):
    cash=''

    #絵文字を取り除く
    for m in context:
        if ord(m) < 65536:
            cash += m

    #@なんたらを取り除く
    i=0
    if len(cash)>0:
        while cash[i]=='@':
            while cash[i]!=' ':
                i=i+1
                if i==len(cash)-1:
                    i=i-1
                    break
            i=i+1
            if i==len(cash)-1:
                break

        if cash[i]=='\n':
            i=i+1
        cash=cash[i:]

    #http以降を取り除くハッシュタグも取り除くついでにRTも
    j=0
    while j<len(cash):
        if cash[j]==' ' or cash[j]=='\n':
            if j+1<len(cash):
                if cash[j+1]=='#':
                    break
        if cash[j]=='h':
            if j+3<len(cash):
                if cash[j+1]=='t' and cash[j+2]=='t' and cash[j+3]=='p':
                    break
        if cash[j]=='R':
            if j+2<len(cash):
                if cash[j+1]=='T':
                    if cash[j+2]==' ' or cash[j+2]=='\n':
                        break
        j=j+1
    cash=cash[:j]

    cash=cash.replace('\\','\\\\')
    cash=cash.replace('"','""')

    return cash

"""
あるツイートがどのツイートに対しての返事のidをもとに、対話をたどる関数
"""
def maketaiwa( id ):
    taiwa = []

    try:
        tweet = api.statuses.show( _id=id )
    except:
        print("GetStatus失敗")
        return taiwa

    zokusei = ( tweet["id"] ,  tweet["in_reply_to_status_id"] , tweet["user"]["id"] , tweet["created_at"] , tweet["text"] )


    if tweet["in_reply_to_status_id"] == None:
        taiwa.append( zokusei )
        print("最初の文をクロールしました")

    else:
        taiwa = maketaiwa( tweet["in_reply_to_status_id"] )
        print("1文クロールしました")
        taiwa.append( zokusei )
    return taiwa


def ReadTimeLine(userid,nexttweet=None):
    global arunokacontrol

    try:
        if nexttweet is None:
            users = api.statuses.user_timeline(user_id=userid,count=200)
        else:
            users = api.statuses.user_timeline(user_id=userid,max_id=nexttweet,count=200)
    except:
        print("GetUserTimeline失敗")
        return -1
    if len(users)==0:
        print("UserTimelineは空っぽ？")
        return -1
    if len(users) == 200:
        nexttweet=users[199]["id"]
        users.pop(199)
    else:
        nexttweet=-1
    taiwa=[]
    arunokaflag=0
    with codecs.open("rawdataEND.csv","a","utf-8") as f:
        csvWriter=csv.writer(f)
        for tweet in users:
            if tweet["in_reply_to_status_id"] is not None:
                #同じ会話をとらないようにするフラグ
                arunokaflag=0
                if tweet["id"] in arunokacontrol:
                    arunokaflag=1
                if arunokaflag==0:
                    taiwa=maketaiwa(tweet["in_reply_to_status_id"])
                    zokusei=( tweet["id"] ,  tweet["in_reply_to_status_id"] , tweet["user"]["id"] , tweet["created_at"] , tweet["text"] )
                    print("最後の文をクロールしました")
                    taiwa.append(zokusei)

                    #mysqlへコネクト
                    connect=mysqlconnector.connect(user='root',password='pswd',host='localhost',port=8888,database='database_name',charset='utf8')
                    print("sqlにつなげました")
                    cursor=connect.cursor()
                    cursor.execute('select max(conversation_id) from tc_rawdata')
                    maxcon_id=cursor.fetchone()
                    con_id=int(maxcon_id[0])+1

                    for i in taiwa:
                        if i[0] not in arunokacontrol and i[2]==userid:
                            arunokacontrol.append(i[0])
                        
                        if i[1] is None:
                            repry_id=-1
                        else:
                            repry_id=int(i[1])
                        content=i[4]
                        if repry_id==-1:
                            if content[0]=='@' or 'RT ' in content or 'RT\n' in content:
                                repry_id=0

                        content=WashContext(content)

                        cursor.execute('insert into tc_rawdata values('+str(con_id)+','+str(i[0])+','+str(repry_id)+','+str(i[2])+',"'+str(i[3])+'","'+content+'")')
                        print("sqlに１文入れました")

                    #これがないと結果を入れてくれない
                    connect.commit()
                    print("結果をsqlに入れました")
                    #データベースから切断
                    cursor.close()
                    connect.close()
                    print("sqlを切断しました")
                    zokusei=(" "," "," "," "," ")
                    taiwa.append(zokusei)
                    csvWriter.writerows(taiwa)

    f.close()

    return nexttweet


"""
あるユーザのタイムラインをよんで、そこから対話を探す関数
csvファイルで対話を記録してくれる。対話の終わりには空白を入れてくれる
"""
def ReadUserTimeLine(userid):
    global arunokacontrol
    arunokacontrol=[]
    nexttweet=ReadTimeLine(userid)
    while nexttweet!=-1:
        print("ReadTimelime開始")
        nexttweet=ReadTimeLine(userid,nexttweet)



"""
どのユーザのタイムラインをクロールするかを決定する。
"""
def DecideUserID( userid ):
    global userlist
    friend = []
    friendfriend = []
    nextuserid = -1


#ツイッターのストリームからuseridを取る。ランダムにとるため
    try:
        streams = TwitterStream(auth=AUTH)
        lis = streams.statuses.sample()
        cnt = 0
        userIDs = []

        for tweet in lis:

            # stop after getting 100 tweets. You can adjust this to any number
            if cnt == 100:
                break;

            cnt += 1
            if 'user' in tweet:
                userIDs.append(tweet['user']['id'])


        userIDs = list(set(userIDs))    # To remove any duplicated user IDs

        for i in userIDs:
            if i in userlist:
                print("すでにクロールしたユーザです")
            else:
                try:
                    lang = api.users.show( user_id=i )

                    #botが名前に入っているかどうか判定する
                    botflag=0
                    for char in lang["screen_name"]:
                        if char=="b":
                            botflag=1
                        elif botflag==1 and char=="o":
                            botflag=2
                        elif botflag==2 and char=="t":
                            botflag=3
                            break
                        else:
                            botflag=0

                    if botflag==3:
                        print("botをみつけたよ")
                    else:
                        if lang["lang"] == "ja":
                            print("ランダムにユーザを決めたよ")
                            return i

                except:
                    print("言語判定のGetUser失敗")
    except:
        print("GetStream失敗")
#ランダムに取れなかったら友達の友達からとる
    try:
        friend = api.friends.ids( user_id=userid,count = 10 )
    except:
        print("GetFriend失敗")
        return -1

    i = len( friend )

    while i != 0:
        try:
            friendfriend = api.friends.ids( user_id=friend[ i - 1 ] , count = 100 )
    
            for j in range( len( friendfriend ) - 1 , -1 , -1 ):
                if friendfriend[j] in userlist:
                    print("すでにクロールしたユーザです")
                else:
                    try:
                        lang = api.users.show( user_id=friendfriend[ j ] )
                    
                        if lang["lang"] == "ja":
                            print("友達の友達を選んだよ")
                            return friendfriend[ j ]
                 
                    except:
                        print("GetUser失敗")

        except:
            print("GetFriend失敗")

        i = i-1

    return nextuserid

"""
こっから↓がメイン関数
"""
data=""

with open("userdata.csv","r") as r:
    data = r.read()
    data = data.split(",")
    for i in data:
        userlist.add(i)


while True:
    b=0
    userid=0
    for id in userlist:
        userid=DecideUserID(id)
        if userid!=-1:
            b=1
            break
    if b==0:
        print("ユーザの取得に失敗しました")
        exit()
    if userid!=-1:
        userlist.add(userid)
        with open("userdata.csv","w") as w:
            csvWriter=csv.writer(w)
            csvWriter.writerow(userlist)
        ReadUserTimeLine(userid)
    print("終了しました")

