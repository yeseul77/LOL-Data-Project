import requests
import pandas as pd
import pymysql
import random
from tqdm import tqdm
import time

api_key = 'RGAPI-3a4da499-c1da-4432-ade4-5770b7db0090'
seoul_api_key = '73734d4770796f6f36354474436765'


def get_df(url):
    url_re = url.replace('(인증키)', seoul_api_key).replace('xml', 'json').replace('/5/', '/1000/')
    res = requests.get(url_re).json()
    key = list(res.keys())[0]
    df = pd.DataFrame(res[key]['row'])
    return df


def connect_mysql(db='mydb'):
    conn = pymysql.connect(host='localhost', port=3306,
                           user='icia', password='1234',
                           db=db, charset='utf8')
    return conn


def sql_execute(conn, query):
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    return result


def sql_execute_dict(conn, query):
    cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
    cursor.execute(query)
    result = cursor.fetchall()
    return result


def get_puuid(nickname, tag):
    url = f'https://asia.api.riotgames.com/riot/account/v1/accounts/by-riot-id/{nickname}/{tag}?api_key={api_key}'
    res = requests.get(url).json()
    puuid = res['puuid']
    return puuid


def get_match_id(puuid, num):
    url = f'https://asia.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids?type=ranked&start=0&count={num}&api_key={api_key}'
    match_list = requests.get(url).json()
    return match_list


def get_matches_timelines(matchid):
    url1 = f'https://asia.api.riotgames.com/lol/match/v5/matches/{matchid}?api_key={api_key}'
    url2 = f'https://asia.api.riotgames.com/lol/match/v5/matches/{matchid}/timeline?api_key={api_key}'
    matches = requests.get(url1).json()
    timelines = requests.get(url2).json()
    return matches, timelines


def get_rawdata(tier):
    division_list = ['I', 'II', 'III', 'IV']
    lst = []
    page = random.randrange(1, 20)
    print('get summonerId....')

    for division in tqdm(division_list):
        url = f'https://kr.api.riotgames.com/lol/league/v4/entries/RANKED_SOLO_5x5/{tier}/{division}?page={page}&api_key={api_key}'
        res = requests.get(url).json()
        lst += random.sample(res, 3)
    # lst라는 변수에서 summonerId만 리스트에 담기
    summoner_id_list = list(map(lambda x: x['summonerId'], lst))
    # summonerId가 담긴 리스트를 통해 puuId
    print('get puuId.....')
    puu_id_list = []
    for summoner_id in tqdm(summoner_id_list):
        url = f'https://kr.api.riotgames.com/lol/summoner/v4/summoners/{summoner_id}?api_key={api_key}'
        res = requests.get(url).json()
        puu_id = res['puuid']
        puu_id_list.append(puu_id)

    print('get match_id....')
    match_id_list = []
    # puuId를 통해 matchId를 가져오기 -> 3개씩 담기
    for puu_id in tqdm(puu_id_list):
        match_ids = get_match_id(puu_id, 3)
        match_id_list.extend(match_ids)
    print('get matches & timeline....')
    df_create = []
    for match_id in tqdm(match_id_list):
        matches, timelines = get_matches_timelines(match_id)
        df_create.append([match_id, matches, timelines])
    # matches,timeline을 불러서 이중리스트를 만들고 데이터프레임으로 만들어서 - [match_id,matches,timelines]
    df = pd.DataFrame(df_create, columns=['match_id', 'matches', 'timelines'])
    return df

def sql_execute(conn, query):
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    return result


def sql_execute_dict(conn, query):
    cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
    cursor.execute(query)
    result = cursor.fetchall()
    return result


def get_puuid(nickname, tag):
    url = f'https://asia.api.riotgames.com/riot/account/v1/accounts/by-riot-id/{nickname}/{tag}?api_key={api_key}'
    res = requests.get(url).json()
    puuid = res['puuid']
    return puuid


def get_match_id(puuid,num):
    url = f'https://asia.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids?type=ranked&start=0&count={num}&api_key={api_key}'
    match_list = requests.get(url).json()
    return match_list


def get_matches_timelines(matchid):
    url1 = f'https://asia.api.riotgames.com/lol/match/v5/matches/{matchid}?api_key={api_key}'
    url2 = f'https://asia.api.riotgames.com/lol/match/v5/matches/{matchid}/timeline?api_key={api_key}'
    matches = requests.get(url1).json()
    timelines = requests.get(url2).json()
    return matches, timelines

def get_rawdata(tier):
    division_list = ['I','II','III','IV']
    lst = []
    page = random.randrange(1,20)
    print('get summonerId....')
    
    for division in tqdm(division_list):
        url = f'https://kr.api.riotgames.com/lol/league/v4/entries/RANKED_SOLO_5x5/{tier}/{division}?page={page}&api_key={api_key}'
        res = requests.get(url).json()
        lst += random.sample(res,3)
    # lst라는 변수에서 summonerId만 리스트에 담기
    summoner_id_list = list(map(lambda x:x['summonerId'] ,lst))
    # summonerId가 담긴 리스트를 통해 puuId
    print('get puuId.....')
    puu_id_list = []
    for summoner_id in tqdm(summoner_id_list):
        url = f'https://kr.api.riotgames.com/lol/summoner/v4/summoners/{summoner_id}?api_key={api_key}'
        res = requests.get(url).json()
        puu_id = res['puuid']
        puu_id_list.append(puu_id)
    
    print('get match_id....')
    match_id_list = []
    #puuId를 통해 matchId를 가져오기 -> 3개씩 담기
    for puu_id in tqdm(puu_id_list):
        match_ids = get_match_id(puu_id,3)
        match_id_list.extend(match_ids)
    print('get matches & timeline....')
    df_create = []
    for match_id in tqdm(match_id_list):
        matches,timelines = get_matches_timelines(match_id)
        df_create.append([match_id,matches,timelines])
    #matches,timeline을 불러서 이중리스트를 만들고 데이터프레임으로 만들어서 - [match_id,matches,timelines]
    df =pd.DataFrame(df_create,columns = ['match_id','matches','timelines'])
    return df
    
def get_match_timeline_df(df):
    # df를 한개로 만들기
    df_creater = []
    print('소환사 스텟 생성중.....')
    for i in tqdm(range(len(df))):       
        # matches 관련된 데이터 
        try:
            if df.iloc[i].matches['info']['gameDuration'] > 900:   # 게임시간이 15분이 안넘을경우에는 패스하기
                for j in range(10):
                    tmp = []
                    tmp.append(df.iloc[i].match_id)
                    tmp.append(df.iloc[i].matches['info']['gameDuration'])
                    tmp.append(df.iloc[i].matches['info']['gameVersion'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['summonerName'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['summonerLevel'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['participantId'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['championName'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['champExperience'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['teamPosition'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['teamId'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['win'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['kills'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['deaths'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['assists'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['totalDamageDealtToChampions'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['totalDamageTaken'])


            #timeline 관련된 데이터
                    for k in range(5,26):
                        try:
                            tmp.append(df.iloc[i].timelines['info']['frames'][k]['participantFrames'][str(j+1)]['totalGold'])
                        except:
                            tmp.append(0)
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['item0'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['item1'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['item2'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['item3'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['item4'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['item5'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['item6'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['summoner1Id'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['summoner2Id'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['turretKills'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['firstTowerKill'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['totalMinionsKilled'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['pentaKills'])

                    if j < 5:
                        tmp.append(df.iloc[i].matches['info']['teams'][0]['bans'][j]['championId'])
                        tmp.append(df.iloc[i].matches['info']['teams'][0]['bans'][j]['pickTurn'])
                    else:
                        tmp.append(df.iloc[i].matches['info']['teams'][1]['bans'][j - 5]['championId'])
                        tmp.append(df.iloc[i].matches['info']['teams'][1]['bans'][j - 5]['pickTurn'])

                    df_creater.append(tmp)
        except:
            print(i)
            continue
    columns = ['gameId', 'gameDuration', 'gameVersion', 'summonerName', 'summonerLevel', 'participantId',
               'championName', 'champExperience',
               'teamPosition', 'teamId', 'win', 'kills', 'deaths', 'assists', 'totalDamageDealtToChampions',
               'totalDamageTaken', 'g_5', 'g_6', 'g_7', 'g_8', 'g_9', 'g_10', 'g_11', 'g_12', 'g_13', 'g_14', 'g_15',
               'g_16', 'g_17',
               'g_18', 'g_19', 'g_20', 'g_21', 'g_22', 'g_23', 'g_24', 'g_25', 'item0', 'item1', 'item2', 'item3',
               'item4', 'item5', 'item6', 'summoner1Id', 'summoner2Id',
               'turretKills', 'firstTowerKill', 'totalMinionsKilled', 'pentaKills', 'championId', 'pickTurn']
    df = pd.DataFrame(df_creater,columns = columns).drop_duplicates()
    print('df 제작이 완료되었습니다. 현재 df의 수는 %d 입니다'%len(df))
    return df

def insert_matches_timeline_mysql(row,conn):
    # lambda를 이용해서 progress_apply를 통해 insert할 구문 만들기
    query = (
        f'insert into lol_datas(gameId, gameDuration, gameVersion, summonerName, summonerLevel, participantId,'
        f'championName, champExperience, teamPosition, teamId, win, kills, deaths, assists,'
        f'totalDamageDealtToChampions, totalDamageTaken, g_5, g_6, g_7, g_8, g_9, g_10, g_11, g_12 ,g_13,g_14,'
        f'g_15, g_16, g_17, g_18, g_19, g_20, g_21, g_22, g_23, g_24, g_25, item0, item1, item2, item3, item4,'
        f'item5, item6, summoner1Id, summoner2Id, turretKills, firstTowerKill, totalMinionsKilled, pentaKills,'
        f'championId, pickTurn)'
        f'values(\'{row.gameId}\',{row.gameDuration},\'{row.gameVersion}\',\'{row.summonerName}\','
        f'{row.summonerLevel},{row.participantId},\'{row.championName}\',{row.champExperience},'
        f'\'{row.teamPosition}\',{row.teamId},\'{row.win}\',{row.kills},{row.deaths},{row.assists},'
        f'{row.totalDamageDealtToChampions},{row.totalDamageTaken},{row.g_5},{row.g_6},{row.g_7},{row.g_8},'
        f'{row.g_9},{row.g_10},{row.g_11},{row.g_12},{row.g_13},{row.g_14},{row.g_15},{row.g_16},{row.g_17},'
        f'{row.g_18},{row.g_19},{row.g_20},{row.g_21},{row.g_22},{row.g_23},{row.g_24},{row.g_25})'
        f'{row.item0}, {row.item1}, {row.item2},{row.item3},{row.item4},{row.item5},{row.item6}'
        f'{row.summoner1Id},{row.summoner2Id},{row.turretKills},\'{row.firstTowerKill}\',{row.totalMinionsKilled}'
        f'{row.pentaKills},{row.championId},{row.pickTurn}'
        f'ON DUPLICATE KEY UPDATE '
        f'gameId = \'{row.gameId}\', gameDuration = {row.gameDuration}, gameVersion = \'{row.gameVersion}\', summonerName= \'{row.summonerName}\','
        f'summonerLevel = {row.summonerLevel},participantId = {row.participantId},championName = \'{row.championName}\','
        f'champExperience = {row.champExperience}, teamPosition = \'{row.teamPosition}\', teamId = {row.teamId},win = \'{row.win}\','
        f'kills = {row.kills}, deaths = {row.deaths}, assists = {row.assists}, totalDamageDealtToChampions = {row.totalDamageDealtToChampions},'
        f'totalDamageTaken = {row.totalDamageTaken},g_5 = {row.g_5},g_6 = {row.g_6},g_7 = {row.g_7},g_8 = {row.g_8},g_9 = {row.g_9},'
        f'g_10 = {row.g_10},g_11 = {row.g_11},g_12 = {row.g_12},g_13 = {row.g_13},g_14 = {row.g_14},g_15 = {row.g_15},g_16 = {row.g_16},g_17 = {row.g_17},'
        f'g_18 = {row.g_18},g_19 = {row.g_19},g_20 = {row.g_20},g_21 = {row.g_21},g_22 = {row.g_22},g_23 = {row.g_23},g_24 = {row.g_24},g_25 = {row.g_25},'
        f'item0 = {row.item0}, item1 = {row.item1}, item2 = {row.item2}, item3 = {row.item3}, item4 = {row.item4}, item5 = {row.item5}, item6 = {row.item6},'
        f'summoner1Id = {row.summoner1Id}, summoner2Id = {row.summoner2Id}, turretKills = {row.turretKills}, firstTowerKill = \'{row.firstTowerKill}\','
        f'totalMinionsKilled = {row.totalMinionsKilled}, pentaKills = {row.pentaKills}, championId = {row.championId}, pickTurn = {row.pickTurn}'
    )
    sql_execute(conn,query)
    return query
