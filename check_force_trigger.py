import json
import redis
from datetime import date, timedelta, datetime
import boto3
from boto3.dynamodb.conditions import Key, Attr
import json
import pandas as pd
import awswrangler as wr
from warnings import simplefilter
simplefilter(action="ignore", category=pd.errors.PerformanceWarning)
import time

sns_client = boto3.client('sns')
dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')
rnew = redis.Redis(host='prod-redis.64wnxk.clustercfg.memorydb.ap-south-1.amazonaws.com', port=6379, decode_responses=True)

def getlist(key):
    return rnew.lrange(key,0,-1)
    
def save_trips(date_key, trips):
    rnew.delete(date_key)
    for trip in trips:
        rnew.rpush(date_key, trip)

def get_sessions(imei, start_time=0):
    is_truncated = True
    bucket_name = "glochistorydata"
    continuation_token  = ""
    sessions_found = []
    while is_truncated:
        if continuation_token != "":
            res = s3_client.list_objects_v2(Bucket=bucket_name, Prefix="data/"+imei+"/", ContinuationToken=continuation_token)
        else:
            res = s3_client.list_objects_v2(Bucket=bucket_name, Prefix="data/"+imei+"/")
        is_truncated = (bool(res['IsTruncated']))
        if is_truncated:
            continuation_token = res['NextContinuationToken']
        if 'Contents' not in res:
            return list(set(sessions_found))
        for obj in res['Contents']:
            name = obj['Key'].split('/')
            try:
                session_id = name[2]
                if int(session_id) >= start_time:
                    sessions_found.append(int(session_id))
            except:
                pass
    sessions = list(set(sessions_found))
    print("Sessions Found :- ", len(sessions))
    return sessions

def find_closest_session_id(session, sessions_found):
#     print("Target_session -> ", session)
    closest_session = None
    closest_session_diff = float('inf')
    
    for session_id in sessions_found:
        diff_temp = abs(session_id-session)
#         print(session_id, diff_temp)
        if diff_temp < closest_session_diff:
            closest_session_diff = diff_temp
            closest_session = session_id
    
    return closest_session, closest_session_diff

def try_map_sessionids(df, sessions):
#     print("Available Sessions ->", sessions)
    closest_sessions = []
    closest_sessions_offset = []

    for index, row in df.iterrows():
        session, diff = find_closest_session_id(row["start_datetime_epoch"], sessions)
        closest_sessions.append(session)
        closest_sessions_offset.append(min(diff, 500))
        
    df["closest_session_id"] = closest_sessions
    df["closest_session_offset"] = closest_sessions_offset

    return df

def get_trip(trip):
    trip_stats = {}
    
    trip_stats["imei"] = trip["Vin-Imei"].split("-")[1]
    if int(trip["closest_session_offset"]) <= 120 :
        trip_stats["session_id"] = str(trip["closest_session_id"])
    else:
        trip_stats["session_id"] = str(trip["start_datetime_epoch"])
    trip_stats["start_dttm"] = trip["start_datetime_epoch"]
    trip_stats["stop_dttm"] = int(trip["start_datetime_epoch"] + trip["total_duration_seconds"])
    trip_stats["ride_distance"] = trip["total_distance"]
    trip_stats["energy_consumed"] = trip["dsg_energy"]
    trip_stats["energy_recovered"] = trip["chg_energy"]
    trip_stats["diff_sec"] = trip["total_duration_seconds"]
    trip_stats["top_speed"] = trip["top_speed"]
    trip_stats["avg_speed"] = trip["average_speed"]
    trip_stats["incognito"] = False
    trip_stats["exp"] = 15 * 24 * 60 * 60;
    
    return trip_stats

def check_in_redis(trip_stats):
    imei = trip_stats["imei"]
    session_id = trip_stats["session_id"]
    
    trip_history_key = "{" + imei + "}_" + session_id + "_trip_history"
    
    if rnew.exists(trip_history_key):
        trip_history_str = rnew.get(trip_history_key)
        trip_history = json.loads(trip_history_str)
        properties_check = ['avg_speed', 'top_speed', 'diff_sec', 'energy_consumed', 'energy_recovered', 
                            'ride_distance', 'start_dttm', 'stop_dttm']
        for pc in properties_check:
            if trip_history[pc] != trip_stats[pc]:
                trip_stats[pc] = trip_history[pc]
        return trip_history_key, trip_stats, False
    return trip_history_key, trip_stats, True
    
def get_messages(event):
    
    vin_imeis = []
    start_date = ""
    end_date = ""
    for record in event["Records"]:
        message = record["Sns"]["Message"]
        vin_imeis += message["vin_imeis"].split(",")
        start_date = message["start_date"]
        end_date = message["end_date"]
        
    return vin_imeis, start_date, end_date

def lambda_handler(event, context):
    # #dates
    # end_date = date.today()
    # start_date = end_date - timedelta(days=1)
    
    # #datetimes
    # start_datetime = datetime(year=start_date.year, month=start_date.month, day=start_date.day)
    # end_datetime = datetime(year=end_date.year, month=end_date.month, day=end_date.day)
    

    vin_imeis, start_date, end_date = get_messages(event)
    #datetimes
    start_datetime = datetime.strptime(start_date, "%Y-%m-%d")
    end_datetime = datetime.strptime(end_date, "%Y-%m-%d")
    
    #timestamp
    start_timestamp = int(start_datetime.timestamp()) - 19800
    end_timestamp = int(end_datetime.timestamp()) - 19800
    
    print(start_timestamp, end_timestamp)
    print(len(vin_imeis))
    recreate_trips = []
    correct_trips = {}
    update_trips = {}
    flag = 0
    
    response = {}
    for cust in vin_imeis:
        print(cust)
        trips_df = wr.s3.read_parquet(path="s3://datalogs-summary/vcu/vin/"+cust+"/"+cust+"-session.parquet")
        # print("read_parquet for cust and row count  :- ", trips_df.shape)
        trips_df["start_datetime"] = pd.to_datetime(trips_df["start_time"])
        trips_df["start_datetime_epoch"] = [int(x.timestamp())-19800 for x in trips_df["start_datetime"]]
        trips_df = trips_df[
            (trips_df['start_datetime_epoch'] >= start_timestamp) & 
            (trips_df['start_datetime_epoch'] <= end_timestamp) & 
            (trips_df['ride_distance'] > 0)
        ]
        trips_df = trips_df[[
            'start_datetime', 
            'total_distance', 
            'top_speed', 
            'total_duration_seconds', 
            'average_speed',
            'dsg_energy',
            'chg_energy',
            'start_datetime_epoch'
        ]]
        trips_df['Vin-Imei'] = cust
        
        response[cust] = {}
        
        if len(trips_df)>0:
            print("trips to consider columns :- ", trips_df.shape)
            trips_df = try_map_sessionids(trips_df, get_sessions(cust.split("-")[1], start_timestamp))
            for index, row in trips_df.iterrows():
                trip_stats = get_trip(row)
                key, updated_stats, recreation_needed = check_in_redis(trip_stats)
                
                trip_date = datetime.fromtimestamp(int(trip_stats["start_dttm"])).strftime('%Y-%m-%d')
                if cust not in response:
                    response[cust] = {}
                if trip_date not in response[cust]:
                    response[cust][trip_date] = []
                
                if recreation_needed:
                    recreate_trips.append(trip_stats)
                    response[cust][trip_date].append(trip_stats)
                    
                elif updated_stats != trip_stats:
                    update_trips[key] = updated_stats
                    response[cust][trip_date].append(updated_stats)
                else:
                    correct_trip = correct_trips.get(cust, [])
                    correct_trip.append(key)
                    correct_trips[cust] = correct_trip
                    response[cust][trip_date].append(trip_stats)
                    

    print("Recreate :- ", len(recreate_trips), "\nCorrect :- ", len(correct_trips), "\nUpdate :- ", len(update_trips))
    
    # store_trips_redis = []

    # for trip_stats in recreate_trips:
    #     if trip_stats["ride_distance"] > 3:
    #         trip_stats["create_thumbnail"] = False
    #     store_trips_redis.append(trip_stats)
    
    # store_trips_entries = []
    # store_trips_part = []
    
    # for st in store_trips_redis:
    #     session_id = str(st["session_id"])
    #     imei = str(st["imei"])
    #     create_history = {}
    #     key = imei + "_" + session_id
    #     create_history['Id'] = key
    #     create_history['Message'] = json.dumps(st)
    #     if len(store_trips_part) == 10:
    #         store_trips_entries.append(store_trips_part)
    #         store_trips_part = []
    #     store_trips_part.append(create_history)

    # if len(store_trips_part) > 0:
    #     store_trips_entries.append(store_trips_part)
    
    # print("total store trip entries :- ", len(store_trips_entries))

    # i = 0
    # for store_trips_entry in store_trips_entries:
    #     req = sns_client.publish_batch(
    #         TopicArn = 'arn:aws:sns:ap-south-1:776601892319:tf-create-thumbnail',
    #         PublishBatchRequestEntries = store_trips_entry
    #     )
    #     print("Req no ", i, " -> success :- ", len(req["Successful"]), " , failed :- ", len(req["Failed"]))
    
    # date_keys = {}

    # for st in store_trips_redis:
    #     trip_key = "{" + st["imei"] + "}_" + st["session_id"] + "_trip_history"
    #     date_epoch = str(((int(st["session_id"]) // 86400) * 86400) - 19800)
    #     date_key = "{" + st["imei"] + "}_" + date_epoch + "_trips"
    #     date_keys_temp = date_keys.get(date_key, getlist(date_key))
    #     date_keys_temp.append(trip_key)
    #     date_keys[date_key] = date_keys_temp
    
    # for date_key in date_keys:
    #     trips = date_keys[date_key]
    #     trips.sort()
    #     save_trips(date_key, trips)

    return {
        'statusCode': 200,
        'body': json.dumps(response)
    }
