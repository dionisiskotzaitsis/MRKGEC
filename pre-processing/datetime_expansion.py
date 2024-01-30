import json
import datetime

if __name__ == '__main__':
    datetimes={}


    with open('./reviewDictChronological2.json', encoding='utf-8') as rvw:
        for line in rvw.readlines():
            item=json.loads(line)
            mykeys = [*item][0]
            myvals = [*item.values()][0]
            print(myvals)
            datetimes[mykeys] = {
                "datetime": str(datetime.datetime.fromtimestamp(int(myvals['timestamp'])))
            }


    result={}
    review_ids=list(datetimes.keys())
    seasons = ["winter","winter","spring","spring","spring","summer","summer","summer","fall","fall","fall","winter"]
    week = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
    month_to_season = dict(zip(range(1, 13), seasons))
    day_to_day=dict(zip(range(0, 7), week))


    for review_id in review_ids:
        result[review_id]={
            "day": day_to_day[int(datetime.datetime.strptime(datetimes[review_id]["datetime"],"%Y-%m-%d %H:%M:%S").weekday())],
            "year":datetime.datetime.strptime(datetimes[review_id]["datetime"],"%Y-%m-%d %H:%M:%S").year,
            "month": datetime.datetime.strptime(datetimes[review_id]["datetime"], "%Y-%m-%d %H:%M:%S").month,
            "hour": datetime.datetime.strptime(datetimes[review_id]["datetime"], "%Y-%m-%d %H:%M:%S").hour,
            "season": month_to_season[datetime.datetime.strptime(datetimes[review_id]["datetime"], "%Y-%m-%d %H:%M:%S").month]
        }

    with open("./yelp_academic_dataset_review_timeframe.json","w") as rv_tm:
        json.dump(result,rv_tm,indent=4,sort_keys=True)






