import csv
import json
import os
import multiprocessing as mp
import math
from itertools import islice
import glob
from time import sleep


def splitter(file):
    with open(file+'.json', 'r',encoding='utf-8') as infile:
        o = json.load(infile)
        keys = list(o.keys())
        n = len(keys) // 12
        flag=1
        for i in range(0, len(keys), n):
            with open('jsons/'+file+'_'+str(flag)+'.json','w') as out:
                json.dump({k: o[k] for k in keys[i: i + n]},out)
                flag=flag+1

def func(file,input,unique_days,unique_months,unique_seasons,unique_user_day,unique_user_month,unique_month_season,unique_day_month,unique_day_bus,unique_month_bus,unique_season_bus):

    with open('jsons/'+file,'r',encoding='utf-8') as file:
        inp=json.load(file)

        
        for review_id in inp:

            unique_days.append(input[review_id]["day"])
            unique_months.append(input[review_id]["month"])
            unique_seasons.append(input[review_id]["season"])

            unique_user_day.append((inp[review_id]['user_id'], input[review_id]["day"], review_id))
            unique_user_month.append((inp[review_id]['user_id'], input[review_id]["month"], review_id))
            unique_month_season.append((input[review_id]["month"], input[review_id]["season"]))
            unique_day_month.append((input[review_id]["day"], input[review_id]["month"]))
            unique_day_bus.append((input[review_id]["day"], inp[review_id]['business_id'], review_id))
            unique_month_bus.append((input[review_id]["month"], inp[review_id]['business_id'], review_id))
            unique_season_bus.append((input[review_id]["season"], inp[review_id]['business_id'], review_id))


if __name__ == '__main__':

    def write_header(file_name, columns):
        with open('reviewDictChronological/'+str(file_name), 'w') as file_csv:
            writer = csv.writer(file_csv)
            writer.writerow(columns)

    if not os.path.isfile("reviewDictChronological/business_header.csv"):
        with open("yelp_academic_dataset_business.json", encoding='utf-8') as business_json, \
                open("reviewDictChronological/reviewDictChronological.json", encoding='utf-8') as reviewDict, \
                open("reviewDictChronological/business.csv", 'w', encoding='utf-8') as business_csv:

            input = json.load(reviewDict)
            business_list=[]
            users_list=[]
            for i in input.values():
                users_list.append(i['user_id'])
                business_list.append(i['business_id'])
            write_header("business_header.csv", ['id:ID(Business)', 'name', 'address', 'city', 'state', 'latitude:double', 'longitude:double','stars:float','attributes','hours'])

            business_writer = csv.writer(business_csv, escapechar='\\', quotechar='"', quoting=csv.QUOTE_ALL)

            for line in business_json.readlines():
                item = json.loads(line)
                if item['business_id'] in business_list:
                    try:
                        if "attributes" in item:
                            atts=str(item['attributes'])
                        else:
                            atts=""
                        if "hours" in item:
                            hrs=str(item['hours'])
                        else:
                            hrs=""

                        business_writer.writerow(
                            [item['business_id'], item['name'], item['address'], item['city'], item['state'],item['latitude'],item['longitude'],item['stars'],atts,hrs])
                    except Exception as e:
                        print(item)
                        raise e

    if not os.path.isfile("reviewDictChronological/city_header.csv"):
        with open("yelp_academic_dataset_business.json", encoding='utf-8') as business_json, \
                open("reviewDictChronological/city.csv", "w", encoding='utf-8') as city_csv, \
                open("reviewDictChronological/business_IN_CITY_city.csv", "w", encoding='utf-8') as business_city_csv:

            write_header("city_header.csv", ['name:ID(City)'])
            write_header("business_IN_CITY_city_header.csv", [':START_ID(Business)', ':END_ID(City)'])

            business_city_writer = csv.writer(business_city_csv, escapechar='\\', quotechar='"', quoting=csv.QUOTE_ALL)
            city_writer = csv.writer(city_csv, escapechar='\\', quotechar='"', quoting=csv.QUOTE_ALL)

            unique_cities = set()
            for line in business_json.readlines():
                item = json.loads(line)

                if item["business_id"] in business_list:
                    unique_cities.add(item["city"])
                    business_city_writer.writerow([item["business_id"], item["city"]])

            for city in unique_cities:
                city_writer.writerow([city])


    if not os.path.isfile("reviewDictChronological/area_header.csv"):
        with open("yelp_academic_dataset_business_locations.json", encoding='utf-8') as business_locations_json, \
                open("reviewDictChronological/area.csv", "w", encoding='utf-8') as area_csv, \
                open("reviewDictChronological/business_IN_AREA_area.csv", "w", encoding='utf-8') as business_area_csv, \
                open("reviewDictChronological/city_IN_area.csv", "w", encoding='utf-8') as city_area_csv:
            input = json.load(business_locations_json)

            write_header("area_header.csv", ['name:ID(Area)'])

            write_header("city_IN_area_header.csv", [':START_ID(City)', ':END_ID(Area)'])
            write_header("business_IN_AREA_area_header.csv", [':START_ID(Business)', ':END_ID(Area)'])


            area_writer = csv.writer(area_csv, escapechar='\\', quotechar='"', quoting=csv.QUOTE_ALL)

            city_area_writer = csv.writer(city_area_csv, escapechar='\\', quotechar='"', quoting=csv.QUOTE_ALL)
            bus_area_writer = csv.writer(business_area_csv, escapechar='\\', quotechar='"', quoting=csv.QUOTE_ALL)

            unique_areas = set()
            unique_city_areas = set()
            unique_business_areas = set()

            for business_id in input:
                if business_id in business_list:
                #if input[business_id]["admin1"]:
                    unique_areas.add(input[business_id]["admin1"])

                    unique_city_areas.add((input[business_id]["city"], input[business_id]["admin1"]))

                    unique_business_areas.add((business_id, input[business_id]["admin1"]))

            for area in unique_areas:
                area_writer.writerow([area])

            for bus,area in unique_business_areas:
                bus_area_writer.writerow([bus,area])

            for city, area in unique_city_areas:
                city_area_writer.writerow([city, area])


    if not os.path.isfile("reviewDictChronological/category_header.csv"):
        with open("yelp_academic_dataset_business.json", encoding='utf-8') as business_json, \
                open("reviewDictChronological/category.csv", 'w', encoding='utf-8') as categories_csv, \
                open("reviewDictChronological/business_IN_CATEGORY_category.csv", 'w', encoding='utf-8') as business_category_csv:

            write_header("category_header.csv", ['name:ID(Category)'])
            write_header("business_IN_CATEGORY_category_header.csv", [':START_ID(Business)', ':END_ID(Category)'])

            business_category_writer = csv.writer(business_category_csv, escapechar='\\', quotechar='"', quoting=csv.QUOTE_ALL)
            category_writer = csv.writer(categories_csv, escapechar='\\', quotechar='"', quoting=csv.QUOTE_ALL)

            unique_cities = set()
            for line in business_json.readlines():
                item = json.loads(line)
                if item['business_id'] in business_list:
                    if item["categories"] is not None:
                        #print(list(item["categories"].split(", ")))
                        for category in item["categories"].split(", "):
                            unique_cities.add(category)
                            business_category_writer.writerow([item["business_id"], category])
                    else:
                        #print("NONETYPE",item["categories"])
                        unique_cities.add("None")
                        business_category_writer.writerow([item["business_id"], "None"])
                        continue

            for category in unique_cities:
                try:
                    category_writer.writerow([category])
                except Exception as e:
                    print(category)
                    raise e

    '''if not os.path.isfile("user_header.csv"):
        with open("yelp_academic_dataset_user.json", encoding='utf-8') as user_json, \
                open("user.csv", 'w', encoding='utf-8') as user_csv, \
                open("user_FRIENDS_user.csv", 'w', encoding='utf-8') as user_user_csv:

            write_header("user_header.csv", ['id:ID(User)', 'name','average_stars:float','review_count:int','fans:int'])
            write_header("user_FRIENDS_user_header.csv", [':START_ID(User)', ':END_ID(User)'])

            user_writer = csv.writer(user_csv, escapechar='\\', quotechar='"', quoting=csv.QUOTE_ALL)
            user_user_writer = csv.writer(user_user_csv, escapechar='\\', quotechar='"', quoting=csv.QUOTE_ALL)


            for line in user_json.readlines():
                item = json.loads(line)
                if item["friends"] is not None:
                    # print(list(item["categories"].split(", ")))
                    user_writer.writerow(
                        [item["user_id"], item["name"], item['average_stars'], item['review_count'], item['fans']])
                    for friend_id in item["friends"].split(", "):
                        user_user_writer.writerow([item["user_id"], friend_id])
                        # user_writer.writerow([item[friend_id], item["name"], item['average_stars'], item['review_count'],item['fans']])
                else:
                    continue'''



    '''if not os.path.isfile("user_REVIEW_business_header.csv"):
        with open("yelp_academic_dataset_review.json", encoding='utf-8') as review_json, \
                open("reviewDictChronological_TRAIN.json", encoding='utf-8') as reviewDict, \
                open("user_REVIEW_business.csv", 'w', encoding='utf-8') as user_business_csv:

            input = json.load(reviewDict)
            write_header("user_REVIEW_business_header.csv", [':START_ID(User)', ':END_ID(Business)'])

            user_business_writer = csv.writer(user_business_csv, escapechar='\\', quotechar='"', quoting=csv.QUOTE_ALL)

            for line in review_json.readlines():
                item = json.loads(line)
                if item['review_id'] in list(input.keys()):
                    user_business_writer.writerow([item["user_id"], item["business_id"]])'''







    if not os.path.isfile("reviewDictChronological/month_header.csv"):
        with open("yelp_academic_dataset_review_timeframe.json", encoding='utf-8') as time_json, \
                open("reviewDictChronological/reviewDictChronological_TRAIN.json", encoding='utf-8') as reviewDict, \
                open("reviewDictChronological/day.csv", 'w', encoding='utf-8') as day_csv, \
                open("reviewDictChronological/month.csv", 'w', encoding='utf-8') as month_csv, \
                open("reviewDictChronological/season.csv", 'w', encoding='utf-8') as season_csv, \
                open("reviewDictChronological/user_WROTE_IN_DAY_day.csv", 'w', encoding='utf-8') as user_day, \
                open("reviewDictChronological/user_WROTE_IN_MONTH_month.csv", 'w', encoding='utf-8') as user_month, \
                open("reviewDictChronological/day_DAY_OF_MONTH_month.csv", 'w', encoding='utf-8') as day_month, \
                open("reviewDictChronological/month_PART_OF_season.csv", 'w', encoding='utf-8') as month_season, \
                open("reviewDictChronological/day_REVIEW_DAY_business.csv", 'w', encoding='utf-8') as day_bus, \
                open("reviewDictChronological/month_REVIEW_MONTH_business.csv", 'w', encoding='utf-8') as month_bus, \
                open("reviewDictChronological/season_REVIEW_SEASON_business.csv", 'w', encoding='utf-8') as season_bus:


            input=json.load(time_json)
            input_2=json.load(reviewDict)

            write_header("day_header.csv", ['id:ID(Day)'])
            write_header("month_header.csv", ['id:ID(Month)'])
            write_header("season_header.csv", ['name:ID(Season)'])

            write_header("user_WROTE_IN_DAY_day_header.csv", [':START_ID(User)', ':END_ID(Day)','review_id'])
            write_header("user_WROTE_IN_MONTH_month_header.csv", [':START_ID(User)', ':END_ID(Month)','review_id'])
            write_header("day_DAY_OF_MONTH_month_header.csv", [':START_ID(Day)', ':END_ID(Month)'])
            write_header("month_PART_OF_season_header.csv", [':START_ID(Month)', ':END_ID(Season)'])
            write_header("day_REVIEW_DAY_business_header.csv", [':START_ID(Day)', ':END_ID(Business)','review_id'])
            write_header("month_REVIEW_MONTH_business_header.csv", [':START_ID(Month)', ':END_ID(Business)','review_id'])
            write_header("season_REVIEW_SEASON_business_header.csv", [':START_ID(Season)', ':END_ID(Business)','review_id'])


            day_writer = csv.writer(day_csv, escapechar='\\', quotechar='"', quoting=csv.QUOTE_ALL)
            month_writer = csv.writer(month_csv, escapechar='\\', quotechar='"', quoting=csv.QUOTE_ALL)
            season_writer = csv.writer(season_csv, escapechar='\\', quotechar='"', quoting=csv.QUOTE_ALL)

            user_day_writer = csv.writer(user_day, escapechar='\\', quotechar='"', quoting=csv.QUOTE_ALL)
            user_month_writer = csv.writer(user_month, escapechar='\\', quotechar='"', quoting=csv.QUOTE_ALL)
            day_month_writer = csv.writer(day_month, escapechar='\\', quotechar='"', quoting=csv.QUOTE_ALL)
            month_season_writer = csv.writer(month_season, escapechar='\\', quotechar='"', quoting=csv.QUOTE_ALL)
            day_bus_writer = csv.writer(day_bus, escapechar='\\', quotechar='"', quoting=csv.QUOTE_ALL)
            month_bus_writer = csv.writer(month_bus, escapechar='\\', quotechar='"', quoting=csv.QUOTE_ALL)
            season_bus_writer = csv.writer(season_bus, escapechar='\\', quotechar='"', quoting=csv.QUOTE_ALL)



            unique_days= set()
            unique_months = set()
            unique_seasons = set()

            unique_user_day = set()
            unique_user_month = set()
            unique_day_month = set()
            unique_month_season = set()
            unique_day_bus = set()
            unique_month_bus = set()
            unique_season_bus = set()


            '''numOfWorkers=13
            manager = mp.Manager()

            unique_days = manager.list()
            unique_months = manager.list()
            unique_seasons = manager.list()

            unique_user_day = manager.list()
            unique_user_month = manager.list()
            unique_day_month = manager.list()
            unique_month_season = manager.list()
            unique_day_bus = manager.list()
            unique_month_bus = manager.list()
            unique_season_bus = manager.list()
            #q = manager.Queue()
            #pool = mp.Pool(numOfWorkers)
            processes=[]

            keys=list(input_2.keys())

            if not os.path.isfile("jsons/reviewDict_1.json"):
                splitter("reviewDict")


            for i in os.listdir("jsons"):
                print(i)
                #sleep(1)
                p=mp.Process(target=func,
                                       args=(i,input,unique_days,unique_months,unique_seasons,unique_user_day,unique_user_month,unique_month_season,unique_day_month,unique_day_bus,unique_month_bus,unique_season_bus))
                p.start()
                processes.append(p)

            for p in processes:
                p.join()
                #p.kill()


            print("I'm out")
            unique_days_set = set(unique_days)
            unique_months_set = set(unique_months)
            unique_seasons_set = set(unique_seasons)

            #unique_user_day = set(unique_user_day)
            #unique_user_month = set(unique_user_month)
            unique_day_month_set = set(unique_day_month)
            unique_month_season_set = set(unique_month_season)
            #unique_day_bus = set(unique_day_bus)
            #unique_month_bus = set(unique_month_bus)
            #unique_season_bus = set(unique_season_bus)



            #keys=list(input.keys())'''

            for review_id in input_2:
                print(review_id)
                #if review_id in keys:
                unique_days.add(input[review_id]["day"])
                unique_months.add(input[review_id]["month"])
                unique_seasons.add(input[review_id]["season"])

                unique_user_day.add((input_2[review_id]['user_id'], input[review_id]["day"], review_id))
                unique_user_month.add((input_2[review_id]['user_id'], input[review_id]["month"], review_id))
                unique_month_season.add((input[review_id]["month"], input[review_id]["season"]))
                unique_day_month.add((input[review_id]["day"], input[review_id]["month"]))
                unique_day_bus.add((input[review_id]["day"], input_2[review_id]['business_id'], review_id))
                unique_month_bus.add((input[review_id]["month"], input_2[review_id]['business_id'], review_id))
                unique_season_bus.add((input[review_id]["season"], input_2[review_id]['business_id'], review_id))




            for day in unique_days:
                #print(day)
                day_writer.writerow([day])

            for month in unique_months:
                #print(month)
                month_writer.writerow([month])

            for season in unique_seasons:
                #print(season)
                season_writer.writerow([season])

            for user, day, review in unique_user_day:
                user_day_writer.writerow([user,day,review])

            for user, month, review in unique_user_month:
                user_month_writer.writerow([user, month, review])

            for day, month in unique_day_month:
                #print((day,month))
                day_month_writer.writerow([day, month])

            for month, season in unique_month_season:
                #print((month, season))
                month_season_writer.writerow([month, season])

            for day, bus, review in unique_day_bus:
                day_bus_writer.writerow([day, bus, review])

            for month, bus, review in unique_month_bus:
                month_bus_writer.writerow([month, bus, review])

            for season, bus, review in unique_season_bus:
                season_bus_writer.writerow([season, bus, review])
                
