import reverse_geocoder as rg
import json

if __name__ == '__main__':
    lat_longs={}

    with open('./business.json', encoding='utf-8') as bsn:
        for line in bsn.readlines():
            item=json.loads(line)
            if item["latitude"] and item["longitude"]:
                lat_longs[item["business_id"]]={
                    "lat_long" : (item["latitude"],item["longitude"]),
                    "city" : item["city"]
                }

    result={}
    business_ids=list(lat_longs.keys())
    locations=rg.search([value["lat_long"] for value in lat_longs.values()])

    for business_id, location in zip(business_ids,locations):
        result[business_id]={
            "country":location["cc"],
            "name":location["name"],
            "admin1":location["admin1"],
            "admin2":location["admin2"],
            "city":lat_longs[business_id]["city"]
        }

    with open("./yelp_academic_dataset_business_locations.json","w") as business_locations:
        json.dump(result,business_locations,indent=4,sort_keys=True)


