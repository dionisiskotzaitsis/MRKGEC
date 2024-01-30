# This is used to sample negative buss that user has not interactions with, so as to balance model training process

import json
import argparse
import math
from random import randint


def load_data(file):

    dictionary=json.load(file)
    train_dict = {}
    all_bus_list = []
    itterator = 0
    length = len(list(dictionary.keys()))
    
    for val in dictionary.values():
        print(str(itterator) + '\tof\t' + str(length))
        user=val['user_id']
        bus=val['business_id']

        if user not in train_dict:
            init_bus_list = []
            init_bus_list.append(bus)
            train_dict.update({user: init_bus_list})
        else:
            train_dict[user].append(bus)

        if bus not in all_bus_list:
            all_bus_list.append(bus)
        itterator=itterator+1

    return train_dict, all_bus_list


def negative_sample(train_dict, all_bus_list, shrink, fw_negative):
    all_bus_size = len(all_bus_list)
    negativeDict={}
    itterator=0
    length=len(list(train_dict.keys()))

    for user in train_dict:
        print(str(itterator) + '\tof\t' + str(length))
        user_train_bus = train_dict[user]
        user_train_bus_size = len(user_train_bus)
        negative_size = math.ceil(user_train_bus_size * shrink)
        user_negative_bus = []

        while (len(user_negative_bus) < negative_size):
            negative_index = randint(0, (all_bus_size - 1))
            negative_bus = str(all_bus_list[negative_index])
            if negative_bus not in user_train_bus and negative_bus not in user_negative_bus:
                user_negative_bus.append(negative_bus)
                negativeDict[itterator]={
                    'user_id':user,
                    'business_id': negative_bus
                }
            itterator=itterator+1
    json.dump(negativeDict,fw_negative)


if __name__ == '__main__':
    train_file = 'reviewDictChronological_TRAIN.json'
    negative_file = 'negativeSampling.json'
    shrink = 0.05

    fr_train = open(train_file, 'r')
    fw_negative = open(negative_file, 'w')

    train_dict, all_bus_list = load_data(fr_train)
    negative_sample(train_dict, all_bus_list, shrink, fw_negative)

    fr_train.close()
    fw_negative.close()
