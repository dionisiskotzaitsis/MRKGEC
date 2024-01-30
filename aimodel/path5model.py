# This is part is aims to feed connected paths into the recurrent neural network to train and test the proposed methods.
import ast
import io

import numpy as np
import argparse
import torch
import torch.autograd as autograd
from torch import nn, optim
from torch.autograd import Variable
import torch.nn.functional as F
from Tagger import Tagger
from Train import Train
from datetime import datetime
import multiprocessing as mp
import os
import json
import ujson


def load_paths(fr_file, isPositive, counter):
    global node_count, all_variables, paths_between_pairs, positive_label, all_user, all_movie
    i = 0
    for line in fr_file:
        line = line.replace('\n', '')
        lines = line.split(',')
        user = lines[0]
        movie = lines[-1]

        if user not in all_user:
            all_user.append(user)
        if movie not in all_movie:
            all_movie.append(movie)

        key = (user, movie)
        value = []
        path = []

        if isPositive:
            if key not in positive_label:
                positive_label.append(key)

        for node in lines:
            if node not in all_variables:
                all_variables.update({node: node_count})
                node_count = node_count + 1
            path.append(node)

        if key not in paths_between_pairs:
            value.append(path)
            paths_between_pairs.update({key: value})
        else:
            paths_between_pairs[key].append(path)

        print(str(i) + '/' + str(counter))
        i += 1


def load_pre_embedding(fr_pre_file, isUser):
    global pre_embedding, all_variables

    for line in fr_pre_file:
        lines = line.split('|')
        node = lines[0]
        if isUser:
            node = 'u' + node
        else:
            node = 'i' + node

        if node in all_variables:
            node_id = all_variables[node]
            embedding = [float(x) for x in lines[1].split()]
            embedding = np.array(embedding)
            pre_embedding[node_id] = embedding


def load_data(fr_file):
    data_dict = {}

    for line in fr_file:
        lines = line.replace('\n', '').split('\t')
        user = 'u' + lines[0]
        item = 'i' + lines[1]

        if user not in data_dict:
            data_dict.update({user: [item]})
        elif item not in data_dict[user]:
            data_dict[user].append(item)

    return data_dict


def write_results(fw_results, precision_1, precision_5, precision_10, mrr_10):
    line = 'precision@1: ' + str(precision_1) + '\n' + 'precision@5: ' + str(precision_5) + '\n' \
           + 'precision@10: ' + str(precision_10) + '\n' + 'mrr: ' + str(mrr_10) + '\n'
    fw_results.write(line)


if __name__ == '__main__':
    print(torch.cuda.get_device_name(0))
    parsed_args = parser.parse_args()
    input_dim = 20
    hidden_dim = 32
    out_dim = 1
    iteration = 5
    learning_rate = 0.01

    pre_train_user_embedding = 'pre-train-user-embedding.txt'
    pre_train_movie_embedding = 'pre-train-item-embedding.txt'
    train_file = 'training.txt'
    test_file = 'test.txt'
    results_file = default='results.txt'

    start_time = datetime.now()

    counters = []
    for i in range(3, 6):
        filename = open('paths_Random/positive_path_' + str(i) + '.txt', 'r')
        counters.append(len(list(filename)))
        filename.close()

    ################################################
    fr_postive_5 = open(positive_path, 'r')
    fr_negative_5 = open(negative_path, 'r')


    ###############################################
    fr_pre_user = open(pre_train_user_embedding, 'r')
    fr_pre_movie = open(pre_train_movie_embedding, 'r')
    fr_train = open(train_file, 'r')
    fr_test = open(test_file, 'r')
    fw_results = open(results_file, 'w')

    ###############################  EMBEDDING 5 #####################################

    node_count = 0  # count the number of all entities (user, movie and attributes)
    all_variables = {}  # save variable and corresponding id
    paths_between_pairs = {}  # save all the paths (both positive and negative) between a user-movie pair
    positive_label = []  # save the positive user-movie pairs
    all_user = []  # save all the users
    all_movie_5 = []
    all_movie = []  # save all the movies

    load_paths(fr_postive_5, True, counters[2])
    load_paths(fr_negative_5, False, counters[2])

    print('The number of all variables is :' + str(len(all_variables)))
    end_time = datetime.now()
    duration = end_time - start_time
    print('the duration for loading user path is ' + str(duration) + '\n')

    start_time = datetime.now()
    node_size = len(all_variables)
    pre_embedding = np.random.rand(node_size, input_dim)  # embeddings for all nodes
    load_pre_embedding(fr_pre_user, True)
    load_pre_embedding(fr_pre_movie, False)
    pre_embedding = torch.FloatTensor(pre_embedding)
    end_time = datetime.now()
    duration = end_time - start_time
    print('the duration for loading embedding is ' + str(duration) + '\n')

    start_time = datetime.now()
    model = Tagger(node_size, input_dim, hidden_dim, out_dim, pre_embedding)
    if torch.cuda.is_available():
        model = model.cuda()
        print("Model became cuda")

    model_train_1 = Train(model, iteration, learning_rate, paths_between_pairs, positive_label, all_variables,
                              all_user, all_movie, 5)

    embedding_dict_1 = model_train_1.train()

    print("Embedding 5")
    files = ['paths_Random/paths_between_pairs_5.txt', 'paths_Random/positive_label_5.txt']
    path_between_file = open(files[0], "w+")
    positive_label_file = open(files[1], "w+")
    all_movie_5 = all_movie
    for key, value in paths_between_pairs.items():
        path_between_file.write(str(key) + '\t' + str(value) + '\n')
    for j in positive_label:
        positive_label_file.write(str(j) + '\n')


    files = ['paths_Random/node_count_5.txt', 'paths_Random/all_variables_5.txt', 'paths_Random/all_user_5.txt', 'paths_Random/all_movie_5.txt']
    node_count_file = open(files[0], "w+")
    all_variable_file = open(files[1], "w+")
    all_user_file = open(files[2], "w+")
    all_movie_file = open(files[3], "w+")

    for j in all_user:
        all_user_file.write(str(j) + '\n')
    for j in all_movie:
        all_movie_file.write(str(j) + '\n')
    node_count_file.write(str(node_count))
    all_variable_file.write(json.dumps(all_variables))

    all_user_file.close()
    all_movie_file.close()
    node_count_file.close()
    all_variable_file.close()
    fr_postive_5.close()
    fr_negative_5.close()
    fr_pre_user.close()
    fr_pre_movie.close()
    fr_train.close()
    fr_test.close()
    fw_results.close()
