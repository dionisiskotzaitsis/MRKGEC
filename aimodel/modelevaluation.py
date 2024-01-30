#This is part is aims to feed connected paths into the recurrent neural network to train and test the proposed methods.
import ast
import io

import numpy as np
import argparse
import torch
import torch.autograd as autograd
from torch import nn, optim
from torch.autograd import Variable
import torch.nn.functional as F
from Evaluation import Evaluation
from datetime import datetime
import multiprocessing as mp
import os
import json
import ujson
import gc


def load_paths(fr_file, isPositive):
	global node_count, all_variables, paths_between_pairs, positive_label, all_user, all_movie
	i=0
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
		print(all_variables)

		if key not in paths_between_pairs:
			value.append(path)
			paths_between_pairs.update({key: value})
		else:
			paths_between_pairs[key].append(path)
		print(paths_between_pairs)



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
	line = 'precision@1: ' + str(precision_1) + '\n' + 'precision@3: ' + str(precision_5) + '\n' \
		+ 'precision@5: ' + str(precision_10) + '\n' + 'mrr: ' + str(mrr_10) + '\n'
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

	###############################################
	fr_pre_user = open(pre_train_user_embedding, 'r')
	listOfItems=[]
	listOfBus=[]
	for i in fr_pre_user:
		listOfItems.append('u'+str(i.split('|')[0]))
	fr_pre_movie = open(pre_train_movie_embedding, 'r')
	for i in fr_pre_movie:
		listOfBus.append('i'+str(i.split('|')[0]))
		listOfItems.append('i'+str(i.split('|')[0]))
	fr_train = open(train_file,'r')
	fr_test = open(test_file,'r')
	fw_results = open(results_file, 'w')

	embedding_dict_3 = {}
	embedding_dict_4 = {}
	embedding_dict_5 = {}
	embedding_dict = {}

	all_movie3 = []
	all_movie4 = []
	all_movie5 = []
	all_movie = []

	with open('paths_Random/embedding_dictionary_3.txt') as f:
		for i in f:
			line = i.split('\t')
			key = line[0]
			mylist = ast.literal_eval(line[1])
			value = [float(n) for n in mylist]
			embedding_dict_3[key] = value


	with open('paths_Random/embedding_dictionary_4.txt') as f:
		for i in f:
			line = i.split('\t')
			key = line[0]
			mylist = ast.literal_eval(line[1])
			value = [float(n) for n in mylist]
			embedding_dict_4[key] = value


	with open('paths_Random/embedding_dictionary_5.txt') as f:
		for i in f:
			line = i.split('\t')
			key = line[0]
			mylist = ast.literal_eval(line[1])
			value = [float(n) for n in mylist]
			embedding_dict_5[key] = value



	with open('paths_Random/all_movie_3.txt') as f:
		for i in f:
			all_movie3.append(str(i.strip()))

	with open('paths_Random/all_movie_4.txt') as f:
		for i in f:
			all_movie4.append(str(i.strip()))

	with open('paths_Random/all_movie_5.txt') as f:
		for i in f:
			all_movie5.append(str(i.strip()))


	for key in listOfItems:
		if key in embedding_dict_5:
			dict1 = [float(x) * 100 for x in embedding_dict_5[key]]
		else:
			dict1 = np.full_like(embedding_dict_5[list(embedding_dict_5)[0]],-10)
		if key in embedding_dict_3:
			dict2 = [float(x) * 1000 for x in embedding_dict_3[key]]
		else:
			dict2 = np.full_like(embedding_dict_3[list(embedding_dict_3)[0]],-10)
		if key in embedding_dict_4:
			dict3 = [float(x) * 500 for x in embedding_dict_4[key]]
		else:
			dict3 = np.full_like(embedding_dict_4[list(embedding_dict_4)[0]],-10)

		if np.all(dict2 == 0) and np.all(dict3 == 0) and np.all(dict1 == 0):
			continue
		else:
			embedding_dict[key] = np.concatenate((dict2, dict3, dict1))

	all_movie = list(set().union(all_movie3, all_movie4, all_movie5))
	del embedding_dict_3
	del embedding_dict_4
	del embedding_dict_5
	del dict3
	del dict2
	del dict1
	del all_movie3
	del all_movie5
	del all_movie4
	gc.collect()

	print(embedding_dict)
	print("Size of embedding "+str(len(embedding_dict)))

	###################### EVALUATION #######################

	print('model training finished')
	end_time = datetime.now()
	duration = end_time - start_time
	print ('the duration for model training is ' + str(duration) + '\n')

	start_time = datetime.now()
	train_dict = load_data(fr_train)
	test_dict = load_data(fr_test)
	model_evaluation = LSTMEvaluation(embedding_dict, all_movie, train_dict, test_dict)
	top_score_dict = model_evaluation.calculate_ranking_score()
	precision_1,_ = model_evaluation.calculate_results(top_score_dict, 1)
	precision_3,mrr_3 = model_evaluation.calculate_results(top_score_dict, 3)
	precision_5, _ = model_evaluation.calculate_results(top_score_dict, 5)
	end_time = datetime.now()
	duration = end_time - start_time
	print ('the duration for model evaluation is ' + str(duration) + '\n')

	write_results(fw_results, precision_1, precision_3, precision_5, mrr_3)

	end_time = datetime.now()
	duration = end_time - start_time
	print ('the duration for loading item embedding is ' + str(duration) + '\n')

	fr_pre_user.close()
	fr_pre_movie.close()
	fr_train.close()
	fr_test.close()
	fw_results.close()
