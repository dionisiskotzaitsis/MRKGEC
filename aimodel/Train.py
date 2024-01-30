import json

import numpy as np
import torch
import torch.autograd as autograd
from torch import nn, optim
from torch.autograd import Variable
import torch.nn.functional as F

class Train(object):
	def __init__(self, model, iteration, learning_rate, paths_between_pairs, positive_label, \
		all_variables, all_user, all_movie,flag):
		super(Train, self).__init__()
		self.model = model
		self.iteration = iteration
		self.learning_rate = learning_rate
		self.paths_between_pairs = paths_between_pairs
		self.positive_label = positive_label
		self.all_variables = all_variables
		self.all_user = all_user
		self.all_movie = all_movie
		self.flag=flag


	def dump_post_embedding(self):
		embedding_dict = {}
		node_list = self.all_user + self.all_movie

		for node in node_list:
			node_id = torch.LongTensor([int(self.all_variables[node])])
			node_id = Variable(node_id)
			if torch.cuda.is_available():
				node_id=node_id.cuda()
			print("TRIAL")
			print(node_id.is_cuda)
			print('gpu\t', node_id)
			print('cpu\t', node_id.cpu())
			node_embedding = self.model.embedding(node_id).squeeze().cpu().data.numpy()
			if node not in embedding_dict:
				embedding_dict.update({node:node_embedding})

		return embedding_dict
			
		
	def train(self):
		criterion = nn.BCELoss()
		optimizer = optim.SGD(self.model.parameters(), lr=self.learning_rate)


		for epoch in range (self.iteration):
			running_loss = 0.0
			data_size = len(self.paths_between_pairs)
			label = Variable(torch.Tensor())    
			
			for pair in self.paths_between_pairs:
				
				paths_between_one_pair = self.paths_between_pairs[pair]
				paths_between_one_pair_size = len(paths_between_one_pair)
				paths_between_one_pair_id = []

				for path in paths_between_one_pair:
					path_id = [self.all_variables[x] for x in path]
					#print(path_id)
					paths_between_one_pair_id.append(path_id)


				paths_between_one_pair_id = np.array(paths_between_one_pair_id)
				paths_between_one_pair_id = Variable(torch.LongTensor(paths_between_one_pair_id))


				if torch.cuda.is_available():
					paths_between_one_pair_id = paths_between_one_pair_id.cuda()

				out = self.model(paths_between_one_pair_id)
				#########################
				out = out.squeeze(1)
				out = out.squeeze(1)
				########################
				if pair in self.positive_label:
					label = Variable(torch.Tensor([1]))
				else:
					label = Variable(torch.Tensor([0]))



				loss = criterion(out.cpu(), label)
				#loss = criterion(out.cuda(), label)
				running_loss += loss.item() * label.item()

				optimizer.zero_grad()
				loss.backward()
				optimizer.step()


			print('epoch[' + str(epoch) + ']: loss is ' + str(running_loss))

		with open('paths_Random/embedding_dictionary_'+str(self.flag)+'.txt', 'w+') as f:
			for key, value in self.dump_post_embedding().items():
				f.write(str(key) + '\t' + str(value.tolist()) + '\n')

		return self.dump_post_embedding()
