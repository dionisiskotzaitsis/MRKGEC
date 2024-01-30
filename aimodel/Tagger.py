import numpy as np
import torch
import torch.autograd as autograd
from torch import nn, optim
from torch.autograd import Variable
import torch.nn.functional as F


class Tagger(nn.Module):


    def __init__(self, node_size, input_dim, hidden_dim, out_dim, pre_embedding, \
                 nonlinearity='relu', n_layers=1, dropout=0.5):
        super(Tagger, self).__init__()
        self.node_size = node_size
        self.input_dim = input_dim
        self.hidden_dim = hidden_dim
        self.out_dim = out_dim
        self.pre_embedding = pre_embedding
        self.embedding = nn.Embedding(node_size, input_dim)
        self.embedding.weight = nn.Parameter(pre_embedding)
        self.lstm = nn.LSTM(input_dim, hidden_dim)
        self.linear = nn.Linear(hidden_dim, out_dim, bias=True)
        self.lstm = nn.LSTM(input_dim, hidden_dim, n_layers, dropout=dropout)
        self.multihead_attention = nn.MultiheadAttention(embed_dim=hidden_dim, num_heads=32)


    def forward(self, paths_between_one_pair_id):

        sum_hidden = Variable(torch.Tensor(), requires_grad=True)
        paths_size = len(paths_between_one_pair_id)

        for i in range(paths_size):
            path = paths_between_one_pair_id[i]
            path_size = len(path)
            path_embedding = self.embedding(path)
            path_embedding = path_embedding.view(path_size, 1, self.input_dim)

            if torch.cuda.is_available():
                path_embedding = path_embedding.cuda()
            _, h = self.lstm(path_embedding)
            if i == 0:
                sum_hidden = h[0]
            else:
                sum_hidden = torch.cat((sum_hidden, h[0]), 1)
        print("SUM HIDDEN")
        print(sum_hidden.size(), sum_hidden)
        attention, _ = self.multihead_attention(sum_hidden, sum_hidden, sum_hidden)
        print("ATTENTION")
        print(attention.size(), attention)

        pool = nn.MaxPool2d((paths_size, 1), stride=(paths_size, 1))
        max_pool = pool(attention)
        out = self.linear(max_pool)
        out = F.sigmoid(out)

        print("MAX POOL")
        print(max_pool.size(), max_pool)
        print("OUT")
        print(out.size(), out)
        return out

