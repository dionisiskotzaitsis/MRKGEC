import numpy as np
import os
import random
import numpy as np

def embeddings(uFilem,writer):
    dictionary={}
    first_line = uFilem.readline()
    for i in uFilem:
        print(int(i.rstrip().strip('\"')))
        dictionary[int(i.rstrip().strip('\"'))] = np.zeros(20)
    for key,value in dictionary.items():
        writer.write(str(key)+'|'+" ".join(str(item) for item in value)+'\n')

if __name__ == '__main__':
    file_item=open('pre-train-item-embedding.txt', 'w+')
    file_user=open('pre-train-user-embedding.txt', 'w+')
    #Files that contain the ID of the users and businesses
    with open('ID_U.csv','r') as users,\
        open('ID_B.csv','r') as business:
        embeddings(users,file_user)
        embeddings(business,file_item)

    file_user.close()
    file_item.close()

