from neo4j import GraphDatabase
from datetime import datetime
import os
import multiprocessing as mp
import json
from random import sample
from datetime import datetime


def splitter(file,folder):
    with open(file+'.json', 'r',encoding='utf-8') as infile:
        o = json.load(infile)
        keys = list(o.keys())
        n = len(keys) // 10
        flag=1
        for i in range(0, len(keys), n):
            with open(folder+'/'+file+'_'+str(flag)+'.json','w') as out:
                json.dump({k: o[k] for k in keys[i: i + n]},out)
                flag=flag+1
              

def listener(q,file):
    with open(file, 'w') as f:
        while 1:
            m = q.get()
            if m == 'kill':
                #f.write('killed')
                break
            f.write(m)
            f.flush()



def positivePath3(flag,q):
    with open('jsons/reviewDictChronological_TRAIN_'+str(flag)+'.json','r') as file:
        dictionary=json.load(file)
    driver = GraphDatabase.driver('bolt://localhost:7687', auth=("username", "pass"), database="db")
    for i in dictionary:
        paths = []

        query2 = """
                MATCH p=(u:User)-[:REVIEW]-(bu:Business)-[:IN_CITY]-(d:City)-[:IN]-(m:Area)
                WHERE u.id="{uid}" AND bu.id="{bid}" 
                RETURN ID(u),ID(d),ID(m),ID(bu) LIMIT 1
                """.format(uid=str(dictionary[i]['user_id']), bid=str(dictionary[i]['business_id']))
        results = driver.session().run(query2)
        values = results.values()
        if len(values) > 0:
            for p in values:
                path = ['u' + str(p[0]), 'c' + str(p[1]), 'a' + str(p[2]), 'i' + str(p[3])]
                paths.append(path)

        '''query2 = """
                MATCH p=(u:User)-[:REVIEW]-(bu:Business)-[:IN_CITY]-(d:City)
                MATCH q=(bu)-[:IN_CATEGORY]-(m:Category)
                WHERE u.id="{uid}" AND bu.id="{bid}" 
                RETURN ID(u),ID(d),ID(m),ID(bu) LIMIT 1
                """.format(uid=str(dictionary[i]['user_id']), bid=str(dictionary[i]['business_id']))
        results = driver.session().run(query2)
        values = results.values()
        if len(values) > 0:
            for p in values:
                path = ['u' + str(p[0]), 'c' + str(p[1]), 'g' + str(p[2]), 'i' + str(p[3])]
                paths.append(path)'''

        '''query2 = """
                MATCH p=(u:User)-[:REVIEW]-(d:Business)-[:IN_CITY]-(m:City)-[:IN_CITY]-(bu:Business)
                WHERE u.id="{uid}" AND bu.id="{bid}" 
                RETURN ID(u),ID(d),ID(m),ID(bu) LIMIT 1
                """.format(uid=str(dictionary[i]['user_id']), bid=str(dictionary[i]['business_id']))
        results = driver.session().run(query2)
        values = results.values()
        if len(values) > 0:
            for p in values:
                path = ['u' + str(p[0]), 'i' + str(p[1]), 'c' + str(p[2]), 'i' + str(p[3])]
                paths.append(path)'''

        '''query2 = """
                MATCH p=(u:User)-[:REVIEW]-(d:Business)-[:IN_AREA]-(m:Area)-[:IN_AREA]-(bu:Business)
                WHERE u.id="{uid}" AND bu.id="{bid}" 
                RETURN ID(u),ID(d),ID(m),ID(bu) LIMIT 1
                """.format(uid=str(dictionary[i]['user_id']), bid=str(dictionary[i]['business_id']))
        results = driver.session().run(query2)
        values = results.values()
        if len(values) > 0:
            for p in values:
                path = ['u' + str(p[0]), 'i' + str(p[1]), 'a' + str(p[2]), 'i' + str(p[3])]
                paths.append(path)'''

        lenghtOfList=min(len(paths),5)
        samp=sample(paths,lenghtOfList)
        for path in samp:
            line = str(path[0]) + ',' + str(path[1]) + ',' + str(path[2]) + ',' + str(path[3]) + '\n'
            print(line)
            q.put(line)


def negativePath3(flag,q):
    with open('jsonsNeg/negativeSampling_'+str(flag)+'.json','r') as file:
        dictionary=json.load(file)
    driver = GraphDatabase.driver('bolt://localhost:7687', auth=("username", "pass"), database="db")
    for i in dictionary:
        paths = []

        query2 = """
                MATCH p=(u:User)
                MATCH q=(bu:Business)-[:IN_CITY]-(d:City)-[:IN]-(m:Area)
                WHERE u.id="{uid}" AND bu.id="{bid}" 
                RETURN ID(u),ID(d),ID(m),ID(bu) LIMIT 1
                """.format(uid=str(dictionary[i]['user_id']), bid=str(dictionary[i]['business_id']))
        results = driver.session().run(query2)
        values = results.values()
        if len(values) > 0:
            for p in values:
                path = ['u' + str(p[0]), 'c' + str(p[1]), 'a' + str(p[2]), 'i' + str(p[3])]
                paths.append(path)

        '''query2 = """
                MATCH p=(u:User)
                MATCH q=(d:City)-[:IN_CITY]-(bu:Business)-[:IN_CATEGORY]-(m:Category)
                WHERE u.id="{uid}" AND bu.id="{bid}" 
                RETURN ID(u),ID(d),ID(m),ID(bu) LIMIT 1
                """.format(uid=str(dictionary[i]['user_id']), bid=str(dictionary[i]['business_id']))
        results = driver.session().run(query2)
        values = results.values()
        if len(values) > 0:
            for p in values:
                path = ['u' + str(p[0]), 'c' + str(p[1]), 'g' + str(p[2]), 'i' + str(p[3])]
                paths.append(path)'''

        '''query2 = """
                MATCH p=(u:User)-[:REVIEW]-(d:Business)-[:IN_CITY]-(m:City)-[:IN_CITY]-(bu:Business)
                WHERE u.id="{uid}" AND bu.id="{bid}" 
                RETURN ID(u),ID(d),ID(m),ID(bu) LIMIT 1
                """.format(uid=str(dictionary[i]['user_id']), bid=str(dictionary[i]['business_id']))
        results = driver.session().run(query2)
        values = results.values()
        if len(values) > 0:
            for p in values:
                path = ['u' + str(p[0]), 'i' + str(p[1]), 'c' + str(p[2]), 'i' + str(p[3])]
                paths.append(path)

        query2 = """
                MATCH p=(u:User)-[:FRIENDS]-(d:User)-[:REVIEW]-(bu:Business)-[:IN_CITY]-(m:City)
                WHERE u.id="{uid}" AND bu.id="{bid}" 
                RETURN ID(u),ID(d),ID(m),ID(bu) ORDER BY d.review_count DESC LIMIT 1
                """.format(uid=str(dictionary[i]['user_id']), bid=str(dictionary[i]['business_id']))
        results = driver.session().run(query2)
        values = results.values()
        if len(values) > 0:
            for p in values:
                path = ['u' + str(p[0]), 'u' + str(p[1]), 'c' + str(p[2]), 'i' + str(p[3])]
                paths.append(path)'''

        '''query2 = """
                MATCH p=(u:User)-[:REVIEW]-(d:Business)-[:IN_AREA]-(m:Area)-[:IN_AREA]-(bu:Business)
                WHERE u.id="{uid}" AND bu.id="{bid}" 
                RETURN ID(u),ID(d),ID(m),ID(bu) LIMIT 1
                """.format(uid=str(dictionary[i]['user_id']), bid=str(dictionary[i]['business_id']))
        results = driver.session().run(query2)
        values = results.values()
        if len(values) > 0:
            for p in values:
                path = ['u' + str(p[0]), 'i' + str(p[1]), 'a' + str(p[2]), 'i' + str(p[3])]
                paths.append(path)'''

        lenghtOfList = min(len(paths), 5)
        samp = sample(paths, lenghtOfList)
        for path in samp:
            line = str(path[0]) + ',' + str(path[1]) + ',' + str(path[2]) + ',' + str(path[3]) + '\n'
            print(line)
            q.put(line)


def positivePath4(flag,q):
    with open('jsons/reviewDictChronological_TRAIN_'+str(flag)+'.json','r') as file:
        dictionary=json.load(file)
    driver = GraphDatabase.driver('bolt://localhost:7687', auth=("username", "pass"), database="db")
    for i in dictionary:
        paths = []
        '''query2 = """
                MATCH p=(u:User)-[:REVIEW]-(bu:Business)-[:IN_CITY]-(d:City)-[:IN]-(m:Area)
                MATCH q=(bu)-[:IN_CATEGORY]-(s:Category)
                WHERE u.id="{uid}" AND bu.id="{bid}"
                RETURN ID(u),ID(d),ID(m),ID(s),ID(bu) LIMIT 1
                """.format(uid=str(dictionary[i]['user_id']), bid=str(dictionary[i]['business_id']))
        results = driver.session().run(query2)
        values = results.values()
        if len(values) > 0:
            for p in values:
                path = ['u' + str(p[0]), 'c' + str(p[1]), 'a' + str(p[2]), 'g' + str(p[3]), 'i' + str(p[4])]
                paths.append(path)'''

        query2 = """
                MATCH p=(u:User)-[:REVIEW]-(d:Business)-[:IN_CITY]-(m:City)-[:IN]-(s:Area)
                MATCH q=(bu:Business)-[:IN_CITY]-(m)-[:IN]-(s)
                WHERE u.id="{uid}" AND bu.id="{bid}"
                RETURN ID(u),ID(d),ID(m),ID(s),ID(bu) LIMIT 1
                """.format(uid=str(dictionary[i]['user_id']), bid=str(dictionary[i]['business_id']))
        results = driver.session().run(query2)
        values = results.values()
        if len(values) > 0:
            for p in values:
                path = ['u' + str(p[0]), 'i' + str(p[1]), 'c' + str(p[2]), 'a' + str(p[3]), 'i' + str(p[4])]
                paths.append(path)

        '''query2 = """
                MATCH p=(u:User)-[:REVIEW]-(d:Business)-[:IN_CATEGORY]-(m:Category)-[:IN_CATEGORY]-(bu:Business)-[:IN_CITY]-(s:City)
                WHERE u.id="{uid}" AND bu.id="{bid}"
                RETURN ID(u),ID(d),ID(m),ID(s),ID(bu) LIMIT 1
                """.format(uid=str(dictionary[i]['user_id']), bid=str(dictionary[i]['business_id']))
        results = driver.session().run(query2)
        values = results.values()
        if len(values) > 0:
            for p in values:
                path = ['u' + str(p[0]), 'i' + str(p[1]), 'g' + str(p[2]), 'c' + str(p[3]), 'i' + str(p[4])]
                paths.append(path)'''




        lenghtOfList=min(len(paths),5)
        samp=sample(paths,lenghtOfList)
        for path in samp:
            line = str(path[0]) + ',' + str(path[1]) + ',' + str(path[2]) + ',' + str(path[3]) + ',' + str(path[4]) + '\n'
            print(line)
            q.put(line)


def negativePath4(flag,q):
    with open('jsonsNeg/negativeSampling_' + str(flag) + '.json', 'r') as file:
        dictionary = json.load(file)
    driver = GraphDatabase.driver('bolt://localhost:7687', auth=("username", "pass"), database="db")
    for i in dictionary:
        paths = []
        '''query2 = """
                MATCH p=(u:User)-[:REVIEW]-(bu:Business)-[:IN_CITY]-(d:City)-[:IN]-(m:Area)
                MATCH q=(bu)-[:IN_CATEGORY]-(s:Category)
                WHERE u.id="{uid}" AND bu.id="{bid}"
                RETURN ID(u),ID(d),ID(m),ID(s),ID(bu) LIMIT 1
                """.format(uid=str(dictionary[i]['user_id']), bid=str(dictionary[i]['business_id']))
        results = driver.session().run(query2)
        values = results.values()
        if len(values) > 0:
            for p in values:
                path = ['u' + str(p[0]), 'c' + str(p[1]), 'a' + str(p[2]), 'g' + str(p[3]), 'i' + str(p[4])]
                paths.append(path)'''

        query2 = """
                MATCH p=(u:User)-[:REVIEW]-(d:Business)-[:IN_CITY]-(m:City)-[:IN]-(s:Area)
                MATCH q=(bu:Business)-[:IN_CITY]-(m)-[:IN]-(s)
                WHERE u.id="{uid}" AND bu.id="{bid}"
                RETURN ID(u),ID(d),ID(m),ID(s),ID(bu) LIMIT 1
                """.format(uid=str(dictionary[i]['user_id']), bid=str(dictionary[i]['business_id']))
        results = driver.session().run(query2)
        values = results.values()
        if len(values) > 0:
            for p in values:
                path = ['u' + str(p[0]), 'i' + str(p[1]), 'c' + str(p[2]), 'a' + str(p[3]), 'i' + str(p[4])]
                paths.append(path)

        '''query2 = """
                MATCH p=(u:User)-[:REVIEW]-(d:Business)-[:IN_CATEGORY]-(m:Category)-[:IN_CATEGORY]-(bu:Business)-[:IN_CITY]-(s:City)
                WHERE u.id="{uid}" AND bu.id="{bid}"
                RETURN ID(u),ID(d),ID(m),ID(s),ID(bu) LIMIT 1
                """.format(uid=str(dictionary[i]['user_id']), bid=str(dictionary[i]['business_id']))
        results = driver.session().run(query2)
        values = results.values()
        if len(values) > 0:
            for p in values:
                path = ['u' + str(p[0]), 'i' + str(p[1]), 'g' + str(p[2]), 'c' + str(p[3]), 'i' + str(p[4])]
                paths.append(path)'''



        lenghtOfList = min(len(paths), 5)
        samp = sample(paths, lenghtOfList)
        for path in samp:
            line = str(path[0]) + ',' + str(path[1]) + ',' + str(path[2]) + ',' + str(path[3]) + ',' + str(
                path[4]) + '\n'
            print(line)
            q.put(line)



def positivePath5(flag,q):
    with open('jsons/reviewDictChronological_TRAIN_'+str(flag)+'.json','r') as file:
        dictionary=json.load(file)
    driver = GraphDatabase.driver('bolt://localhost:7687', auth=("username", "pass"), database="db")
    for i in dictionary:
        paths = []

        '''query2 = """
                MATCH p=(u:User)-[:REVIEW]-(s:Business)-[:IN_CITY]-(d:City)-[:IN_CITY]-(m:Business)-[:IN_CITY]-(ci:City)-[:IN_CITY]-(bu:Business)
                WHERE u.id="{uid}" AND bu.id="{bid}"
                RETURN ID(u),ID(s),ID(d),ID(m),ID(ci),ID(bu) ORDER BY s.review_count LIMIT 1
                """.format(uid=str(dictionary[i]['user_id']), bid=str(dictionary[i]['business_id']))
        results = driver.session().run(query2)
        values = results.values()
        if len(values) > 0:
            for p in values:
                path = ['u' + str(p[0]), 'i' + str(p[1]), 'c' + str(p[2]), 'i' + str(p[3]), 'c' + str(p[4]),
                        'i' + str(p[5])]
                paths.append(path)'''

        query2 = """
                MATCH p=(u:User)-[:REVIEW]-(s:Business)-[:IN_AREA]-(d:Area)-[:IN_AREA]-(m:Business)-[:IN_CITY]-(ci:City)-[:IN_CITY]-(bu:Business)
                WHERE u.id="{uid}" AND bu.id="{bid}"
                RETURN ID(u),ID(s),ID(d),ID(m),ID(ci),ID(bu) LIMIT 1
                """.format(uid=str(dictionary[i]['user_id']), bid=str(dictionary[i]['business_id']))
        results = driver.session().run(query2)
        values = results.values()
        if len(values) > 0:
            for p in values:
                path = ['u' + str(p[0]), 'i' + str(p[1]), 'a' + str(p[2]), 'i' + str(p[3]), 'c' + str(p[4]),
                        'i' + str(p[5])]
                paths.append(path)



        '''query2 = """
                MATCH p=(u:User)-[:REVIEW]-(s:Business)-[:REVIEW]-(d:User)-[:REVIEW]-(m:Business)-[:IN_CITY]-(ci:City)-[:IN_CITY]-(bu:Business)
                WHERE u.id="{uid}" AND bu.id="{bid}"
                RETURN ID(u),ID(s),ID(d),ID(m),ID(ci),ID(bu) ORDER BY s.review_count LIMIT 1
                """.format(uid=str(dictionary[i]['user_id']), bid=str(dictionary[i]['business_id']))
        results = driver.session().run(query2)
        values = results.values()
        if len(values) > 0:
            for p in values:
                path = ['u' + str(p[0]), 'i' + str(p[1]), 'u' + str(p[2]), 'i' + str(p[3]), 'c' + str(p[4]),
                        'i' + str(p[5])]
                paths.append(path)
        query2 = """
                MATCH p=(u:User)-[:REVIEW]-(s:Business)-[:REVIEW]-(d:User)-[:REVIEW]-(m:Business)-[:IN_AREA]-(ci:Area)-[:IN_AREA]-(bu:Business)
                WHERE u.id="{uid}" AND bu.id="{bid}"
                RETURN ID(u),ID(s),ID(d),ID(m),ID(ci),ID(bu) ORDER BY s.review_count LIMIT 1
                """.format(uid=str(dictionary[i]['user_id']), bid=str(dictionary[i]['business_id']))
        results = driver.session().run(query2)
        values = results.values()
        if len(values) > 0:
            for p in values:
                path = ['u' + str(p[0]), 'i' + str(p[1]), 'u' + str(p[2]), 'i' + str(p[3]), 'a' + str(p[4]),
                        'i' + str(p[5])]
                paths.append(path)'''



        lenghtOfList=min(len(paths),5)
        samp=sample(paths,lenghtOfList)
        for path in samp:
            line = str(path[0]) + ',' + str(path[1]) + ',' + str(path[2]) + ',' + str(path[3]) + ',' + str(path[4]) +  ',' + str(path[5]) + '\n'
            print(line)
            q.put(line)


def negativePath5(flag,q):
    with open('jsonsNeg/negativeSampling_' + str(flag) + '.json', 'r') as file:
        dictionary = json.load(file)
    driver = GraphDatabase.driver('bolt://localhost:7687', auth=("username", "pass"), database="db")
    for i in dictionary:
        paths = []

        '''query2 = """
                MATCH p=(u:User)-[:REVIEW]-(s:Business)-[:IN_CITY]-(d:City)-[:IN_CITY]-(m:Business)-[:IN_CITY]-(ci:City)-[:IN_CITY]-(bu:Business)
                WHERE u.id="{uid}" AND bu.id="{bid}"
                RETURN ID(u),ID(s),ID(d),ID(m),ID(ci),ID(bu) ORDER BY s.review_count LIMIT 1
                """.format(uid=str(dictionary[i]['user_id']), bid=str(dictionary[i]['business_id']))
        results = driver.session().run(query2)
        values = results.values()
        if len(values) > 0:
            for p in values:
                path = ['u' + str(p[0]), 'i' + str(p[1]), 'c' + str(p[2]), 'i' + str(p[3]), 'c' + str(p[4]),
                        'i' + str(p[5])]
                paths.append(path)'''

        query2 = """
                MATCH p=(u:User)-[:REVIEW]-(s:Business)-[:IN_AREA]-(d:Area)-[:IN_AREA]-(m:Business)-[:IN_CITY]-(ci:City)-[:IN_CITY]-(bu:Business)
                WHERE u.id="{uid}" AND bu.id="{bid}"
                RETURN ID(u),ID(s),ID(d),ID(m),ID(ci),ID(bu) LIMIT 1
                """.format(uid=str(dictionary[i]['user_id']), bid=str(dictionary[i]['business_id']))
        results = driver.session().run(query2)
        values = results.values()
        if len(values) > 0:
            for p in values:
                path = ['u' + str(p[0]), 'i' + str(p[1]), 'a' + str(p[2]), 'i' + str(p[3]), 'c' + str(p[4]),
                        'i' + str(p[5])]
                paths.append(path)

        '''query2 = """
                MATCH p=(u:User)-[:REVIEW]-(s:Business)-[:REVIEW]-(d:User)-[:REVIEW]-(m:Business)-[:IN_CITY]-(ci:City)-[:IN_CITY]-(bu:Business)
                WHERE u.id="{uid}" AND bu.id="{bid}"
                RETURN ID(u),ID(s),ID(d),ID(m),ID(ci),ID(bu) ORDER BY s.review_count LIMIT 1
                """.format(uid=str(dictionary[i]['user_id']), bid=str(dictionary[i]['business_id']))
        results = driver.session().run(query2)
        values = results.values()
        if len(values) > 0:
            for p in values:
                path = ['u' + str(p[0]), 'i' + str(p[1]), 'u' + str(p[2]), 'i' + str(p[3]), 'c' + str(p[4]),
                        'i' + str(p[5])]
                paths.append(path)

        query2 = """
                MATCH p=(u:User)-[:REVIEW]-(s:Business)-[:REVIEW]-(d:User)-[:REVIEW]-(m:Business)-[:IN_AREA]-(ci:Area)-[:IN_AREA]-(bu:Business)
                WHERE u.id="{uid}" AND bu.id="{bid}"
                RETURN ID(u),ID(s),ID(d),ID(m),ID(ci),ID(bu) ORDER BY s.review_count LIMIT 1
                """.format(uid=str(dictionary[i]['user_id']), bid=str(dictionary[i]['business_id']))
        results = driver.session().run(query2)
        values = results.values()
        if len(values) > 0:
            for p in values:
                path = ['u' + str(p[0]), 'i' + str(p[1]), 'u' + str(p[2]), 'i' + str(p[3]), 'a' + str(p[4]),
                        'i' + str(p[5])]
                paths.append(path)'''

        lenghtOfList = min(len(paths), 5)
        samp = sample(paths, lenghtOfList)
        for path in samp:
            line = str(path[0]) + ',' + str(path[1]) + ',' + str(path[2]) + ',' + str(path[3]) + ',' + str(
                path[4]) + ',' + str(path[5]) + '\n'
            print(line)
            q.put(line)



if __name__ == '__main__':


    if not os.path.isfile("jsons/reviewDictChronological_TRAIN_1.json"):
        splitter("reviewDictChronological_TRAIN",'jsons')

    if not os.path.isfile("jsonsNeg/negativeSampling_1.json"):
        splitter("negativeSampling",'jsonsNeg')
    timeFile=open('paths_Spatial_less/timeExtractionSpatial.txt','a+')
    ######### CHANGE THE FILEPATH FOR EACH LEN OF PATH ##################
    #####################################################################
    positive_path='paths_Spatial_less/positive_path_5.txt'
    negative_path='paths_Spatial_less/negative_path_5.txt'
    #####################################################################
    #####################################################################

    ##################################POSITIVE PATHS########################################
    numOfWorkers = len([entry for entry in os.listdir("jsons") if os.path.isfile(os.path.join("jsons", entry))])
    manager=mp.Manager()
    q = manager.Queue()
    pool = mp.Pool(numOfWorkers)

    startTime=datetime.now()
    print("POSITIVE PATHS")
    watcher = pool.apply_async(listener, (q, positive_path))
    jobs = []
    for i in range(1,numOfWorkers+1):
        ######### CHANGE THE DEF NAME FOR EACH LEN OF PATH #########
        ############################################################
        ############################################################
        job = pool.apply_async(positivePath5, args=(str(i),q))
        ############################################################
        ############################################################
        ############################################################
        jobs.append(job)

    for job in jobs:
        job.get()

    # now we are done, kill the listener
    q.put('kill')
    pool.close()
    pool.join()

    #################################NEGATIVE PATHS##########################################
    print("NEGATIVE PATHS")
    numOfWorkers = len([entry for entry in os.listdir("jsonsNeg") if os.path.isfile(os.path.join("jsonsNeg", entry))])
    manager = mp.Manager()
    q = manager.Queue()
    pool = mp.Pool(numOfWorkers)
    watcher = pool.apply_async(listener, (q, negative_path))
    jobs = []
    for i in range(1, numOfWorkers + 1):
        ######### CHANGE THE DEF NAME FOR EACH LEN OF PATH #########
        ############################################################
        ############################################################
        job = pool.apply_async(negativePath5, args=(str(i), q))
        ############################################################
        ############################################################
        ############################################################
        jobs.append(job)

    for job in jobs:
        job.get()

    # now we are done, kill the listener
    q.put('kill')
    pool.close()
    pool.join()
    endTime=datetime.now()
    duration=endTime-startTime
    writer='Extraction of paths of length 5: '+str(duration)+'\n'
    timeFile.write(writer)
    timeFile.close()



