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
                MATCH p=(u:User)-[a:WROTE_IN_DAY]-(d:Day)-[:DAY_OF_MONTH]-(m:Month)-[b:REVIEW_MONTH]-(bu:Business)
                WHERE u.id="{uid}" AND bu.id="{bid}" AND a.review_id=b.review_id
                RETURN ID(u),ID(d),ID(m),ID(bu) LIMIT 2
                """.format(uid=str(dictionary[i]['user_id']), bid=str(dictionary[i]['business_id']))
        results = driver.session().run(query2)
        values = results.values()
        if len(values) > 0:
            for p in values:
                path = ['u' + str(p[0]), 'd' + str(p[1]), 'm' + str(p[2]), 'i' + str(p[3])]
                paths.append(path)


        query2 = """
                MATCH p=(u:User)-[a:WROTE_IN_MONTH]-(d:Month)-[:PART_OF]-(m:Season)-[b:REVIEW_SEASON]-(bu:Business)
                WHERE u.id="{uid}" AND bu.id="{bid}" AND a.review_id=b.review_id
                RETURN ID(u),ID(d),ID(m),ID(bu) LIMIT 2
                """.format(uid=str(dictionary[i]['user_id']), bid=str(dictionary[i]['business_id']))
        results = driver.session().run(query2)
        values = results.values()
        if len(values) > 0:
            for p in values:
                path = ['u' + str(p[0]), 'm' + str(p[1]), 's' + str(p[2]), 'i' + str(p[3])]
                paths.append(path)

        '''query2 = """
                MATCH p=(u:User)-[a:WROTE_IN_DAY]-(d:Day)-[:REVIEW_DAY]-(bu:Business)-[:IN_CATEGORY]-(m:Category)
                WHERE u.id="{uid}" AND bu.id="{bid}" 
                RETURN ID(u),ID(d),ID(m),ID(bu) LIMIT 2
                """.format(uid=str(dictionary[i]['user_id']), bid=str(dictionary[i]['business_id']))
        results = driver.session().run(query2)
        values = results.values()
        if len(values) > 0:
            for p in values:
                path = ['u' + str(p[0]), 'd' + str(p[1]), 'g' + str(p[2]), 'i' + str(p[3])]
                paths.append(path)

        query2 = """
                MATCH p=(u:User)-[a:WROTE_IN_MONTH]-(d:Month)-[:REVIEW_MONTH]-(bu:Business)-[:IN_CATEGORY]-(m:Category)
                WHERE u.id="{uid}" AND bu.id="{bid}" 
                RETURN ID(u),ID(d),ID(m),ID(bu) LIMIT 2
                """.format(uid=str(dictionary[i]['user_id']), bid=str(dictionary[i]['business_id']))
        results = driver.session().run(query2)
        values = results.values()
        if len(values) > 0:
            for p in values:
                path = ['u' + str(p[0]), 'm' + str(p[1]), 'g' + str(p[2]), 'i' + str(p[3])]
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
                MATCH p=(u:User)-[a:WROTE_IN_DAY]-(d:Day)-[:DAY_OF_MONTH]-(m:Month)-[b:REVIEW_MONTH]-(bu:Business)
                WHERE u.id="{uid}" AND bu.id="{bid}"
                RETURN ID(u),ID(d),ID(m),ID(bu) LIMIT 2
                """.format(uid=str(dictionary[i]['user_id']), bid=str(dictionary[i]['business_id']))
        results = driver.session().run(query2)
        values = results.values()
        if len(values) > 0:
            for p in values:
                path = ['u' + str(p[0]), 'd' + str(p[1]), 'm' + str(p[2]), 'i' + str(p[3])]
                paths.append(path)

        query2 = """
                MATCH p=(u:User)-[a:WROTE_IN_MONTH]-(d:Month)-[:PART_OF]-(m:Season)-[b:REVIEW_SEASON]-(bu:Business)
                WHERE u.id="{uid}" AND bu.id="{bid}"
                RETURN ID(u),ID(d),ID(m),ID(bu) LIMIT 2
                """.format(uid=str(dictionary[i]['user_id']), bid=str(dictionary[i]['business_id']))
        results = driver.session().run(query2)
        values = results.values()
        if len(values) > 0:
            for p in values:
                path = ['u' + str(p[0]), 'm' + str(p[1]), 's' + str(p[2]), 'i' + str(p[3])]
                paths.append(path)

        '''query2 = """
                MATCH p=(u:User)-[a:WROTE_IN_DAY]-(d:Day)-[:REVIEW_DAY]-(bu:Business)-[:IN_CATEGORY]-(m:Category)
                WHERE u.id="{uid}" AND bu.id="{bid}" 
                RETURN ID(u),ID(d),ID(m),ID(bu) LIMIT 2
                """.format(uid=str(dictionary[i]['user_id']), bid=str(dictionary[i]['business_id']))
        results = driver.session().run(query2)
        values = results.values()
        if len(values) > 0:
            for p in values:
                path = ['u' + str(p[0]), 'd' + str(p[1]), 'g' + str(p[2]), 'i' + str(p[3])]
                paths.append(path)

        query2 = """
                MATCH p=(u:User)-[a:WROTE_IN_MONTH]-(d:Month)-[:REVIEW_MONTH]-(bu:Business)-[:IN_CATEGORY]-(m:Category)
                WHERE u.id="{uid}" AND bu.id="{bid}" 
                RETURN ID(u),ID(d),ID(m),ID(bu) LIMIT 2
                """.format(uid=str(dictionary[i]['user_id']), bid=str(dictionary[i]['business_id']))
        results = driver.session().run(query2)
        values = results.values()
        if len(values) > 0:
            for p in values:
                path = ['u' + str(p[0]), 'm' + str(p[1]), 'g' + str(p[2]), 'i' + str(p[3])]
                paths.append(path)
'''
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
        query2 = """
                MATCH p=(u:User)-[a:WROTE_IN_DAY]-(d:Day)-[:DAY_OF_MONTH]-(m:Month)-[:PART_OF]-(s:Season)-[b:REVIEW_SEASON]-(bu:Business)
                MATCH (u)-[z:WROTE_IN_MONTH]-(m)-[l:REVIEW_MONTH]-(bu)
                WHERE u.id="{uid}" AND bu.id="{bid}" AND a.review_id=b.review_id=z.review_id=l.review_id 
                RETURN ID(u),ID(d),ID(m),ID(s),ID(bu) LIMIT 2
                """.format(uid=str(dictionary[i]['user_id']), bid=str(dictionary[i]['business_id']))
        results = driver.session().run(query2)
        values = results.values()
        if len(values) > 0:
            for p in values:
                path = ['u' + str(p[0]), 'd' + str(p[1]), 'm' + str(p[2]), 's' + str(p[3]), 'i' + str(p[4])]
                paths.append(path)



        '''query2 = """
                MATCH p=(u:User)-[:REVIEW]-(s:Business)-[a:REVIEW]-(d:User)-[:WROTE_IN_DAY]-(m:Day)-[b:REVIEW_DAY]-(bu:Business)
                WHERE u.id="{uid}" AND bu.id="{bid}" 
                RETURN ID(u),ID(s),ID(d),ID(m),ID(bu) LIMIT 2
                """.format(uid=str(dictionary[i]['user_id']), bid=str(dictionary[i]['business_id']))
        results = driver.session().run(query2)
        values = results.values()
        if len(values) > 0:
            for p in values:
                path = ['u' + str(p[0]), 'i' + str(p[1]), 'u' + str(p[2]), 'd' + str(p[3]), 'i' + str(p[4])]
                paths.append(path)

        query2 = """
                MATCH p=(u:User)-[:REVIEW]-(s:Business)-[:REVIEW]-(d:User)-[:WROTE_IN_MONTH]-(m:Month)-[b:REVIEW_MONTH]-(bu:Business)
                WHERE u.id="{uid}" AND bu.id="{bid}" 
                RETURN ID(u),ID(s),ID(d),ID(m),ID(bu) LIMIT 2
                """.format(uid=str(dictionary[i]['user_id']), bid=str(dictionary[i]['business_id']))
        results = driver.session().run(query2)
        values = results.values()
        if len(values) > 0:
            for p in values:
                path = ['u' + str(p[0]), 'i' + str(p[1]), 'u' + str(p[2]), 'm' + str(p[3]), 'i' + str(p[4])]
                paths.append(path)'''

        # query2 = """
        #         MATCH p=(s:Business)-[:REVIEW]-(u:User)-[a:WROTE_IN_DAY]-(d:Day)-[:DAY_OF_MONTH]-(m:Month)-[b:REVIEW_MONTH]-(bu:Business)
        #         WHERE u.id="{uid}" AND bu.id="{bid}" AND a.review_id=b.review_id
        #         RETURN ID(u),ID(s),ID(d),ID(m),ID(bu) LIMIT 2
        #         """.format(uid=str(dictionary[i]['user_id']), bid=str(dictionary[i]['business_id']))
        # results = driver.session().run(query2)
        # values = results.values()
        # if len(values) > 0:
        #     for p in values:
        #         path = ['u' + str(p[0]), 'i' + str(p[1]), 'd' + str(p[2]), 'm' + str(p[3]), 'i' + str(p[4])]
        #         paths.append(path)
        #
        # query2 = """
        #         MATCH p=(s:Business)-[:REVIEW]-(u:User)-[a:WROTE_IN_MONTH]-(d:Month)-[:PART_OF]-(m:Season)-[b:REVIEW_SEASON]-(bu:Business)
        #         WHERE u.id="{uid}" AND bu.id="{bid}" AND a.review_id=b.review_id
        #         RETURN ID(u),ID(s),ID(d),ID(m),ID(bu) LIMIT 2
        #         """.format(uid=str(dictionary[i]['user_id']), bid=str(dictionary[i]['business_id']))
        # results = driver.session().run(query2)
        # values = results.values()
        # if len(values) > 0:
        #     for p in values:
        #         path = ['u' + str(p[0]), 'i' + str(p[1]), 'm' + str(p[2]), 's' + str(p[3]), 'i' + str(p[4])]
        #         paths.append(path)




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
        query2 = """
                MATCH p=(u:User)-[a:WROTE_IN_DAY]-(d:Day)-[:DAY_OF_MONTH]-(m:Month)-[:PART_OF]-(s:Season)-[b:REVIEW_SEASON]-(bu:Business)
                MATCH (u)-[z:WROTE_IN_MONTH]-(m)-[l:REVIEW_MONTH]-(bu)
                WHERE u.id="{uid}" AND bu.id="{bid}" 
                RETURN ID(u),ID(d),ID(m),ID(s),ID(bu) LIMIT 2
                """.format(uid=str(dictionary[i]['user_id']), bid=str(dictionary[i]['business_id']))
        results = driver.session().run(query2)
        values = results.values()
        if len(values) > 0:
            for p in values:
                path = ['u' + str(p[0]), 'd' + str(p[1]), 'm' + str(p[2]), 's' + str(p[3]), 'i' + str(p[4])]
                paths.append(path)

        '''query2 = """
                MATCH p=(u:User)-[:REVIEW]-(s:Business)-[a:REVIEW]-(d:User)-[:WROTE_IN_DAY]-(m:Day)-[b:REVIEW_DAY]-(bu:Business)
                WHERE u.id="{uid}" AND bu.id="{bid}" 
                RETURN ID(u),ID(s),ID(d),ID(m),ID(bu) LIMIT 2
                """.format(uid=str(dictionary[i]['user_id']), bid=str(dictionary[i]['business_id']))
        results = driver.session().run(query2)
        values = results.values()
        if len(values) > 0:
            for p in values:
                path = ['u' + str(p[0]), 'i' + str(p[1]), 'u' + str(p[2]), 'd' + str(p[3]), 'i' + str(p[4])]
                paths.append(path)

        query2 = """
                MATCH p=(u:User)-[:REVIEW]-(s:Business)-[:REVIEW]-(d:User)-[:WROTE_IN_MONTH]-(m:Month)-[b:REVIEW_MONTH]-(bu:Business)
                WHERE u.id="{uid}" AND bu.id="{bid}" 
                RETURN ID(u),ID(s),ID(d),ID(m),ID(bu) LIMIT 2
                """.format(uid=str(dictionary[i]['user_id']), bid=str(dictionary[i]['business_id']))
        results = driver.session().run(query2)
        values = results.values()
        if len(values) > 0:
            for p in values:
                path = ['u' + str(p[0]), 'i' + str(p[1]), 'u' + str(p[2]), 'm' + str(p[3]), 'i' + str(p[4])]
                paths.append(path)'''


        # query2 = """
        #         MATCH p=(s:Business)-[:REVIEW]-(u:User)-[a:WROTE_IN_DAY]-(d:Day)-[:DAY_OF_MONTH]-(m:Month)-[b:REVIEW_MONTH]-(bu:Business)
        #         WHERE u.id="{uid}" AND bu.id="{bid}"
        #         RETURN ID(u),ID(s),ID(d),ID(m),ID(bu) LIMIT 2
        #         """.format(uid=str(dictionary[i]['user_id']), bid=str(dictionary[i]['business_id']))
        # results = driver.session().run(query2)
        # values = results.values()
        # if len(values) > 0:
        #     for p in values:
        #         path = ['u' + str(p[0]), 'i' + str(p[1]), 'd' + str(p[2]), 'm' + str(p[3]), 'i' + str(p[4])]
        #         paths.append(path)
        #
        # query2 = """
        #         MATCH p=(s:Business)-[:REVIEW]-(u:User)-[a:WROTE_IN_MONTH]-(d:Month)-[:PART_OF]-(m:Season)-[b:REVIEW_SEASON]-(bu:Business)
        #         WHERE u.id="{uid}" AND bu.id="{bid}"
        #         RETURN ID(u),ID(s),ID(d),ID(m),ID(bu) LIMIT 2
        #         """.format(uid=str(dictionary[i]['user_id']), bid=str(dictionary[i]['business_id']))
        # results = driver.session().run(query2)
        # values = results.values()
        # if len(values) > 0:
        #     for p in values:
        #         path = ['u' + str(p[0]), 'i' + str(p[1]), 'm' + str(p[2]), 's' + str(p[3]), 'i' + str(p[4])]
        #         paths.append(path)

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
        query2 = """
                MATCH p=(u:User)-[a:WROTE_IN_DAY]-(d:Day)-[:DAY_OF_MONTH]-(m:Month)-[:PART_OF]-(s:Season)-[b:REVIEW_SEASON]-(bu:Business)-[:IN_CATEGORY]-(cat:Category)
                MATCH (u)-[z:WROTE_IN_MONTH]-(m)-[l:REVIEW_MONTH]-(bu)
                WHERE u.id="{uid}" AND bu.id="{bid}" AND a.review_id=b.review_id=z.review_id=l.review_id
                RETURN ID(u),ID(d),ID(m),ID(s),ID(cat),ID(bu) LIMIT 2
                """.format(uid=str(dictionary[i]['user_id']), bid=str(dictionary[i]['business_id']))
        results = driver.session().run(query2)
        values = results.values()
        if len(values) > 0:
            for p in values:
                path = ['u' + str(p[0]), 'd' + str(p[1]), 'm' + str(p[2]), 's' + str(p[3]), 'g' + str(p[4]),
                        'i' + str(p[5])]
                paths.append(path)

        query2 = """
                MATCH p=(u:User)-[a:WROTE_IN_DAY]-(d:Day)-[:DAY_OF_MONTH]-(m:Month)-[:PART_OF]-(s:Season)-[b:REVIEW_SEASON]-(bu:Business)-[:IN_CITY]-(cat:City)
                MATCH (u)-[z:WROTE_IN_MONTH]-(m)-[l:REVIEW_MONTH]-(bu)
                WHERE u.id="{uid}" AND bu.id="{bid}" AND a.review_id=b.review_id=z.review_id=l.review_id
                RETURN ID(u),ID(d),ID(m),ID(s),ID(cat),ID(bu) LIMIT 2
                """.format(uid=str(dictionary[i]['user_id']), bid=str(dictionary[i]['business_id']))
        results = driver.session().run(query2)
        values = results.values()
        if len(values) > 0:
            for p in values:
                path = ['u' + str(p[0]), 'd' + str(p[1]), 'm' + str(p[2]), 's' + str(p[3]), 'c' + str(p[4]),
                        'i' + str(p[5])]
                paths.append(path)

        # query2 = """
        #         MATCH p=(s:Business)-[:REVIEW]-(u:User)-[a:WROTE_IN_DAY]-(d:Day)-[:DAY_OF_MONTH]-(m:Month)-[:PART_OF]-(cat:Season)-[b:REVIEW_SEASON]-(bu:Business)
        #         MATCH (u)-[z:WROTE_IN_MONTH]-(m)-[l:REVIEW_MONTH]-(bu)
        #         WHERE u.id="{uid}" AND bu.id="{bid}" AND a.review_id=b.review_id=z.review_id=l.review_id
        #         RETURN ID(u),ID(s),ID(d),ID(m),ID(cat),ID(bu) LIMIT 2
        #         """.format(uid=str(dictionary[i]['user_id']), bid=str(dictionary[i]['business_id']))
        # results = driver.session().run(query2)
        # values = results.values()
        # if len(values) > 0:
        #     for p in values:
        #         path = ['u' + str(p[0]), 'i' + str(p[1]), 'd' + str(p[2]), 'm' + str(p[3]), 's' + str(p[4]), 'i'+str(p[5])]
        #         paths.append(path)

        '''query2 = """
                MATCH p=(u:User)-[:REVIEW]-(s:Business)-[:REVIEW]-(d:User)-[a:WROTE_IN_DAY]-(m:Day)-[:DAY_OF_MONTH]-(n:Month)-[b:REVIEW_MONTH]-(bu:Business)
                WHERE u.id="{uid}" AND bu.id="{bid}" AND a.review_id=b.review_id
                RETURN ID(u),ID(s),ID(d),ID(m),ID(n),ID(bu) LIMIT 2
                """.format(uid=str(dictionary[i]['user_id']), bid=str(dictionary[i]['business_id']))
        results = driver.session().run(query2)
        values = results.values()
        if len(values) > 0:
            for p in values:
                path = ['u' + str(p[0]), 'i' + str(p[1]), 'u' + str(p[2]), 'd' + str(p[3]), 'm' + str(p[4]), 'i'+str(p[5])]
                paths.append(path)

        query2 = """
                MATCH p=(u:User)-[:REVIEW]-(s:Business)-[:REVIEW]-(d:User)-[a:WROTE_IN_MONTH]-(m:Month)-[:PART_OF]-(n:Season)-[b:REVIEW_SEASON]-(bu:Business)
                WHERE u.id="{uid}" AND bu.id="{bid}" AND a.review_id=b.review_id
                RETURN ID(u),ID(s),ID(d),ID(m),ID(n),ID(bu) LIMIT 2
                """.format(uid=str(dictionary[i]['user_id']), bid=str(dictionary[i]['business_id']))
        results = driver.session().run(query2)
        values = results.values()
        if len(values) > 0:
            for p in values:
                path = ['u' + str(p[0]), 'i' + str(p[1]), 'u' + str(p[2]), 'm' + str(p[3]), 's' + str(p[4]), 'i'+str(p[5])]
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

        query2 = """
                MATCH p=(u:User)-[a:WROTE_IN_DAY]-(d:Day)-[:DAY_OF_MONTH]-(m:Month)-[:PART_OF]-(s:Season)-[b:REVIEW_SEASON]-(bu:Business)-[:IN_CATEGORY]-(cat:Category)
                WHERE u.id="{uid}" AND bu.id="{bid}" 
                RETURN ID(u),ID(d),ID(m),ID(s),ID(cat),ID(bu) LIMIT 2
                """.format(uid=str(dictionary[i]['user_id']), bid=str(dictionary[i]['business_id']))
        results = driver.session().run(query2)
        values = results.values()
        if len(values) > 0:
            for p in values:
                path = ['u' + str(p[0]), 'd' + str(p[1]), 'm' + str(p[2]), 's' + str(p[3]), 'g' + str(p[4]),
                        'i' + str(p[5])]
                paths.append(path)

        query2 = """
                MATCH p=(u:User)-[a:WROTE_IN_DAY]-(d:Day)-[:DAY_OF_MONTH]-(m:Month)-[:PART_OF]-(s:Season)-[b:REVIEW_SEASON]-(bu:Business)-[:IN_CITY]-(cat:City)
                WHERE u.id="{uid}" AND bu.id="{bid}"
                RETURN ID(u),ID(d),ID(m),ID(s),ID(cat),ID(bu) LIMIT 2
                """.format(uid=str(dictionary[i]['user_id']), bid=str(dictionary[i]['business_id']))
        results = driver.session().run(query2)
        values = results.values()
        if len(values) > 0:
            for p in values:
                path = ['u' + str(p[0]), 'd' + str(p[1]), 'm' + str(p[2]), 's' + str(p[3]), 'c' + str(p[4]),
                        'i' + str(p[5])]
                paths.append(path)

        # query2 = """
        #         MATCH p=(s:Business)-[:REVIEW]-(u:User)-[a:WROTE_IN_DAY]-(d:Day)-[:DAY_OF_MONTH]-(m:Month)-[:PART_OF]-(cat:Season)-[b:REVIEW_SEASON]-(bu:Business)
        #         WHERE u.id="{uid}" AND bu.id="{bid}"
        #         RETURN ID(u),ID(s),ID(d),ID(m),ID(cat),ID(bu) LIMIT 2
        #         """.format(uid=str(dictionary[i]['user_id']), bid=str(dictionary[i]['business_id']))
        # results = driver.session().run(query2)
        # values = results.values()
        # if len(values) > 0:
        #     for p in values:
        #         path = ['u' + str(p[0]), 'i' + str(p[1]), 'd' + str(p[2]), 'm' + str(p[3]), 's' + str(p[4]),
        #                 'i' + str(p[5])]
        #         paths.append(path)

        '''query2 = """
                MATCH p=(u:User)-[:REVIEW]-(s:Business)-[:REVIEW]-(d:User)-[a:WROTE_IN_DAY]-(m:Day)-[:DAY_OF_MONTH]-(n:Month)-[b:REVIEW_MONTH]-(bu:Business)
                WHERE u.id="{uid}" AND bu.id="{bid}"
                RETURN ID(u),ID(s),ID(d),ID(m),ID(n),ID(bu) LIMIT 2
                """.format(uid=str(dictionary[i]['user_id']), bid=str(dictionary[i]['business_id']))
        results = driver.session().run(query2)
        values = results.values()
        if len(values) > 0:
            for p in values:
                path = ['u' + str(p[0]), 'i' + str(p[1]), 'u' + str(p[2]), 'd' + str(p[3]), 'm' + str(p[4]),
                        'i' + str(p[5])]
                paths.append(path)

        query2 = """
                MATCH p=(u:User)-[:REVIEW]-(s:Business)-[:REVIEW]-(d:User)-[a:WROTE_IN_MONTH]-(m:Month)-[:PART_OF]-(n:Season)-[b:REVIEW_SEASON]-(bu:Business)
                WHERE u.id="{uid}" AND bu.id="{bid}"
                RETURN ID(u),ID(s),ID(d),ID(m),ID(n),ID(bu) LIMIT 2
                """.format(uid=str(dictionary[i]['user_id']), bid=str(dictionary[i]['business_id']))
        results = driver.session().run(query2)
        values = results.values()
        if len(values) > 0:
            for p in values:
                path = ['u' + str(p[0]), 'i' + str(p[1]), 'u' + str(p[2]), 'm' + str(p[3]), 's' + str(p[4]),
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
    timeFile=open('timeExtractionTemporal.txt','a+')
    ######### CHANGE THE FILEPATH FOR EACH LEN OF PATH ##################
    #####################################################################
    positive_path='paths_Temporal/positive_path_5.txt'
    negative_path='paths_Temporal/negative_path_5.txt'
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




