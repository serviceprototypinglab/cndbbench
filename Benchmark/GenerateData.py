import random
import json
from loremipsum import generate_paragraphs

# Options of multi tenants
# A: Shared DBMS, Shared database, shared schema, shared table/collection
# B: Shared DBMS, Shared database, shared schema, one table/collection per tenant
# C: Shared DBMS, Shared database, one schema per tenant
# D: Shared DBMS, one database per tenant
# E: One DBMS per tenant
#  F: No multitenant options (by default)


def write_results(results, name):
    try:
        f = open(name, "w")
        json.dump(results, f)
        f.close()
    except Exception, e:
        print e
        print "error saving results in " + name


def write_blob(text, name):
    try:
        f = open(name, "w")
        f.write(text)
        f.close()
    except Exception, e:
        print e
        print "error saving results in " + name


def create_tenants_json(number_tenants, multi_tenant_option, prefix):
    res_json = []
    for user in range(0, number_tenants):
        aux_json = {'user': user, 'option': multi_tenant_option, 'tenant': user, 'path': "", 'pass': user,
                    'port': 30010 + user, 'other_id': user}
        res_json.append(aux_json)
    if multi_tenant_option == 'A' \
            or multi_tenant_option == 'B' \
            or multi_tenant_option == 'C' \
            or multi_tenant_option == 'D' \
            or multi_tenant_option == 'E':
        write_results(res_json, '../sharedData/data/' + prefix + 'Users.json')
    else:
        write_results(res_json, '../sharedData/data/users.json')
    return res_json


def get_blob():
    res = ''
    for p in generate_paragraphs(10, False):
        res += p[2]
    return res


def create_aux_json(prefix_name, multi_tenant_option, count, user):
    blob = get_blob()
    name = blob[:5]
    path = '/data/blobs/' + prefix_name + '_' + multi_tenant_option + '_' + str(count) + '_' + name + '.txt'
    number_aux = random.randint(1, number_tenants_10 / 2)
    aux_json = {'title': name,
                'tenant': user,
                'user': user,
                'name': name,
                'tenant_option': multi_tenant_option,
                'other_id': count,
                'path': path,
                'blob': blob,
                'number': number_aux}
    # TODO: Next line is for create the json files
    # aux_path = '../sharedData' + path
    # write_blob(blob, aux_path)
    return aux_json


def create_blobs_json(number_tenants, rows_per_tenant, multi_tenant_option, prefix_name):
    res_json = []
    count = 0
    if multi_tenant_option == 'A':
        res_json = []
        for user in range(0, number_tenants):
            for j in range(0, rows_per_tenant):
                aux_json = create_aux_json(prefix_name, multi_tenant_option, count, user)
                res_json.append(aux_json)
                count += 1
        end_path = '../sharedData/data/' + prefix_name + '_' + multi_tenant_option + '.json'
        write_results(res_json, end_path)
    elif multi_tenant_option == 'B':
        for user in range(0, number_tenants):
            res_json = []
            for j in range(0, rows_per_tenant):
                aux_json = create_aux_json(prefix_name, multi_tenant_option, count, user)
                res_json.append(aux_json)
                count += 1
            end_path = '../sharedData/data/' + prefix_name + '_' + multi_tenant_option + '_user_' + str(user) + '.json'
            write_results(res_json, end_path)
    elif multi_tenant_option == 'C':
        for user in range(0, number_tenants):
            res_json = []
            for j in range(0, rows_per_tenant):
                aux_json = create_aux_json(prefix_name, multi_tenant_option, count, user)
                res_json.append(aux_json)
                count += 1
            end_path = '../sharedData/data/' + prefix_name + '_' + multi_tenant_option + '_user_' + str(user) + '.json'
            write_results(res_json, end_path)
    elif multi_tenant_option == 'D':
        for user in range(0, number_tenants):
            res_json = []
            for j in range(0, rows_per_tenant):
                aux_json = create_aux_json(prefix_name, multi_tenant_option, count, user)
                res_json.append(aux_json)
                count += 1
            end_path = '../sharedData/data/' + prefix_name + '_' + multi_tenant_option + '_user_' + str(user) + '.json'
            write_results(res_json, end_path)
    elif multi_tenant_option == 'E':
        for user in range(0, number_tenants):
            res_json = []
            for j in range(0, rows_per_tenant):
                aux_json = create_aux_json(prefix_name, multi_tenant_option, count, user)
                res_json.append(aux_json)
                count += 1
            end_path = '../sharedData/data/' + prefix_name + '_' + multi_tenant_option + '_user_' + str(user) + '.json'
            write_results(res_json, end_path)
    else:
        res_json = []
        for user in range(0, number_tenants):
            for j in range(0, rows_per_tenant):
                aux_json = create_aux_json(prefix_name, multi_tenant_option, count, user)
                res_json.append(aux_json)
                count += 1
        end_path = '../sharedData/data/documents.json'
        write_results(res_json, end_path)
    return res_json


multi_tenant_option_A = 'A'
multi_tenant_option_B = 'B'
multi_tenant_option_C = 'C'
multi_tenant_option_D = 'D'
multi_tenant_option_E = 'E'

number_tenants_10 = 10
rows_per_tenant_100 = 100

print "start"

with open('config.json') as data_file:
    data = json.load(data_file)

multi_tenant_option1 = data['generate_data_mt_option']
rows_per_tenant1 = data['generate_data_number_tenants']
number_tenants1 = data['generate_data_number_rows_per_tenants']
prefix1 = data['generate_data_prefix_multitenant']

create_tenants_json(number_tenants1, multi_tenant_option1,  prefix1)
create_blobs_json(number_tenants1, rows_per_tenant1, multi_tenant_option1, prefix1)

print "end"
