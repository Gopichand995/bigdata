from collections import OrderedDict

import vobject
import codecs
import re
import pandas as pd
import numpy as np

##################################################################################
# def parse_vcard(path):
#     with open(path, 'r') as f:
#         vcard = vobject.readOne(f.read())
#         return {vcard.contents['fn'][0].value: [tel.value for tel in vcard.contents['tel']]}


# contacts = parse_vcard('C:\\Users\\GopichandBarri\\Documents\\Github\\peace\\Data\\contacts_25_08_2021_356.vcf')
# print(contacts)
##################################################################################

# file = open('C:\\Users\\GopichandBarri\\Documents\\Github\\peace\\Data\\contacts_25_08_2021_356.vcf', 'r')
# names = list()
# contacts = list()
#
# for line in file:
#     name = re.findall('FN:(.*)', line)
#     nm = ''.join(name)
#     if len(nm) == 0:
#         continue
#     names.append(nm)
# print(len(names))
##################################################################################
path = 'C:\\Users\\GopichandBarri\\Documents\\Github\\peace\\Data\\contacts_15_03_22_384.vcf'
path1 = 'C:\\Users\\GopichandBarri\\Documents\\Github\\peace\\Data\\contacts_25_08_2021_356.vcf'
obj = vobject.readComponents(codecs.open(path, encoding='utf-8').read())
contacts = [contact for contact in obj]
# print(contacts)
contacts_dir = dict()
for i in range(len(contacts)):
    contact_name = contacts[i].contents['fn'][0].value
    contact_number = {(re.sub('[^A-Za-z0-9+]+', '', tel.value))[-10:] for tel in contacts[i].contents['tel']}
    contacts_dir[contact_name] = contact_number
contacts_dir = dict(OrderedDict(sorted(contacts_dir.items())))
# print(contacts_dir)
contacts_df = pd.DataFrame.from_dict(contacts_dir, orient='index').to_csv(
    'C:\\Users\\GopichandBarri\\Documents\\Github\\peace\\Data\\contacts_15_03_22_384.csv')



