#! /usr/bin/python3
## usearch3
## LDAP3 version

import ldap3
import argparse
import sys

server = ldap3.Server('ldaps://foo.bar.baz')
conn = ldap3.Connection(server)
conn.bind()
basedn = 'dc=foobar,dc=dorq,dc=baz'
attribs = ['mail', 'uid', 'cn']

parser = argparse.ArgumentParser(
    description='searches the LDAP server and returns user, full name and email. Accepts any partial entry',
    usage='usearch3 PARTIAL_MATCH (email, name, username)',
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('istr', help='searches stuffz')
parser.print_help
args = parser.parse_args(None if sys.argv[1:] else ['-h'])

str1 = args.istr

sfilter = "(|(sn=*{}*)(mail=*{}*)(uid=*{}*))".format(str1, str1, str1)

conn.search(basedn, sfilter)
conn.search(basedn, sfilter, attributes=attribs)

leng = len(conn.entries)
for i in range(leng):
    user = conn.entries[i].uid
    fullname = conn.entries[i].cn
    email = conn.entries[i].mail

    print("user:\t{}\nname:\t{}\nemail:\t{}\n\n".format(user, fullname, email))

####################

# ! /usr/bin/python3

### usearch
### searches in the LDAP database for part of a name, uid or email and returns mail, uid, and full name

import ldap
import argparse
import sys
import ldif

l = ldap.initialize('ldaps://your.fancy.server.url', bytes_mode=False)

basedn = "dc=foo,dc=bar,dc=baz"

## ARGPARSE stuff!!!

parser = argparse.ArgumentParser(
    description='searches the LDAP server',
    usage='usearch PARTIAL_MATCH (email, name, username)',
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('istr', help='searches stuffz')
parser.print_help
args = parser.parse_args(None if sys.argv[1:] else ['-h'])

str1 = args.istr

sfilter = "(|(sn=*{}*)(mail=*{}*)(uid=*{}*))".format(str1, str1, str1)
attributes = ["mail", "uid", "cn"]
scope = ldap.SCOPE_SUBTREE

r = l.search_s(basedn, scope, sfilter, attributes)

ldif_writer = ldif.LDIFWriter(sys.stdout)

for dn, entry in r:
    ldif_writer.unparse(dn, entry)

#############################

l = ldap.initialize('ldap://ldap.myserver.com:389')
binddn = "cn=myUserName,ou=GenericID,dc=my,dc=company,dc=com"
pw = "myPassword"
basedn = "ou=UserUnits,dc=my,dc=company,dc=com"
searchFilter = "(&(gidNumber=123456)(objectClass=posixAccount))"
searchAttribute = ["mail", "department"]
# this will scope the entire subtree under UserUnits
searchScope = ldap.SCOPE_SUBTREE
# Bind to the server
try:
    l.protocol_version = ldap.VERSION3
    l.simple_bind_s(binddn, pw)
except ldap.INVALID_CREDENTIALS:
    print("Your username or password is incorrect.")
    sys.exit(0)
except ldap.LDAPError as e:
    if type(e.message) == dict and e.message.has_key('desc'):
        print(e.message['desc'])
    else:
        print(e)
    sys.exit(0)
try:
    ldap_result_id = l.search(basedn, searchScope, searchFilter, searchAttribute)
    result_set = []
    while 1:
        result_type, result_data = l.result(ldap_result_id, 0)
        if (result_data == []):
            break
        else:
            ## if you are expecting multiple results you can append them
            ## otherwise you can just wait until the initial result and break out
            if result_type == ldap.RES_SEARCH_ENTRY:
                result_set.append(result_data)
    print(result_set)
except ldap.LDAPError as e:
    print(e)
l.unbind_s()
