list_of_users = [
    {'id': 'WID-ab09fbf3-e85a-4a85-817e-3c80a85bfadb', 'email': 'hemanth.y@in.ibm.com', 'firstName': 'Hemanth',
     'lastName': 'Kumar', 'role': ''},
    {'id': 'WID-ca5e2e56-0b9a-4edb-bd99-d1d4f9114051', 'email': 'sbhartiy@in.ibm.com', 'firstName': 'Shiwang',
     'lastName': 'Bhartiya', 'role': ''},
    {'id': 'WIDf222b327-cecf-47d6-a0b7-115e3781ce46', 'email': 'avdutta7@in.ibm.com', 'firstName': 'Avik',
     'lastName': 'Dutta', 'role': 'Business Admin'},
    {'id': 'WID9058d0f2-d94d-472f-a48a-95760647c415', 'email': 'bhaskar.bhattacharjee@in.ibm.com',
     'firstName': 'Bhaskar', 'lastName': 'Bhattacharjee', 'role': 'Preparer'},
    {'id': 'WID9553de5a-e481-42c9-b14e-67a603011530', 'email': 'dpurkait@in.ibm.com', 'firstName': 'Debasmita',
     'lastName': 'Purkait', 'role': 'Preparer'},
    {'id': 'WID5c0ccc09-226e-404d-b546-a6a9e3baa711', 'email': 'monika.murmu@in.ibm.com', 'firstName': 'Monika',
     'lastName': 'Tudu', 'role': 'Approver'},
    {'id': 'WID443a7304-2329-454e-adfa-c437ed6250f5', 'email': 'purbashc@in.ibm.com', 'firstName': 'Purbasha',
     'lastName': 'Chattopadhyay', 'role': 'Approver'},
    {'id': 'WIDd5650be3-2928-4bdd-927e-91d796c30ee2', 'email': 'rajeevmitra@in.ibm.com', 'firstName': 'Rajeev',
     'lastName': 'Mitra', 'role': 'Reviewer'},
    {'id': 'WID3e618b69-fe0d-4d50-81a5-6d2ee251adf5', 'email': 'sayantas@in.ibm.com', 'firstName': 'Sayantan',
     'lastName': 'Sen', 'role': 'Business Admin'},
    {'id': 'WIDd7dfed5b-7043-4dba-9431-bfdd1c227349', 'email': 'sudutta1@in.ibm.com', 'firstName': 'Sudip',
     'lastName': 'Dutta', 'role': 'Preparer'},
    {'id': 'WID02d4eafb-e721-48ad-a0e9-5b78a8588cbc', 'email': 'suparnch@in.ibm.com', 'firstName': 'Suparna',
     'lastName': 'Chatterjee', 'role': 'Reviewer'}]

list_of_unique_preparer = ['WID3e618b69-fe0d-4d50-81a5-6d2ee251adf5', 'WIDf222b327-cecf-47d6-a0b7-115e3781ce46']
list_of_acted_det = []
if list_of_users and list_of_unique_preparer:
    for _, each_acted_user in enumerate(list_of_unique_preparer):
        user_det = ""
        for _, each_user in enumerate(list_of_users):
            if each_user["id"] == each_acted_user:
                user_det = each_user["firstName"] + " " + each_user["lastName"] \
                           + " " + "(" + each_user["email"] + ")"
                break
        list_of_acted_det.append({"id": each_acted_user, "details": user_det})
output = {"isSuccess": True, "listOfActedDet": list_of_acted_det}
print(output)

