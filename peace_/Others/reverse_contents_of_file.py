with open('data/friend_names.txt', 'r') as reader:
    content = reader.readlines()
    print(content)
    # reverse the contents
    with open('data/friend_names.txt', 'w') as writer:
        for line in reversed(content):
            writer.write(line)



# with open('data/friend_names.txt', 'w') as writer:
#     for reverse in range(len(names_list)-1):
#         writer.writelines(names_list[len(names_list) - reverse + 1])
