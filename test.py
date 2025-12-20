import json

dictionary = {'one': {'two': {'three': {'one': 'testing', 'two':'thing', 'three':'thing2'}, 'four':'leaf'}}}


string = json.dumps(dictionary)

print(string)

dict_again = json.loads(string)

print(dict_again)
if dictionary != dict_again:
    print("Different?")
