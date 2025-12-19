from ingestion.ingestion_lib import resetValueOrDefault, setNestedValue, retrieveNestedValue

def test_setNestedValue():
    test1 = {'one': {'two': {'three':'test', 'four':{'leaf':'testing'}}, 'three': {'four': 'testing'}}}
    test_dict = {}
    setNestedValue(test_dict, ['one', 'two', 'three'], 'test')
    setNestedValue(test_dict, ['one', 'two', 'four', 'leaf'], 'testing')
    setNestedValue(test_dict, ['one', 'three', 'four'], 'testing')

    assert test_dict == test1

def test_retrieveNestedValue():
    test1 = {'one': {'two': {'three': {'one': 'testing', 'two':'thing', 'three':'thing2'}, 'four':'leaf'}}}

    test_value = retrieveNestedValue(test1, ['one','two','three','two'], None)
    assert test_value == 'thing'


#new_dict, new_key_list, old_dict, old_key_list, default=None
def test_resetValueOrDefault():
    test1 = {'one': 'testing'}
    test_dict = {'one': {'two': {'three': {'one': 'testing', 'two':'thing', 'three':'thing2'}, 'four':'leaf'}}}
   
    resetValueOrDefault(test_dict, ['one'], test_dict, ['one', 'two', 'three', 'one'], None)
    assert test1 == test_dict
