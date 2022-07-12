import re

rule_query = "STARTSWITH(output[Branch], 'abc') OR output[Credit] == 105.6 AND ENDSWITH(output[Cost Center], 'xyz')"
words = {'AND', 'OR'}
operators = []
function_mapping = {"": "Operator", "STARTSWITH": "Start With", "ENDSWITH": "End With", "LEN": "Length",
                    "SUM": "Sum", "MAX": "Max", "MIN": "Min", "AVG": "Avg"}


def get_output_char_from_function(x):
    if "'" in x:
        pattern = re.search("'(.+)'", x)
        if pattern:
            return pattern.group(1)
        else:
            return ""
    else:
        pattern = re.search("(\!=|==|=|[<>]=?|<>)\s([A-Za-z0-9.]+)", x)
        if pattern and pattern.group(2) != 'null':
            return float(pattern.group(2))
        else:
            return None


def get_char_type(x):
    if x == "" or type(x) == str:
        return "Abc"
    elif not x or type(x) == float:
        return "123"


for operator in rule_query.split():
    if operator in words:
        operators.append(operator)
print(operators)

columns = re.findall(r"\[([A-Za-z0-9 _]+)\]", rule_query)
print("columns: ", columns)

rule_query_partition = rule_query.replace(' AND ', '~|^').replace(' OR ', '~|^').split('~|^')
function_list = list(map(lambda x: "" if '(' not in x else re.search('(.+?)\(', x).group(1), rule_query_partition))
# print("function_list: ", function_list)
function_list_from_mapping = list(map(lambda x: function_mapping.get(x), function_list))
print("function_list_from_mapping: ", function_list_from_mapping)

operator_list = list(map(lambda x: "" if re.search("(\!=|==|=|[<>]=?|<>)\s[A-Za-z0-9.]+", x) is None else re.search(
    "(\!=|==|=|[<>]=?|<>)\s[A-Za-z0-9.]+", x).group(1), rule_query_partition))
print("operator_list: ", operator_list)

output_char_list = list(map(lambda x: get_output_char_from_function(x), rule_query_partition))
print("output_char_list: ", output_char_list)

output_char_type_list = list(map(lambda x: get_char_type(x), output_char_list))
print("output_char_type_list: ", output_char_type_list)


# test output data for
# rule_query = "STARTSWITH(output[Branch], '') AND ENDSWITH(output[Cost Center], 'abc') OR SUM(output[Credit]) == null AND LEN(output[JE Line description]) < 25 OR MAX(output[Debit]) > 36"
# ['AND', 'OR', 'AND', 'OR']
# columns:  ['Branch', 'Cost Center', 'Credit', 'JE Line description', 'Debit']
# function_list_from_mapping:  ['Start With', 'End With', 'Sum', 'Length', 'Max']
# operator_list:  ['', '', '==', '<', '>']
# output_char_list:  ['', 'abc', '', 25, 36]
