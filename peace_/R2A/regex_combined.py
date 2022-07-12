import re

# rule_query = "(   output[Branch] <  OR  STARTSWITH(output[Cost Center], 10.5) WHERE ( output[Credit] == '8' AND  output[Debit] == 100 WHERE ( STARTSWITH(output[JE Line description], 'i') ) ) == (   STARTSWITH(output[Date], '') AND SUM(output[Debit]) ==100.5 WHERE (LEN(output[JE Line description]) ==10 ) )"
rule_query = "( SUM(output[Amount]) WHERE ( output[Posting key] == 40 ) ) == ( SUM(output[Amount]) WHERE ( output[Posting key] == 50 ) )"
words = {'AND', 'OR', 'WHERE'}
function_mapping = {"": "Operator", "STARTSWITH": "Start With", "ENDSWITH": "End With", "LEN": "Length",
                    "SUM": "Sum", "MAX": "Max", "MIN": "Min", "AVG": "Avg"}


def combined_compare_value(x):
    if "'" in x:
        pattern = re.search("'(.+)'", x)
        if pattern:
            return pattern.group(1)
        else:
            return ""
    else:
        pattern = re.search("([\d.]+)", x)
        if pattern:
            return float(pattern.group(1))
        else:
            return None


def combined_compare_value_type(x):
    if x == "" or type(x) == str:
        return "Abc"
    elif not x or type(x) == float:
        return "123"


pattern = re.search("(.+)\)[\s+](\!=|==|=|[<>]=?|<>)[\s+]\((.+)", rule_query)
expression1, expression2 = pattern.group(1), pattern.group(3)
operators = []
for operator in expression1.split():
    if operator in words:
        operators.append(operator)
print("operators: ", operators)

columns = re.findall(r"\[([A-Za-z0-9 _]+)\]", expression1)
print("columns: ", columns)

expression1, expression2 = expression1.replace(' ', ''), expression2.replace(' ', '')
print("expression1: ", expression1)
print("expression2: ", expression2)


rule_query_partition = expression1.replace('AND', '~|^').replace('OR', '~|^').replace('WHERE', '~|^').split('~|^')
print("rule_query_partition: ", rule_query_partition)

func = list(function_mapping.keys())
func.remove('')
function_list = list(map(lambda x: [i for i in func if i in x][0] if [i for i in func if i in x] else '', rule_query_partition))
print("function_list: ", function_list)


function_list_from_mapping = list(map(lambda x: function_mapping.get(x), function_list))
print("function_list_from_mapping: ", function_list_from_mapping)

operator_list = list(map(lambda x: "" if re.search("(\!=|==|=|[<>]=?|<>).*", x) is None else re.search(
    "(\!=|==|=|[<>]=?|<>).*", x).group(1), rule_query_partition))
print("operator_list: ", operator_list)

output_char_list = list(map(lambda x: combined_compare_value(x), rule_query_partition))
print("output_char_list: ", output_char_list)

output_char_type_list = list(map(lambda x: combined_compare_value_type(x), output_char_list))
print("output_char_type_list: ", output_char_type_list)

# rule_query = "(   output[Branch] <  OR  STARTSWITH(output[Cost Center], 10.5) WHERE ( output[Credit] == '8' AND  output[Debit] == 100 WHERE ( STARTSWITH(output[JE Line description], 'i') ) ) == (   STARTSWITH(output[Date], '') AND SUM(output[Debit]) ==100.5 WHERE (LEN(output[JE Line description]) ==10 ) )"
# operators:  ['OR', 'WHERE', 'AND', 'WHERE']
# expression1:  output[Branch]<ORSTARTSWITH(output[CostCenter],10.5)WHERE(output[Credit]=='8'ANDoutput[Debit]==100WHERE(STARTSWITH(output[JELinedescription],'i')))
# expression2:  STARTSWITH(output[Date],'')ANDSUM(output[Debit])==100.5WHERE(LEN(output[JELinedescription])==10))
# columns:  ['Branch', 'CostCenter', 'Credit', 'Debit', 'JELinedescription']
# rule_query_partition:  ['output[Branch]<', 'STARTSWITH(output[CostCenter],10.5)', "(output[Credit]=='8'", 'output[Debit]==100', "(STARTSWITH(output[JELinedescription],'i')))"]
# function_list:  ['', 'STARTSWITH', '', '', 'STARTSWITH']
# function_list_from_mapping:  ['Operator', 'Start With', 'Operator', 'Operator', 'Start With']
# operator_list:  ['<', '', '==', '==', '']
# output_char_list:  [None, 10.5, '8', 100.0, 'i']
# output_char_type_list:  ['123', '123', 'Abc', '123', 'Abc']

