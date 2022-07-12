import re

rule_query = "output[Branch] == 'B' AND STARTSWITH(output[Credit], 'C') OR LEN(output[JE Line description]) >= 10 WHERE ( output[Cost Center] == 40 AND AVERAGE(output[Credit]) <= 100.5 OR STARTSWITH(output[Main Acct#], 'M'))"
function_mapping = {"": "Operator", "STARTSWITH": "Start With", "ENDSWITH": "End With", "LEN": "Length",
                    "SUM": "Sum", "MAX": "Max", "MIN": "Min", "AVG": "Avg"}


def compare_value(x):
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


def compare_value_type(x):
    if x == "'char'" or x == "'numeric'":
        return ''
    elif x == "" or type(x) == str:
        return "Abc"
    elif not x or type(x) == float:
        return "123"


if 'WHERE' in rule_query.split():
    rule_creator_query = rule_query.split('WHERE')[0]
    condition_creator_query = rule_query.split('WHERE')[1]
    words = {'AND', 'OR'}

    rule_operators = []
    for operator in rule_creator_query.split():
        if operator in words:
            rule_operators.append(operator)
    print("rule_operators: ", rule_operators)

    rule_columns = re.findall(r"\[([A-Za-z0-9 _#]+)\]", rule_creator_query)
    print("rule_columns: ", rule_columns)

    condition_operators = []
    for operator in condition_creator_query.split():
        if operator in words:
            condition_operators.append(operator)
    print("condition_operators: ", condition_operators)

    condition_columns = re.findall(r"\[([A-Za-z0-9 _#]+)\]", condition_creator_query)
    print("condition_columns: ", condition_columns)

    condition_creator_query = condition_creator_query.replace(' ', '')
    print("condition_creator_query: ", condition_creator_query)

    condition_creator_query_partition = condition_creator_query.replace('AND', '~|^').replace('OR', '~|^').split('~|^')
    print("condition_creator_query_partition: ", condition_creator_query_partition)

    func = list(function_mapping.keys())
    func.remove('')
    condition_function_list = list(map(lambda x: [i for i in func if i in x][0] if [i for i in func if i in x] else '',
                                       condition_creator_query_partition))
    print("condition_function_list: ", condition_function_list)

    condition_function_list_from_mapping = list(map(lambda x: function_mapping.get(x), condition_function_list))
    print("condition_function_list_from_mapping: ", condition_function_list_from_mapping)

    condition_operator_list = list(map(
        lambda x: "" if re.search("(\!=|==|=|[<>]=?|<>).*", x) is None else re.search(
            "(\!=|==|=|[<>]=?|<>).*", x).group(1), condition_creator_query_partition))
    print("condition_operator_list: ", condition_operator_list)

    condition_output_char_list = list(map(lambda x: compare_value(x), condition_creator_query_partition))
    print("condition_output_char_list: ", condition_output_char_list)

    condition_output_char_type_list = list(map(lambda x: compare_value_type(x), condition_output_char_list))
    print("condition_output_char_type_list: ", condition_output_char_type_list)

else:
    # retrieve the columns & operators from the rule query
    words = {'AND', 'OR'}
    operators = []
    for operator in rule_query.split():
        if operator in words:
            operators.append(operator)
    columns = re.findall(r"\[([A-Za-z0-9 _#]+)\]", rule_query)

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
