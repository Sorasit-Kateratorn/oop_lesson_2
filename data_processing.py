import copy
import csv
import os

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))

cities = []
with open(os.path.join(__location__, 'Cities.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        cities.append(dict(r))

countries = []
with open(os.path.join(__location__, 'Countries.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        countries.append(dict(r))
players = []
with open(os.path.join(__location__, 'Players.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        players.append(dict(r))
teams = []
with open(os.path.join(__location__, 'Teams.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        teams.append(dict(r))
titanic = []
with open(os.path.join(__location__, 'Titanic.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        titanic.append(dict(r))


class DB:
    def __init__(self):
        self.database = []

    def insert(self, table):
        self.database.append(table)

    def search(self, table_name):
        for table in self.database:
            if table.table_name == table_name:
                return table
        return None


class Table:
    def __init__(self, table_name, table):
        self.table_name = table_name
        self.table = table

    def join(self, other_table, common_key):
        joined_table = Table(self.table_name + '_joins_' +
                             other_table.table_name, [])
        for item1 in self.table:
            for item2 in other_table.table:
                if item1[common_key] == item2[common_key]:
                    dict1 = copy.deepcopy(item1)
                    dict2 = copy.deepcopy(item2)
                    dict1.update(dict2)
                    joined_table.table.append(dict1)
        return joined_table

    def filter(self, condition):
        filtered_table = Table(self.table_name + '_filtered', [])
        for item1 in self.table:
            if condition(item1):
                filtered_table.table.append(item1)
        return filtered_table

    def aggregate(self, function, aggregation_key):
        temps = []
        for item1 in self.table:
            temps.append(float(item1[aggregation_key]))
        return function(temps)

    def select(self, attributes_list):
        temps = []
        for item1 in self.table:
            dict_temp = {}
            for key in item1:
                if key in attributes_list:
                    dict_temp[key] = item1[key]
            temps.append(dict_temp)
        return temps

    def __str__(self):
        return self.table_name + ':' + str(self.table)

    def pivot_table(self, keys_to_pivot_list, keys_to_aggreagte_list, aggregate_func_list):
        unique_values_list = []
        for key in keys_to_pivot_list:
            _list = []
            for d in self.select(keys_to_pivot_list):
                if d.get(key) not in _list:
                    _list.append(d.get(key))
            unique_values_list.append(_list)
        # create list 2-D unique_values_list to aggregate

        from combination_gen import gen_comb_list
        comb = gen_comb_list(unique_values_list)
        pivoted = []
        for i in comb:
            temp = self.filter(lambda x: x[keys_to_pivot_list[0]] == i[0])
            for j in range(1, len(keys_to_pivot_list)):
                temp = temp.filter(lambda x: x[keys_to_pivot_list[j]] == i[j])
            temp_list = []
            for a in range(len(keys_to_aggreagte_list)):
                result = temp.aggregate(
                    aggregate_func_list[a], keys_to_aggreagte_list[a])
                temp_list.append(result)
            pivoted.append([i, temp_list])
        return pivoted

        # for loop in data for keys_to_pivot_list
        # average min max sum count in keys_to_aggreagte_list
        # aggregate in function list = average min max sum count(select one of this function)

    def __is_float(self, element):
        if element is None:
            return False
        try:
            float(element)
            return True
        except ValueError:
            return False


table1 = Table('cities', cities)
table2 = Table('countries', countries)
table3 = Table('players', players)
table4 = Table('teams', teams)
table5 = Table('titanic', titanic)

my_DB = DB()
my_DB.insert(table1)
my_DB.insert(table2)
my_DB.insert(table3)
my_DB.insert(table4)
my_DB.insert(table5)
my_table1 = my_DB.search('cities')
my_table3 = my_DB.search('players')
my_table4 = my_DB.search('teams')
my_table5 = my_DB.search('titanic')
table3_team = my_table3.filter(lambda x: "ia" in x["team"]).filter(
    lambda x: int(x['minutes']) <= 200).filter(lambda x: int(x['passes']) >= 100)
print(table3_team)

my_table4_ranking_below = my_table4.filter(lambda x: int(x["ranking"]) < 10)
my_table4_ranking_high = my_table4.filter(lambda x: int(x["ranking"]) >= 10)
print("ranking below 10", my_table4_ranking_below.aggregate(
    lambda x: sum(x)/len(x), 'games'))
print("ranking high 10", my_table4_ranking_high.aggregate(
    lambda x: sum(x)/len(x), 'games'))
my_table_mid = my_table3.filter(lambda x: x["position"] == "midfielder")
my_table_forward = my_table3.filter(lambda x: x["position"] == "forward")
print("average midfielder:", my_table_mid.aggregate(lambda x: sum(x)/len(x), 'passes'), "forward:",
      my_table_forward.aggregate(lambda x: sum(x)/len(x), 'passes'))

my_first_class = my_table5.filter(lambda x: int(x["class"]) == 1)
my_third_class = my_table5.filter(lambda x: int(x["class"]) == 3)
print("average first class:", my_first_class.aggregate(lambda x: sum(x)/len(x), 'fare'), "third class:",
      my_third_class.aggregate(lambda x: sum(x)/len(x), 'fare'))
men = my_table5.filter(lambda x: x['gender'] == 'M')
women = my_table5.filter(lambda x: x['gender'] == 'F')
men_survival = men.filter(lambda x: x['survived'] == 'yes')
women_survival = women.filter(lambda x: x['survived'] == 'yes')
num_men = men_survival.filter(lambda x: len(x))
num_women = women_survival.filter(lambda x: len(x))
print(" men survival rate: ", num_men.aggregate(lambda x: len(x), 'fare')/men.aggregate(lambda x: len(x), "fare") * 100, "women survival rate:",
      num_women.aggregate(lambda x: len(x), 'fare')/women.aggregate(lambda x: len(x), "fare")*100)


# print("Test filter: only filtering out cities in Italy")
# my_table1_filtered = my_table1.filter(lambda x: x['country'] == 'Italy')
# print(my_table1_filtered)
# print()

# print("Test select: only displaying two fields, city and latitude, for cities in Italy")
# my_table1_selected = my_table1_filtered.select(['city', 'latitude'])
# print(my_table1_selected)
# print()

# print("Calculting the average temperature without using aggregate for cities in Italy")
# temps = []
# for item in my_table1_filtered.table:
#     temps.append(float(item['temperature']))
# print(sum(temps)/len(temps))
# print()

# print("Calculting the average temperature using aggregate for cities in Italy")
# print(my_table1_filtered.aggregate(lambda x: sum(x)/len(x), 'temperature'))
# print()

# print("Test join: finding cities in non-EU countries whose temperatures are below 5.0")
# my_table2 = my_DB.search('countries')
# my_table3 = my_table1.join(my_table2, 'country')
# my_table3_filtered = my_table3.filter(lambda x: x['EU'] == 'no').filter(
#     lambda x: float(x['temperature']) < 5.0)
# print(my_table3_filtered.table)
# print()
# print("Selecting just three fields, city, country, and temperature")
# print(my_table3_filtered.select(['city', 'country', 'temperature']))
# print()

# print("Print the min and max temperatures for cities in EU that do not have coastlines")
# my_table3_filtered = my_table3.filter(
#     lambda x: x['EU'] == 'yes').filter(lambda x: x['coastline'] == 'no')
# print("Min temp:", my_table3_filtered.aggregate(
#     lambda x: min(x), 'temperature'))
# print("Max temp:", my_table3_filtered.aggregate(
#     lambda x: max(x), 'temperature'))
# print()

# print("Print the min and max latitude for cities in every country")
# for item in my_table2.table:
#     my_table1_filtered = my_table1.filter(
#         lambda x: x['country'] == item['country'])
#     if len(my_table1_filtered.table) >= 1:
#         print(item['country'], my_table1_filtered.aggregate(lambda x: min(
#             x), 'latitude'), my_table1_filtered.aggregate(lambda x: max(x), 'latitude'))
# print()

def aggregate(self, function, aggregation_key):
    temps = []
    temp_value = 0
    count = 0
    # min max cnt(count) avg(average)
    if function == "min":
        for item1 in self.table:
            if self.__is_float(item1[aggregation_key]):
                temp_value = item1
                if temp_value < item1:
                    temps.append(temp_value)
                else:
                    temps.append(float(item1[aggregation_key]))
            else:
                temps.append(item1[aggregation_key])
    if function == "max":
        for item1 in self.table:
            if self.__is_float(item1[aggregation_key]):
                temp_value = item1
                if temp_value > item1:
                    temps.append(temp_value)
                else:
                    temps.append(float(item1[aggregation_key]))
            else:
                temps.append(item1[aggregation_key])

    if function == "cnt":
        for item1 in self.table:
            if self.__is_float(item1[aggregation_key]):
                count = count + 1
            else:
                return count

    if function == "avg":
        for item1 in self.table:
            if self.__is_float(item1[aggregation_key]):
                temps.append(float(item1[aggregation_key]))
                count = count + 1
            else:
                temps.append(item1[aggregation_key])
                if count != 0:
                    return (temps.__getitem__/count)
