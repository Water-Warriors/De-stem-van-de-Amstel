import pandas 

food_pfas = pandas.read_csv('pfas_food_data.csv')
food_mass = pandas.read_csv('avg_mass_food.csv')


food_pfas['sum_of_pfas'] = food_pfas[['PFOS_UB', 'PFOA_UB', 'PFNA_UB']].sum(axis=1)
sum_table = food_pfas[['Food category', 'sum_of_pfas']]

food_alias_dict = {
    "Fruit and fruit products": "Apple",
    "Poultry": "Chicken (portion)",
    "Alcoholic beverages": "Beer (glass 250 mL)"
}


sum_table['Food category'] = sum_table['Food category'].replace(food_alias_dict)


merged_table = sum_table.merge(food_mass, left_on='Food category', right_on='Food')
merged_table = merged_table[['sum_of_pfas', 'Average_mass_g']]

merged_table['total_pfas_per_food'] = merged_table['sum_of_pfas'] * merged_table['Average_mass_g'] / 100

