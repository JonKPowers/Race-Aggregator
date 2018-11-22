# This provides mappings of the tables to their index number in the table-structure dictionaries
#
# For example, to find the field corresponding to horse_name in the horse_pps table, you would
# lookup consolidated_table_structure[horse_name][3]

TABLE_TO_INDEX_MAPPINGS = {
    # Past performance consolidated tables:
    'horses_consolidated_performances': 1,
    'race_horse_info' : 2,
    'horse_pps': 3,

    # Race consolidations tables:
    'horses_consolidated_races': 1,
    'race_general_results': 2,
    'race_info': 3,
}