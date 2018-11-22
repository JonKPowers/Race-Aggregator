from race_table_refactor_new import AggRacesDataHandler
from race_table_refactor_new import RaceAggregator

# Constants
from aggregation_datapack import DataPack
from constant_aggregated_races_table_structure import CONSOLIDATED_TABLE_STRUCTURE
from constant_aggregated_races_table_to_index_mappings import TABLE_TO_INDEX_MAPPINGS
from constant_aggregated_races_unique import UNIQUE
data_pack = DataPack(CONSOLIDATED_TABLE_STRUCTURE=CONSOLIDATED_TABLE_STRUCTURE,
                     TABLE_TO_INDEX_MAPPINGS=TABLE_TO_INDEX_MAPPINGS,
                     UNIQUE=UNIQUE)

# SQL results limit to speed up debugging cycle during development
# todo DELETE FOR PRODUCTION
data_limit = 'LIMIT 20000'

# Spin up the data handlers for the race aggregation tables
consolidated_races_data_handler = AggRacesDataHandler('horses_consolidated_races', 'horses_consolidated_races',
                                                      data_pack, include_horse=False, other='',
                                                      initialize_table=True, verbose_db=False)

# Spin up data handler and aggregator for the races_info table. Fire it up.
race_info_data_handler = AggRacesDataHandler('horses_data', 'race_info',
                                             data_pack, include_horse=False, other=data_limit)
race_info_adder = RaceAggregator(race_info_data_handler, consolidated_races_data_handler, include_horse=False)
race_info_adder.add_to_consolidated_data()

# Spin up data handler and aggregator for the past performances table. Fire it up.
horse_pps_data_handler = AggRacesDataHandler('horses_data', 'horse_pps',
                                             data_pack, include_horse=False, other=data_limit)
horse_pps_adder = RaceAggregator(horse_pps_data_handler, consolidated_races_data_handler, include_horse=False)
horse_pps_adder.add_to_consolidated_data()

# Spin up data handler and aggregator for the race_results table. Fire it up.
race_results_data_handler = AggRacesDataHandler('horses_data', 'race_general_results',
                                                data_pack, include_horse=False, other=data_limit)
race_results_adder = RaceAggregator(race_results_data_handler, consolidated_races_data_handler, include_horse=False)
race_results_adder.add_to_consolidated_data()











