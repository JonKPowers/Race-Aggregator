import horse_performances_refactor as hp

# Import configuration constants and encapsulate them into a data pack
from datapack import DataPack
from constants.constant_horse_performances_consolidated_table_structure import CONSOLIDATED_TABLE_STRUCTURE
from constants.constant_horse_performances_additional_fields import ADDITIONAL_FIELDS
from constants.constant_horse_performances_table_to_index_mappings import TABLE_TO_INDEX_MAPPINGS
from constants.constant_horse_performances_distances_to_process import DISTANCES_TO_PROCESS
from constants.constant_horse_performances_position_distance_mappings import POSITION_DISTANCE_MAPPINGS
from constants.constant_horses_performances_lead_or_beaten_distance_mappings import LEAD_OR_BEATEN_DISTANCE_MAPPINGS
from constants.constant_aggregated_performances_unique import UNIQUE

data_pack = DataPack(CONSOLIDATED_TABLE_STRUCTURE=CONSOLIDATED_TABLE_STRUCTURE,
                     ADDITIONAL_FIELDS=ADDITIONAL_FIELDS,
                     TABLE_TO_INDEX_MAPPINGS=TABLE_TO_INDEX_MAPPINGS,
                     DISTANCES_TO_PROCESS=DISTANCES_TO_PROCESS,
                     POSITION_DISTANCE_MAPPINGS=POSITION_DISTANCE_MAPPINGS,
                     LEAD_OR_BEATEN_DISTANCE_MAPPINGS=LEAD_OR_BEATEN_DISTANCE_MAPPINGS,
                     UNIQUE=UNIQUE)

# FOR DEVELOPMENT PURPOSES: Limit the number of records we pull from the databases.
# todo REMOVE FOR PRODUCTION
data_limit = ''
source_database = 'horses_test'

# Generate the data handler for the consolidated races aggregated db
consolidated_races_db_handler= hp.AdderDataHandler('horses_consolidated_races', 'horses_consolidated_races',
                                                   data_pack, include_horse=False, other='', verbose_db=False)

# Generate the data handler for the consolidated performances aggregated db
consolidated_performances_db_handler = hp.AdderDataHandler('horses_consolidated_races', 'horses_consolidated_performances',
                                                           data_pack, include_horse=True, initialize_table=True,
                                                           other='', verbose_db=False)


# Build up data handlers for the data sources, spin up the aggregators, and fire them up.
race_results_db_handler = hp.PPAdderDataHandler(source_database, 'race_horse_info', data_pack,
                                                include_horse=True, other=data_limit)
race_results_adder = hp.PPRaceProcessor(race_results_db_handler, consolidated_performances_db_handler,
                                        consolidated_races_db_handler, include_horse=True, verbose=False)
race_results_adder.add_to_consolidated_data()

# Reset consolidated data handler to reflect the just-added data
consolidated_performances_db_handler.set_up_data()

past_performances_db_handler = hp.PPAdderDataHandler(source_database, 'horse_pps', data_pack,
                                                   include_horse=True, other=data_limit)
past_performances_adder = hp.PPRaceProcessor(past_performances_db_handler, consolidated_performances_db_handler,
                                             consolidated_races_db_handler, include_horse=True, verbose=False)
past_performances_adder.add_to_consolidated_data()

# todo Set up system to give warning if the program is generating too many DB integrity errors--it's doing something wrong if that happens


