CONSOLIDATED_TABLE_STRUCTURE = {
    # Format 'sql_col_name': ('sql_datatype', 'horses_consolidated_races_col', 'race_general_results_col', 'race_info col', 'horse_pps col')
    'source_file':              ('VARCHAR(255)',    'source_file',          'source_file',      'source_file',  'source_file',),
    'track':                    ('VARCHAR(255)',    'track',                'track',            'track',        'track_code',),
    'date':                     ('DATE',            'date',                 'date',             'date',         'race_date',),
    'race_num':                 ('TINYINT',         'race_num',             'race_num',         'race_num',     'race_num',),
    'race_name':                ('VARCHAR(255)',    'race_name',            'race_name',        None,           None,),
    'time_local':               ('INT',             'time_local',           'off_time',         None,           None,),
    'time_zone':                ('VARCHAR(255)',    'time_zone',            None,               None,           None,),

    'temperature':              ('INT',             'temperature',          'race_temp',        None,           None,),
    'weather':                  ('VARCHAR(255)',    'weather',              'weather',          None,           None,),

    'distance':                 ('INT',             'distance',             'distance',         'distance',     'distance',),
    'run_up_distance':          ('INT',             'run_up_distance',      'run_up_dist',      None,           None,),
    'temp_rail_distance':       ('INT',             'temp_rail_distance',   'temp_rail_dist',   None,           None),
    'about_distance_flag':      ('TINYINT',         'about_distance_flag',  'about_distance',   None,           None,),

    'standard_weight':          ('SMALLINT',        'standard_weight',      None,               'standard_weight',          None),
    'three_year_old_weight':    ('SMALLINT',        'three_year_old_weight',None,               'three_year_old_weight',    None),

    'surface':                  ('VARCHAR(255)',    'surface',              'surface_new',      'surface',      'surface',),
    # 'sealed_track'
    'track_condition':          ('VARCHAR(255)',    'track_condition',      'track_condition',  None,           'track_condition',),
    'chute_start':              ('TINYINT',         'chute_start',          'chute_start',      None,           'special_chute',),

    'off_turf':                 ('TINYINT',         'off_turf',             'off_turf',                     None,   None,),
    'off_turf_dist_change':     ('TINYINT',         'off_turf_dist_change', 'off_turf_distance_change',     None,   None),

    'field_size':               ('INT',             'field_size',           'field_size',       None,           'num_of_horses',),
    'breed':                    ('VARCHAR(255)',    'breed',                'breed',            'breed',        None,),

    'purse':                    ('INT',             'purse',                'purse',            'purse',        'race_purse',),

    'race_type':                ('VARCHAR(255)',    'race_type',            'race_type_BRIS',   'race_type',    'race_type',),

    'claiming_price_base':      ('INT',             'claiming_price_base',      'max_claim',                'claiming_price',           'highest_claim_price',),
    'optional_claiming_price':  ('INT',             'optional_claiming_price',  'optional_claiming_price',  'optional_claiming_price',  None),

    'time_440': ('FLOAT',   'time_440',     None, None, None,),        # 2 furlongs
    'time_660': ('FLOAT',   'time_660',     None, None, None,),        # 3_furlongs
    'time_880': ('FLOAT',   'time_880',     None, None, None,),        # 4 furlongs
    'time_1100': ('FLOAT',  'time_1100',    None, None, None,),       # 5 furlongs
    'time_1210': ('FLOAT',  'time_1210',    None, None, None,),       # 5.5 furlongs
    'time_1320': ('FLOAT',  'time_1320',    None, None, None,),       # 6 furlongs
    'time_1430': ('FLOAT',  'time_1430',    None, None, None,),       # 6.5 furlongs
    'time_1540': ('FLOAT',  'time_1540',    None, None, None,),       # 7 furlongs
    'time_1650': ('FLOAT',  'time_1650',    None, None, None,),       # 7.5 furlongs
    'time_1760': ('FLOAT',  'time_1760',    None, None, None,),       # 1 mile
    'time_1830': ('FLOAT',  'time_1830',    None, None, None,),       # 1 mile, 70 yards
    'time_1870': ('FLOAT',  'time_1870',    None, None, None,),       # 1 1/8 miles (9 furlongs)
    'time_1980': ('FLOAT',  'time_1980',    None, None, None,),       # 1 1/4 miles (10 furlongs)


    'allowed_age_two':      ('TINYINT', 'allowed_age_two',          'allowed_age_two',          'allowed_age_two',          'allowed_age_two',),
    'allowed_age_three':    ('TINYINT', 'allowed_age_three',        'allowed_age_three',        'allowed_age_three',        'allowed_age_three',),
    'allowed_age_four':     ('TINYINT', 'allowed_age_four',         'allowed_age_four',         'allowed_age_four',         'allowed_age_four',),
    'allowed_age_five':     ('TINYINT', 'allowed_age_five',         'allowed_age_five',         'allowed_age_five',         'allowed_age_five',),
    'allowed_age_older':    ('TINYINT', 'allowed_age_older',        'allowed_age_older',        'allowed_age_older',        'allowed_age_older',),

    'allowed_fillies':      ('TINYINT', 'allowed_fillies',          'allowed_fillies',          'allowed_fillies',          'allowed_fillies',),
    'allowed_mares':        ('TINYINT', 'allowed_mares',            'allowed_mares',            'allowed_mares',            'allowed_mares',),
    'allowed_colts_geldings': ('TINYINT', 'allowed_colts_geldings', 'allowed_colts_geldings',   'allowed_colts_geldings',   'allowed_colts_geldings',),

    'statebred_race':       ('TINYINT', 'statebred_race',           'statebred_race',           'statebred_race',           'statebred_race',),

    'race_conditions_1_claim_start_req_price': ('INT', 'race_conditions_1_claim_start_req_price', 'condition_1_claim_start_required_price', 'condition_1_claim_start_required_price', None,),
    'race_conditions_1_claim_start_time_limit': ('INT', 'race_conditions_1_claim_start_time_limit', 'condition_1_claim_start_required_months', 'condition_1_claim_start_required_months', None,),
    'race_conditions_1_not_won_limit': ('INT', 'race_conditions_1_not_won_limit', 'condition_1_number_limit', 'condition_1_number_limit', None,),
    'race_conditions_1_money_limit': ('INT', 'race_conditions_1_money_limit', 'condition_1_money_limit', 'condition_1_money_limit', None,),
    'race_conditions_1_time_limit': ('FLOAT', 'race_conditions_1_time_limit', 'condition_1_time_limit_months', 'condition_1_time_limit_months', None,),
    'race_conditions_1_excluded_claiming': ('TINYINT', 'race_conditions_1_excluded_claiming', 'condition_1_excluded_claiming', 'condition_1_excluded_claiming', None,),
    'race_conditions_1_excluded_maiden': ('TINYINT', 'race_conditions_1_excluded_maiden', 'condition_1_excluded_maiden', 'condition_1_excluded_maiden', None,),
    'race_conditions_1_excluded_optional': ('TINYINT', 'race_conditions_1_excluded_optional', 'condition_1_excluded_optional', 'condition_1_excluded_optional', None,),
    'race_conditions_1_excluded_restricted': ('TINYINT', 'race_conditions_1_excluded_restricted', 'condition_1_excluded_restricted', 'condition_1_excluded_restricted', None,),
    'race_conditions_1_excluded_restricted_allowance': ('TINYINT', 'race_conditions_1_excluded_restricted_allowance', 'condition_1_excluded_restricted_allowance', 'condition_1_excluded_restricted_allowance', None,),
    'race_conditions_1_excluded_starter': ('TINYINT', 'race_conditions_1_excluded_starter', 'condition_1_excluded_starter', 'condition_1_excluded_starter', None,),
    'race_conditions_1_excluded_state_sired': ('TINYINT', 'race_conditions_1_excluded_state_sired', 'condition_1_excluded_state_sired', 'condition_1_excluded_state_sired', None),
    'race_conditions_1_excluded_state_sired_stakes': ('TINYINT', 'race_conditions_1_excluded_state_sired_stakes', 'condition_1_excluded_state_sired_stakes', 'condition_1_excluded_state_sired_stakes', None,),
    'race_conditions_1_excluded_statebred': ('TINYINT', 'race_conditions_1_excluded_statebred', 'condition_1_excluded_statebred', 'condition_1_excluded_statebred', None,),
    'race_conditions_1_excluded_statebred_allowance': ('TINYINT', 'race_conditions_1_excluded_statebred_allowance', 'condition_1_excluded_statebred_allowance', 'condition_1_excluded_statebred_allowance', None,),
    'race_conditions_1_excluded_statebred_stakes': ('TINYINT', 'race_conditions_1_excluded_statebred_stakes', 'condition_1_excluded_statebred_stakes', 'condition_1_excluded_statebred_stakes', None,),
    'race_conditions_1_excluded_trial': ('TINYINT', 'race_conditions_1_excluded_trial', 'condition_1_excluded_trial', 'condition_1_excluded_trial', None,),
    'race_conditions_1_excluded_waiver': ('TINYINT', 'race_conditions_1_excluded_waiver', 'condition_1_excluded_waiver', 'condition_1_excluded_waiver', None,),
    'race_conditions_1_excluded_waiver_claiming': ('TINYINT', 'race_conditions_1_excluded_waiver_claiming', 'condition_1_excluded_waiver_claiming', 'condition_1_excluded_waiver_claiming', None,),

    'race_conditions_2_claim_start_req_price': ('INT', 'race_conditions_2_claim_start_req_price', 'condition_2_claim_start_required_price', 'condition_2_claim_start_required_price', None,),
    'race_conditions_2_claim_start_time_limit': ('INT', 'race_conditions_2_claim_start_time_limit', 'condition_2_claim_start_required_months', 'condition_2_claim_start_required_months', None,),
    'race_conditions_2_not_won_limit': ('INT', 'race_conditions_2_not_won_limit', 'condition_2_number_limit', 'condition_2_number_limit', None,),
    'race_conditions_2_money_limit': ('INT', 'race_conditions_2_money_limit', 'condition_2_money_limit', 'condition_2_money_limit', None,),
    'race_conditions_2_time_limit': ('FLOAT', 'race_conditions_2_time_limit', 'condition_2_time_limit_months', 'condition_2_time_limit_months', None,),
    'race_conditions_2_excluded_claiming': ('TINYINT', 'race_conditions_2_excluded_claiming', 'condition_2_excluded_claiming', 'condition_2_excluded_claiming', None,),
    'race_conditions_2_excluded_maiden': ('TINYINT', 'race_conditions_2_excluded_maiden', 'condition_2_excluded_maiden', 'condition_2_excluded_maiden', None,),
    'race_conditions_2_excluded_optional': ('TINYINT', 'race_conditions_2_excluded_optional', 'condition_2_excluded_optional', 'condition_2_excluded_optional', None,),
    'race_conditions_2_excluded_restricted': ('TINYINT', 'race_conditions_2_excluded_restricted', 'condition_2_excluded_restricted', 'condition_2_excluded_restricted', None,),
    'race_conditions_2_excluded_restricted_allowance': ('TINYINT', 'race_conditions_2_excluded_restricted_allowance', 'condition_2_excluded_restricted_allowance', 'condition_2_excluded_restricted_allowance', None,),
    'race_conditions_2_excluded_starter': ('TINYINT', 'race_conditions_2_excluded_starter', 'condition_2_excluded_starter', 'condition_2_excluded_starter', None,),
    'race_conditions_2_excluded_state_sired': ('TINYINT', 'race_conditions_2_excluded_state_sired', 'condition_2_excluded_state_sired', 'condition_2_excluded_state_sired', None),
    'race_conditions_2_excluded_state_sired_stakes': ('TINYINT', 'race_conditions_2_excluded_state_sired_stakes', 'condition_2_excluded_state_sired_stakes', 'condition_2_excluded_state_sired_stakes', None,),
    'race_conditions_2_excluded_statebred': ('TINYINT', 'race_conditions_2_excluded_statebred', 'condition_2_excluded_statebred', 'condition_2_excluded_statebred', None,),
    'race_conditions_2_excluded_statebred_allowance': ('TINYINT', 'race_conditions_2_excluded_statebred_allowance', 'condition_2_excluded_statebred_allowance', 'condition_2_excluded_statebred_allowance', None,),
    'race_conditions_2_excluded_statebred_stakes': ('TINYINT', 'race_conditions_2_excluded_statebred_stakes', 'condition_2_excluded_statebred_stakes', 'condition_2_excluded_statebred_stakes', None,),
    'race_conditions_2_excluded_trial': ('TINYINT', 'race_conditions_2_excluded_trial', 'condition_2_excluded_trial', 'condition_2_excluded_trial', None,),
    'race_conditions_2_excluded_waiver': ('TINYINT', 'race_conditions_2_excluded_waiver', 'condition_2_excluded_waiver', 'condition_2_excluded_waiver', None,),
    'race_conditions_2_excluded_waiver_claiming': ('TINYINT', 'race_conditions_2_excluded_waiver_claiming', 'condition_2_excluded_waiver_claiming', 'condition_2_excluded_waiver_claiming', None,),

    'race_conditions_3_claim_start_req_price': ('INT', 'race_conditions_3_claim_start_req_price', 'condition_3_claim_start_required_price', 'condition_3_claim_start_required_price', None,),
    'race_conditions_3_claim_start_time_limit': ('INT', 'race_conditions_3_claim_start_time_limit', 'condition_3_claim_start_required_months', 'condition_3_claim_start_required_months', None,),
    'race_conditions_3_not_won_limit': ('INT', 'race_conditions_3_not_won_limit', 'condition_3_number_limit', 'condition_3_number_limit', None,),
    'race_conditions_3_money_limit': ('INT', 'race_conditions_3_money_limit', 'condition_3_money_limit', 'condition_3_money_limit', None,),
    'race_conditions_3_time_limit': ('FLOAT', 'race_conditions_3_time_limit', 'condition_3_time_limit_months', 'condition_3_time_limit_months', None,),
    'race_conditions_3_excluded_claiming': ('TINYINT', 'race_conditions_3_excluded_claiming', 'condition_3_excluded_claiming', 'condition_3_excluded_claiming', None,),
    'race_conditions_3_excluded_maiden': ('TINYINT', 'race_conditions_3_excluded_maiden', 'condition_3_excluded_maiden', 'condition_3_excluded_maiden', None,),
    'race_conditions_3_excluded_optional': ('TINYINT', 'race_conditions_3_excluded_optional', 'condition_3_excluded_optional', 'condition_3_excluded_optional', None,),
    'race_conditions_3_excluded_restricted': ('TINYINT', 'race_conditions_3_excluded_restricted', 'condition_3_excluded_restricted', 'condition_3_excluded_restricted', None,),
    'race_conditions_3_excluded_restricted_allowance': ('TINYINT', 'race_conditions_3_excluded_restricted_allowance', 'condition_3_excluded_restricted_allowance', 'condition_3_excluded_restricted_allowance', None,),
    'race_conditions_3_excluded_starter': ('TINYINT', 'race_conditions_3_excluded_starter', 'condition_3_excluded_starter', 'condition_3_excluded_starter', None,),
    'race_conditions_3_excluded_state_sired': ('TINYINT', 'race_conditions_3_excluded_state_sired', 'condition_3_excluded_state_sired', 'condition_3_excluded_state_sired', None),
    'race_conditions_3_excluded_state_sired_stakes': ('TINYINT', 'race_conditions_3_excluded_state_sired_stakes', 'condition_3_excluded_state_sired_stakes', 'condition_3_excluded_state_sired_stakes', None,),
    'race_conditions_3_excluded_statebred': ('TINYINT', 'race_conditions_3_excluded_statebred', 'condition_3_excluded_statebred', 'condition_3_excluded_statebred', None,),
    'race_conditions_3_excluded_statebred_allowance': ('TINYINT', 'race_conditions_3_excluded_statebred_allowance', 'condition_3_excluded_statebred_allowance', 'condition_3_excluded_statebred_allowance', None,),
    'race_conditions_3_excluded_statebred_stakes': ('TINYINT', 'race_conditions_3_excluded_statebred_stakes', 'condition_3_excluded_statebred_stakes', 'condition_3_excluded_statebred_stakes', None,),
    'race_conditions_3_excluded_trial': ('TINYINT', 'race_conditions_3_excluded_trial', 'condition_3_excluded_trial', 'condition_3_excluded_trial', None,),
    'race_conditions_3_excluded_waiver': ('TINYINT', 'race_conditions_3_excluded_waiver', 'condition_3_excluded_waiver', 'condition_3_excluded_waiver', None,),
    'race_conditions_3_excluded_waiver_claiming': ('TINYINT', 'race_conditions_3_excluded_waiver_claiming', 'condition_3_excluded_waiver_claiming', 'condition_3_excluded_waiver_claiming', None,),

    'race_conditions_text_1': ('VARCHAR(255)', 'race_conditions_text_1', 'race_conditions_1', 'race_cond_1', None,),
    'race_conditions_text_2': ('VARCHAR(255)', 'race_conditions_text_2', 'race_conditions_2', 'race_cond_2', None,),
    'race_conditions_text_3': ('VARCHAR(255)', 'race_conditions_text_3', 'race_conditions_3', 'race_cond_3', None,),
    'race_conditions_text_4': ('VARCHAR(255)', 'race_conditions_text_4', 'race_conditions_4', 'race_cond_4', None,),
    'race_conditions_text_5': ('VARCHAR(255)', 'race_conditions_text_5', 'race_conditions_5', 'race_cond_5', None,),
    'race_conditions_text_6': ('VARCHAR(255)', 'race_conditions_text_6', None, 'race_cond_6', None,),

    # 'race_notes': (),
}