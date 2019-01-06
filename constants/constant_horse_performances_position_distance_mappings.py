POSITION_DISTANCE_MAPPINGS = {
    # Format: distance(int): {'consolidated_col_1': [None, None, 'race_horse_info_col', 'horse_pps_col'] ...}
    #       - Position out of the gate
    #       - Position at first call
    #       - Position at second call
    #       - Position at third call (if any)
    #       - Position at stretch call (1 furlong from finish)
    #       - Position at finish (unofficial--we don't care about disqualification for purposes of model training)
    1100: {
        'position_0':       [None, None, 'position_start_call',         'position_start_call'],
        'position_330':     [None, None, 'position_1st_call',           'position_1st_call'],
        'position_660':     [None, None, 'position_2d_call',            'position_2d_call'],
        'position_880':     [None, None, 'position_stretch_call',       'position_stretch_call'],
        'position_1100':    [None, None, 'position_finish',             'position_finish'],
    },
    1210: {
        'position_0':       [None, None, 'position_start_call',         'position_start_call'],
        'position_440':     [None, None, 'position_1st_call',           'position_1st_call'],
        'position_660':     [None, None, 'position_2d_call',            'position_2d_call'],
        'position_990':     [None, None, 'position_stretch_call',       'position_stretch_call'],
        'position_1210':    [None, None, 'position_finish',             'position_finish_call'],
    },
    1320: {
        'position_0':       [None, None, 'position_start_call',         'position_start_call'],
        'position_440':     [None, None, 'position_1st_call',           'position_1st_call'],
        'position_880':     [None, None, 'position_2d_call',            'position_2d_call'],
        'position_1100':    [None, None, 'position_stretch_call',       'position_stretch_call'],
        'position_1320':    [None, None, 'position_finish',             'position_finish'],
    },
    1430: {
        'position_0':       [None, None, 'position_start_call',         'position_start_call'],
        'position_440':     [None, None, 'position_1st_call',           'position_1st_call'],
        'position_880':     [None, None, 'position_2d_call',            'position_2d_call'],
        'position_1210':    [None, None, 'position_stretch_call',       'position_stretch_call'],
        'position_1430':    [None, None, 'position_finish',             'position_finish'],
    },
    1540: {
        'position_0':       [None, None, 'position_start_call',         'position_start_call'],
        'position_440':     [None, None, 'position_1st_call',           'position_1st_call'],
        'position_880':     [None, None, 'position_2d_call',            'position_2d_call'],
        'position_1320':    [None, None, 'position_stretch_call',       'position_stretch_call'],
        'position_1540':    [None, None, 'position_finish',             'position_finish_call'],
    },
    1650: {
        'position_0':       [None, None, 'position_start_call',         'position_start_call'],
        'position_440':     [None, None, 'position_1st_call',           'position_1st_call'],
        'position_880':     [None, None, 'position_2d_call',            'position_2d_call'],
        'position_1430':    [None, None, 'position_stretch_call',       'position_stretch_call'],
        'position_1650':    [None, None, 'position_finish',             'position_finish'],
    },
    1760: {
        'position_0':       [None, None, 'position_start_call',         'position_gate_call'],
        'position_440':     [None, None, 'position_1st_call',           'position_start_call'],
        'position_880':     [None, None, 'position_2d_call',            'position_1st_call'],
        'position_1320':    [None, None, 'position_3d_call',            'position_2d_call'],
        'position_1540':    [None, None, 'position_stretch_call',       'position_stretch_call'],
        'position_1760':    [None, None, 'position_finish',             'position_finish'],
    },
    1830: {
        'position_0':       [None, None, 'position_start_call',         'position_gate_call'],
        'position_440':     [None, None, 'position_1st_call',           'position_start_call'],
        'position_880':     [None, None, 'position_2d_call',            'position_1st_call'],
        'position_1320':    [None, None, 'position_3d_call',            'position_2d_call'],
        'position_1610':    [None, None, 'position_stretch_call',       'position_stretch_call'],
        'position_1830':    [None, None, 'position_finish',             'position_finish_call'],
    },
    1870: {
        'position_0':       [None, None, 'position_start_call',         'position_gate_call'],
        'position_440':     [None, None, 'position_1st_call',           'position_start_call'],
        'position_880':     [None, None, 'position_2d_call',            'position_1st_call'],
        'position_1320':    [None, None, 'position_3d_call',            'position_2d_call'],
        'position_1650':    [None, None, 'position_stretch_call',       'position_stretch_call'],
        'position_1870':    [None, None, 'position_finish',             'position_finish'],
    },
    1980: {
        'position_0':       [None, None, 'position_start_call',         'position_gate_call'],
        'position_440':     [None, None, 'position_1st_call',           'position_start_call'],
        'position_880':     [None, None, 'position_2d_call',            'position_1st_call'],
        'position_1320':    [None, None, 'position_3d_call',            'position_2d_call'],
        'position_1760':    [None, None, 'position_stretch_call',       'position_stretch_call'],
        'position_1980':    [None, None, 'position_finish_unofficial',  'position_finish'],
    },
}