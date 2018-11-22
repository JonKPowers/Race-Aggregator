LEAD_OR_BEATEN_DISTANCE_MAPPINGS = {
    # Format: distance(int): {'consolidated_col_1': [None, None, 'race_horse_info_col', 'horse_pps_col'] ...}
    #       - Margin out of the gate
    #       - Margin at first call
    #       - Margin at second call
    #       - Margin at third call (if any)
    #       - Margin at stretch call (1 furlong from finish)
    #       - Margin at finish (unofficial--we don't care about disqualification for purposes of model training)
    1100: {
        'lead_or_beaten_0':     [None, None, 'lead_or_beaten_lengths_start_call',   'lead_or_beaten_lengths_start_call'],
        'lead_or_beaten_330':   [None, None, 'lead_or_beaten_lengths_1st_call',     'lead_or_beaten_lengths_1st_call'],
        'lead_or_beaten_660':   [None, None, 'lead_or_beaten_lengths_2d_call',      'lead_or_beaten_lengths_2d_call'],
        'lead_or_beaten_880':   [None, None, 'lead_or_beaten_lengths_stretch_call', 'lead_or_beaten_lengths_stretch_call'],
        'lead_or_beaten_1100':  [None, None, 'lead_or_beaten_lengths_finish',       'lead_or_beaten_lengths_finish'],
    },
    1210: {
        'lead_or_beaten_0':     [None, None, 'lead_or_beaten_lengths_start_call',   'lead_or_beaten_lengths_start_call'],
        'lead_or_beaten_440':   [None, None, 'lead_or_beaten_lengths_1st_call',     'lead_or_beaten_lengths_1st_call'],
        'lead_or_beaten_660':   [None, None, 'lead_or_beaten_lengths_2d_call',      'lead_or_beaten_lengths_2d_call'],
        'lead_or_beaten_990':   [None, None, 'lead_or_beaten_lengths_stretch_call', 'lead_or_beaten_lengths_stretch_call'],
        'lead_or_beaten_1210':  [None, None, 'lead_or_beaten_lengths_finish',       'lead_or_beaten_lengths_finish'],
    },
    1320: {
        'lead_or_beaten_0':     [None, None, 'lead_or_beaten_lengths_start_call',   'lead_or_beaten_lengths_start_call'],
        'lead_or_beaten_440':   [None, None, 'lead_or_beaten_lengths_1st_call',     'lead_or_beaten_lengths_1st_call'],
        'lead_or_beaten_880':   [None, None, 'lead_or_beaten_lengths_2d_call',      'lead_or_beaten_lengths_2d_call'],
        'lead_or_beaten_1100':  [None, None, 'lead_or_beaten_lengths_stretch_call', 'lead_or_beaten_lengths_stretch_call'],
        'lead_or_beaten_1320':  [None, None, 'lead_or_beaten_lengths_finish',       'lead_or_beaten_lengths_finish'],
    },
    1430: {
        'lead_or_beaten_0':     [None, None, 'lead_or_beaten_lengths_start_call',   'lead_or_beaten_lengths_start_call'],
        'lead_or_beaten_440':   [None, None, 'lead_or_beaten_lengths_1st_call',     'lead_or_beaten_lengths_1st_call'],
        'lead_or_beaten_880':   [None, None, 'lead_or_beaten_lengths_2d_call',      'lead_or_beaten_lengths_2d_call'],
        'lead_or_beaten_1210':  [None, None, 'lead_or_beaten_lengths_stretch_call', 'lead_or_beaten_lengths_stretch_call'],
        'lead_or_beaten_1430':  [None, None, 'lead_or_beaten_lengths_finish',       'lead_or_beaten_lengths_finish'],
    },
    1540: {
        'lead_or_beaten_0':     [None, None, 'lead_or_beaten_lengths_start_call',   'lead_or_beaten_lengths_start_call'],
        'lead_or_beaten_440':   [None, None, 'lead_or_beaten_lengths_1st_call',     'lead_or_beaten_lengths_1st_call'],
        'lead_or_beaten_880':   [None, None, 'lead_or_beaten_lengths_2d_call',      'lead_or_beaten_lengths_2d_call'],
        'lead_or_beaten_1320':  [None, None, 'lead_or_beaten_lengths_stretch_call', 'lead_or_beaten_lengths_stretch_call'],
        'lead_or_beaten_1540':  [None, None, 'lead_or_beaten_lengths_finish',       'lead_or_beaten_lengths_position_finish'],
    },
    1650: {
        'lead_or_beaten_0':     [None, None, 'lead_or_beaten_lengths_start_call',   'lead_or_beaten_lengths_start_call'],
        'lead_or_beaten_440':   [None, None, 'lead_or_beaten_lengths_1st_call',     'lead_or_beaten_lengths_1st_call'],
        'lead_or_beaten_880':   [None, None, 'lead_or_beaten_lengths_2d_call',      'lead_or_beaten_lengths_2d_call'],
        'lead_or_beaten_1430':  [None, None, 'lead_or_beaten_lengths_stretch_call', 'lead_or_beaten_lengths_stretch_call'],
        'lead_or_beaten_1650':  [None, None, 'lead_or_beaten_lengths_finish',       'lead_or_beaten_lengths_finish'],
    },
    1760: {
        'lead_or_beaten_0':     [None, None, 'lead_or_beaten_lengths_start_call',   None],
        'lead_or_beaten_440':   [None, None, 'lead_or_beaten_lengths_1st_call',     'lead_or_beaten_lengths_start_call'],
        'lead_or_beaten_880':   [None, None, 'lead_or_beaten_lengths_2d_call',      'lead_or_beaten_lengths_1st_call'],
        'lead_or_beaten_1320':  [None, None, 'lead_or_beaten_lengths_3d_call',      'lead_or_beaten_lengths_2d_call'],
        'lead_or_beaten_1540':  [None, None, 'lead_or_beaten_lengths_stretch_call', 'lead_or_beaten_lengths_stretch_call'],
        'lead_or_beaten_1760':  [None, None, 'lead_or_beaten_lengths_finish',       'lead_or_beaten_lengths_finish'],
    },
    1830: {
        'lead_or_beaten_0':     [None, None, 'lead_or_beaten_lengths_start_call',   None],
        'lead_or_beaten_440':   [None, None, 'lead_or_beaten_lengths_1st_call',     'lead_or_beaten_lengths_start_call'],
        'lead_or_beaten_880':   [None, None, 'lead_or_beaten_lengths_2d_call',      'lead_or_beaten_lengths_1st_call'],
        'lead_or_beaten_1320':  [None, None, 'lead_or_beaten_lengths_3d_call',      'lead_or_beaten_lengths_2d_call'],
        'lead_or_beaten_1610':  [None, None, 'lead_or_beaten_lengths_stretch_call', 'lead_or_beaten_lengths_stretch_call'],
        'lead_or_beaten_1830':  [None, None, 'lead_or_beaten_lengths_finish',       'lead_or_beaten_lengths_finish'],
    },
    1870: {
        'lead_or_beaten_0':     [None, None, 'lead_or_beaten_lengths_start_call',   None],
        'lead_or_beaten_440':   [None, None, 'lead_or_beaten_lengths_1st_call',     'lead_or_beaten_lengths_start_call'],
        'lead_or_beaten_880':   [None, None, 'lead_or_beaten_lengths_2d_call',      'lead_or_beaten_lengths_1st_call'],
        'lead_or_beaten_1320':  [None, None, 'lead_or_beaten_lengths_3d_call',      'lead_or_beaten_lengths_2d_call'],
        'lead_or_beaten_1650':  [None, None, 'lead_or_beaten_lengths_stretch_call', 'lead_or_beaten_lengths_stretch_call'],
        'lead_or_beaten_1870':  [None, None, 'lead_or_beaten_lengths_finish',       'lead_or_beaten_lengths_finish'],
    },
    1980: {
        'lead_or_beaten_0':     [None, None, 'lead_or_beaten_lengths_start_call',   None],
        'lead_or_beaten_440':   [None, None, 'lead_or_beaten_lengths_1st_call',     'lead_or_beaten_lengths_start_call'],
        'lead_or_beaten_880':   [None, None, 'lead_or_beaten_lengths_2d_call',      'lead_or_beaten_lengths_1st_call'],
        'lead_or_beaten_1320':  [None, None, 'lead_or_beaten_lengths_3d_call',      'lead_or_beaten_lengths_2d_call'],
        'lead_or_beaten_1760':  [None, None, 'lead_or_beaten_lengths_stretch_call', 'lead_or_beaten_lengths_stretch_call'],
        'lead_or_beaten_1980':  [None, None, 'lead_or_beaten_lengths_finish',       'lead_or_beaten_lengths_finish'],
    },
}