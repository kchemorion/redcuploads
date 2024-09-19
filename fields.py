import os
import pandas as pd
import requests
from urllib.parse import urljoin

# This dictionary maps your CSV column names to REDCap field names
FIELD_MAP = {
 
    # dbo_04_kinetic_grf fields
    'Sample': 'dbo_04_kinetic_grf_sample',
    'fRGRVE.M': 'dbo_04_kinetic_grf_frgrvem',
    'fRGRVE.S': 'dbo_04_kinetic_grf_frgrves',
    'fLGRVE.M': 'dbo_04_kinetic_grf_flgrvem',
    'fLGRVE.S': 'dbo_04_kinetic_grf_flgrves',
    'fRGRAP.M': 'dbo_04_kinetic_grf_frgrapm',
    'fRGRAP.S': 'dbo_04_kinetic_grf_frgraps',
    'fLGRAP.M': 'dbo_04_kinetic_grf_flgrapm',
    'fLGRAP.S': 'dbo_04_kinetic_grf_flgraps',
    'fRGRML.M': 'dbo_04_kinetic_grf_frgrmlm',
    'fRGRML.S': 'dbo_04_kinetic_grf_frgrmls',
    'fLGRML.M': 'dbo_04_kinetic_grf_flgrmlm',
    'fLGRML.S': 'dbo_04_kinetic_grf_flgrmls',
    'fLAFE.M': 'dbo_04_kinetic_grf_flafem',
    'fLAFE.S': 'dbo_04_kinetic_grf_flafes',
    'fRKFE.M': 'dbo_04_kinetic_grf_frkfem',
    'fRKFE.S': 'dbo_04_kinetic_grf_frkfes',
    'fLKFE.M': 'dbo_04_kinetic_grf_flkfem',
    'fLKFE.S': 'dbo_04_kinetic_grf_flkfes',
    'fRKAA.M': 'dbo_04_kinetic_grf_frkaam',
    'fRKAA.S': 'dbo_04_kinetic_grf_frkaas',
    'fLKAA.M': 'dbo_04_kinetic_grf_flkaam',
    'fLKAA.S': 'dbo_04_kinetic_grf_flkaas',
    'fRKIE.M': 'dbo_04_kinetic_grf_frkiem',
    'fRKIE.S': 'dbo_04_kinetic_grf_frkies',
    'fLKIE.M': 'dbo_04_kinetic_grf_flkiem',
    'fLKIE.S': 'dbo_04_kinetic_grf_flkies',
    'fRHPFE.M': 'dbo_04_kinetic_grf_frhpfem',
    'fRHPFE.S': 'dbo_04_kinetic_grf_frhpfes',
    'fRHPAA.M': 'dbo_04_kinetic_grf_frhpaam',
    'fRHPAA.S': 'dbo_04_kinetic_grf_frhpaas',
    'fLHPFE.M': 'dbo_04_kinetic_grf_flhpfem',
    'fLHPFE.S': 'dbo_04_kinetic_grf_flhpfes',
    'fLHPAA.M': 'dbo_04_kinetic_grf_flhpaam',
    'fLHPAA.S': 'dbo_04_kinetic_grf_flhpaas',
    'fRHPIE.M': 'dbo_04_kinetic_grf_frhpiem',
    'fRHPIE.S': 'dbo_04_kinetic_grf_frhpies',
    'fLHPIE.M': 'dbo_04_kinetic_grf_flhpiem',
    'fLHPIE.S': 'dbo_04_kinetic_grf_flhpies',
    'fRAFE.M': 'dbo_04_kinetic_grf_frafem',
    'fRAFE.S': 'dbo_04_kinetic_grf_frafes',

    # dbo_06_times fields
    'tRSTRIDE.M': 'dbo_06_times_trstridem',
    'tRSTRIDE.S': 'dbo_06_times_trstrides',
    'tRSTANCE.M': 'dbo_06_times_trstancem',
    'tRSTANCE.S': 'dbo_06_times_trstances',
    'tRSWING.M': 'dbo_06_times_trswingm',
    'tRSWING.S': 'dbo_06_times_trswings',
    'tRDBLSTANCE.M': 'dbo_06_times_trdblstancem',
    'tRDBLSTANCE.S': 'dbo_06_times_trdblstances',
    'tLSTRIDE.M': 'dbo_06_times_tlstridem',
    'tLSTRIDE.S': 'dbo_06_times_tlstrides',
    'tLSTANCE.M': 'dbo_06_times_tlstancem',
    'tLSTANCE.S': 'dbo_06_times_tlstances',
    'tLSWING.M': 'dbo_06_times_tlswingm',
    'tLSWING.S': 'dbo_06_times_tlswings',
    'tLDBLSTANCE.M': 'dbo_06_times_tldblstancem',
    'tLDBLSTANCE.S': 'dbo_06_times_tldblstances',

    # dbo_03_kinetic_p fields
    'pRKFE.M': 'dbo_03_kinetic_p_prkfem',
    'pRKFE.S': 'dbo_03_kinetic_p_prkfes',
    'pRAFE.M': 'dbo_03_kinetic_p_prafem',
    'pRAFE.S': 'dbo_03_kinetic_p_prafes',
    'pLAFE.M': 'dbo_03_kinetic_p_plafem',
    'pLAFE.S': 'dbo_03_kinetic_p_plafes',
    'pRKAA.M': 'dbo_03_kinetic_p_prkaam',
    'pRKAA.S': 'dbo_03_kinetic_p_prkaas',
    'pRKIE.M': 'dbo_03_kinetic_p_prkiem',
    'pRKIE.S': 'dbo_03_kinetic_p_prkies',
    'pLKFE.M': 'dbo_03_kinetic_p_plkfem',
    'pLKFE.S': 'dbo_03_kinetic_p_plkfes',
    'pLKAA.M': 'dbo_03_kinetic_p_plkaam',
    'pLKAA.S': 'dbo_03_kinetic_p_plkaas',
    'pLKIE.M': 'dbo_03_kinetic_p_plkiem',
    'pLKIE.S': 'dbo_03_kinetic_p_plkies',
    'pRHPFE.M': 'dbo_03_kinetic_p_prhpfem',
    'pRHPFE.S': 'dbo_03_kinetic_p_prhpfes',
    'pRHPAA.M': 'dbo_03_kinetic_p_prhpaam',
    'pRHPAA.S': 'dbo_03_kinetic_p_prhpaas',
    'pRHPIE.M': 'dbo_03_kinetic_p_prhpiem',
    'pRHPIE.S': 'dbo_03_kinetic_p_prhpies',
    'pLHPFE.M': 'dbo_03_kinetic_p_plhpfem',
    'pLHPFE.S': 'dbo_03_kinetic_p_plhpfes',
    'pLHPAA.M': 'dbo_03_kinetic_p_plhpaam',
    'pLHPAA.S': 'dbo_03_kinetic_p_plhpaas',
    'pLHPIE.M': 'dbo_03_kinetic_p_plhpiem',
    'pLHPIE.S': 'dbo_03_kinetic_p_plhpies',

    # dbo_exp_long_stand fields
    'Col_1': 'dbo_exp_long_stand_col_1',
    'Col_2': 'dbo_exp_long_stand_col_2',
    'Col_3': 'dbo_exp_long_stand_col_3',
    'Col_4': 'dbo_exp_long_stand_col_4',
    'Col_5': 'dbo_exp_long_stand_col_5',
    'Col_6': 'dbo_exp_long_stand_col_6',
    'Col_7': 'dbo_exp_long_stand_col_7',
    'Col_8': 'dbo_exp_long_stand_col_8',
    'Col_9': 'dbo_exp_long_stand_col_9',

    # dbo_09_posture fields
    'amRAIE.M': 'dbo_09_posture_amraiem',
    'amRAIE.S': 'dbo_09_posture_amraies',
    'amLAIE.M': 'dbo_09_posture_amlaiem',
    'amLAIE.S': 'dbo_09_posture_amlaies',
    'amRKFE.M': 'dbo_09_posture_amrkfem',
    'amRKFE.S': 'dbo_09_posture_amrkfes',
    'amRKAA.M': 'dbo_09_posture_amrkaam',
    'amRKAA.S': 'dbo_09_posture_amrkaas',
    'amRKIE.M': 'dbo_09_posture_amrkiem',
    'amRKIE.S': 'dbo_09_posture_amrkies',
    'amLKFE.M': 'dbo_09_posture_amlkfem',
    'amLKFE.S': 'dbo_09_posture_amlkfes',
    'amLKAA.M': 'dbo_09_posture_amlkaam',
    'amLKAA.S': 'dbo_09_posture_amlkaas',
    'amLKIE.M': 'dbo_09_posture_amlkiem',
    'amLKIE.S': 'dbo_09_posture_amlkies',
    'amRPTILT.M': 'dbo_09_posture_amrptiltm',
    'amRPTILT.S': 'dbo_09_posture_amrptilts',
    'amRPOBLI.M': 'dbo_09_posture_amrpoblim',
    'amRPOBLI.S': 'dbo_09_posture_amrpoblis',
    'amRPROT.M': 'dbo_09_posture_amrprotm',
    'amRPROT.S': 'dbo_09_posture_amrprots',
    'amLPTILT.M': 'dbo_09_posture_amlptiltm',
    'amLPTILT.S': 'dbo_09_posture_amlptilts',
    'amLPOBLI.M': 'dbo_09_posture_amlpoblim',
    'amLPOBLI.S': 'dbo_09_posture_amlpoblis',
    'amLPROT.M': 'dbo_09_posture_amlprotm',
    'amLPROT.S': 'dbo_09_posture_amlprots',
    'amRHPFE.M': 'dbo_09_posture_amrhpfem',
    'amRHPFE.S': 'dbo_09_posture_amrhpfes',
    'amRHPAA.M': 'dbo_09_posture_amrhpaam',
    'amRHPAA.S': 'dbo_09_posture_amrhpaas',
    'amRHPIE.M': 'dbo_09_posture_amrhpiem',
    'amRHPIE.S': 'dbo_09_posture_amrhpies',
    'amLHPFE.M': 'dbo_09_posture_amlhpfem',
    'amLHPFE.S': 'dbo_09_posture_amlhpfes',
    'amLHPAA.M': 'dbo_09_posture_amlhpaam',
    'amLHPAA.S': 'dbo_09_posture_amlhpaas',
    'amLHPIE.M': 'dbo_09_posture_amlhpiem',
    'amLHPIE.S': 'dbo_09_posture_amlhpies',
    'amRSHILT.M': 'dbo_09_posture_amrshiltm',
    'amRSHILT.S': 'dbo_09_posture_amrshilts',
    'amRSHOBLI.M': 'dbo_09_posture_amrshoblim',
    'amRSHOBLI.S': 'dbo_09_posture_amrshoblis',
    'amRSHROT.M': 'dbo_09_posture_amrshrotm',
    'amRSHROT.S': 'dbo_09_posture_amrshrots',
    'amLSHTILT.M': 'dbo_09_posture_amlshtiltm',
    'amLSHTILT.S': 'dbo_09_posture_amlshtilts',
    'amLSHOBLI.M': 'dbo_09_posture_amlshoblim',
    'amLSHOBLI.S': 'dbo_09_posture_amlshoblis',
    'amLSHROT.M': 'dbo_09_posture_amlshrotm',
    'amLSHROT.S': 'dbo_09_posture_amlshrots',
    'amRSPFE.M': 'dbo_09_posture_amrspfem',
    'amRSPFE.S': 'dbo_09_posture_amrspfes',
    'amRSPML.M': 'dbo_09_posture_amrspmlm',
    'amRSPML.S': 'dbo_09_posture_amrspmls',
    'amRSPIE.M': 'dbo_09_posture_amrspiem',
    'amRSPIE.S': 'dbo_09_posture_amrspies',
    'amLSPFE.M': 'dbo_09_posture_amlspfem',
    'amLSPFE.S': 'dbo_09_posture_amlspfes',
    'amLSPML.M': 'dbo_09_posture_amlspmlm',
    'amLSPML.S': 'dbo_09_posture_amlspmls',
    'amLSPIE.M': 'dbo_09_posture_amlspiem',
    'amLSPIE.S': 'dbo_09_posture_amlspies',
    'amRAFE.M': 'dbo_09_posture_amrafem',
    'amRAFE.S': 'dbo_09_posture_amrafes',
    'amRFAA.M': 'dbo_09_posture_amrfaam',
    'amRFAA.S': 'dbo_09_posture_amrfaas',
    'amRFIE.M': 'dbo_09_posture_amrfiem',
    'amRFIE.S': 'dbo_09_posture_amrfies',
    'amLFIE.M': 'dbo_09_posture_amlfiem',
    'amLFIE.S': 'dbo_09_posture_amlfies',
    'amLAFE.M': 'dbo_09_posture_amlafem',
    'amLAFE.S': 'dbo_09_posture_amlafes',
    'amLFAA.M': 'dbo_09_posture_amlfaam',
    'amLFAA.S': 'dbo_09_posture_amlfaas',

    # dbo_01_kinematic fields
    'aRAFE.M': 'dbo_01_kinematic_arafem',
    'aRAFE.S': 'dbo_01_kinematic_arafes',
    'aLAFE.M': 'dbo_01_kinematic_alafem',
    'aLAFE.S': 'dbo_01_kinematic_alafes',
    'aRAIE.M': 'dbo_01_kinematic_araiem',
    'aRAIE.S': 'dbo_01_kinematic_araies',
    'aLAIE.M': 'dbo_01_kinematic_alaiem',
    'aLAIE.S': 'dbo_01_kinematic_alaies',
    'aRKFE.M': 'dbo_01_kinematic_arkfem',
    'aRKFE.S': 'dbo_01_kinematic_arkfes',
    'aRKAA.M': 'dbo_01_kinematic_arkaam',
    'aRKAA.S': 'dbo_01_kinematic_arkaas',
    'aRKIE.M': 'dbo_01_kinematic_arkiem',
    'aRKIE.S': 'dbo_01_kinematic_arkies',
    'aLKFE.M': 'dbo_01_kinematic_alkfem',
    'aLKFE.S': 'dbo_01_kinematic_alkfes',
    'aLKAA.M': 'dbo_01_kinematic_alkaam',
    'aLKAA.S': 'dbo_01_kinematic_alkaas',
    'aLKIE.M': 'dbo_01_kinematic_alkiem',
    'aLKIE.S': 'dbo_01_kinematic_alkies',
    'aRHPFE.M': 'dbo_01_kinematic_arhpfem',
    'aRHPFE.S': 'dbo_01_kinematic_arhpfes',
    'aRHPAA.M': 'dbo_01_kinematic_arhpaam',
    'aRHPAA.S': 'dbo_01_kinematic_arhpaas',
    'aRHPIE.M': 'dbo_01_kinematic_arhpiem',
    'aRHPIE.S': 'dbo_01_kinematic_arhpies',
    'aLHPFE.M': 'dbo_01_kinematic_alhpfem',
    'aLHPFE.S': 'dbo_01_kinematic_alhpfes',
    'aLHPAA.M': 'dbo_01_kinematic_alhpaam',
    'aLHPAA.S': 'dbo_01_kinematic_alhpaas',
    'aLHPIE.M': 'dbo_01_kinematic_alhpiem',
    'aLHPIE.S': 'dbo_01_kinematic_alhpies',
    'aRPTILT.M': 'dbo_01_kinematic_arptiltm',
    'aRPTILT.S': 'dbo_01_kinematic_arptilts',
    'aRPOBLI.M': 'dbo_01_kinematic_arpoblim',
    'aRPOBLI.S': 'dbo_01_kinematic_arpoblis',
    'aRPROT.M': 'dbo_01_kinematic_arprotm',
    'aRPROT.S': 'dbo_01_kinematic_arprots',
    'aLPTILT.M': 'dbo_01_kinematic_alptiltm',
    'aLPTILT.S': 'dbo_01_kinematic_alptilts',
    'aLPOBLI.M': 'dbo_01_kinematic_alpoblim',
    'aLPOBLI.S': 'dbo_01_kinematic_alpoblis',
    'aLPROT.M': 'dbo_01_kinematic_alprotm',
    'aLPROT.S': 'dbo_01_kinematic_alprots',
    'aRSHROT.M': 'dbo_01_kinematic_arshrotm',
    'aRSHROT.S': 'dbo_01_kinematic_arshrots',
    'aLSHROT.M': 'dbo_01_kinematic_alshrotm',
    'aLSHROT.S': 'dbo_01_kinematic_alshrots',
    'aRSPML.M': 'dbo_01_kinematic_arspmlm',
    'aRSPML.S': 'dbo_01_kinematic_arspmls',
    'aRSPIE.M': 'dbo_01_kinematic_arspiem',
    'aRSPIE.S': 'dbo_01_kinematic_arspies',
    'aLSPML.M': 'dbo_01_kinematic_alspmlm',
    'aLSPML.S': 'dbo_01_kinematic_alspmls',
    'aLSPIE.M': 'dbo_01_kinematic_alspiem',
    'aLSPIE.S': 'dbo_01_kinematic_alspies',
    'aRSHTILT.M': 'dbo_01_kinematic_arshtiltm',
    'aRSHTILT.S': 'dbo_01_kinematic_arshtilts',
    'aRSHOBLI.M': 'dbo_01_kinematic_arshoblim',
    'aRSHOBLI.S': 'dbo_01_kinematic_arshoblis',
    'aLSHTILT.M': 'dbo_01_kinematic_alshtiltm',
    'aLSHTILT.S': 'dbo_01_kinematic_alshtilts',
    'aLSHOBLI.M': 'dbo_01_kinematic_alshoblim',
    'aLSHOBLI.S': 'dbo_01_kinematic_alshoblis',
    'aRSHTILTOFF.M': 'dbo_01_kinematic_arshtiltoffm',
    'aRSHTILTOFF.S': 'dbo_01_kinematic_arshtiltoffs',
    'aLSHTILTOFF.M': 'dbo_01_kinematic_alshtiltoffm',
    'aLSHTILTOFF.S': 'dbo_01_kinematic_alshtiltoffs',
    'aRSHOBLIOFF.M': 'dbo_01_kinematic_arshoblioffm',
    'aRSHOBLIOFF.S': 'dbo_01_kinematic_arshoblioffs',
    'aLSHOBLIOFF.M': 'dbo_01_kinematic_alshoblioffm',
    'aLSHOBLIOFF.S': 'dbo_01_kinematic_alshoblioffs',
    'aRSHROTOFF.M': 'dbo_01_kinematic_arshrotoffm',
    'aRSHROTOFF.S': 'dbo_01_kinematic_arshrotoffs',
    'aLSHROTOFF.M': 'dbo_01_kinematic_alshrotoffm',
    'aLSHROTOFF.S': 'dbo_01_kinematic_alshrotoffs',
    'aRSPFE.M': 'dbo_01_kinematic_arspfem',
    'aRSPFE.S': 'dbo_01_kinematic_arspfes',
    'aLSPFE.M': 'dbo_01_kinematic_alspfem',
    'aLSPFE.S': 'dbo_01_kinematic_alspfes',

    # dbo_07_1d_point fields
    'dTH': 'dbo_07_1d_point_dth',
    'dAB': 'dbo_07_1d_point_dab',
    'dRPD': 'dbo_07_1d_point_drpd',
    'dLPD': 'dbo_07_1d_point_dlpd',
    'dRLL': 'dbo_07_1d_point_drll',
    'dLLL': 'dbo_07_1d_point_dlll',

    # dbo_02_kinetic_t fields
    'tRGRVE.M': 'dbo_02_kinetic_t_trgrvem',
    'tRGRVE.S': 'dbo_02_kinetic_t_trgrves',
    'tLGRVE.M': 'dbo_02_kinetic_t_tlgrvem',
    'tLGRVE.S': 'dbo_02_kinetic_t_tlgrves',
    'tRGRAP.M': 'dbo_02_kinetic_t_trgrapm',
    'tRGRAP.S': 'dbo_02_kinetic_t_trgraps',
    'tLGRAP.M': 'dbo_02_kinetic_t_tlgrapm',
    'tLGRAP.S': 'dbo_02_kinetic_t_tlgraps',
    'tRGRML.M': 'dbo_02_kinetic_t_trgrmlm',
    'tRGRML.S': 'dbo_02_kinetic_t_trgrmls',
    'tLGRML.M': 'dbo_02_kinetic_t_tlgrmlm',
    'tLGRML.S': 'dbo_02_kinetic_t_tlgrmls',
    'tRAFE.M': 'dbo_02_kinetic_t_trafem',
    'tRAFE.S': 'dbo_02_kinetic_t_trafes',
    'tLAFE.M': 'dbo_02_kinetic_t_tlafem',
    'tLAFE.S': 'dbo_02_kinetic_t_tlafes',
    'tRKFE.M': 'dbo_02_kinetic_t_trkfem',
    'tRKFE.S': 'dbo_02_kinetic_t_trkfes',
    'tLKFE.M': 'dbo_02_kinetic_t_tlkfem',
    'tLKFE.S': 'dbo_02_kinetic_t_tlkfes',
    'tLKAA.M': 'dbo_02_kinetic_t_tlkaam',
    'tLKAA.S': 'dbo_02_kinetic_t_tlkaas',
    'tRKAA.M': 'dbo_02_kinetic_t_trkaam',
    'tRKAA.S': 'dbo_02_kinetic_t_trkaas',
    'tRKIE.M': 'dbo_02_kinetic_t_trkiem',
    'tRKIE.S': 'dbo_02_kinetic_t_trkies',
    'tLKIE.M': 'dbo_02_kinetic_t_tlkiem',
    'tLKIE.S': 'dbo_02_kinetic_t_tlkies',
    'tRHPFE.M': 'dbo_02_kinetic_t_trhpfem',
    'tRHPFE.S': 'dbo_02_kinetic_t_trhpfes',
    'tRHPAA.M': 'dbo_02_kinetic_t_trhpaam',
    'tRHPAA.S': 'dbo_02_kinetic_t_trhpaas',
    'tRHPIE.M': 'dbo_02_kinetic_t_trhpiem',
    'tRHPIE.S': 'dbo_02_kinetic_t_trhpies',
    'tLHPFE.M': 'dbo_02_kinetic_t_tlhpfem',
    'tLHPFE.S': 'dbo_02_kinetic_t_tlhpfes',
    'tLHPAA.M': 'dbo_02_kinetic_t_tlhpaam',
    'tLHPAA.S': 'dbo_02_kinetic_t_tlhpaas',
    'tLHPIE.M': 'dbo_02_kinetic_t_tlhpiem',
    'tLHPIE.S': 'dbo_02_kinetic_t_tlhpies',

    # dbo_08_velocities fields
    'velMEAN.M': 'dbo_08_velocities_velmeanm',
    'velMEAN.S': 'dbo_08_velocities_velmeans',
    'velRSWING.M': 'dbo_08_velocities_velrswingm',
    'velRSWING.S': 'dbo_08_velocities_velrswings',
    'velLSWING.M': 'dbo_08_velocities_vellswingm',
    'velLSWING.S': 'dbo_08_velocities_vellswings',

    # Additional fields that might be in your CSV files but not explicitly mentioned in the REDCap field list
    'Sample': 'dbo_04_kinetic_grf_sample',  # This mapping is repeated for consistency across different tables
    'Sample_1': 'dbo_03_kinetic_p_sample',
    'Sample_2': 'dbo_01_kinematic_sample',
    'Sample_3': 'dbo_02_kinetic_t_sample',

    # Ensure all patient_id fields are mapped correctly
    'patient_id_1': 'dbo_04_kinetic_grf_patient_id',
    'patient_id_2': 'dbo_06_times_patient_id',
    'patient_id_3': 'dbo_03_kinetic_p_patient_id',
    'patient_id_4': 'dbo_exp_long_stand_patient_id',
    'patient_id_5': 'dbo_09_posture_patient_id',
    'patient_id_6': 'dbo_01_kinematic_patient_id',
    'patient_id_7': 'dbo_07_1d_point_patient_id',
    'patient_id_8': 'dbo_02_kinetic_t_patient_id',
    'patient_id_9': 'dbo_08_velocities_patient_id',

    # Ensure all reclutado_id fields are mapped correctly
    'reclutado_1': 'dbo_05_massas_mtb',
    'reclutado_2': 'dbo_05_massas_mtb',
    'reclutado_3': 'dbo_05_massas_mtb',
    'reclutado_4': 'dbo_05_massas_mtb',
    'reclutado_5': 'dbo_05_massas_mtb',
    'reclutado_6': 'dbo_05_massas_mtb',
    'reclutado_7': 'dbo_05_massas_mtb',
    'reclutado_8': 'dbo_05_massas_mtb',
    'reclutado_9': 'dbo_05_massas_mtb',
}

def clean_and_map_data(df, filename):
def clean_and_map_data(df, filename):
    # Map column names to REDCap field names
    mapped_columns = {}
    for col in df.columns:
        if col in FIELD_MAP:
            mapped_columns[col] = FIELD_MAP[col]
        elif filename.startswith('dbo_05_Massas'):
            # Special handling for dbo_05_Massas.csv
            mapped_columns[col] = FIELD_MAP.get(col, col)
        else:
            # For other files, prefix the column name with the filename (without extension)
            prefix = filename.split('.')[0].lower()
            mapped_columns[col] = f"{prefix}_{col.lower()}"
    
    df = df.rename(columns=mapped_columns)
    
    # Ensure the record ID field is present
    if 'dbo_05_massas_mtb' not in df.columns:
        print(f"Error: Record identifier field missing in {filename}")
        return None
    
    # Handle potential duplicates by grouping by the record ID
    df = df.groupby('dbo_05_massas_mtb').first().reset_index()
    
    return df

def upload_data_to_redcap(api_url, api_key, data):
    fields = {
        'token': api_key,
        'content': 'record',
        'format': 'csv',
        'type': 'flat',
        'data': data,
        'overwriteBehavior': 'overwrite',
        'returnContent': 'count',
        'returnFormat': 'json'
    }
    try:
        response = requests.post(api_url, data=fields)
        response.raise_for_status()
        
        print(f"Response status code: {response.status_code}")
        print(f"Response content: {response.text[:200]}...")
        
        if response.text.strip():
            return response.json()
        else:
            print("Warning: Empty response from REDCap API")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error making request to REDCap API: {e}")
        print(f"Response content: {response.text}")
        return None

def process_csv_files(directory, api_url, api_key):
    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            file_path = os.path.join(directory, filename)
            try:
                df = pd.read_csv(file_path)
                df = clean_and_map_data(df, filename)
                if df is None:
                    continue
                print(f"Successfully read {filename}. Shape: {df.shape}")
                print(f"Columns after mapping: {df.columns.tolist()}")
                
                csv_data = df.to_csv(index=False)
                result = upload_data_to_redcap(api_url, api_key, csv_data)
                
                if result is None:
                    print(f"Failed to upload {filename}")
                elif isinstance(result, dict) and 'error' in result:
                    print(f"Error uploading {filename}: {result['error']}")
                else:
                    print(f"Successfully uploaded {filename}: {result} records processed")
                
            except Exception as e:
                print(f"Failed to process {filename}: {str(e)}")
                import traceback
                print(traceback.format_exc())

# Parameters
BASE_URL = 'http://localhost/redcap_v14.6.8/'
API_ENDPOINT = 'API/'
API_URL = urljoin(BASE_URL, API_ENDPOINT)
API_KEY = '6CB10A9DC74EC213EB0AF032337BE4DE'  # Your REDCap API key
CSV_DIRECTORY = 'csv_output/'

print(f"Using API URL: {API_URL}")
process_csv_files(CSV_DIRECTORY, API_URL, API_KEY)