import json
from typing import List

from odm2_postgres_api.schemas.schemas import SamplingFeaturesCreate, MethodsCreate, VariablesCreate, \
    ControlledVocabularyCreate


def mass_spec_sampling_features() -> List[SamplingFeaturesCreate]:
    sampling_features = [{
        "samplingfeatureuuid": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
        "samplingfeaturetypecv": "Site",
        "samplingfeaturecode": "901-3-8",
        "samplingfeaturename": "Dalsvatnet",
        "samplingfeaturedescription": "Station ID: 3275"
    }]
    return [SamplingFeaturesCreate(**sf) for sf in sampling_features]


mass_spec_cv = [
    {
        "name": "LC_QTOF_Raw",
        "term": "Raw data produced by Liquid Chmoatography coupled to Quadrupole Time of Flight instrument",
        "definition": "List of peaks and their properties to be further identified as chemicals",
        "category": "Chemistry",
        "controlled_vocabulary_table_name": "cv_variablename"
    }, {
        "name": "LC_QTOF_mzXML",
        "term": "Data produced by Liquid Chmoatography coupled to Quadrupole Time of Flight instrument"
                "converted to mzXML format",
        "definition": "List of peaks and their properties to be further identified as chemicals",
        "category": "Chemistry",
        "controlled_vocabulary_table_name": "cv_variablename"
    }, {
        "name": "LC_QTOF_Peaks",
        "term": "Peaks detected by Liquid Chmoatography coupled to Quadrupole Time of Flight instrument",
        "definition": "List of peaks and their properties to be further identified as chemicals",
        "category": "Chemistry",
        "controlled_vocabulary_table_name": "cv_variablename"
    }, {
        "name": "LC_QTOF_Peaks_and_Fragments",
        "term": "Peaks and their fragments "
                "detected by Liquid Chmoatography coupled to Quadrupole Time of Flight instrument",
        "definition": "List of peaks, associated peaks and their properties to be further identified as chemicals",
        "category": "Chemistry",
        "controlled_vocabulary_table_name": "cv_variablename"
    }, {
        "name": "LC_QTOF_Chemicals",
        "term": "Chemicals detected by Liquid Chmoatography coupled to Quadrupole Time of Flight instrument",
        "definition": "List of chemicals identified based on LC_QTOF_Peaks_and_Fragments",
        "category": "Chemistry",
        "controlled_vocabulary_table_name": "cv_variablename"
    }]


def mass_spec_controlled_vocabularies() -> List[ControlledVocabularyCreate]:
    return [ControlledVocabularyCreate(**cv) for cv in mass_spec_cv]


def mass_spec_variables() -> List[VariablesCreate]:
    mass_spec_variables_list = [{
        "variabletypecv": "Chemistry",
        "variablenamecv": "LC_QTOF_Raw",
        "variabledefinition": "Raw data produced by Liquid Chmoatography coupled to Quadrupole Time of Flight "
                              "instrument",
        "variablecode": f'mass_spec_00',
        "nodatavalue": -9999
    }, {
        "variabletypecv": "Chemistry",
        "variablenamecv": "LC_QTOF_Peaks",
        "variabledefinition": "Data produced by Liquid Chmoatography coupled to Quadrupole Time of Flight "
                              "instrument and converted to mzXML format",
        "variablecode": f'mass_spec_01',
        "nodatavalue": -9999
    }, {
        "variabletypecv": "Chemistry",
        "variablenamecv": "LC_QTOF_Peaks",
        "variabledefinition": "Peaks detected by Liquid Chmoatography coupled to Quadrupole Time of Flight instrument",
        "variablecode": f'mass_spec_1',
        "nodatavalue": -9999
    }, {
        "variabletypecv": "Chemistry",
        "variablenamecv": "LC_QTOF_Peaks_and_Fragments",
        "variabledefinition": "Peaks and fragments detected by Liquid Chmoatography coupled to Quadrupole "
                              "Time of Flight instrument",
        "variablecode": f'mass_spec_2',
        "nodatavalue": -9999
    }, {
        "variabletypecv": "Chemistry",
        "variablenamecv": "LC_QTOF_Chemicals",
        "variabledefinition": "Peaks identified in Liquid Chmoatography coupled to Quadrupole Time of Flight "
                              "instrument",
        "variablecode": f'mass_spec_3',
        "nodatavalue": -9999
    }]

    return [VariablesCreate(**v) for v in mass_spec_variables_list]


def mass_spec_methods(org_id: int) -> List[MethodsCreate]:
    methods = [
        {
            "methodtypecv": "Specimen collection",
            "methodcode": "collect_sample",
            "methodname": "collect_sample",
            "methoddescription": "Collecting sample in the field",
            "organizationid": org_id
        }, {
            "methodtypecv": "Specimen fractionation",
            "methodcode": "fractionate_sample",
            "methodname": "fractionate_sample",
            "methoddescription": "Create a set of sub-samples",
            "organizationid": org_id
        }, {
            "methodtypecv": "Specimen analysis",
            "methodcode": "create_ms_data",
            "methodname": "ms run",
            "methoddescription": "Running mass spectrometer",
            "organizationid": org_id
        }, {
            "methodtypecv": "Derivation",
            "methodcode": "ms_convert_filter_scanEvent_1_2",
            "methodname": "ms convert",
            "methoddescription": "",
            "organizationid": org_id,
            "annotations": [{
                "annotationtypecv": "Method annotation",
                "annotationtext": "The json field holds the parameters with which this method will be executed",
                "annotationjson": json.dumps({
                    "verbose": '-v',
                    "bits": '--32',
                    "output": '--mzXML',
                    "filter": ' --filter \"scanEvent 1-2\"'
                })
            }]
        }, {
            "methodtypecv": "Derivation",
            "methodcode": "ms_convert",
            "methodname": "ms convert",
            "methoddescription": "",
            "organizationid": org_id,
            "annotations": [{
                "annotationtypecv": "Method annotation",
                "annotationtext": "The json field holds the parameters with which this method will be executed",
                "annotationjson": json.dumps({
                    "verbose": '-v',
                    "bits": '--32',
                    "output": '--mzXML'
                })
            }]
        }, {
            "methodtypecv": "Derivation",
            "methodcode": "fd_nointerp_nscan40_allmz",
            "methodname": "feature detection",
            "methoddescription": "Detects features in raw data.  Parameters are "
                                 "stored as json annotation on this method. In particular no"
                                 "interpolation is perfomred prior to fitting the data, retention "
                                 "window is set to 40 and full mass range is used.",
            "organizationid": org_id,
            "annotations": [{
                "annotationtypecv": "Method annotation",
                "annotationtext": "The json field holds the parameters with which this method will be executed",
                "annotationjson": json.dumps({
                    "mz_range": [0, 0],
                    "n_iter": 15000,
                    "n_scan": 40,
                    "mz_res": 20000,
                    "mz_win": 0.02,
                    "adj_r2": 0.85,
                    "min_int": 2000,
                    "int_var": 5,
                    "s2n": 2,
                    "min_nscan": 3,
                    "peak_interp": 0,
                })
            }]
        }, {
            "methodtypecv": "Derivation",
            "methodcode": "fd_nointerp_nscan40_allmz_test",
            "methodname": "feature detection",
            "methoddescription": "Detects features in raw data. This is a test version of method "
                                 "fd_nointerp_nscan40_allmz where number of iteration is limited to 25. Parameters "
                                 "are stored as json annotation on this method. In particular no"
                                 "interpolation is perfomred prior to fitting the data, retention "
                                 "window is set to 40 and full mass range is used.",
            "organizationid": org_id,
            "annotations": [{
                "annotationtypecv": "Method annotation",
                "annotationtext": "The json field holds the parameters with which this method will be executed",
                "annotationjson": json.dumps({
                    "mz_range": [0, 0],
                    "n_iter": 25,
                    "n_scan": 40,
                    "mz_res": 20000,
                    "mz_win": 0.02,
                    "adj_r2": 0.85,
                    "min_int": 2000,
                    "int_var": 5,
                    "s2n": 2,
                    "min_nscan": 3,
                    "peak_interp": 0,
                })
            }]
        }, {
            "methodtypecv": "Derivation",
            "methodcode": "fid_positive_16032020",
            "methodname": "feature identification",
            "methoddescription": "Identifies features previously detected using a fd_* and fdc methods. "
                                 "Raw data were obtained with positive ESI. Mass bank version 16032020 is "
                                 "used for identification."
                                 "Additional parameters are stored stored as annotation to this method,",
            "organizationid": org_id,
            "annotations": [{
                "annotationtypecv": "Method annotation",
                "annotationtext": "The json field holds the parameters with which this method will be executed",
                "annotationjson": json.dumps({
                    "id_massbank_version": "16032020",
                    "id_mode": "POSITIVE",
                    "id_source": "ESI",
                    "id_parent": 0,
                    "id_massbank": "MassBankJulia.jld",
                    "id_feature_wgts": [1, 1, 1, 1, 1, 1, 1]
                })
            }]
        }, {
            "methodtypecv": "Derivation",
            "methodcode": "fdc",
            "methodname": "feature deconvolution",
            "methoddescription": "Find fragments for peaks detected with fd_* method."
                                 "Parameters are stored stored as annotation to this method,",
            "organizationid": org_id,
            "annotations": [{
                "annotationtypecv": "Method annotation",
                "annotationtext": "The json field holds the parameters with which this method will be executed",
                "annotationjson": json.dumps({
                    "mz_range": [0, 0],
                    "id_corr_tresh": 0.9,
                    "id_min_int": 500,
                    "id_mz_win_pc": 0.8,
                    "id_rt_win_pc": 0.25
                })
            }]
        },
    ]

    return [MethodsCreate(**m) for m in methods]
