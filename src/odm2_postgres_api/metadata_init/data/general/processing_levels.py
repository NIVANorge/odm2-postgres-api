# processing levels copied from : https://github.com/ODM2/ODM2/blob/master/doc/ODM2Docs/core_processinglevels.md
from typing import List

from odm2_postgres_api.schemas.schemas import ProcessingLevelsCreate

processing_levels = [{
    "processinglevelcode":
    "0",
    "definition":
    "Raw Data",
    "explanation":
    "Raw data is defined as unprocessed data and data products that have not undergone quality "
    "control. Depending on the data type and data transmission system, raw data may be available "
    "within seconds or minutes after real-time. Examples include real time precipitation, streamflow "
    "and water quality measurements."
}, {
    "processinglevelcode":
    "0.05",
    "definition":
    "Automated QC",
    "explanation":
    "Automated procedures have been applied as an initial quality control. These procedures leave the "
    "data as is but flag obviously erroneous values using a qualitycode"
}, {
    "processinglevelcode":
    "0.1",
    "definition":
    "First Pass QC",
    "explanation":
    "A first quality control pass has been performed to remove out of range and obviously erroneous "
    "values. These values are either deleted from the record, or, for short durations, values are "
    "interpolated."
}, {
    "processinglevelcode":
    "0.2",
    "definition":
    "Second Pass QC",
    "explanation":
    "A second quality control pass has been performed to adjust values for instrument drift. Drift "
    "corrections are performed using a linear drift correction algorithm and field notes designating "
    "when sensor cleaning and calibration occurred."
}, {
    "processinglevelcode":
    "1",
    "definition":
    "Quality Controlled Data",
    "explanation":
    "Quality controlled data have passed quality assurance procedures such as routine estimation of "
    "timing and sensor calibration or visual inspection and removal of obvious errors. An example is "
    "USGS published streamflow records following parsing through USGS quality control procedures."
}, {
    "processinglevelcode":
    "2",
    "definition":
    "Derived Products",
    "explanation":
    "Derived products require scientific and technical interpretation and include multiple-sensor "
    "data. An example might be basin average precipitation derived from rain gages using an "
    "interpolation procedure."
}, {
    "processinglevelcode":
    "3",
    "definition":
    "Interpreted Products",
    "explanation":
    "These products require researcher (PI) driven analysis and interpretation, model-based "
    "interpretation using other data and/or strong prior assumptions. An example is basin average "
    "precipitation derived from the combination of rain gages and radar return data."
}, {
    "processinglevelcode":
    "4",
    "definition":
    "Knowledge Products",
    "explanation":
    "These products require researcher (PI) driven scientific interpretation and multidisciplinary "
    "data integration and include model-based interpretation using other data and/or strong prior "
    "assumptions. An example is percentages of old or new water in a hydrograph inferred from an "
    "isotope analysis."
}]


def general_processing_levels() -> List[ProcessingLevelsCreate]:
    return [ProcessingLevelsCreate(**p) for p in processing_levels]
