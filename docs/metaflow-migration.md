# Metaflow/tsb migration

The purpose of this document is to describe the relationship between the, at time of writing, the current metadata structure in metaflow and ODM2

## mapping

Below is a mapping between concepts in metaflow/tsb and their corresponding table in ODM2


|metaflow ttype | tsb table | ODM2 metadata     | ODM2 data             |
|---|---|---|---|
| vessel      |      |   |   |
| component      |      |   |   |
| gpstrack      | track     | samplingfeatures  | trackresultlocations  |
| tseries       | ts        | trackresults      | trackresultvalues     |
| qctseries     | flag      |               |                   |

