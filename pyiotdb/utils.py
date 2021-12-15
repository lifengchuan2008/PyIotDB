import pandas as pd
from iotdb.utils import SessionDataSet
from iotdb.utils.Field import Field
# for package
from iotdb.utils.IoTDBConstants import TSDataType
import sys


py2k = sys.version_info < (3, 0)

def resultset_to_pandas(result_set: SessionDataSet) -> pd.DataFrame:
    """
    Transforms a SessionDataSet from IoTDB to a Pandas Data Frame
    Each Field from IoTDB is a column in Pandas
    :param result_set:
    :return:
    """
    # get column names and fields
    column_names = result_set.get_column_names()

    value_dict = {}

    if "Time" in column_names:
        offset = 1
    else:
        offset = 0

    for i in range(len(column_names)):
        value_dict[column_names[i]] = []

    while result_set.has_next():
        record = result_set.next()

        if "Time" in column_names:
            value_dict["Time"].append(record.get_timestamp())

        for col in range(len(record.get_fields())):
            field: Field = record.get_fields()[col]

            value_dict[column_names[col + offset]].append(get_typed_point(field))

    return pd.DataFrame(value_dict)


def get_typed_point(field: Field, none_value=None):
    choices = {
        # In Case of Boolean, cast to 0 / 1
        TSDataType.BOOLEAN: lambda field: 1 if field.get_bool_value() else 0,
        TSDataType.TEXT: lambda field: field.get_string_value(),
        TSDataType.FLOAT: lambda field: field.get_float_value(),
        TSDataType.INT32: lambda field: field.get_int_value(),
        TSDataType.DOUBLE: lambda field: field.get_double_value(),
        TSDataType.INT64: lambda field: field.get_long_value(),
    }

    result_next_type: TSDataType = field.get_data_type()

    if result_next_type in choices.keys():
        return choices.get(result_next_type)(field)
    elif result_next_type is None:
        return none_value
    else:
        raise Exception(f"Unknown DataType {result_next_type}!")