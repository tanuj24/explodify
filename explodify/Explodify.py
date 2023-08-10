from pyspark.sql import types as t
from pyspark.sql import functions as F
from UnfoldDF import UnfoldDF

class Explodify:
    def __init__(self) -> None:
        pass

    @staticmethod
    def get_array_type_fields(schema, prefix=None):
        """
        Returns all the array fields in a nested spark dataframe schema
        Args:
            schema: pyspark dataframe schema
            prefix: prefix to add to the arrayfields
        """
        array_fields = []
        for field in schema.fields:
            name = f"{prefix}.{field.name}" if prefix else field.name
            dtype = field.dataType
            if isinstance(dtype, t.ArrayType):
                if isinstance(dtype.elementType, t.StructType):
                    array_fields.append(name)
            if isinstance(dtype, t.StructType):
                array_fields = array_fields + Explodify.get_array_type_fields(dtype, prefix=name)
        return array_fields

    @staticmethod
    def explode_recurse(df, _dict=dict(), generic=[], array_col_set=set(), prefix=None):
        """
        Recursively convert each struct in the nested df into a separate dataframe in a dictonary.
        Explodes the arrays if present.
        Args:
            df: pyspark dataframe
            _dict: dictonary used to collect output
            generic: generic keys
            array_col_set: set containing array columns of prev level
        """
        schema = df.schema
        select=[]
        fields = Explodify.get_array_type_fields(schema)
        _array_col_set=set(array_col_set)
        for _f in fields:
            if prefix is not None:
                _lst=_f.split('.')
                _key=f"{prefix}.{'.'.join(_lst[1:])}"
            else: _key = _f
            _select= generic + [F.posexplode(_f).alias(f"{_f.split('.')[-1]}@rankid",_f.split('.')[-1])]
            _dict[_key]=df.select(_select)
            _gen = generic+[f"{_f.split('.')[-1]}@rankid"]
            _array_col_set.add(_f.split('.')[-1])
            _dict, _array_col_set = Explodify.explode_recurse(_dict[_key], _dict,_gen,_array_col_set, prefix=_key)
        _cols = df.columns
        for _col in _cols:
            if _col not in generic and _col not in _array_col_set:
                _dict[_col] = df.select(generic+[_col])
        return _dict,_array_col_set
