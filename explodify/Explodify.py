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
            prefix: prefix to access the nested array field
        """
        array_fields = []
        for field in schema.fields:
            name = f"{prefix}.{field.name}" if prefix else field.name
            dtype = field.dataType
            if isinstance(dtype, t.ArrayType) and isinstance(dtype.elementType, t.StructType):
                array_fields.append(name)
            if isinstance(dtype, t.StructType):
                array_fields += Explodify.get_array_type_fields(dtype, prefix=name)
        return array_fields

    @staticmethod
    def explode_recurse(df, collector=dict(), generic=[], array_col_set=set(), prefix=None):
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
        fields = Explodify.get_array_type_fields(schema)
        array_col_set=set(array_col_set)

        for field in fields:
            if prefix is not None:
                feild_key=f"{prefix}.{'.'.join(field.split('.')[1:])}"
            else:
                feild_key = field

            select= generic + [F.posexplode(field).alias(f"{field.split('.')[-1]}@rankid",field.split('.')[-1])]
            collector[feild_key]=df.select(select)
            _generic = generic+[f"{field.split('.')[-1]}@rankid"]
            array_col_set.add(field.split('.')[-1])

            collector, array_col_set = Explodify.explode_recurse(
                collector[feild_key],
                collector,
                _generic,
                array_col_set,
                prefix=feild_key
            )

        cols = df.columns
        for _col in cols:
            if _col not in generic and _col not in array_col_set:
                collector[_col] = df.select(generic+[_col])
        return collector,array_col_set

    def explode(self, df, pks=[]):
        dfs = {}
        unflat_dfs = self.explode(df, generic=pks)
        for k,v in unflat_dfs.items():
            dfs[k] = UnfoldDF.flatten_df(v,skipArrays=True)
        return dfs
