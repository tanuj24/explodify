from pyspark.sql import types as t
from pyspark.sql import functions as F

class UnfoldDF:

    def __init__(self):
        pass

    @staticmethod
    def skip_array_df(df):
        """
        Drops all the array type fields
        at root level in the dataframe
        Args:
            df: pyspark dataframe
        """
        for (name, dtype) in df.dtypes:
            if "array" in dtype:
                df = df.drop(name)
        return df

    @staticmethod
    def get_struct_type_fields(schema, prefix=None):
        """
        Returns all the struct fields in a nested pyspark dataframe schema
        Args:
            schema: pyspark dataframe schema
            prefix: prefix to add to the fields if any
        """
        fields = []
        for field in schema.fields:
            name = prefix + '.' + field.name if prefix else field.name
            dtype = field.dataType
            if isinstance(dtype, t.StructType):
                fields += UnfoldDF.get_struct_type_fields(dtype, prefix=name)
            else:
                fields.append(name)
        return fields

    @staticmethod
    def df_is_flat(df):
        """
        Returns True if the dataframe is flat else returns False
        Args:
            df: pyspark dataframe
        Returns:
            is_flat: boolean indicating whether the dataframe is flat or not
        """
        for (_, dtype) in df.dtypes:
            if ("array" in dtype) or ("struct" in dtype):
                return False
        return True

    @staticmethod
    def explode_array_df(df):
        """
        Explodes arrays in the dataframe
        Args:
            df: pyspark dataframe
        Returns:
            exploded_df: pyspark dataframe with exploded arrays
        """
        for (name, dtype) in df.dtypes:
            if "array" in dtype:
                df = df.withColumn(name, F.explode(name))
        return df

    @staticmethod
    def flatten_df(df,skipArrays=False):
        """
        Return a flat dataframe given a nested dataframe
        Args:
            df: pyspark dataframe
            skipArrays: if true then drop arrays in the flatten df else explode arrays
        """
        keepGoing = True
        while keepGoing:
            fields = UnfoldDF.get_struct_type_fields(df.schema)
            new_fields = [item.replace(".", "#") for item in fields]
            df = df.select(fields).toDF(*new_fields)
            if skipArrays:
                df=UnfoldDF.skip_array_df(df)
            else:
                df = UnfoldDF.explode_array_df(df)
            if UnfoldDF.df_is_flat(df):
                keepGoing = False
        return df
