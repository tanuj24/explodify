from setuptools import setup

setup(
    name="explodify",
    version="1.0.0",
    packages=["explodify"],
    install_requires=[
        "pyspark"
    ],
    entry_points={
        "console_scripts": [
            "explodify=explodify.__main__:main"
        ]
    },
    author="Tanuj Soni",
    author_email="tanujsoni027@gmail.com",
    description="""Package Description: Explodify
The Explodify package provides functionality for manipulating nested Spark DataFrames.
Explodify reads multiple deeply nested json data into spark dataframes and explodes them to multiple dataframes.
Each dataframe after explosion is related to its parent dataframe by columns id and <child_df>@rankid, hence preserving row-level relations.
To use the Explodify package, simply import it and call the method explode_it on your nested DataFrame.
The package seamlessly integrates with PySpark and provides a clean and easy-to-use interface for working with nested DataFrames.""",
url="https://github.com/tanuj24/explodify"
)