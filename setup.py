from setuptools import setup, find_packages

setup(
    name="ml_pipeline_similarity",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pyspark>=3.4.0",
        "sentence-transformers>=2.2.2",
        "delta-spark>=2.4.0",
        "pandas",
        "numpy",
    ],
    python_requires=">=3.8",
)