from setuptools import setup

setup(
    name='bigquerytest',
    version='0.1.0',
    author='Andreas Jansson',
    author_email='andreas@jansson.me.uk',
    description='unittest TestCase with BigQuery table mocks in human-readable format',
    license='MIT',
    install_requires=[
        'google-cloud-bigquery>=0.20.0',
    ]
)
