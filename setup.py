from setuptools import setup, find_packages

with open('README.md') as file:
    long_description = file.read()

setup(
    name='aerospike-py',
    version='0.1.0',
    description='A native python client for Aerospike',
    long_description=long_description,
    author='William Pitcock',
    author_email='nenolod@dereferenced.org',
    url='https://github.com/kaniini/aerospike-py',
    packages=find_packages(),
    install_requires=[],
    classifiers=[
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: ISC License (ISCL)',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3 :: Only',
    ]
)
