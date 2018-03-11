from setuptools import setup

setup(
    name='rk-launcher-aws',
    version='0.0.1',
    packages=['rklauncher'],
    url='github.com:alex-cognitiv/rk-launcher-aws.git',
    license='Apache 2.0',
    author='alex',
    author_email='alex@cognitiv.ai',
    description='',
    install_requires=[
        'rk', 'paramiko', 'scp'
    ]
)
