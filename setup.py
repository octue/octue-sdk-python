from distutils.core import setup


setup(name='octue-sdk-python',
      version='0.1',
      # packages=['octue', 'octue.utils', 'octue.config', 'octue.manifest', 'octue.resources'],
      url='https://www.github.com/octue/octue-sdk-python',
      license='',
      author='thclark',
      author_email='support@octue.com',
      description='A python package providing a python SDK to the Octue API and providing supporting functions to Octue applications')


# TODO move the following into octue example app template
# setup(
#     py_modules=['cli'],
#     install_requires=['Click'],
#     entry_points='''
#     [console_scripts]
#     octue=octue_app
#     ''',
# )
