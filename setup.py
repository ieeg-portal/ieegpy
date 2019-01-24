from distutils.core import setup, Extension

setup(name='pyhabitat',
      version='1.0',
      description='API for the habitat platform (IEEG.org)',
      install_requires=['deprecation','requests','numpy'],
      py_modules=['hbt_auth', 'hbt_dataset'])
